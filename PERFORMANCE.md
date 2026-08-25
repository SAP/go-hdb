# Investigating Performance Issues with go-hdb

A practical guide for diagnosing latency in Go services that use the
[`go-hdb`](https://github.com/SAP/go-hdb) driver to talk to SAP HANA.

It applies to any observation that database performance is worse than expected —
whether calls are *consistently* slower than they should be, or *intermittently*
spike for no obvious reason. A frequent and often puzzling case is when calls
such as `Prepare`, `Exec`, or `Query` occasionally take much longer than
expected even though the individual statements are trivial (e.g. a single join
or a primary-key update) and the HANA backend is not obviously under load — but
the same approach applies to steady slowness just as well.

The goal here is not to guess a cause but to give you a repeatable way to
*localize* where the time is spent, using the observability the driver and
`database/sql` already provide.

---

## Mental model: where the time can go

A single `Prepare` + `Exec` from an application involves several distinct
phases. Each has a different cause and a different fix, so the first job is to
identify *which* phase is slow rather than treating "slow SQL" as one thing.

```
your code
  │
  ▼
database/sql pool ── (A) waiting for a free connection  ── pool size / contention
  │
  ▼
get a connection:
  ├─ reuse idle conn ── (B) optional Ping round-trip ──── SetPingInterval
  └─ NO idle conn → open a new one:
        ├─ (C) TCP connect ──────────────────────────── network / DNS / routing
        └─ (D) HANA auth handshake ───────────────────── AuthTime
                └─ (E) auth refresh callback (if set) ── SetRefreshPassword/Token/ClientCert
  │
  ▼
Prepare ── (F) round-trip to HANA ───────────────────── SQLTimes["prepare"]
  │
  ▼
Exec/Query ── (G) round-trip + fetch ────────────────── SQLTimes["exec"|"query"|"fetch"]
  │
  ▼
HANA execution ── (H) actual query work ─────────────── HANA plan cache / expensive statements
```

Application-level tracing (e.g. via `otelsql`) typically measures the
wall-clock of `Prepare`/`Exec`/`Query` as the calling code sees it — that is,
it includes **all of phases A through H combined**. As a result, a call that
looks like a "slow Exec" can be almost entirely connection acquisition and
setup (A–E) with a fast statement (H). To split these apart you need the
driver's own instrumentation, described below.

A useful thing to know about HANA specifically: establishing a *new* physical
connection (TCP connect plus the full authentication handshake, phases C–E) is
comparatively expensive, whereas reusing an already-authenticated connection
from the pool skips all of it. Connection churn is therefore a frequent source
of added latency — steady if it happens on most calls, intermittent if only on
bursts — that has nothing to do with the statements themselves.

---

## Before the driver deep-dive: check the environment

Slowness can originate *outside* the driver — in the HANA backend (under steady
or transient load), or in the network path between the service and HANA. If one
of these is the cause, driver tuning will not address it. Confirm the
environment is healthy and the measurement setup is reliable before diving into
driver capabilities.

### HANA backend

Even for trivial, index-based statements, the backend can occasionally be the
cause:

- Check **load, CPU, and memory** on the HANA host over the affected window. A
  transient spike (from another workload, backups, savepoints, delta merges,
  etc.) can slow otherwise-fast statements without anything being wrong on the
  client.
- Check **expensive statements** and the **SQL plan cache** for the statements
  in question around the slow timestamps. An occasionally recompiled plan or a
  plan-cache eviction can cause a one-off spike even for a simple query.

If the backend consistently shows the statement executing quickly at the times
your service saw a slow call, the time is being spent on the client / network /
pool side, and the driver observability below will localize it.

### Network

The path between the service and HANA can introduce latency that looks
identical to a slow query from the application's point of view:

- Intermittent packet loss, retransmits, or jitter add unpredictable delay to
  every round-trip (and each `Prepare`/`Exec`/`fetch` is at least one).
- Firewalls, NAT gateways, and load balancers may silently drop idle
  connections, so the *next* use of a pooled connection stalls or fails and
  forces a reconnect (see the ping-interval option under *Diagnose & tune*).
- DNS resolution and TLS handshakes on new connections add to connection setup
  time (phases C–D).

Basic reachability and stability checks (latency and loss to the HANA host,
whether slowdowns coincide with network events) are worth doing before
attributing anything to the driver.

### A reliable reference setup

It helps to have one controlled setup in which driver behaviour can be measured
in isolation, separate from the environment where the slowdown was observed. The
aim is not to reproduce every production condition, but to have a clean,
repeatable baseline for comparison: if the numbers in the reference match what
the observed environment shows, the investigation stays on the driver; if the
reference is fast and stable while the other environment is not, that difference
points at the environment (backend load, network, configuration) and helps
narrow down where to look next.

go-hdb ships such a benchmark: [`cmd/bulkbench`](https://github.com/SAP/go-hdb/tree/main/cmd/bulkbench),
a throughput benchmark used for the driver's own performance analysis. It can be
run interactively or as a Go benchmark (`go test -bench .`) and lets you vary
the batch count and size, sequential vs. concurrent execution, and the driver's
TCP buffer size — a good starting point for a reference measurement. (Note its
caution: do not run it against a productive HANA instance, as it creates schemas
and tables.)

A few things keep the reference trustworthy:

- Run against a stable, representative HANA instance — not one shared with
  unrelated heavy workloads that add their own noise.
- Use a well-understood network path, and record its baseline latency so driver
  time can be told apart from network time.
- Drive it with a controlled, repeatable load, so a change's effect is
  observable and reproducible.
- Warm the connection pool before measuring, so first-call connection setup is
  not mistaken for per-statement latency.

With a reference in place, a report from a different environment can be compared
against it directly, and the driver's own observability (below) tells you which
phase the remaining time is in.

For a lower-level look, the driver also supports CPU profiling that isolates
driver-side work: collecting a `pprof` profile and filtering with `tagignore=db`
excludes samples tagged with network and server-wait activity, so only the
driver's own CPU cost remains (see the CPU Profiling section of the go-hdb
[README](https://github.com/SAP/go-hdb#cpu-profiling)). That is more relevant to
driver-internal optimization than to the latency investigation here, but it
is worth knowing about when you need to measure the driver in isolation.

---

## Observe: where is the time going?

The tools below turn a vague "it's slow" into a measured statement about which
phase the time is in. Start with the aggregate stats (they answer
*get-a-connection vs. run-SQL* directly), export them so the behaviour is
captured over time — including intermittent spikes — and reach for the
per-statement SQL trace when you need to see individual statements.

### Driver stats

The driver aggregates gauges, counters, and time histograms independently of
`database/sql`. This is the primary tool for distinguishing "slow to *get* a
connection" from "slow to *run* SQL".

#### Getting the stats

```go
// Global driver stats — always available:
stats := connector.NativeDriver().Stats()   // *driver.Stats — across all uses of the driver

// The same stats, but scoped to a single DB (requires driver.OpenDB):
db := driver.OpenDB(connector)
stats := db.ExStats()                        // per-*driver.DB scope
```

`NativeDriver().Stats()` is *global* to the shared driver instance and is always
available. `db.ExStats()` returns the same set of stats but scoped to that one
`*driver.DB` — useful when several DSNs/tenants share a process and you want to
attribute activity to a specific one. Per-DB events also roll up into the global
stats, so the global stats remain the superset.

#### The `Stats` struct

See the [`Stats`](https://pkg.go.dev/github.com/SAP/go-hdb/driver#Stats) and
[`StatsHistogram`](https://pkg.go.dev/github.com/SAP/go-hdb/driver#StatsHistogram)
types for the full field list. In short it carries gauges
(`OpenConnections`, `OpenTransactions`, `OpenStatements`), counters
(`ReadBytes`, `WrittenBytes`, `SessionConnects`), and time histograms in
`TimeUnit` (default `"ms"`): `ReadTime`, `WriteTime`, `AuthTime`, and the
per-operation `SQLTimes` map (keys `prepare`, `query`, `exec`, `call`, `fetch`,
`fetchlob`, `rollback`, `commit`). Each histogram exposes `Count`, `Sum`, and
`Buckets`.

#### What to look at

| Signal | What it tells you |
|---|---|
| **`AuthTime.Count`** | How many new physical connections were authenticated. Each increment corresponds to a full connect + auth handshake. If it rises steadily during normal steady-state traffic (rather than only at startup), connections are being created rather than reused. |
| `AuthTime.Sum` / `AuthTime.Buckets` | How *long* each authentication took. `Sum/Count` gives the average; the buckets show the tail. High values here mean each pool miss is costly. |
| `SQLTimes["prepare"]`, `SQLTimes["exec"]`, `SQLTimes["query"]` | Time the driver actually spent on the SQL round-trip. If these are small but the application-side latency for the same call is large, the extra time is connection acquisition (A–E), not the statement. |
| `OpenConnections` | Live connections at the moment of sampling; compare against expected parallelism. |
| `SessionConnects` | Total session connects. |

**A decisive comparison:** sample `AuthTime.Count` at two points during normal
steady operation (e.g. a minute apart, away from startup). If it grew, the
driver opened new connections in that window, and any request that triggered
one paid the auth cost. Correlate the timing of those increments with the
timestamps of slow calls in your application tracing.

### Pool stats (`database/sql`)

The driver stats complement Go's own pool stats, which show the other half of
the picture: contention *waiting for* a connection, and connections being
discarded by the pool.

`db.Stats()` returns a
[`sql.DBStats`](https://pkg.go.dev/database/sql#DBStats); see that type for the
full field list. The fields most relevant here are `WaitCount` and
`WaitDuration` (how often, and how long, goroutines blocked waiting for a
connection), the pool gauges `MaxOpenConnections` / `OpenConnections` / `InUse`
/ `Idle`, and `MaxIdleClosed` / `MaxIdleTimeClosed` (connections closed because
`MaxIdleConns` was exceeded, or `ConnMaxIdleTime` elapsed).

Two patterns to look for:

- **`WaitCount` / `WaitDuration` climbing** → requests are blocking because the
  maximum number of open connections is too low for the observed parallelism
  (phase A).
- **`MaxIdleTimeClosed` / `MaxIdleClosed` climbing** → the pool is discarding
  connections, forcing re-authentication on the next burst (which feeds phase
  D). This is governed by the idle-connection and idle-timeout settings.

Together the two stat sources let you state precisely how many times a request
waited for a connection, how many times a new connection was authenticated, and
how long each authentication took.

### Exporting the stats (Prometheus / OTel)

Reading the stats once shows a snapshot; exporting them continuously shows how
they move over time. That is what lets you correlate the driver's behaviour with
slow-request timestamps — and, when slowness is intermittent, catch the spikes
that a one-off sample would miss. go-hdb ships Prometheus collectors for exactly
this.

Package: [`github.com/SAP/go-hdb/prometheus/collectors`](https://pkg.go.dev/github.com/SAP/go-hdb/prometheus/collectors).
It provides `NewDriverStatsCollector` (global driver stats, always available)
and `NewDBExStatsCollector` (the same stats scoped to a single `driver.DB`).
Register these alongside the standard `database/sql` pool collector
(`prometheus/client_golang/prometheus/collectors.NewDBStatsCollector`) so the
pool stats and the driver stats land in the same scrape. Note that the per-DB
collector requires opening the DB with `driver.OpenDB` rather than `sql.OpenDB`.
See the
[collectors package](https://pkg.go.dev/github.com/SAP/go-hdb/prometheus/collectors)
and the [`prometheus/` directory](https://github.com/SAP/go-hdb/tree/main/prometheus)
for a complete, runnable wiring example plus the exact metric names, types, and
labels.

In practice the go-hdb metrics mirror the `Stats` fields (gauges for open
connections/transactions/statements, counters for bytes and session connects,
and histograms for read/write/auth time and per-operation `sql_time`), all
under a `go_hdb_*` namespace with a `db_name` label.

The `sql_time` histogram is what shows whether latency is in `prepare`/`exec`
themselves versus connection setup, and a useful signal to graph and alert on
is the *rate* of `auth_time` count (or `session_connects`): in a healthy
steady-state service it should be roughly flat after warm-up, so spikes flag
connection-churn events to overlay on a slow-request graph.

### SQL trace (per-statement)

Where the stats aggregate, the SQL trace shows *individual* statements: which
one ran and how long each took. Reach for it once the stats point at SQL
round-trips (rather than auth) and you need to know which statements.

#### Enable / disable at runtime

```go
driver.SetSQLTrace(true)   // enable
driver.SetSQLTrace(false)  // disable
driver.SQLTrace()          // -> bool, current state
```

Or at process start via flag:

```
-hdb.sqlTrace=true
```

**Caveat:** the SQL tracer is attached to a connection when the connection is
*created*. `SetSQLTrace(true)` therefore only affects connections opened
**after** the call — existing pooled connections keep their previous setting
until they are replaced. So immediately after enabling trace you will see
output only for a subset of traffic, and coverage grows as old connections are
recycled (via `ConnMaxLifetime`, the idle timeout, or normal churn). To trace
all traffic promptly, enable it at startup with `-hdb.sqlTrace=true`, or force
the pool to turn over (e.g. temporarily lower `ConnMaxLifetime`).

#### What the log looks like

Trace uses structured `slog` at `INFO`, message `"SQL"`:

```
level=INFO msg=SQL prepare="SELECT * FROM t WHERE id=?" ms=3
level=INFO msg=SQL exec="SELECT * FROM t WHERE id=?" ms=12 arg.1=42
```

Attributes: the operation key (`prepare` / `query` / `exec` / `call` / `ping`)
carries the SQL text, `ms` carries the duration, and `arg.*` carries up to five
parameter values.

#### Routing trace to your logger

The trace (and internal driver error logs) use the logger set on the Connector,
which defaults to `slog.Default()`:

```go
connector.SetLogger(myStructuredLogger) // *slog.Logger
```

This lets the trace feed into the same logging pipeline the service already
uses.

---

## Diagnose & tune

Once the observations point at a phase, these are the levers and causes to
work through.

### Connection-pool configuration

Pool parameters live on `*sql.DB` / `*driver.DB`, **not** on the connector:

```go
db.SetMaxOpenConns(n)      // ceiling on concurrent physical connections
db.SetMaxIdleConns(n)      // how many connections to keep warm (idle)
db.SetConnMaxIdleTime(d)   // close idle connections after this duration; 0 = never for being idle
db.SetConnMaxLifetime(d)   // recycle connections after this age; 0 = no forced recycling
```

How these interact with the phases above:

- **`MaxIdleConns`** sets how many warm connections are kept ready. When more
  concurrent requests arrive than there are idle connections (and the open
  ceiling has not been reached), the pool opens and authenticates a new
  connection — those requests wait through phases C–E. Under bursty concurrency
  this happens irregularly.
- **`ConnMaxIdleTime`** determines how long idle connections survive. After a
  lull longer than this, idle connections are dropped, so the next request or
  burst re-authenticates from scratch.
- **`MaxOpenConns`** caps concurrency; if it is below the real peak parallelism,
  requests block waiting for a connection (phase A), which shows up in
  `sql.DBStats.WaitCount` / `WaitDuration`.

General guidance for a service with steady parallel usage:

- Keeping `MaxIdleConns` close to `MaxOpenConns` avoids discarding connections
  merely for being idle while still under the open ceiling.
- A larger (or zero) `ConnMaxIdleTime` avoids dropping warm connections during
  quiet periods, at the cost of holding HANA-side resources for longer.
- Size `MaxOpenConns` to the *observed* peak parallelism. The stats provide the
  feedback: `AuthTime.Count` staying flat after warm-up and
  `sql.DBStats.WaitCount` staying near zero indicate the values are adequate.

These are trade-offs, not fixed answers: keeping many connections warm suits a
hot service with predictable parallelism, while a service with rare bursts may
prefer a finite idle timeout. Size to observed behaviour rather than assumption.

#### Optionally: detect dead idle connections

If a firewall or NAT device silently drops idle connections, reusing one costs
a failed round-trip and a reconnect. The driver can ping a pooled connection
before handing it out:

```go
connector.SetPingInterval(d) // ping idle conns older than d before reuse; 0 = off (default)
```

The cost is one extra round-trip on checkout after the interval — cheap
compared to a failed query and reconnect.

### Authentication refresh callbacks

If a credential-refresh callback is registered, it runs synchronously during
authentication, so a slow implementation inflates `AuthTime` and serializes
concurrent new-connection attempts:

```go
connector.SetRefreshPassword(fn)    // basic-auth password rotation
connector.SetRefreshToken(fn)       // JWT token rotation
connector.SetRefreshClientCert(fn)  // X509 client cert rotation
```

If any of these is set, review its implementation for latency: does it call a
secrets manager, make an HTTP request, read a file, or take a lock? Any such
work is paid on connection establishment and appears as slow `Prepare`/`Exec`
on the requests that triggered a new connection. Cache where possible and avoid
network I/O on the callback's hot path.

### Cross-check the SQL time against HANA

Once you have driver stats, close the loop with the backend checks from
*Before the driver deep-dive*: compare the driver's `SQLTimes["exec"]` /
`sql_time{sql="exec"}` tail against HANA's own measured execution time for the
same statements. A large gap (driver slow, HANA fast) confirms the time is on
the client / network / pool side (phases A–G) rather than in HANA itself.

---

## Investigation checklist

1. [ ] Check the HANA backend (load/CPU/memory, expensive statements, SQL plan
       cache) around the slow timestamps.
2. [ ] Check the network path (latency, loss, idle-connection drops by
       firewalls/NAT) and confirm slowdowns don't coincide with network events.
3. [ ] Set up a controlled reference measurement (e.g. via `cmd/bulkbench`:
       representative instance, known network baseline, repeatable load, warmed
       pool) to compare the observed environment against.
4. [ ] Sample the driver stats (`NativeDriver().Stats()` or `db.ExStats()`); log
       `AuthTime.Count`, `AuthTime.Sum`, and `SQLTimes[*].Count`.
5. [ ] Check whether `AuthTime.Count` climbs during steady traffic
       (connection churn).
6. [ ] Sample `sql.DBStats`; check `WaitCount`/`WaitDuration` (contention) and
       `MaxIdleTimeClosed`/`MaxIdleClosed` (idle eviction).
7. [ ] Export `go_hdb_*` metrics plus `NewDBStatsCollector` to Prometheus/OTel;
       graph the rate of `auth_time` count / `session_connects`, and correlate
       auth/churn spikes with slow calls in application tracing.
8. [ ] Enable `driver.SetSQLTrace(true)` (new connections only) to see
       per-statement `ms`.
9. [ ] Review pool configuration against observed parallelism
       (`MaxIdleConns`, `ConnMaxIdleTime`, `MaxOpenConns`).
10. [ ] If a refresh callback is registered, review it for synchronous I/O.
11. [ ] Optionally set `SetPingInterval` to weed out dead idle connections.
12. [ ] Cross-check the driver's `SQLTimes["exec"]` tail against HANA's own
        execution time; a large gap points to the client/network/pool side.

---

## Reference links

- Driver package (Stats, SetSQLTrace, SetLogger, SetPingInterval, connector
  options): <https://pkg.go.dev/github.com/SAP/go-hdb/driver>
- Prometheus integration: <https://github.com/SAP/go-hdb/tree/main/prometheus>
- `cmd/bulkbench` throughput benchmark:
  <https://github.com/SAP/go-hdb/tree/main/cmd/bulkbench>
- CPU profiling (driver-side isolation via `tagignore=db`):
  <https://github.com/SAP/go-hdb#cpu-profiling>
- `database/sql` pool tuning (`SetMaxOpenConns`, `SetMaxIdleConns`,
  `SetConnMaxIdleTime`, `SetConnMaxLifetime`, `DBStats`):
  <https://pkg.go.dev/database/sql>
