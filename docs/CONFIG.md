# Configuring the driver with ConnectorConfig

The canonical way to configure a connector is the exported `ConnectorConfig`
struct. The old per-field setters (`SetTimeout`, `SetFetchSize`, …)
keep working but are deprecated and receive no new features.
To surface them in your application, enable `staticcheck` `SA1019`
(bundled in `golangci-lint`).

Why move: `NewConfigConnector` validates once and reports every bad
value (numbers, pairs, required fields); your `ConnectorConfig` is deep-copied at handoff so
later mutations affect nothing; `Connector.Config()` supports
derive-tweak-construct cycles; new settings land here only.

## Basic use

See `ExampleNewConfigConnector` ([godoc](https://pkg.go.dev/github.com/SAP/go-hdb/driver#ExampleNewConfigConnector),
[source](../driver/example_connector_test.go)) — `NewConnectorConfig` for defaults,
fill endpoint and credentials, `NewConfigConnector` to construct,
`sql.OpenDB` to use.

`NewConfigConnector` validates and deep-copies `cfg`: mutating
`cfg` afterwards affects nothing. Mutable values (TLS config, session
variables, cert bytes) are copied; stateless handles (`Logger`,
`Dialer`, `Compressor`, refresh callbacks) stay shared by design.
Invalid values are returned as
`error`, and all violations are reported at once. Start from
`NewConnectorConfig`: `Host` must not be empty, and the function and interface
fields (`Dialer`, CESU-8 factories, `Logger`) are required and
`validate` rejects nil. (Nil `Compressor` is the one legitimate
exception — it disables compression.) A hand-built `ConnectorConfig` works
as long as every required field is set.

From a DSN string, parse to `ConnectorConfig` first — tweak, then construct
(see `ExampleParseDSNConfig`:
[godoc](https://pkg.go.dev/github.com/SAP/go-hdb/driver#ExampleParseDSNConfig)).

Auth variants (within X509: one pair at a time — bytes and files are
mutually exclusive, `validate` rejects both). File identity:
`ExampleNewConfigConnector_x509`
([godoc](https://pkg.go.dev/github.com/SAP/go-hdb/driver#ExampleNewConfigConnector_x509));
JWT with refresh: `ExampleNewConfigConnector_jwt`
([godoc](https://pkg.go.dev/github.com/SAP/go-hdb/driver#ExampleNewConfigConnector_jwt)).
Static bytes (`ClientCert`/`ClientKey`) and custom refresh
(`RefreshClientCert`, same signature as the old callback) have no
dedicated examples yet — field assignment follows the table below.

The refresh callback is independent of that choice — it serves refresh
only, winning over files when set; a callback returning `ok=false`
leaves the current identity in place. With no callback, files are
re-read on every authentication refresh (rotation without
restart). A migrating `SetRefreshClientCert` func keeps working
unchanged — it just moves from the setter into the field.

Switching from the old API:

| old | new |
|---|---|
| `NewConnector()` | `NewConnectorConfig()` + `NewConfigConnector` (set `Host` first) |
| `NewBasicAuthConnector(host, user, pw)` | `cfg.Host` + `cfg.Username` + `cfg.Password` |
| `NewDSNConnector(dsn)` | `ParseDSNConfig(dsn)` + `NewConfigConnector` |
| `NewX509AuthConnector(host, cert, key)` | `cfg.ClientCert` + `cfg.ClientKey` |
| `NewX509AuthConnectorByFiles(host, f, k)` | `cfg.ClientCertFile` + `cfg.ClientKeyFile` |
| `SetRefreshClientCert(fn)` | assign `fn` to `cfg.RefreshClientCert` directly |
| `NewJWTAuthConnector(host, token)` | `cfg.Host` + `cfg.Token = token` |
| `SetTimeout(d)` | `cfg.DialTimeout` + `cfg.ReadTimeout` + `cfg.WriteTimeout` |
| `WithDatabase(db)` | `cfg.DatabaseName` at construction |
| other `SetX(v)` | `cfg.X` field directly |

Warning: a derived `ConnectorConfig` inherits *all* credentials, and
with several methods present the server chooses — e.g. deriving an
X509 connector from a basic-auth one still authenticates as the basic
user, unless the basic credentials are cleared.

With no `Username` and no `Token`, `Password` doubles as the external-ticket
carrier (same as the SAP HANA clients): a password starting with
`ey` is offered as a JWT ticket. Prefer the explicit `Token`
field for JWT.

## Custom dialers

Custom dialers implement [`dial.Dialer`](../driver/dial/dialer.go) and plug in via
`cfg.Dialer`. `DialerOptions` carries the timeout/keep-alive settings
(mirroring the `net.Dialer` fields).

Keep-alive: `TCPKeepAlive` and `TCPKeepAliveConfig` are forwarded 1:1
to `net.Dialer` as before (see `dial.DialerOptions`). `TCPKeepAlive`
mirrors `net.Dialer`: zero uses the net default (15s), negative
disables; it is ignored whenever `TCPKeepAliveConfig.Enable` is true
per stdlib semantics.

## Field notes

The `protTrace`/`sqlTrace` globals are copied into the trace configs
by `NewConnectorConfig` — set flags before building the config.
Later changes apply only to configs built afterwards. The deprecated
setters remain live and still affect their connector's future
connections.

- `TLSConfig *tls.Config`: assign directly, or build file-based
  config with `driver.NewTLSConfig(serverName, insecureSkipVerify,
  rootCAFiles...)`.
- `DialTimeout`/`ReadTimeout`/`WriteTimeout`: independent budgets for
  establishment, socket reads, and socket writes (`NewConnectorConfig`
  defaults all three to 5 minutes, `0` disables). They split the old
  single `Timeout`; legacy `SetTimeout` sets all three at once. The DSN
  `timeout` key fans out the same way when present — a DSN without the
  key leaves all three at `0` (no deadlines), preserving long-standing
  DSN behavior.
- `CancelTimeout`: budget for the synchronous `CANCEL` plus victim drain
  after request-context cancellation (`NewConnectorConfig` defaults to
  2 seconds, `0` selects the legacy async-`DISCONNECT` sever). The
  caller's return waits up to this budget.
- `SQLTrace SQLTraceConfig`: per-statement logging. `Enabled` logs
  every statement at `Info` (prefilled from the global `sqlTrace`
  flag). Statements reaching `ServerThreshold` (server processing
  time from the reply `StatementContext`) or
  `TotalThreshold` (client elapsed) are logged at `Warn` instead,
  traced or not. `0` disables a threshold.
- `ProtTrace ProtTraceConfig`: `Enabled` dumps protocol parts,
  prefilled from the global `protTrace` flag.
