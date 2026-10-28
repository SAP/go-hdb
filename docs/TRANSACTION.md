# Transaction handling

This document explains how the driver reconciles the two transaction settings
HANA exposes through SQL — **isolation level** and **access mode** (read
only / read write) — with the `database/sql` model, and the guarantees it makes
as a result.

## The problem

`database/sql` delivers isolation level and access mode *per transaction*,
through `driver.TxOptions` passed to `BeginTx`. HANA, in contrast, treats both
as **session state**: a `SET TRANSACTION ISOLATION LEVEL ...` or
`SET TRANSACTION READ ONLY|READ WRITE` statement changes the connection and
stays in effect until the next such statement — it is *not* scoped to a single
transaction.

Without care, a setting applied for one transaction leaks past its
`Commit`/`Rollback` onto whatever runs next on the same connection. The access
mode leak is the harmful one: after a read-only transaction commits, the next
autocommit write on a pooled connection fails with

    SQL Error 129 - cannot change this transaction's access mode from
    read-only to update directly: please use "SET TRANSACTION READ WRITE"
    statement first

Isolation leaks are quieter — an autocommit read between transactions may run
under a stronger level than the caller expects — but both are observable, and
both surface where `database/sql` reuses connections from its pool.

## Relevant HANA behaviour

- A fresh session starts at **READ COMMITTED** isolation and **READ WRITE**
  access mode. These are fixed server defaults, not configurable through any
  `indexserver.ini` key.
- HANA offers no way to *read back* the current isolation level or access mode:
  `SET TRANSACTION` has no readable session variable, and the monitoring view
  `M_TRANSACTIONS` carries a row only while a transaction is open. The current
  values can only be known by tracking the changes the client itself observes.
- The server reports every accepted change back to the client, and only when
  the value actually changed — symmetrically on set and reset. A statement that
  sets a value already in effect is reported as no change. The client can
  therefore track the current values by observing these reports — the only way
  to know them, since there is no read-back.

## Guarantees

- A transaction runs at exactly the isolation level and access mode its
  `TxOptions` specify (`sql.LevelDefault` and `ReadOnly == false` mean the HANA
  defaults, read committed / read write).
- When a transaction ends, the connection is returned to the isolation level
  and access mode it had **before** that transaction began — not to a fixed
  driver default. Nothing is left behind for whatever runs next, and a level or
  access mode an application established out of band (a raw `SET TRANSACTION ...`
  on a held `sql.Conn`) survives an intervening transaction instead of being
  silently reset.

Most applications never touch isolation or access mode outside `TxOptions` and
need not read further. The guarantee matters only in the niche case of pinning a
connection and setting a level on it directly, where that level is preserved
across an unrelated transaction rather than silently reset:

    conn, _ := db.Conn(ctx)
    defer conn.Close()
    conn.ExecContext(ctx, "set transaction isolation level serializable")

    tx, _ := conn.BeginTx(ctx, nil) // default: read committed
    // ... work at read committed ...
    tx.Commit()

    // conn is serializable again, not reset to the default.

## Scope and limitations

- Isolation levels beyond those `database/sql` and HANA share
  (`sql.LevelSnapshot`, `sql.LevelLinearizable`, …) are unsupported and are
  rejected by `BeginTx` with `ErrUnsupportedIsolationLevel`.
