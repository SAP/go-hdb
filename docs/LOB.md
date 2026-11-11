# LOB handling

This document describes how the driver reads HANA large object (LOB) columns —
CLOB, NCLOB, BLOB and their TEXT / BINTEXT variants — into Go scan destinations,
which destinations are supported, and how the legacy `Lob` / `NullLob` types map
onto them.

The scan contract described here applies to **Go 1.27 and later**. Go 1.26 is
different and is covered separately at the end.

## Two ways to read a LOB

A LOB column can be read in one of two modes, chosen by the scan destination:

- **Streaming** — the LOB is written to an `io.Writer` as it arrives from the
  server, chunk by chunk, without ever holding the whole value in memory.
- **Materializing** — the LOB is read in full and assigned to a `string` or
  `[]byte`.

Streaming is the better choice for large values: memory stays bounded by the
chunk size rather than the LOB size. Materializing is simpler when the value is
small and the caller wants it as a plain Go value.

## Supported scan destinations (Go 1.27+)

| Destination            | Mode        | Notes                                             |
| ---------------------- | ----------- | ------------------------------------------------- |
| `string`               | materialize |                                                   |
| `[]byte`               | materialize | reuses the slice's capacity across scans          |
| `io.Writer`            | stream      | the caller supplies the writer                    |
| a custom `sql.Scanner` | materialize | takes precedence; receives the full LOB as `[]byte`, or `nil` for NULL |

A defined type whose underlying kind is string or `[]byte` (for example
`type ID string`) is a materialize destination too, read the same way as the
corresponding built-in type.

Each destination has a NULL-aware counterpart. A SQL NULL value scanned into a
plain `string` or `io.Writer` returns a scan error; scanned into a plain `[]byte`
it yields a `nil` slice without error, following the `database/sql` convention
for byte slices. A nullable destination reports the NULL through its `Valid`
field instead.

| Nullable destination          | Wraps          | NULL yields       |
| ----------------------------- | -------------- | ----------------- |
| `sql.NullString`              | `string`       | `Valid == false`  |
| `driver.NullBytes`            | `[]byte`       | `Valid == false`  |
| `sql.Null[string]`            | `string`       | `Valid == false`  |
| `sql.Null[[]byte]`            | `[]byte`       | `Valid == false`  |
| `sql.Null[io.Writer]`         | `io.Writer`    | `Valid == false`  |
| `sql.Null[T]` (custom scanner) | a `sql.Scanner` | `Valid == false` |

`sql.Null[io.Writer]` streams, like the bare `io.Writer`, and so requires the
caller to supply the writer: set the `V` field to a concrete writer before
scanning. A non-null value is streamed into that writer and `Valid` is set; a
NULL value leaves the writer untouched and sets `Valid` to false. Scanning a
non-null value with a nil `V` is a usage error — there is nothing to stream into.

    var dst sql.Null[io.Writer]
    buf := new(bytes.Buffer)
    dst.V = buf                       // caller supplies the sink
    err := rows.Scan(&dst)            // streams into buf, sets dst.Valid

A custom `sql.Scanner` takes precedence over `io.Writer`: if the destination
implements `sql.Scanner`, the LOB is read in full and its `Scan` method is called
with the complete value as `[]byte` (or `nil` for a SQL NULL), even if the same
type also implements `io.Writer`.

## Legacy types: `Lob` and `NullLob`

The driver's own `Lob` and `NullLob` types predate native LOB scanning. They are
retained for compatibility and still work as scan destinations, but new code
does not need them: every use maps onto one of the standard destinations above,
with the same behavior.

| Legacy use                          | Replace with                              |
| ----------------------------------- | ----------------------------------------- |
| `*Lob` with an `io.Writer` set      | the `io.Writer` directly                  |
| `*Lob`, reading `.Writer()` bytes   | `[]byte` or `string`                      |
| `*NullLob`                          | `sql.Null[io.Writer]`, `sql.NullString`, `driver.NullBytes`, or `sql.Null[[]byte]` |

For example, streaming a LOB into a buffer no longer needs a `Lob`:

    // legacy
    var l Lob
    l.SetWriter(buf)
    rows.Scan(&l)

    // equivalent, without the Lob type
    rows.Scan(buf)              // buf is an io.Writer

The generic `sql.Null[Lob]` and `sql.Null[*Lob]` forms are also supported, for
symmetry with the other `sql.Null[T]` destinations; they carry the same legacy
status as `Lob` itself.

## Go 1.26

On Go 1.26 the `database/sql` package cannot route a LOB value into an arbitrary
destination: a LOB reaches a destination only through `sql.Scanner`. The
supported scan destinations are therefore `*Lob`, `*NullLob`, `sql.Null[Lob]`
and `sql.Null[*Lob]`. To read a LOB into a `string`, `[]byte` or `io.Writer`,
use the helper functions `ScanLobString`, `ScanLobBytes` and `ScanLobWriter`,
which take the scanned source value and the target explicitly. On Go 1.26,
`*Lob` and `*NullLob` implement `sql.Scanner`; on Go 1.27 they implement
`io.Writer`, the standard destinations are supported natively, and the
`ScanLob*` helpers are deprecated.
