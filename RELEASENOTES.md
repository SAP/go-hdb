Release Notes
=============

## v1.5.0

### New features

- Added support of tenant database connection via tenant database name:
  - see new Connector method WithDatabase and
  - new DSN parameter DSNDatabaseName

## v1.4.0

### Minor revisions

#### v1.4.7
- updated dependencies

#### v1.4.6
- added go1.22 database/sql Null[T any] support

#### v1.4.5
- updated dependencies

#### v1.4.4
- source code cleanups

#### v1.4.3
- updated documentation

#### v1.4.2
- updated dependencies

#### v1.4.1
- fixed bug: connection retry in case refresh callback has new version
- updated dependencies

### Changes

- Added support of Go 1.21.
- Dropped support of Go language versions < Go 1.20.

### Incompatible changes

- None.

## v1.3.0

### Minor revisions

#### v1.3.16
- updated dependencies

#### v1.3.15
- fixed REUSE warnings

#### v1.3.14
- preparation of go1.21 release

#### v1.3.13
- updated dependencies

#### v1.3.12
- source code cleanups

#### v1.3.11
- fixed compiling bug due to incompatible changes in https://github.com/golang/exp/commit/302865e7556b4ae5de27248ce625d443ef4ad3ed

#### v1.3.10
- updated dependencies

#### v1.3.9
- fixed bug: deadlock if refresh is called concurrently

#### v1.3.8
- fixed bug: connection retry even in case refresh callback would not provide updates

#### v1.3.7
- fixed bug pinging the database to often on resetting a connection
- updated dependencies

#### v1.3.6
- source code cleanups

#### v1.3.5
- fixed authentication refresh issue when using more than one authentication method

#### v1.3.4
- updated dependencies
- slog go1.19 support

#### v1.3.3
- updated dependencies

#### v1.3.2
- updated dependencies
- connection attribute logger instance getter and setter available for go1.20

#### v1.3.1
- use slog LogAttrs to improve performance and provide context if available

### New features

go-hdb [slog](https://pkg.go.dev/golang.org/x/exp/slog) support. The slog package does provide structured logging and replaces
the standard logging output including SQL and protocol traces.

### Incompatible changes

- Logging output format changes, including SQL and protocol trace.

Most go-hdb users shouldn't be affected by these incompatible changes.

## v1.2.0

### Minor revisions

#### v1.2.7
- updated dependencies

#### v1.2.6
- fixed bug in cesu8 encoding

#### v1.2.5
- updated dependencies
- fixed typo in constant

#### v1.2.4
- updated dependencies
- removed outdated utf8 code in cesu8 encoding

#### v1.2.3
- updated dependencies

#### v1.2.2
- updated dependencies
- pipx reuse check in Makefile
- use errors.Join for go versions greater 1.19

#### v1.2.1
- updated dependencies

This version is mainly about performance improvements and source code cleanups.

### Incompatible changes

- Package driver/sqltrace was removed. For enabling / disabling connection SQL trace please use driver method driver.SetSQLTrace instead.
- Flag hdb.protocol.trace was renamed to hdb.protTrace. 

Most go-hdb users shouldn't be affected by these incompatible changes.

## v1.1.0

### Minor revisions

#### v1.1.7
- replaced build tag 'edan' by connector attribute EmptyDateAsNull

#### v1.1.6
- fixed https://github.com/SAP/go-hdb/issues/113

#### v1.1.5
- added build tag 'edan' to return NULL in case of empty dates for all data format versions
- removed old build tags from source code

#### v1.1.4
- performance improvements
- updated dependencies

#### v1.1.3
- updated dependencies

#### v1.1.2
- updated dependencies

#### v1.1.1
- fixed slice resize runtime error: slice bounds out of range

This version is mainly about performance improvements and source code cleanups.

### Changes

Formerly with a ping interval defined the driver pinged all open connections periodically. Now a ping
is executed only when an idle connection is reused and the time since the last connection access
is greater than the ping interval. This avoids keeping idle connections to the server alive, improves
performance and is a compatible change from a driver's usage perspective.

## v1.0.0

### Minor revisions

#### v1.0.9
- upgraded dependencies

#### v1.0.8
- test performance improvements

#### v1.0.7
- added implicit instantiation of NullLob and NullDecimal owned references in respective Scan methods

#### v1.0.6
- some minor performance improvements and additional test

#### v1.0.5
- fixed blob bulk sql statement issue

#### v1.0.4
- fixed panic in case of bulk sql statement execution error

#### v1.0.3
- fixed scan type for 'nullable' database fields

#### v1.0.2
- fixed go.mod Go version

#### v1.0.1
- added Go 1.20 Unwrap() to driver.Error()

### Incompatible changes

Removal of already deprecated driver.NullTime alias (please use sql.NullTime instead).

Bulk operations:
- The following bulk operations are no longer supported:
  - via query ("bulk insert ...")
  - via named arguments (Flush / NoFlush)
  - via 'many' supporting one and two dimensional slices, arrays
- Please use the following bulk operations instead:
  - via extended parameter list with (len(args)%'#of paramerters' == 0
  - via function argument (func(args []any) error)

Stored procedures:
- Calling stored procedures with sql.Query methods are no longer supported.
- Please use sql.Exec methods instead and [sql.Rows](https://golang.org/pkg/database/sql/#Rows) for table output parameters.

### New features

- Stored procedures executed by sql.Exec with parameters do
  - support [named](https://pkg.go.dev/database/sql#Named) parameters and
  - support [out](https://pkg.go.dev/database/sql#Out) output parameters
  
- Custom types for reading Lobs

  Whereas string and []byte types are supported as Lob input parameters for output parameters and query results (scan) the driver.Lob type was needed.
  With the help of one of the following functions a string, []byte or io.Writer based custom type can now be used as well:
  - driver.ScanLobBytes
  - driver.ScanLobString
  - driver.ScanLobWriter

  Example:
  ```golang
  // BytesLob defines a []byte based data type for scanning Lobs.
  type BytesLob []byte
  // Scan implements the database.sql.Scanner interface.
  func (b *BytesLob) Scan(arg any) error { return driver.ScanLobBytes(arg, (*[]byte)(b)) }    
  ```
