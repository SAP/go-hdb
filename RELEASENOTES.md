Release Notes
=============

## v1.13.0

### Minor revisions

#### v1.13.4 - v1.13.5
- updated dependencies

#### v1.13.3
- fixed synchronize refresh calls

#### v1.13.2
- fixed toolchain

#### v1.13.1
- performance improvements

### Changes

- Added support of Go 1.24.
- Dropped support of Go language versions < Go 1.23.

## v1.12.0

### Minor revisions

#### v1.12.12
- updated dependencies

#### v1.12.11
- go1.24 preparation

#### v1.12.10
- updated dependencies

#### v1.12.9
- fixed linter issues

#### v1.12.6 - v1.12.8
- updated dependencies

#### v1.12.5
- updated documentation

#### v1.12.4
- updated dependencies
- fixed README.me HANA client link

#### v1.12.3
- updated dependencies
- fixed linter issues

#### v1.12.2
- [fixed typo in test name](https://github.com/SAP/go-hdb/pull/139)

#### v1.12.1
- updated dependencies
- support of additional linter checks

### Changes

- Changed 'prometheus' into own go module to reduce dependencies on go-hdb.

## v1.11.0

### Minor revisions

#### v1.11.3
- fixed bug (panic) in sql trace output in case the sql trace is not switched on
- updated dependencies

#### v1.11.2
- fixed bug in converting integer arguments to HANA boolean type

#### v1.11.1
- fixed linter issues

### New features

- Added bulk insert via iterator (see example) for go versions >= go1.23.0
- Added net.KeepAliveConfig to connector (TCPKeepAliveConfig)
  and enable on default with default settings for go versions >= go1.23.0

### Changes

- Added support of Go 1.23.
- Dropped support of Go language versions < Go 1.22.

## v1.10.0

### Minor revisions

#### v1.10.4
- fixed bug (panic) in go-hdb/driver/stmt.go function execCall on calling stored procedures with table output parameters without providing arguments (args)

#### v1.10.3
- updated dependencies

#### v1.10.2
- fixed race condition closing transactions

#### v1.10.1
- updated dependencies

### New features

- Added user switch on existing connection (see example).

## v1.9.0

### Minor revisions

#### v1.9.11
- fixed race conditions on cancelled statement calls

#### v1.9.10
- updated dependencies
- avoid race conditions due to lob scans

#### v1.9.9
- updated dependencies
- added go1.23rc1 make

#### v1.9.8
- changed go-mod go version back to go1.21

#### v1.9.7
- fixed panic calling driver.DB.Close()
- updated dependencies

#### v1.9.6
- updated dependencies

#### v1.9.5
- replaced testing hook by context

#### v1.9.4
- removed dependency

#### v1.9.3
- switching SQL trace on / off including pooled connections

#### v1.9.2
- performance improvements
- fixed race conditions

#### v1.9.1
- performance improvements

### New features

- Added statement metadata (see example).
- Improved sql trace.

## v1.8.0

### Minor revisions

#### v1.8.30
- updated README root certificate sections

#### v1.8.29
- updated dependencies

#### v1.8.28
- restructured examples

#### v1.8.27
- added call procedure with input table example and test

#### v1.8.22 - v1.8.26
- updated dependencies
- source code cleanups
- performance improvements

#### v1.8.21
- experimental statement metadata

#### v1.8.20
- changed minimal go version in go.mod to 1.21.0

#### v1.8.18 - v1.8.19
- updated dependencies

#### v1.8.17
- CWE-770 mitigation in bulkbench benchmark

#### v1.8.16
- fixed minimal go version in go.mod to comply to the 1.N.P syntax (https://go.dev/doc/toolchain#version)

#### v1.8.15
- fixed sql value conversion for integers and floats

#### v1.8.14
- fixed race condition in writing metrics

#### v1.8.13
- updated dependencies
- source code cleanups

#### v1.8.12
- updated dependencies

#### v1.8.11
- fixed typo
- fixed SQL datatype in StructScanner

#### v1.8.10
- fixed regression on authentication refresh
- updated dependencies

#### v1.8.8 - v1.8.9
- updated dependencies

#### v1.8.7
- fixed github action issue

#### v1.8.4 - v1.8.6
- source code cleanups
- performance improvements
- updated dependencies

#### v1.8.3
- updated dependencies

#### v1.8.2
- fixed github actions & CodeQL toolchain issues

#### v1.8.1
- added toolchain

### Changes

- Added support of Go 1.22.
- Dropped support of Go language versions < Go 1.21.

### Incompatible changes

- None.

## v1.7.0

### Minor revisions

#### v1.7.12 - v1.7.13
- updated dependencies

#### v1.7.11
- updated github action versions

#### v1.7.10
- bulkbench fixes

#### v1.7.9
- go1.22 preparation

#### v1.7.8
- fixed issue closing connection before concurrent db calls are finalized

#### v1.7.7
- source code cleanups

#### v1.7.6
- updated dependencies
- bulkbench refresh

#### v1.7.5
- updated dependencies
- fixed issue starting go routines during driver initialization
  - https://github.com/SAP/go-hdb/issues/130
  - https://github.com/SAP/go-hdb/issues/131

#### v1.7.4
- performance improvements

#### v1.7.3
- updated dependencies
- test performance improvements

#### v1.7.1 - v1.7.2
- updated dependencies
- go1.22 preparation

### New features

- Added driver support of scanning database rows into go structs.

## v1.6.0

### Minor revisions

### v1.6.11
- fixed github actions

### v1.6.10
- source code cleanups
- performance improvements

#### v1.6.9
- CESU8 decoding performance improvement
- added time unit to stats

#### v1.6.8
- updated dependencies
- source code cleanups

#### v1.6.6 - v1.6.7
- source code cleanups

#### v1.6.5
- fixed linter issues

#### v1.6.4
- updated dependencies
- fixed linter issues

#### v1.6.3
- updated dependencies

#### v1.6.2
- fixed sqlscript parsing bug
- updated dependencies

#### v1.6.1
- renamed sqlscript SplitFunc to ScanFunc

### New features

- Added experimental sqlscript package.

## v1.5.0

### Minor revisions

#### v1.5.6 - v1.5.10
- updated dependencies

#### v1.5.5
- fixed driver.OpenDB dial tcp: missing address

#### v1.5.4
- updated dependencies

#### v1.5.3
- performance improvements

#### v1.5.2
- fixed race condition in connection conn and stmt methods in case of context cancelling

#### v1.5.1
- updated dependencies

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
