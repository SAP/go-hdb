Release Notes
=============

## v1.0.0

### Minor revisions

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

### New features:

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
