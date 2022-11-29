
## Version 1.0 Migration

go-hdb version 1.0 is going to be released after Go 1.20 is available which is expected to be released in February 2023. The minimal Go version for go-hdb 1.0 is [Go 1.19](https://go.dev/doc/devel/release#go1.19).

__To migrate to version 1.0 from the current latest version please switch off the connector 'legacy' mode (which is the default since version 0.107) and please replace the already deprecated type driver.NullTime by sql.NullTime.__

### Incompatible changes

Removal of already deprecated driver.NullTime alias (please use sql.NullTime instead)

Bulk operations:

- The following bulk operations available via legacy mode are no longer supported:
  - via query ("bulk insert ...")
  - via named arguments (Flush / NoFlush)
  - via 'many' supporting one and two dimensional slices, arrays
- Please use the following bulk operations instead:
  - via extended parameter list with (len(args)%'#of paramerters' == 0
  - via function argument (func(args []any) error)

Stored procedures with table output parameters:

- Retrieving table output parameter content via separate query call is no longer supported (legacy mode)
- Please use [sql.Rows](https://golang.org/pkg/database/sql/#Rows) instead (available since version 0.99)

### New features:

- Stored procedures with parameters can now be executed by sql.Exec
  - Support of [named](https://pkg.go.dev/database/sql#Named) parameters and
  - Support of [out](https://pkg.go.dev/database/sql#Out) output parameters
  
   Exception: Stored procedures with table output parameters need further to be called by sql.Query ([example](https://github.com/SAP/go-hdb/blob/main/driver/example_call_test.go)).

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
