Preliminary Release Notes (upcoming v1.18.0)
============================================

The following notes describe work planned for v1.18.0. They are preliminary and
will take effect once Go 1.27 is released.

## v1.18.0

### Major changes

- Added support of Go 1.27.
- Dropped support of Go language versions < Go 1.26.

- Starting with Go 1.27 lob values are scanned natively into `string`, `[]byte`
  and `io.Writer` based destinations. The functions `driver.ScanLobBytes`,
  `driver.ScanLobString` and `driver.ScanLobWriter` are no longer needed for
  scanning lob values.

  This is possible because Go 1.27 introduces the `driver.RowsColumnScanner`
  interface, which lets drivers scan directly into the user provided scan
  destination.

### Deprecations

- Starting with Go 1.27 the functions `driver.ScanLobBytes`,
  `driver.ScanLobString` and `driver.ScanLobWriter` are deprecated. They are kept
  as no-op stubs for compatibility.

### Incompatible changes

- Starting with Go 1.28 the functions `driver.ScanLobBytes`,
  `driver.ScanLobString` and `driver.ScanLobWriter` panic if called. Applications
  still using them need to adapt to the native lob scan into `string`, `[]byte`
  and `io.Writer` based destinations (supported since Go 1.27).

- The examples demonstrating the usage of `driver.ScanLobBytes`,
  `driver.ScanLobString` and `driver.ScanLobWriter` are build restricted to Go
  1.26 and earlier.

Most go-hdb users shouldn't be affected by these changes.
