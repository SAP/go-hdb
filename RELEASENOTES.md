Release Notes
=============

## Release 0.102

#### Minor revisions

Release 0.102.1 (need to upgrade: no)

- Fixed some report card issues

#### Release Notes

- Added support of HANA server versions < 2.00.042 again
- Fixed bug when executing stored procedures without output (return) parameters (+ added test) 
- Updated bulk example (bulk operations need to be executed in transaction or connection context!)
- Added full support of sql.Conn.Raw()
- Added method ServerInfo to driver.Conn (+ example)
- driver.Driver is now public and provides some additional methods (e.g. Stats) 
- Support of sql.driver.Validator interface (go 1.15)
- Added 'execute many' (experimental - please see Example_manyInsert())
- Performance improvements

#### Incompatibilities

- some minor type incompatibilities most users should not be affected of

## Release 0.101

#### Minor revisions

Release 0.101.2

- Added multi platform build to Github workflow

Release 0.101.1

- Added linux/arm 32-bit support

#### Release Notes

- The default Data Format Version is now '8' which does support boolean values on protocol level
- Dropped support of HANA server versions < 2.00.042

## Release 0.100

#### Minor revisions

Release 0.100.14

- Added support of Go 1.3 again

Release 0.100.13

- Added support of Go 1.5
- Dropped support of Go language versions < Go 1.4.7
- Added clientContext protocol support (new Connector attribute: ApplicationName)
- Set session variables via clientInfo instead of sql set
- Fixed issue with lob fields in bulk statements

Release 0.100.11 - 0.100.12

- Added FSF license compliancy

Release 0.100.10

- Rename master to main branch

Release 0.100.9

- Added custom dialers for driver - database connections
- Added float to integer conversion in case the float value can be represented as integer
- Added no-timeout option for db reads and writes (timeout == 0)
- Added connection ping interval to database connector object

Release 0.100.8

- Fixed authentication issue for DB instances not supporting PBKDF2
- Added tcp keep-alive parameter to connector


Release 0.100.6 - 0.100.7

- Added alpha version of Bintext support

Release 0.100.5

- Go 1.13 compatibility

Release 0.100.1 - 0.100.4
- Minor optimizations
- Bug fixes
- Linter fixes
- Additional lob example (read / write lob in chunks via pipe)

#### Release Notes

- Added support of [PBKDF2](https://tools.ietf.org/html/rfc2898) authentication.

PBKDF2 authentification is now used as default. Standard user / password authentication is used as fallback solution.


## Release 0.99

#### Minor revisions

Release 0.99.1
- Additional conversions for query parameters
  - now strings can be used for integer and float types


#### Release Notes

Dear go-hdb users, please find a description of the main features of this release below. Going from 0.14.4 to 0.99 should indicate,
that this is a huge step into the direction of a 1.0 release. So, while most effort was spent to prepare for 1.0 features like

- Support of [Named Arguments](https://golang.org/pkg/database/sql/#NamedArg)
- Support of [Output Parameters](https://golang.org/pkg/database/sql/#Out) calling Stored Procedures

this release brings some interesting and hopefully useful features as well:

#### Main Features

1. Data Format Version.

   The so-called Data Format Version was increased, so that the following HANA data types will be recognized by the driver and
   correctly reported by https://golang.org/pkg/database/sql/#ColumnType.DatabaseTypeName
    - DAYDATE
    - SECONDTIME
    - LONGDATE
    - SECONDDATE
    - SHORTTEXT
    - ALPHANUM


2. Usage of [sql.Rows](https://golang.org/pkg/database/sql/#Rows) in Stored Procedures with table output parameters.

   Until now, table output parameter content was retrieved via a separate query call. As the Go sql package does now
   support sql.Rows in [Rows.Scan](https://golang.org/pkg/database/sql/#Rows.Scan), the workaround via a separate
   query call is obsolete. Nonetheless, like this change is incompatible compared to the former releases, the feature
   needs to be opted in. To support a smooth transition, procedure calls works per default like in the past (legacy mode).
   Anyway, the use of 'separate queries' is deprecated and the default is going to be changed within the next releases.
   Release 1.0 will only support the new version of retrieving table output parameter content, so new projects based
   on (go-hdb)[https://github.com/SAP/go-hdb] should opt in the feature already now:

   - please use a Connector object to open a database
   - please set the legacy mode via the Connector object to false

   ```golang
   connector, err := NewDSNConnector(TestDSN)
   if err != nil {
     log.Fatal(err)
   }
   // *Switch to non-legacy mode.
   connector.SetLegacy(false)
   db := sql.OpenDB(connector)
   defer db.Close()
   ```
   For a complete example please see [Example_callTableOut](driver/example_call_test.go).

#### Incompatibilities

- no known incompatibilities