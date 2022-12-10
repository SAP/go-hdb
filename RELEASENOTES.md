Release Notes
=============

## Release 0.111

Release 0.111.7 (upgrade urgency: no need for upgrade)

- Changed version 1.0 migration description

Release 0.111.6 (upgrade urgency: medium)

- Fixed issue in reporting wrong number of affected rows using LOBs (https://github.com/SAP/go-hdb/pull/100)

Release 0.111.5 (upgrade urgency: no need for upgrade)

- Upgraded dependencies (see go.mod)

Release 0.111.4 (upgrade urgency: high)

- Fixed protocol read timeout for "optimized" hana backend build

Release 0.111.3 (upgrade urgency: no need for upgrade)

- Version 1.0 announcement

Release 0.111.2 (upgrade urgency: no need for upgrade)

- Some source code cleanups

Release 0.111.1 (upgrade urgency: low)

- Some minor fixes and source code cleanups

#### Release Notes

- X509 authentication: check validity period of client certificates before connecting to database (incompatible change)

  added additional error return parameter to NewX509AuthConnector because of early X509 certificate and key validation

## Release 0.110

Release 0.110.1 (upgrade urgency: no need for upgrade)

- Added .reuse/dep5 to replace individual licence comments / files

#### Release Notes

- Bulk operation (incompatible change)
  
  Due to 'historical' reasons go-hdb does support the following alternatives executing 'bulk' operations:  
  - via query ("bulk insert ...")
  - via named arguments (Flush / NoFlush)
  - via 'many' supporting one and two dimensional slices, arrays
  - via extended parameter list with (len(args)%'#of paramerters' == 0
  - via function argument (func(args []any) error)
  
  As to the restrictions and redundancy comming with some of the options the first three are going to be set to deprecated
  and the latter two (extended arguments and function argument) are kept going forward. Until go-hdb release V1.0 would
  be available the deprecated options can be used further by switching on connector 'legacy mode'.
    
## Release 0.109

Release 0.109.2 (upgrade urgency: high)

- Fixed bulk function execute issue

Release 0.109.1 (upgrade urgency: no need for upgrade)

- Some minor source code cleanups

#### Release Notes

- Moved prometheus collector from ./driver/prometheus to ./prometheus (incompatible change)
- Removed deprecated /driver/hdb package
- Dropped support of Go language versions < Go 1.18
- Added bulk function execute (experimental - please see Example_fctInsert())
- Upgraded dependencies (see go.mod)

## Release 0.108

Release 0.108.3 (upgrade urgency: low)

- Error handling improvement (https://github.com/SAP/go-hdb/pull/95)
- Some minor source code cleanups

Release 0.108.2 (upgrade urgency: high)

- Fixed hdb version parsing error using HANA v1
- Fixed some comments and error message texts
- Enhanced makefile to install further go versions

Release 0.108.1 (upgrade urgency: medium)

- Fixed driver.Decimal conversion overflow error

#### Release Notes

- Added prometheus collectors for driver and extended database statistics
- Added driver.DB for support of extended database statistics

## Release 0.107

Release 0.107.4 (upgrade urgency: medium)

- Fixed authentication issue session cookie (https://github.com/SAP/go-hdb/pull/93)
- Fixed connection error handling in case of invalid queries
- Incompatible change of time stats
- Updated: github.com/prometheus/client_golang to v1.13.0
- Source code cleanups

Release 0.107.3 (upgrade urgency: low)

- Changed stats configuration
- Added metrics
- Source code cleanups

Release 0.107.2 (upgrade urgency: low)

- Fixed stats for commit
- Changed stats configuration attributes

Release 0.107.1 (upgrade urgency: low)

- Added support of Go 1.9
- Dropped support of Go language versions < Go 1.7
- Added Prometheus collectors (experimental)

#### Release Notes

- Added X509 and JWT authentication

#### Incompatibilities

- Some minor connector incompatibilities most users should not be affected of

## Release 0.106

Release 0.106.1 (upgrade urgency: low)

- Fixed README.md for HANA cloud connection

#### Release Notes

- The 'legacy mode' now defaults to false

#### Incompatibilities

- Please see Main Features (second point) of Release 0.99

## Release 0.105

Release 0.105.8 (upgrade urgency: low)

- Added support of Go 1.8
- Dropped support of Go language versions < Go 1.6

Release 0.105.7 (upgrade urgency: low)

- Update README.md

Release 0.105.6 (upgrade urgency: low)

- Upgrade golang.org/x/crypto to v0.0.0-20211215153901-e495a2d5b3d3
- Update Copyright Text

Release 0.105.5 (upgrade urgency: medium)

- Upgrade golang.org/x/crypto to v0.0.0-20210921155107-089bfa567519
- Upgrade golang.org/x/text to v0.3.7

Release 0.105.4 (upgrade urgency: low)

- Fixed error transforming invalid CESU-8 data

Release 0.105.3 (upgrade urgency: low)

- Added support of Go 1.7
- Dropped support of Go language versions < Go 1.5

Release 0.105.2 (upgrade urgency: low)

- Added 'raw' connection methods DatabaseName and DBConnectInfo

Release 0.105.1 (upgrade urgency: low)

- Added custom CESU-8 encodings

#### Release Notes

- Removed driver.common package
- Additional internal package cleanups

#### Incompatibilities

- Some minor type incompatibilities most users should not be affected of:
  - driver.common.HDBVersion -> driver.hdb.Version
  - driver.common.DriverConn -> driver.Conn
  - driver.common.DriverConn.ServerInfo() -> driver.Conn.HDBVersion()

## Release 0.104

Release 0.104.1 (upgrade urgency: high)

- Fixed runtime error raised by missing error check executing sql exec statements

#### Release Notes

- Added complete support of HANA version 4

#### Incompatibilities

- HANA version 4 does not continue to support the following data types: shorttext, alphanum, text, bintext
- HANA 4 changes the reported database type names for
  - char to nchar
  - varchar to nvarchar
  - clob to nclob

## Release 0.103

Release 0.103.3 (upgrade urgency: medium)

- Fixed runtime error raised by missing error check executing sql statements
- Added spatial encoders, an example and additional tests

Release 0.103.2 (upgrade urgency: medium)

- Fixed conversion issue selecting spatial types

Release 0.103.1 (upgrade urgency: low)

- Added https://go.dev package link to README.md
- Linter fixes

#### Release Notes

- Added support of Go 1.6
- Dropped support of Go language versions < Go 1.4
- Deprecation of io/ioutil starting with Go 1.6
- Support of bulk / many inserts using geospatial data types
- Additional context information is provided in case of UTF-8 / CESU-8 conversion errors

#### Incompatibilities

- no known incompatibilities

## Release 0.102

Release 0.102.7 (upgrade urgency: high)

- Fixed go-hdb runtime error (index out of range) using DECIMAL fields with precision and scale specified

Release 0.102.6 (upgrade urgency: low)

- 'execute many' is officially supported now (please see Example_manyInsert())
- Some minor cleanups

Release 0.102.5 (upgrade urgency: moderate)

- Early garbage collection of bulk / many attributes.
- Fixed issue using context cancelCtx (context.WithCancel(...)) in sql.DB statements

Release 0.102.4 (upgrade urgency: moderate)

- Support stmt.Exec() for stored procedures with no or only input parameters

Release 0.102.3 (upgrade urgency: low)

- Reset the default Data Format Version to '8'
- Full support of Data Format Version 8 fixed decimals data types Fixed8, Fixed12 and Fixed16

Release 0.102.2 (upgrade urgency: high)

- Fixed go-hdb 'panic' using DECIMAL fields with precision and scale specified
- Downgrade Data Format Version from '8' to '6'

Release 0.102.1 (upgrade urgency: low)

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

- Some minor type incompatibilities most users should not be affected of

## Release 0.101

Release 0.101.2

- Added multi platform build to Github workflow

Release 0.101.1

- Added linux/arm 32-bit support

#### Release Notes

- The default Data Format Version is now '8' which does support boolean values on protocol level
- Dropped support of HANA server versions < 2.00.042

## Release 0.100

Release 0.100.14

- Added support of Go 1.13 again

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

- Added support of [PBKDF2](https://tools.ietf.org/html/rfc2898) authentication

PBKDF2 authentification is now used as default. Standard user / password authentication is used as fallback solution.


## Release 0.99

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