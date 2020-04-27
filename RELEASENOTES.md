Release Notes
=============

## Release 0.9.0

Dear go-hdb users, please find a description of the main features of this 0.9.0 release below. Going from 0.14.4 to 0.9.0 should indicate,
that this is a huge step into the direction of a 1.0 release. So, while most effort was spent to prepare for 1.0 features like

- Support of [Named Arguments](https://golang.org/pkg/database/sql/#NamedArg)
- Support of [Output Parameters](https://golang.org/pkg/database/sql/#Out) calling Stored Procedures

release 0.9.0 brings some interesting and hopefully useful features as well:

#### Main Features 0.9.0

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
   needs to be opted in. To support a smooth transition, release 0.9.0 works per default like in the past (legacy mode).
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
   For a complete example please see Example_callTableOut in [](driver/example_call_test.go).

#### Incompatibilities to release 0.14.4

- no known incompatibilities