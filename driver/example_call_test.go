//go:build !unit
// +build !unit

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"database/sql"
	"fmt"
	"log"
)

/*
ExampleCallSimpleOut creates a stored procedure with one output parameter and executes it.
Stored procedures with output parameters must be executed by sql.Query or sql.QueryRow.
For TestConnector see main_test.go.
*/
func Example_callSimpleOut() {
	const procOut = `create procedure %s (out message nvarchar(1024))
language SQLSCRIPT as
begin
    message := 'Hello World!';
end
`

	db := sql.OpenDB(DefaultTestConnector())
	defer db.Close()

	procedure := RandomIdentifier("procOut_")

	if _, err := db.Exec(fmt.Sprintf(procOut, procedure)); err != nil { // Create stored procedure.
		log.Fatal(err)
	}

	var out string

	if err := db.QueryRow(fmt.Sprintf("call %s(?)", procedure)).Scan(&out); err != nil {
		log.Fatal(err)
	}

	fmt.Print(out)

	// output: Hello World!
}

/*
ExampleCallTableOutLegacy creates a stored procedure with one table output parameter and executes it in legacy mode.
Legacy mode:
Stored procedures with table output parameters must be executed by sql.Query as sql.QueryRow will close
the query after execution and prevent querying output table values.
The scan type of a table output parameter is a string containing an opaque value to query table output values
by standard sql.Query or sql.QueryRow methods.
For TestConnector see main_test.go.
*/
func Example_callTableOutLegacy() {
	const procTable = `create procedure %[1]s (out t %[2]s)
language SQLSCRIPT as
begin
  create local temporary table #test like %[2]s;
  insert into #test values('Hello, 世界');
  insert into #test values('SAP HANA');
  insert into #test values('Go driver');
  t = select * from #test;
  drop table #test;
end
`

	connector := NewTestConnector()
	// *Switch to legacy mode.
	connector.SetLegacy(true)
	db := sql.OpenDB(connector)
	defer db.Close()

	tableType := RandomIdentifier("TableType_")
	procedure := RandomIdentifier("ProcTable_")

	if _, err := db.Exec(fmt.Sprintf("create type %s as table (x nvarchar(256))", tableType)); err != nil { // Create table type.
		log.Fatal(err)
	}

	if _, err := db.Exec(fmt.Sprintf(procTable, procedure, tableType)); err != nil { // Create stored procedure.
		log.Fatal(err)
	}

	var tableQuery string // Scan variable of table output parameter.

	// Query stored procedure.
	rows, err := db.Query(fmt.Sprintf("call %s(?)", procedure))
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		log.Fatal(rows.Err())
	}
	if err := rows.Scan(&tableQuery); err != nil {
		log.Fatal(err)
	}

	// Query stored procedure output table.
	tableRows, err := db.Query(tableQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer tableRows.Close()

	for tableRows.Next() {
		var x string

		if err := tableRows.Scan(&x); err != nil {
			log.Fatal(err)
		}

		fmt.Println(x)
	}
	if err := tableRows.Err(); err != nil {
		log.Fatal(err)
	}

	// output: Hello, 世界
	// SAP HANA
	// Go driver
}

/*
ExampleCallTableOut creates a stored procedure with one table output parameter and executes it
making use of sql.Rows scan parameters (non-legacy mode - *please see connector.SetLegacy(false)).
Stored procedures with table output parameters must be executed by sql.Query as sql.QueryRow will close
the query after execution and prevent querying output table values.
For TestConnector see main_test.go.
*/
func Example_callTableOut() {
	const procTable = `create procedure %[1]s (out t %[2]s)
language SQLSCRIPT as
begin
  create local temporary table #test like %[2]s;
  insert into #test values('Hello, 世界');
  insert into #test values('SAP HANA');
  insert into #test values('Go driver');
  t = select * from #test;
  drop table #test;
end
`
	connector := NewTestConnector()
	// *Switch to non-legacy mode.
	connector.SetLegacy(false)
	db := sql.OpenDB(connector)
	defer db.Close()

	tableType := RandomIdentifier("TableType_")
	procedure := RandomIdentifier("ProcTable_")

	if _, err := db.Exec(fmt.Sprintf("create type %s as table (x nvarchar(256))", tableType)); err != nil { // Create table type.
		log.Fatal(err)
	}

	if _, err := db.Exec(fmt.Sprintf(procTable, procedure, tableType)); err != nil { // Create stored procedure.
		log.Fatal(err)
	}

	var tableRows sql.Rows // Scan variable of table output parameter.

	// Query stored procedure.
	rows, err := db.Query(fmt.Sprintf("call %s(?)", procedure))
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		log.Fatal(rows.Err())
	}
	if err := rows.Scan(&tableRows); err != nil {
		log.Fatal(err)
	}

	for tableRows.Next() {
		var x string

		if err := tableRows.Scan(&x); err != nil {
			log.Fatal(err)
		}

		fmt.Println(x)
	}
	if err := tableRows.Err(); err != nil {
		log.Fatal(err)
	}

	// output: Hello, 世界
	// SAP HANA
	// Go driver
}
