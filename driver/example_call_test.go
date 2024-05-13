//go:build !unit

package driver_test

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

/*
ExampleCallSimpleOut creates a stored procedure with one output parameter and executes it.
*/
func Example_callSimpleOut() {
	const procedureOut = `create procedure %s (out message nvarchar(1024))
language SQLSCRIPT as
begin
    message := 'Hello World!';
end
`

	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	procedureName := driver.RandomIdentifier("procOut_")

	if _, err := db.Exec(fmt.Sprintf(procedureOut, procedureName)); err != nil { // Create stored procedure.
		log.Panic(err)
	}

	var out string

	if _, err := db.Exec(fmt.Sprintf("call %s(?)", procedureName), sql.Named("MESSAGE", sql.Out{Dest: &out})); err != nil {
		log.Panic(err)
	}

	fmt.Print(out)

	// output: Hello World!
}

/*
ExampleCallTableOut creates a stored procedure with one table output parameter and executes it
making use of sql.Rows scan parameters.
Stored procedures with table output parameters must be prepared by sql.Prepare as the statement needs to
be kept open until the output table values are retrieved.
*/
func Example_callTableOut() {
	const procedureTable = `create procedure %[1]s (out t %[2]s)
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
	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	tableType := driver.RandomIdentifier("TableType_")
	procedureName := driver.RandomIdentifier("ProcTable_")

	if _, err := db.Exec(fmt.Sprintf("create type %s as table (x nvarchar(256))", tableType)); err != nil { // Create table type.
		log.Panic(err)
	}

	if _, err := db.Exec(fmt.Sprintf(procedureTable, procedureName, tableType)); err != nil { // Create stored procedure.
		log.Panic(err)
	}

	var tableRows sql.Rows // Scan variable of table output parameter.

	// Call stored procedure via prepare.
	stmt, err := db.Prepare(fmt.Sprintf("call %s(?)", procedureName))
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(sql.Named("T", sql.Out{Dest: &tableRows})); err != nil {
		log.Panic(err)
	}

	for tableRows.Next() {
		var x string

		if err := tableRows.Scan(&x); err != nil {
			log.Panic(err)
		}

		fmt.Println(x)
	}
	if err := tableRows.Err(); err != nil {
		log.Panic(err)
	}

	// output: Hello, 世界
	// SAP HANA
	// Go driver
}

/*
ExampleCallTableIn creates a stored procedure with one table input and one table output parameter
and executes it making use of sql.Rows scan parameters.
Stored procedure input parameters need to refer by name to an existing database table or temporary table.
Stored procedures with table output parameters must be prepared by sql.Prepare as the statement needs to
be kept open until the output table values are retrieved.
*/
func Example_callTableIn() {
	const procedureTable = `create procedure %[1]s (in t1 %[2]s, out t2 %[2]s)
language SQLSCRIPT as
begin
  t2 = select * from :t1;
end
`
	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	tableType := driver.RandomIdentifier("TableType_")
	tableName := driver.RandomIdentifier("#TableIn_") // local temp table needs to start with "#"
	procedureName := driver.RandomIdentifier("ProcTable_")

	if _, err := db.Exec(fmt.Sprintf("create type %s as table (x nvarchar(256))", tableType)); err != nil { // Create table type.
		log.Panic(err)
	}

	if _, err := db.Exec(fmt.Sprintf(procedureTable, procedureName, tableType)); err != nil { // Create stored procedure.
		log.Panic(err)
	}

	if _, err := db.Exec(fmt.Sprintf("create local temporary table %s like %s", tableName, tableType)); err != nil {
		log.Panic(err)
	}

	if _, err := db.Exec(fmt.Sprintf("insert into %s values (?)", tableName), "Hello, 世界", "SAP HANA", "Go driver"); err != nil {
		log.Panic(err)
	}

	var tableRows sql.Rows // Scan variable of table output parameter.

	// Call stored procedure via prepare.
	stmt, err := db.Prepare(fmt.Sprintf("call %s(%s, ?)", procedureName, tableName))
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(sql.Named("T", sql.Out{Dest: &tableRows})); err != nil {
		log.Panic(err)
	}

	for tableRows.Next() {
		var x string

		if err := tableRows.Scan(&x); err != nil {
			log.Panic(err)
		}

		fmt.Println(x)
	}
	if err := tableRows.Err(); err != nil {
		log.Panic(err)
	}

	// output: Hello, 世界
	// SAP HANA
	// Go driver
}
