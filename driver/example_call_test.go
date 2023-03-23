//go:build !unit

package driver

import (
	"database/sql"
	"fmt"
	"log"
)

/*
ExampleCallSimpleOut creates a stored procedure with one output parameter and executes it.
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

	if _, err := db.Exec(fmt.Sprintf("call %s(?)", procedure), sql.Named("MESSAGE", sql.Out{Dest: &out})); err != nil {
		log.Fatal(err)
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

	// Call stored procedure via prepare.
	stmt, err := db.Prepare(fmt.Sprintf("call %s(?)", procedure))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(sql.Named("T", sql.Out{Dest: &tableRows})); err != nil {
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
