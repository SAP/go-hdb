//go:build !unit

package driver_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func testCallEcho(t *testing.T, db *sql.DB) {
	const procEcho = `create procedure %[1]s (in idata nvarchar(25), out odata nvarchar(25))
language SQLSCRIPT as
begin
    odata := idata;
end
`
	const txt = "Hello World!"
	var out string

	// create procedure
	proc := driver.RandomIdentifier("procEcho_")
	if _, err := db.Exec(fmt.Sprintf(procEcho, proc)); err != nil {
		t.Fatal(err)
	}

	testExecInvNamedPrm := func() { // exec - invalid names (lower instead of upper case)
		if _, err := db.Exec(fmt.Sprintf("call %s(?, ?)", proc), sql.Named("idata", txt), sql.Named("odata", sql.Out{Dest: &out})); err != nil {
			t.Log(err)
		} else {
			t.Fatal("should return invalid argument name error")
		}
	}

	testExec := func() { // exec
		if _, err := db.Exec(fmt.Sprintf("call %s(?, ?)", proc), sql.Named("IDATA", txt), sql.Named("ODATA", sql.Out{Dest: &out})); err != nil {
			t.Fatal(err)
		}
		if out != txt {
			t.Fatalf("value %s - expected %s", out, txt)
		}
	}

	testExecRndPrms := func() { // exec random parameters - switch input / output argument (test named parameters)
		if _, err := db.Exec(fmt.Sprintf("call %s(?, ?)", proc), sql.Named("ODATA", sql.Out{Dest: &out}), sql.Named("IDATA", txt)); err != nil {
			t.Fatal(err)
		}
		if out != txt {
			t.Fatalf("value %s - expected %s", out, txt)
		}
	}

	tests := []struct {
		name string
		fct  func()
	}{
		{"ExecInvNamedPrm", testExecInvNamedPrm},
		{"Exec", testExec},
		{"ExecRndPrms", testExecRndPrms},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct()
		})
	}
}

func testCallBlobEcho(t *testing.T, db *sql.DB) {
	const procBlobEcho = `create procedure %[1]s (in idata nclob, out odata nclob)
language SQLSCRIPT as
begin
  odata := idata;
end
`
	const txt = "Hello World - 𝄞𝄞€€!"

	newLob := func() (*driver.Lob, *bytes.Buffer) {
		b := new(bytes.Buffer)
		lob := driver.NewLob(bytes.NewReader([]byte(txt)), b)
		return lob, b
	}

	proc := driver.RandomIdentifier("procBlobEcho_")
	if _, err := db.Exec(fmt.Sprintf(procBlobEcho, proc)); err != nil {
		t.Fatal(err)
	}

	lob, b := newLob()
	if _, err := db.Exec(fmt.Sprintf("call %s(?, ?)", proc), sql.Named("IDATA", lob), sql.Named("ODATA", sql.Out{Dest: lob})); err != nil {
		t.Fatal(err)
	}
	if b.String() != txt {
		t.Fatalf("value %s - expected %s", b.String(), txt)
	}
}

func testCallTable(t *testing.T, db *sql.DB) {

	type testDataType struct {
		i int
		x string
	}

	checkData := func(data []testDataType, rows *sql.Rows) {
		j := 0
		for rows.Next() {

			var i int
			var x string

			if err := rows.Scan(&i, &x); err != nil {
				log.Fatal(err)
			}

			// log.Printf("i %d x %s", i, x)
			if i != data[j].i {
				t.Fatalf("value i %d - expected %d", i, data[j].i)
			}
			if x != data[j].x {
				t.Fatalf("value x %s - expected %s", x, data[j].x)
			}
			j++
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
		if j != len(data) {
			t.Fatalf("invalid number of records %d - expected %d", j, len(data))
		}
	}

	testCallTableOut := func() {
		const procTableOut = `create procedure %[1]s (in i integer, out t1 %[2]s, out t2 %[2]s, out t3 %[2]s)
	language SQLSCRIPT as
	begin
	  create local temporary table #test like %[2]s;
	  insert into #test values(0, 'A');
	  insert into #test values(1, 'B');
	  insert into #test values(2, 'C');
	  insert into #test values(3, 'D');
	  insert into #test values(4, 'E');
	  t1 = select * from #test;
	  insert into #test values(5, 'F');
	  insert into #test values(6, 'G');
	  insert into #test values(7, 'H');
	  insert into #test values(8, 'I');
	  insert into #test values(9, 'J');
	  t2 = select * from #test;
	  insert into #test values(10, 'K');
	  insert into #test values(11, 'L');
	  insert into #test values(12, 'M');
	  insert into #test values(13, 'N');
	  insert into #test values(14, 'O');
	  t3 = select * from #test;
	  drop table #test;
	end
	`
		testData := [][]testDataType{
			{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}},
			{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}, {5, "F"}, {6, "G"}, {7, "H"}, {8, "I"}, {9, "J"}},
			{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}, {5, "F"}, {6, "G"}, {7, "H"}, {8, "I"}, {9, "J"}, {10, "K"}, {11, "L"}, {12, "M"}, {13, "N"}, {14, "O"}},
		}

		tableType := driver.RandomIdentifier("tableTypeOut_")
		proc := driver.RandomIdentifier("procTableOut_")

		// use same connection
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal()
		}
		defer conn.Close()

		// create table type
		if _, err := conn.ExecContext(t.Context(), fmt.Sprintf("create type %s as table (i integer, x varchar(10))", tableType)); err != nil {
			t.Fatal(err)
		}
		// create procedure
		if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(procTableOut, proc, tableType)); err != nil {
			t.Fatal(err)
		}

		var resultRows1, resultRows2, resultRows3 sql.Rows

		// need to prepare to keep statement open
		stmt, err := conn.PrepareContext(t.Context(), fmt.Sprintf("call %s(?, ?, ?, ?)", proc))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()

		if _, err := stmt.ExecContext(
			t.Context(),
			1,
			sql.Named("T1", sql.Out{Dest: &resultRows1}),
			sql.Named("T2", sql.Out{Dest: &resultRows2}),
			sql.Named("T3", sql.Out{Dest: &resultRows3}),
		); err != nil {
			t.Fatal(err)
		}

		checkData(testData[0], &resultRows1)
		checkData(testData[1], &resultRows2)
		checkData(testData[2], &resultRows3)
	}

	/*
		Input table parameters can only be used referring to existent tables (inclusive temporary tables).
		'Uploadiing' table content while executing a stored procedure is not possible.
		see:
		https://stackoverflow.com/questions/45830478/call-stored-procedure-passing-table-type-argument
		https://stackoverflow.com/questions/60657309/error-while-calling-hana-stored-procedure-from-python-sqlalchemy
	*/
	testCallTableIn := func() {
		const procTableIn = `create procedure %[1]s (in i integer, in t1 %[2]s, out t2 %[2]s)
	language SQLSCRIPT as
	begin
	  t2 = select * from :t1;
	end
	`

		testData := []testDataType{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}}

		// use same connections
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal()
		}
		defer conn.Close()

		tableType := driver.RandomIdentifier("tableTypeIn_")
		tableName := driver.RandomIdentifier("#tableIn_") // local temp table needs to start with "#"
		proc := driver.RandomIdentifier("procTableIn_")

		// create table type
		if _, err := conn.ExecContext(t.Context(), fmt.Sprintf("create type %s as table (i integer, x varchar(10))", tableType)); err != nil {
			t.Fatal(err)
		}
		// create procedure
		if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(procTableIn, proc, tableType)); err != nil {
			t.Fatal(err)
		}
		// create temporary table
		if _, err := conn.ExecContext(t.Context(), fmt.Sprintf("create local temporary table %s like %s", tableName, tableType)); err != nil {
			t.Fatal(err)
		}

		// insert test data into temp table
		j := 0
		if _, err := conn.ExecContext(t.Context(), fmt.Sprintf("insert into %s values (?, ?)", tableName), func(args []any) error {
			if j >= len(testData) {
				return driver.ErrEndOfRows
			}
			args[0], args[1] = testData[j].i, testData[j].x
			j++
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		var resultRows2 sql.Rows

		stmt, err := conn.PrepareContext(t.Context(), fmt.Sprintf("call %s(?, %s, ?)", proc, tableName))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()

		if _, err := stmt.ExecContext(t.Context(), 1, sql.Named("T2", sql.Out{Dest: &resultRows2})); err != nil {
			t.Fatal(err)
		}

		checkData(testData, &resultRows2)

	}

	tests := []struct {
		name string
		fct  func()
	}{
		{"tableOut", testCallTableOut},
		{"tableIn", testCallTableIn},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct()
		})
	}
}

func testCallNoPrm(t *testing.T, db *sql.DB) {
	const procNoPrm = `create procedure %[1]s
language SQLSCRIPT as
begin
end
`
	// create procedure
	proc := driver.RandomIdentifier("procNoPrm_")
	if _, err := db.Exec(fmt.Sprintf(procNoPrm, proc)); err != nil {
		t.Fatal(err)
	}

	// call by execute
	if _, err := db.Exec(fmt.Sprintf("call %s", proc)); err != nil {
		t.Fatal(err)
	}

	// call by query (allow for calls witout parameters)
	rows, err := db.Query(fmt.Sprintf("call %s", proc))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
}

func testCallNoOut(t *testing.T, db *sql.DB) {
	const procNoOut = `create procedure %[1]s (in idata nvarchar(25))
language SQLSCRIPT as
begin
	 insert into %[2]s values(idata);
end
`
	const txt = "Hello World!"

	createDBObjects := func(conn *sql.Conn, ctx context.Context) (driver.Identifier, driver.Identifier) {
		// create table (stored procedure 'side effect')
		table := driver.RandomIdentifier("tableNoOut_")
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("create column table %s (x nvarchar(25))", table)); err != nil {
			t.Fatal(err)
		}
		// create procedure
		proc := driver.RandomIdentifier("procNoOut_")
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(procNoOut, proc, table)); err != nil {
			t.Fatal(err)
		}
		return proc, table
	}

	checkTable := func(conn *sql.Conn, ctx context.Context, table driver.Identifier) {
		var out string
		if err := conn.QueryRowContext(ctx, fmt.Sprintf("select * from %s", table)).Scan(&out); err != nil {
			t.Fatal(err)
		}
		if out != txt {
			t.Fatalf("value %s - expected %s", out, txt)
		}
	}

	// use same connection
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal()
	}
	defer conn.Close()

	proc, table := createDBObjects(conn, t.Context())

	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf("call %s(?)", proc), txt); err != nil {
		t.Fatal(err)
	}
	checkTable(conn, t.Context(), table)
}

func testCallTableOutWithoutArg1(t *testing.T, db *sql.DB) {
	const procWithoutArg = `create procedure %[1]s (in val integer)
language SQLSCRIPT as
begin
	select val from dummy;
end
`
	// use same connection
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal()
	}
	defer conn.Close()

	// create procedure
	proc := driver.RandomIdentifier("procTableOutWithoutArg1_")
	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(procWithoutArg, proc)); err != nil {
		t.Fatal(err)
	}

	var i = 42

	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf("call %s(?)", proc), i); err != nil {
		t.Fatal(err)
	}
}

func testCallTableOutWithoutArg2(t *testing.T, db *sql.DB) {
	const procWithoutArg = `create procedure %[1]s (in input1 integer, in input2 integer, out output1 integer, out output2 integer, out output3 varchar(50))
language SQLSCRIPT as
begin
	output1 := input1;
	output2 := input2;
	output3 := 'not used';
	select 1 from dummy;
end
`
	// use same connection
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal()
	}
	defer conn.Close()

	// create procedure
	proc := driver.RandomIdentifier("procTableOutWithoutArg2_")
	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(procWithoutArg, proc)); err != nil {
		t.Fatal(err)
	}

	var in1, in2 = 42, 43
	var out1, out2 int
	var out3 string

	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf("call %s(?,?,?,?,?)", proc),
		in1,
		in2,
		sql.Named("OUTPUT1", sql.Out{Dest: &out1}),
		sql.Named("OUTPUT2", sql.Out{Dest: &out2}),
		sql.Named("OUTPUT3", sql.Out{Dest: &out3}),
	); err != nil {
		t.Fatal(err)
	}

	const notUsed = "not used"

	if out1 != in1 {
		t.Fatalf("invalid out1 value %d - expected %d", out1, in1)
	}
	if out2 != in2 {
		t.Fatalf("invalid out2 value %d - expected %d", out2, in2)
	}
	if out3 != notUsed {
		t.Fatalf("invalid out3 value %s - expected %s", out3, notUsed)
	}
}

func TestCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fct  func(t *testing.T, db *sql.DB)
	}{
		{"echo", testCallEcho},
		{"blobEcho", testCallBlobEcho},
		{"table", testCallTable},
		{"noPrm", testCallNoPrm},
		{"noOut", testCallNoOut},
		{"tableOutWithoutArg1", testCallTableOutWithoutArg1},
		{"tableOutWithoutArg2", testCallTableOutWithoutArg2},
	}

	db := driver.MT.DB()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t, db)
		})
	}
}
