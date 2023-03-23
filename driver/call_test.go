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

func testCallEcho(db *sql.DB, t *testing.T) {
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

func testCallBlobEcho(db *sql.DB, t *testing.T) {
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

func testCallTableOut(db *sql.DB, t *testing.T) {
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
	type testData struct {
		i int
		x string
	}

	data := [][]testData{
		{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}},
		{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}, {5, "F"}, {6, "G"}, {7, "H"}, {8, "I"}, {9, "J"}},
		{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}, {5, "F"}, {6, "G"}, {7, "H"}, {8, "I"}, {9, "J"}, {10, "K"}, {11, "L"}, {12, "M"}, {13, "N"}, {14, "O"}},
	}

	check := func(data []testData, rows *sql.Rows) {
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

	tableType := driver.RandomIdentifier("tableType_")
	proc := driver.RandomIdentifier("procTableOut_")

	// use same connection
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal()
	}
	defer conn.Close()

	// create table type
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("create type %s as table (i integer, x varchar(10))", tableType)); err != nil {
		t.Fatal(err)
	}
	// create procedure
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(procTableOut, proc, tableType)); err != nil {
		t.Fatal(err)
	}

	var resultRows1, resultRows2, resultRows3 sql.Rows

	// need to prepare to keep statement open
	stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("call %s(?, ?, ?, ?)", proc))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(
		1,
		sql.Named("T1", sql.Out{Dest: &resultRows1}),
		sql.Named("T2", sql.Out{Dest: &resultRows2}),
		sql.Named("T3", sql.Out{Dest: &resultRows3}),
	); err != nil {
		t.Fatal(err)
	}

	check(data[0], &resultRows1)
	check(data[1], &resultRows2)
	check(data[2], &resultRows3)
}

func testCallNoPrm(db *sql.DB, t *testing.T) {
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

	if _, err := db.Exec(fmt.Sprintf("call %s", proc)); err != nil {
		t.Fatal(err)
	}
}

func testCallNoOut(db *sql.DB, t *testing.T) {
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
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal()
	}
	defer conn.Close()

	proc, table := createDBObjects(conn, ctx)

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("call %s(?)", proc), txt); err != nil {
		t.Fatal(err)
	}
	checkTable(conn, ctx, table)
}

func TestCall(t *testing.T) {
	tests := []struct {
		name string
		fct  func(db *sql.DB, t *testing.T)
	}{
		{"echo", testCallEcho},
		{"blobEcho", testCallBlobEcho},
		{"tableOut", testCallTableOut},
		{"noPrm", testCallNoPrm},
		{"noOut", testCallNoOut},
	}

	db := driver.DefaultTestDB()

	for i := range tests {
		func(i int) {
			t.Run(tests[i].name, func(t *testing.T) {
				t.Parallel() // run in parallel to speed up
				tests[i].fct(db, t)
			})
		}(i)
	}
}
