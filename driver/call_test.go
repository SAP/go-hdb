//go:build !unit
// +build !unit

package driver_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
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

	testQueryRow := func(db *sql.DB, proc driver.Identifier, t *testing.T) {
		const txt = "Hello World!"

		var out string

		if err := db.QueryRow(fmt.Sprintf("call %s(?, ?)", proc), txt).Scan(&out); err != nil {
			t.Fatal(err)
		}

		if out != txt {
			t.Fatalf("value %s - expected %s", out, txt)
		}
	}

	// create procedure
	proc := driver.RandomIdentifier("procEcho_")
	if _, err := db.Exec(fmt.Sprintf(procEcho, proc)); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		fct  func(db *sql.DB, proc driver.Identifier, t *testing.T)
	}{
		{"QueryRow", testQueryRow},
		// {"Query", testQuery}, // TODO: 'call' query
		// {"Exec", testExec},   // TODO: output parameter
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(db, proc, t)
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

	proc := driver.RandomIdentifier("procBlobEcho_")

	if _, err := db.Exec(fmt.Sprintf(procBlobEcho, proc)); err != nil {
		t.Fatal(err)
	}

	inlob := new(driver.Lob)
	inlob.SetReader(bytes.NewReader([]byte(txt)))

	b := new(bytes.Buffer)
	outlob := new(driver.Lob)
	outlob.SetWriter(b)

	if err := db.QueryRow(fmt.Sprintf("call %s(?, ?)", proc), inlob).Scan(outlob); err != nil {
		t.Fatal(err)
	}

	out := b.String()

	if out != txt {
		t.Fatalf("value %s - expected %s", out, txt)
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
	testData := [][]struct {
		i int
		x string
	}{
		{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}},
		{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}, {5, "F"}, {6, "G"}, {7, "H"}, {8, "I"}, {9, "J"}},
		{{0, "A"}, {1, "B"}, {2, "C"}, {3, "D"}, {4, "E"}, {5, "F"}, {6, "G"}, {7, "H"}, {8, "I"}, {9, "J"}, {10, "K"}, {11, "L"}, {12, "M"}, {13, "N"}, {14, "O"}},
	}

	stringType := reflect.TypeOf((*string)(nil)).Elem()
	rowsType := reflect.TypeOf((*sql.Rows)(nil)).Elem()

	createObj := func(t reflect.Type) any { return reflect.New(t).Interface() }

	createString := func() any { return createObj(stringType) }
	createRows := func() any { return createObj(rowsType) }

	testCheck := func(testSet int, rows *sql.Rows, t *testing.T) {
		j := 0
		for rows.Next() {

			var i int
			var x string

			if err := rows.Scan(&i, &x); err != nil {
				log.Fatal(err)
			}

			// log.Printf("i %d x %s", i, x)
			if i != testData[testSet][j].i {
				t.Fatalf("value i %d - expected %d", i, testData[testSet][j].i)
			}
			if x != testData[testSet][j].x {
				t.Fatalf("value x %s - expected %s", x, testData[testSet][j].x)
			}
			j++
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
	}

	testCall := func(db *sql.DB, proc driver.Identifier, legacy bool, targets []any, t *testing.T) {
		rows, err := db.Query(fmt.Sprintf("call %s(?, ?, ?, ?)", proc), 1)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if !rows.Next() {
			log.Fatal(rows.Err())
		}

		if err := rows.Scan(targets...); err != nil {
			log.Fatal(err)
		}

		for i, target := range targets {
			if legacy { // read table parameter by separate query
				rows, err := db.Query(*target.(*string))
				if err != nil {
					t.Fatal(err)
				}
				testCheck(i, rows, t)
				rows.Close()
			} else { // use rows directly
				testCheck(i, target.(*sql.Rows), t)
			}
		}
	}

	tableType := driver.RandomIdentifier("tt2_")
	proc := driver.RandomIdentifier("procTableOut_")

	// create table type
	if _, err := db.Exec(fmt.Sprintf("create type %s as table (i integer, x varchar(10))", tableType)); err != nil {
		t.Fatal(err)
	}
	// create procedure
	if _, err := db.Exec(fmt.Sprintf(procTableOut, proc, tableType)); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		legacy  bool
		fct     func(db *sql.DB, proc driver.Identifier, legacy bool, targets []any, t *testing.T)
		targets []any
	}{
		{"tableOutRef", true, testCall, []any{createString(), createString(), createString()}},
		{"tableOutRows", false, testCall, []any{createRows(), createRows(), createRows()}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connector := driver.NewTestConnector()
			connector.SetLegacy(test.legacy)
			db := sql.OpenDB(connector)
			defer db.Close()
			test.fct(db, proc, test.legacy, test.targets, t)
		})
	}
}

func testCallNoPrm(db *sql.DB, t *testing.T) {
	const procNoPrm = `create procedure %[1]s
language SQLSCRIPT as
begin
end
`

	testQueryRow := func(db *sql.DB, proc driver.Identifier, t *testing.T) {
		var out string
		// as the procedure does not have out parameters a try to scan any value should return the right sql error: sql.ErrNoRows.
		if err := db.QueryRow(fmt.Sprintf("call %s", proc)).Scan(&out); err != sql.ErrNoRows {
			t.Fatalf("error %s - expected %s", err, sql.ErrNoRows)
		}
	}

	testQuery := func(db *sql.DB, proc driver.Identifier, t *testing.T) {
		rows, err := db.Query(fmt.Sprintf("call %s", proc))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if rows.Next() { // shouldn't return any row
			log.Fatal("number of rows needs to be 0")
		}
	}

	testExec := func(db *sql.DB, proc driver.Identifier, t *testing.T) {
		if _, err := db.Exec(fmt.Sprintf("call %s", proc)); err != nil {
			t.Fatal(err)
		}
	}

	// create procedure
	proc := driver.RandomIdentifier("procNoPrm_")
	if _, err := db.Exec(fmt.Sprintf(procNoPrm, proc)); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		fct  func(db *sql.DB, proc driver.Identifier, t *testing.T)
	}{
		{"QueryRow", testQueryRow},
		{"Query", testQuery},
		{"Exec", testExec},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(db, proc, t)
		})
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

	createDBObjects := func() (driver.Identifier, driver.Identifier) {
		// create table (stored procedure 'side effect')
		table := driver.RandomIdentifier("tableNoOut_")
		if _, err := db.Exec(fmt.Sprintf("create column table %s (x nvarchar(25))", table)); err != nil {
			t.Fatal(err)
		}
		// create procedure
		proc := driver.RandomIdentifier("procNoOut_")
		if _, err := db.Exec(fmt.Sprintf(procNoOut, proc, table)); err != nil {
			t.Fatal(err)
		}
		return proc, table
	}

	checkTable := func(table driver.Identifier) {
		var out string
		if err := db.QueryRow(fmt.Sprintf("select * from %s", table)).Scan(&out); err != nil {
			t.Fatal(err)
		}
		if out != txt {
			t.Fatalf("value %s - expected %s", out, txt)
		}
	}

	testQueryRow := func(db *sql.DB, t *testing.T) {
		proc, table := createDBObjects()

		var out string
		// as the procedure does not have out parameters a try to scan any value should return the right sql error: sql.ErrNoRows.
		if err := db.QueryRow(fmt.Sprintf("call %s(?)", proc), txt).Scan(&out); err != sql.ErrNoRows {
			t.Fatalf("error %s - expected %s", err, sql.ErrNoRows)
		}

		checkTable(table)
	}

	testQuery := func(db *sql.DB, t *testing.T) {
		proc, table := createDBObjects()

		rows, err := db.Query(fmt.Sprintf("call %s(?)", proc), txt)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if rows.Next() { // shouldn't return any row
			log.Fatal("number of rows needs to be 0")
		}

		checkTable(table)
	}

	testExec := func(db *sql.DB, t *testing.T) {
		proc, table := createDBObjects()

		if _, err := db.Exec(fmt.Sprintf("call %s(?)", proc), txt); err != nil {
			t.Fatal(err)
		}

		checkTable(table)
	}

	tests := []struct {
		name string
		fct  func(db *sql.DB, t *testing.T)
	}{
		{"QueryRow", testQueryRow},
		{"Query", testQuery},
		{"Exec", testExec},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(db, t)
		})
	}
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

	db := sql.OpenDB(driver.NewTestConnector())
	defer db.Close()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(db, t)
		})
	}
}
