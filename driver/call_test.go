// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"testing"
)

func testCallEchoQueryRow(db *sql.DB, proc Identifier, t *testing.T) {
	const txt = "Hello World!"

	var out string

	if err := db.QueryRow(fmt.Sprintf("call %s(?, ?)", proc), txt).Scan(&out); err != nil {
		t.Fatal(err)
	}

	if out != txt {
		t.Fatalf("value %s - expected %s", out, txt)
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

	proc := RandomIdentifier("procBlobEcho_")

	if _, err := db.Exec(fmt.Sprintf(procBlobEcho, proc)); err != nil {
		t.Fatal(err)
	}

	inlob := new(Lob)
	inlob.SetReader(bytes.NewReader([]byte(txt)))

	b := new(bytes.Buffer)
	outlob := new(Lob)
	outlob.SetWriter(b)

	if err := db.QueryRow(fmt.Sprintf("call %s(?, ?)", proc), inlob).Scan(outlob); err != nil {
		t.Fatal(err)
	}

	out := b.String()

	if out != txt {
		t.Fatalf("value %s - expected %s", out, txt)
	}
}

func testCallEcho(db *sql.DB, t *testing.T) {
	const procEcho = `create procedure %[1]s (in idata nvarchar(25), out odata nvarchar(25))
language SQLSCRIPT as
begin
    odata := idata;
end
`
	// create procedure
	proc := RandomIdentifier("procEcho_")
	if _, err := db.Exec(fmt.Sprintf(procEcho, proc)); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		fct  func(db *sql.DB, proc Identifier, t *testing.T)
	}{
		{"QueryRow", testCallEchoQueryRow},
		//		{"Query", testCallEchoQuery},
		//		{"Exec", testCallEchoExec},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(TestDB, proc, t)
		})
	}
}

func testCallTableOut(db *sql.DB, t *testing.T) {
	const procTableOut = `create procedure %[1]s.%[2]s (in i integer, out t1 %[1]s.%[3]s, out t2 %[1]s.%[3]s, out t3 %[1]s.%[3]s)
language SQLSCRIPT as
begin
  create local temporary table #test like %[1]s.%[3]s;
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

	createObj := func(t reflect.Type) interface{} { return reflect.New(t).Interface() }

	createString := func() interface{} { return createObj(stringType) }
	createRows := func() interface{} { return createObj(rowsType) }

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

	testCall := func(db *sql.DB, proc Identifier, legacy bool, targets []interface{}, t *testing.T) {
		rows, err := db.Query(fmt.Sprintf("call %s.%s(?, ?, ?, ?)", TestSchema, proc), 1)
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

	tableType := RandomIdentifier("tt2_")
	proc := RandomIdentifier("procTableOut_")

	// create table type
	if _, err := db.Exec(fmt.Sprintf("create type %s.%s as table (i integer, x varchar(10))", TestSchema, tableType)); err != nil {
		t.Fatal(err)
	}
	// create procedure
	if _, err := db.Exec(fmt.Sprintf(procTableOut, TestSchema, proc, tableType)); err != nil {
		t.Fatal(err)
	}

	connector, err := NewDSNConnector(TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	connector.SetDefaultSchema(TestSchema)

	tests := []struct {
		name    string
		legacy  bool
		fct     func(db *sql.DB, proc Identifier, legacy bool, targets []interface{}, t *testing.T)
		targets []interface{}
	}{
		{"tableOutRef", true, testCall, []interface{}{createString(), createString(), createString()}},
		{"tableOutRows", false, testCall, []interface{}{createRows(), createRows(), createRows()}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connector.SetLegacy(test.legacy)
			db := sql.OpenDB(connector)
			defer db.Close()
			test.fct(db, proc, test.legacy, test.targets, t)
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(TestDB, t)
		})
	}
}
