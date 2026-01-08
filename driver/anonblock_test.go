//go:build !unit

package driver_test

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

type testAnonBlockTable interface {
	id() driver.Identifier
	numRow() int
	scan(t *testing.T, rows *sql.Rows)
}

type testAnonBlockTable1 struct {
	_id     driver.Identifier
	_numRow int
}

func newTestAnonBlockTable1(t *testing.T, db *sql.DB) testAnonBlockTable {
	numRow := 10
	id := driver.RandomIdentifier("ab")

	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, j integer)", id)); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values (?, ?)", id))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// add some data
	args1 := make([]any, numRow*2)
	for i := range numRow {
		args1[i*2], args1[i*2+1] = i, i
	}
	if _, err := stmt.Exec(args1...); err != nil {
		log.Fatal(err)
	}

	return &testAnonBlockTable1{_id: id, _numRow: numRow}
}

func (tb *testAnonBlockTable1) id() driver.Identifier { return tb._id }
func (tb *testAnonBlockTable1) numRow() int           { return tb._numRow }

func (tb *testAnonBlockTable1) scan(t *testing.T, rows *sql.Rows) {
	var i, j int
	if err := rows.Scan(&i, &j); err != nil {
		t.Fatal(err)
	}
}

type testAnonBlockTable2 struct {
	_id     driver.Identifier
	_numRow int
}

func newTestAnonBlockTable2(t *testing.T, db *sql.DB) testAnonBlockTable {
	numRow := 20
	id := driver.RandomIdentifier("ab")

	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, f float)", id)); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values (?, ?)", id))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// add some data
	args2 := make([]any, numRow*2)
	for i := range numRow {
		args2[i*2], args2[i*2+1] = i, float64(i)
	}
	if _, err := stmt.Exec(args2...); err != nil {
		log.Fatal(err)
	}

	return &testAnonBlockTable2{_id: id, _numRow: numRow}
}

func (tb *testAnonBlockTable2) id() driver.Identifier { return tb._id }
func (tb *testAnonBlockTable2) numRow() int           { return tb._numRow }

func (tb *testAnonBlockTable2) scan(t *testing.T, rows *sql.Rows) {
	var i int
	var f float64
	if err := rows.Scan(&i, &f); err != nil {
		t.Fatal(err)
	}
}

func testAnonBlockSimple(t *testing.T, db *sql.DB, tables []testAnonBlockTable) {
	b := strings.Builder{}

	b.WriteString("DO BEGIN")
	b.WriteString("  BEGIN PARALLEL EXECUTION")
	for _, table := range tables {
		b.WriteString(fmt.Sprintf("    SELECT * FROM %s;", table.id().String()))
	}
	b.WriteString("  END;")
	b.WriteString("END;")

	rows, err := db.QueryContext(t.Context(), b.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for i, table := range tables {
		if i != 0 {
			if !rows.NextResultSet() {
				t.Fatal("additional result set expected")
			}
		}

		numRow := 0
		for rows.Next() {
			numRow++
			table.scan(t, rows)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if numRow != table.numRow() {
			t.Fatal(fmt.Errorf("number of rows %d - expected %d", numRow, table.numRow()))
		}
	}
	if rows.NextResultSet() {
		t.Fatal("no additional result set expected")
	}
}

func TestAnonBlock(t *testing.T) {
	t.Parallel()

	db := driver.MT.DB()

	tables := []testAnonBlockTable{
		newTestAnonBlockTable1(t, db),
		newTestAnonBlockTable2(t, db),
	}

	tests := []struct {
		name string
		fct  func(t *testing.T, db *sql.DB, tables []testAnonBlockTable)
	}{
		{"simple", testAnonBlockSimple},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t, db, tables)
		})
	}
}
