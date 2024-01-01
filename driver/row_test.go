//go:build !unit

package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/internal/row"
)

func testScanStruct(t *testing.T, db *sql.DB) {
	type ts struct {
		A string `sql:"S"`
		B int    `sql:"I"`
		C bool
	}

	s := new(ts)

	tableName := driver.RandomIdentifier("scanStruct_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (s varchar(30), i integer, c boolean)", tableName)); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(fmt.Sprintf("insert into %s values (?,?,?)", tableName), "test", 1, true); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	scanner, err := row.NewStructScanner[ts](rows)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		if err := scanner.Scan(s); err != nil {
			t.Fatal(err)
		}
	}
	if rows.Err() != nil {
		t.Fatal(err)
	}
}

func TestRow(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, db *sql.DB)
	}{
		{"testScanStruct", testScanStruct},
	}

	db := driver.MT.DB()
	for _, test := range tests {
		func(fn func(t *testing.T, db *sql.DB)) {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				fn(t, db)
			})
		}(test.fn)
	}
}
