//go:build !unit

package driver_test

import (
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

type testScanRow struct {
	A string `sql:"S"`
	B int    `sql:"I"`
	C bool
	Y string
}

func (ts *testScanRow) Tag(fieldName string) (string, bool) {
	switch fieldName {
	case "Y":
		return `sql:"X"`, true
	default:
		return "", false
	}
}

func TestScanStruct(t *testing.T) {
	db := driver.MT.DB()

	testRow := testScanRow{A: "testRow", B: 42, C: true, Y: "I am a X"}

	tableName := driver.RandomIdentifier("scanStruct_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (s varchar(30), i integer, c boolean, x varchar(20))", tableName)); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(fmt.Sprintf("insert into %s values (?,?,?,?)", tableName), testRow.A, testRow.B, testRow.C, testRow.Y); err != nil {
		t.Fatal(err)
	}

	scanner, err := driver.NewStructScanner[testScanRow]()
	if err != nil {
		t.Fatal(err)
	}

	testScanStructRows := func() error {
		row := new(testScanRow)

		rows, err := db.Query(fmt.Sprintf("select * from %s", tableName))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			if err := scanner.Scan(rows, row); err != nil {
				return err
			}
			if *row != testRow {
				return fmt.Errorf("row %v not equal to %v", row, testRow)
			}
		}
		return rows.Err()
	}

	testScanStructRow := func() error {
		row := new(testScanRow)

		rows, err := db.Query(fmt.Sprintf("select * from %s", tableName))
		if err != nil {
			return err
		}

		if err := scanner.ScanRow(rows, row); err != nil {
			return err
		}
		if *row != testRow {
			return fmt.Errorf("row %v not equal to %v", row, testRow)
		}
		return nil
	}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"testScanStructRows", testScanStructRows},
		{"testScanStructRow", testScanStructRow},
	}

	for _, test := range tests {
		func(fn func() error) {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				if err := fn(); err != nil {
					t.Fatal(err)
				}
			})
		}(test.fn)
	}
}
