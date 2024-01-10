//go:build !unit

package driver

import (
	"fmt"
	"testing"
)

type testScanRow struct {
	A string `sql:"s,varchar(30)"`
	B int    `sql:"i,integer"`
	C bool
	Y string
}

func (ts *testScanRow) Tag(fieldName string) (string, bool) {
	switch fieldName {
	case "Y":
		return `sql:"x,varchar(30)"`, true
	default:
		return "", false
	}
}

func TestScanStruct(t *testing.T) {
	t.Parallel()

	db := MT.DB()

	testRow := testScanRow{A: "testRow", B: 42, C: true, Y: "I am a X"}

	scanner, err := NewStructScanner[testScanRow]()
	if err != nil {
		t.Fatal(err)
	}

	tableName := RandomIdentifier("scanStruct_")
	columnDefs, err := scanner.columnDefs()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("create table %s %s", tableName, columnDefs)); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(fmt.Sprintf("insert into %s values %s", tableName, scanner.queryPlaceholders()), testRow.A, testRow.B, testRow.C, testRow.Y); err != nil {
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
		test := test // new test to run in parallel

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.fn(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
