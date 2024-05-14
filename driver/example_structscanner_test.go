//go:build !unit

package driver_test

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// ExampleScanRow is used to showcase struct scanning.
type ExampleScanRow struct {
	Afield string `sql:"A"` // database field name is "A"
	Bfield int    `sql:"B"` // database field name is "B"
	C      bool   // database field name is "C"
	AsD    string // database field name is "D"
}

// Tag implements the Tagger interface to define tags for ExampleScanRow dynamically.
func (s *ExampleScanRow) Tag(fieldName string) (string, bool) {
	switch fieldName {
	case "AsD":
		return `sql:"D"`, true
	default:
		return "", false
	}
}

// ExampleStructScanner demonstrates how to read database rows into a go struct.
func ExampleStructScanner() {
	// Open Test database.
	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	table := driver.RandomIdentifier("structscanner_")

	// Create table.
	if _, err := db.Exec(fmt.Sprintf("create table %s (a varchar(30), b integer, c boolean, d varchar(20))", table)); err != nil {
		log.Fatal(err)
	}

	// Insert test row data.
	if _, err := db.Exec(fmt.Sprintf("insert into %s values (?,?,?,?)", table), "test", 42, true, "I am D"); err != nil {
		log.Fatal(err)
	}

	// Create scanner.
	scanner, err := driver.NewStructScanner[ExampleScanRow]()
	if err != nil {
		log.Fatal(err)
	}

	// Scan target.
	row := new(ExampleScanRow)

	// Scan rows with the help of the struct scanner.
	if err = func() error {
		rows, err := db.Query(fmt.Sprintf("select * from %s", table))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			if err := scanner.Scan(rows, row); err != nil {
				return err
			}
		}
		if rows.Err() != nil {
			return err
		}
		return rows.Close()
	}(); err != nil {
		log.Fatal(err)
	}

	// Scan a single row with the help of the struct scanner.
	if err = func() error {
		rows, err := db.Query(fmt.Sprintf("select * from %s", table))
		if err != nil {
			return err
		}
		// Rows will be closed by scanner.ScanRow.
		return scanner.ScanRow(rows, row)
	}(); err != nil {
		log.Fatal(err)
	}

	// output:
}
