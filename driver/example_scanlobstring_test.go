//go:build !unit

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// StringLob defines a string based data type for scanning Lobs.
type StringLob string

// Scan implements the database.sql.Scanner interface.
func (s *StringLob) Scan(arg any) error { return driver.ScanLobString(arg, (*string)(s)) }

// ExampleScanLobString demontrates how to read Lob data using a string based data type.
func ExampleScanLobString() {
	// Open Test database.
	db := sql.OpenDB(driver.DefaultTestConnector())
	defer db.Close()

	table := driver.RandomIdentifier("lob_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (n nclob)", table)); err != nil {
		log.Fatalf("create table failed: %s", err)
	}

	tx, err := db.Begin() // Start Transaction to avoid database error: SQL Error 596 - LOB streaming is not permitted in auto-commit mode.
	if err != nil {
		log.Fatal(err)
	}

	// Lob content can be written using a string.
	_, err = tx.ExecContext(context.Background(), fmt.Sprintf("insert into %s values (?)", table), "scan lob string")
	if err != nil {
		log.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	var arg StringLob
	if err := db.QueryRowContext(context.Background(), fmt.Sprintf("select * from %s", table)).Scan(&arg); err != nil {
		log.Fatal(err)
	}
	fmt.Println(arg)
	// output: scan lob string
}
