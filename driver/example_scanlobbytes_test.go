//go:build !unit

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// BytesLob defines a []byte based data type for scanning Lobs.
type BytesLob []byte

// Scan implements the database.sql.Scanner interface.
func (b *BytesLob) Scan(arg any) error { return driver.ScanLobBytes(arg, (*[]byte)(b)) }

// ExampleScanLobBytes demontrates how to read Lob data using a []byte based data type.
func ExampleScanLobBytes() {
	// Open Test database.
	db := sql.OpenDB(driver.DefaultTestConnector())
	defer db.Close()

	table := driver.RandomIdentifier("lob_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (b blob)", table)); err != nil {
		log.Fatalf("create table failed: %s", err)
	}

	tx, err := db.Begin() // Start Transaction to avoid database error: SQL Error 596 - LOB streaming is not permitted in auto-commit mode.
	if err != nil {
		log.Fatal(err)
	}

	// Lob content can be written using a byte slice.
	_, err = tx.ExecContext(context.Background(), fmt.Sprintf("insert into %s values (?)", table), []byte("scan lob bytes"))
	if err != nil {
		log.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	var arg BytesLob
	if err := db.QueryRowContext(context.Background(), fmt.Sprintf("select * from %s", table)).Scan(&arg); err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(arg))
	// output: scan lob bytes
}
