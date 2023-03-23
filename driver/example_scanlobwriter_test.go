//go:build !unit

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// WriterLob defines a io.Writer based data type for scanning Lobs.
type WriterLob []byte

// Write implements the io.Writer interface.
func (b *WriterLob) Write(p []byte) (n int, err error) {
	*b = append(*b, p...)
	return len(p), nil
}

// Scan implements the database.sql.Scanner interface.
func (b *WriterLob) Scan(arg any) error { return driver.ScanLobWriter(arg, b) }

// ExampleScanLobWriter demontrates how to read Lob data using a io.Writer based data type.
func ExampleScanLobWriter() {
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
	_, err = tx.ExecContext(context.Background(), fmt.Sprintf("insert into %s values (?)", table), "scan lob writer")
	if err != nil {
		log.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	var arg WriterLob
	if err := db.QueryRowContext(context.Background(), fmt.Sprintf("select * from %s", table)).Scan(&arg); err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(arg))
	// output: scan lob writer
}
