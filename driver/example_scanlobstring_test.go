//go:build !unit

package driver_test

import (
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
	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	table := driver.RandomIdentifier("lob_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (n1 nclob, n2 nclob)", table)); err != nil {
		log.Fatalf("create table failed: %s", err)
	}

	tx, err := db.Begin() // Start Transaction to avoid database error: SQL Error 596 - LOB streaming is not permitted in auto-commit mode.
	if err != nil {
		log.Fatal(err)
	}

	// Lob content can be written using a string.
	content := "scan lob string"
	_, err = tx.Exec(fmt.Sprintf("insert into %s values (?, ?)", table), content, content)
	if err != nil {
		log.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Select.
	stmt, err := db.Prepare(fmt.Sprintf("select * from %s", table))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// Scan into StringLob and sql.Null[StringLob].
	var s StringLob
	var ns sql.Null[StringLob]
	if err := stmt.QueryRow().Scan(&s, &ns); err != nil {
		log.Fatal(err)
	}
	fmt.Println(s)
	fmt.Println(ns.V)

	// output: scan lob string
	// scan lob string
}
