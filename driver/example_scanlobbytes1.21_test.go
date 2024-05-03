//go:build !unit && !go1.22

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

// NullBytesLob defines a []byte based null data type for scanning Lobs.
type NullBytesLob struct {
	V     BytesLob
	Valid bool
}

// Scan implements the database/sql/Scanner interface.
func (n *NullBytesLob) Scan(value any) error {
	if value == nil {
		n.V, n.Valid = []byte(nil), false
		return nil
	}
	n.Valid = true
	return n.V.Scan(value)
}

// ExampleScanLobBytes demontrates how to read Lob data using a []byte based data type.
func ExampleScanLobBytes() {
	// Open Test database.
	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	table := driver.RandomIdentifier("lob_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (b blob)", table)); err != nil {
		log.Panicf("create table failed: %s", err)
	}

	tx, err := db.Begin() // Start Transaction to avoid database error: SQL Error 596 - LOB streaming is not permitted in auto-commit mode.
	if err != nil {
		log.Panic(err)
	}

	// Lob content can be written using a byte slice.
	_, err = tx.ExecContext(context.Background(), fmt.Sprintf("insert into %s values (?)", table), []byte("scan lob bytes"))
	if err != nil {
		log.Panic(err)
	}

	if err := tx.Commit(); err != nil {
		log.Panic(err)
	}

	// Select.
	stmt, err := db.Prepare(fmt.Sprintf("select * from %s", table))
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	// Scan into BytesLob.
	var b BytesLob
	if err := stmt.QueryRow().Scan(&b); err != nil {
		log.Panic(err)
	}
	fmt.Println(string(b))

	// Scan into NullBytesLob.
	var nb NullBytesLob
	if err := stmt.QueryRow().Scan(&nb); err != nil {
		log.Panic(err)
	}
	fmt.Println(string(nb.V))

	// output: scan lob bytes
	// scan lob bytes
}
