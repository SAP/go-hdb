//go:build !unit && !go1.22

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

// NullWriterLob defines a writer based null data type for scanning Lobs.
type NullWriterLob struct {
	V     WriterLob
	Valid bool
}

// Scan implements the database/sql/Scanner interface.
func (n *NullWriterLob) Scan(value any) error {
	if value == nil {
		n.V, n.Valid = WriterLob(nil), false
		return nil
	}
	n.Valid = true
	return n.V.Scan(value)
}

// ExampleScanLobWriter demontrates how to read Lob data using a io.Writer based data type.
func ExampleScanLobWriter() {
	// Open Test database.
	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	table := driver.RandomIdentifier("lob_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (n nclob)", table)); err != nil {
		log.Panicf("create table failed: %s", err)
	}

	tx, err := db.Begin() // Start Transaction to avoid database error: SQL Error 596 - LOB streaming is not permitted in auto-commit mode.
	if err != nil {
		log.Panic(err)
	}

	// Lob content can be written using a string.
	_, err = tx.ExecContext(context.Background(), fmt.Sprintf("insert into %s values (?)", table), "scan lob writer")
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

	// Scan into WriterLob.
	var w WriterLob
	if err := stmt.QueryRow().Scan(&w); err != nil {
		log.Panic(err)
	}
	fmt.Println(string(w))

	// Scan into NullWriterLob.
	var nw NullWriterLob
	if err := stmt.QueryRow().Scan(&nw); err != nil {
		log.Panic(err)
	}
	fmt.Println(string(nw.V))

	// output: scan lob writer
	// scan lob writer
}
