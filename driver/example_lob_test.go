//go:build !unit

package driver_test

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/SAP/go-hdb/driver"
)

// ExampleLobRead reads data from a largs data object database field into a bytes.Buffer.
// Precondition: the test database table with one field of type BLOB, CLOB or NCLOB must exist.
// For illustrative purposes we assume, that the database table has exactly one record, so
// that we can use db.QueryRow.
func ExampleLob_read() {
	b := new(bytes.Buffer)

	db, err := sql.Open("hdb", "hdb://user:password@host:port")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	lob := new(driver.Lob)
	lob.SetWriter(b) // SetWriter sets the io.Writer object, to which the database content of the lob field is written.

	if err := db.QueryRow("select * from test").Scan(lob); err != nil {
		log.Fatal(err)
	}
}

// ExampleLobWrite inserts data read from a file into a database large object field.
// Precondition: the test database table with one field of type BLOB, CLOB or NCLOB and the
// test.txt file in the working directory must exist.
// Lob fields cannot be written in hdb auto commit mode - therefore the insert has to be
// executed within a transaction.
func ExampleLob_write() {
	file, err := os.Open("test.txt") // Open file.
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	db, err := sql.Open("hdb", "hdb://user:password@host:port")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin() // Start Transaction to avoid database error: SQL Error 596 - LOB streaming is not permitted in auto-commit mode.
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare("insert into test values(?)")
	if err != nil {
		log.Fatal(err)
	}

	lob := new(driver.Lob)
	lob.SetReader(file) // SetReader sets the io.Reader object, which content is written to the database lob field.

	if _, err := stmt.Exec(lob); err != nil {
		log.Fatal(err)
	}

	stmt.Close()

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

/*
ExampleLobPipe:
- inserts data read from a file into a database large object field
- and retrieves the data afterwards
An io.Pipe is used to insert and retrieve Lob data in chunks.
*/
func ExampleLob_pipe() {
	// Open test file.
	file, err := os.Open("example_lob_test.go")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Open Test database.
	db := sql.OpenDB(driver.DefaultTestConnector())
	defer db.Close()

	tx, err := db.Begin() // Start Transaction to avoid database error: SQL Error 596 - LOB streaming is not permitted in auto-commit mode.
	if err != nil {
		log.Fatal(err)
	}

	// Create table.
	table := driver.RandomIdentifier("fileLob")
	if _, err := tx.Exec(fmt.Sprintf("create table %s (file nclob)", table)); err != nil {
		log.Fatalf("create table failed: %s", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values (?)", table))
	if err != nil {
		log.Fatal(err)
	}

	lob := &driver.Lob{} // Lob field.

	pipeReader, pipeWriter := io.Pipe() // Create pipe for writing Lob.
	lob.SetReader(pipeReader)           // Use PipeReader as reader for Lob.

	// Use sync.WaitGroup to wait for go-routines to be ended.
	wg := new(sync.WaitGroup)
	wg.Add(1) // Select statement.

	// Start sql insert in own go-routine.
	// The go-routine is going to be ended when the data write via the PipeWriter is finalized.
	go func() {
		if _, err := stmt.Exec(lob); err != nil {
			log.Fatal(err)
		}
		fmt.Println("exec finalized")
		wg.Done()
	}()

	// Read file line by line and write data to pipe.
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if _, err := pipeWriter.Write(scanner.Bytes()); err != nil {
			log.Fatal(err)
		}
		if _, err := pipeWriter.Write([]byte{'\n'}); err != nil { // Write nl which was stripped off by scanner.
			log.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	// Close pipeWriter (end insert into db).
	pipeWriter.Close()

	// Wait until exec go-routine is ended.
	wg.Wait()
	stmt.Close()

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	pipeReader, pipeWriter = io.Pipe() // Create pipe for reading Lob.
	lob.SetWriter(pipeWriter)          // Use PipeWriter as writer for Lob.

	wg.Add(1) // Exec statement.

	// Start sql select in own go-routine.
	// The go-routine is going to be ended when the data read via the PipeReader is finalized.
	go func() {
		if err := db.QueryRow(fmt.Sprintf("select * from %s", table)).Scan(lob); err != nil {
			log.Fatal(err)
		}
		fmt.Println("scan finalized")
		wg.Done()
	}()

	// Read Lob line by line via bufio.Scanner.
	scanner = bufio.NewScanner(pipeReader)
	for scanner.Scan() {
		// Do something with scan result.
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	pipeReader.Close()

	// Wait until select go-routine is ended.
	wg.Wait()

	// output: exec finalized
	// scan finalized
}
