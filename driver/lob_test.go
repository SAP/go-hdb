//go:build !unit

package driver

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/rand/alphanum"
)

type stringLob string

// Scan implements the database/sql/Scanner interface.
func (s *stringLob) Scan(src any) error { return ScanLobString(src, (*string)(s)) }

type bytesLob []byte

func (b *bytesLob) Scan(src any) error { return ScanLobBytes(src, (*[]byte)(b)) }

func newRandomDataBytesLob(size int) bytesLob {
	b := make([]byte, size)
	rand.Read(b) //nolint: errcheck // never returns error
	return b
}

func testLobInsert(t *testing.T, db *sql.DB) {

	const (
		numRec   = 100
		blobSize = 1000
	)
	testData := make([]string, numRec)

	for i := range numRec {
		testData[i] = alphanum.ReadString(blobSize)
	}

	table := RandomIdentifier("lob_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, n nclob, b blob)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	// use transactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values (?,?,?)", table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// insert as string and byte
	for i, s := range testData {
		if _, err := stmt.Exec(i, s, []byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(t.Context(), fmt.Sprintf("select * from %s", table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var (
		i int
		s stringLob
		b bytesLob
	)

	for rows.Next() {
		if err := rows.Scan(&i, &s, &b); err != nil {
			t.Fatal(err)
		}
		if string(s) != testData[i] {
			t.Fatalf("idx %d got %s - expected %s", i, string(s), testData[i])
		}
		if string(b) != testData[i] {
			t.Fatalf("idx %d got %s - expected %s", i, string(b), testData[i])
		}
	}
	if rows.Err() != nil {
		t.Fatal(err)
	}
}

func testLobPipe(t *testing.T, db *sql.DB) {
	const lobSize = 10000

	table := RandomIdentifier("lobPipe_")

	lrd := io.LimitReader(rand.Reader, lobSize)

	wrBuf := &bytes.Buffer{}
	if _, err := wrBuf.ReadFrom(lrd); err != nil {
		t.Fatal(err)
	}

	cmpBuf := &bytes.Buffer{}

	// use transactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(fmt.Sprintf("create table %s (b blob)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values (?)", table))
	if err != nil {
		t.Fatal(err)
	}

	lob := &Lob{}

	rd, wr := io.Pipe()
	lob.SetReader(rd)

	wg := new(sync.WaitGroup)
	wg.Go(func() {
		if _, err := stmt.Exec(lob); err != nil {
			t.Error(err)
			return
		}
		t.Log("exec finalized")
	})

	mwr := io.MultiWriter(wr, cmpBuf)

	if _, err := wrBuf.WriteTo(mwr); err != nil {
		t.Fatal(err)
	}
	wr.Close()
	wg.Wait()

	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rd, wr = io.Pipe()
	lob.SetWriter(wr)

	wg.Go(func() {
		if err := db.QueryRow(fmt.Sprintf("select * from %s", table)).Scan(lob); err != nil {
			t.Error(err)
			return
		}
		t.Log("scan finalized")
	})

	rdBuf := &bytes.Buffer{}
	if _, err := rdBuf.ReadFrom(rd); err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	if !bytes.Equal(rdBuf.Bytes(), cmpBuf.Bytes()) {
		t.Fatalf("read buffer is not equal to write buffer")
	}
}

func testLobDelayedScan(t *testing.T, db *sql.DB) {
	const lobSize = 10000

	table := RandomIdentifier("lobDelayedScan_")

	rd := io.LimitReader(rand.Reader, lobSize)

	// use transactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(fmt.Sprintf("create table %s (b blob)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values (?)", table))
	if err != nil {
		t.Fatal(err)
	}

	lob := &Lob{}
	lob.SetReader(rd)

	if _, err := stmt.Exec(lob); err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	wr := &bytes.Buffer{}
	lob.SetWriter(wr)

	conn, err := db.Conn(t.Context()) // guarantee that same connection is used
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	row := conn.QueryRowContext(t.Context(), fmt.Sprintf("select * from %s", table))

	if err = conn.PingContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = row.Scan(lob)
	switch {
	case err == nil:
		t.Fatalf("got error: <nil> - expected: %s", errInvalidLobLocatorID)
	case !errors.Is(err, errInvalidLobLocatorID):
		t.Fatalf("got error: %s - expected: %s", err, errInvalidLobLocatorID)
	}
}

func testLobNilPlusBig(t *testing.T, db *sql.DB) {
	// db table with two lobs
	// .one is nil and
	// .the second one big enough, so that it needs to be written in chunks
	// wasn't handled in session writeLobs and was raising an error
	testData := func() []byte {
		b := make([]byte, 1e6) // random Lob size 1MB
		if _, err := alphanum.Read(b); err != nil {
			panic(err) // should never happen
		}
		return b
	}()

	table := RandomIdentifier("lobNilPlusBig_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (n nclob, b blob)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	// use transactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values (?,?)", table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(nil, testData); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

/*
Summary: LOB rowsAffected Fix for HANA 2 vs HANA 4

	The Issue

	When executing UPDATE/INSERT statements with LOB parameters, rowsAffected was incorrectly returning 0 due to differences in the
	HANA database protocol between versions 2 and 4.

	HANA 2 Protocol:
	- Initial UPDATE: rowsAffected = 1
	- LOB chunk writes (including final): rowsAffected = 0

	HANA 4 Protocol:
	- Initial UPDATE: rowsAffected = -1 (error indicator)
	- LOB chunk writes (intermediate): rowsAffected = 0
	- Final LOB write: rowsAffected = 1

	Previously, LOB write responses were ignored. In HANA 2 this worked (initial 1 was kept), but in HANA 4 the initial -1 was the
	only value considered, and since only positive values are aggregated, the final result was 0.

	The Fix

	Modified session.go to accumulate rowsAffected from LOB write operations:

	1. writeLobs() function: Changed return type from error to (int64, error) to return accumulated rows from all LOB chunk writes.
	2. exec() and execCall() functions: Now capture and add the LOB rows to the initial operation's rows.

	Result for both versions:
	- HANA 2: Initial 1 + LOB writes 0 = 1 ✓
	- HANA 4: Initial -1 (ignored as negative) + final LOB write 1 = 1 ✓

	Test

	The test testLobAffectedRows verifies the fix by:
	- tests UPDATE with multiple LOB columns, each requiring chunks.
	- Verifies that multiple LOBs with multiple chunks still return rowsAffected=1.
	- Using a small LOB chunk size (1KB) to force chunking
	- Updating 1 row with 2 LOB columns requiring multiple chunks (2 + 3 = 5 chunks total)
	- Verifying rowsAffected = 1 (not 0 as it was before the fix in HANA 4)
	- Confirming data was actually updated correctly
*/
func testLobAffectedRows(t *testing.T, db *sql.DB) {

	checkRowsAffected := func(result sql.Result) {
		// Check rowsAffected - MUST be 1, not 0 (HANA 4 fix)
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("RowsAffected() failed: %s", err)
		}
		if rowsAffected != 1 {
			t.Fatalf("got rowsAffected %d - expected 1", rowsAffected)
		}
	}

	const lobChunkSize = 1024 // 1KB chunks for testing

	table := RandomIdentifier("lobAffectedRows_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (id integer primary key, b1 blob, b2 blob)", table)); err != nil {
		t.Fatal(err)
	}

	// Create a custom connector with small LOB chunk size
	ctr := MT.NewConnector()
	ctr.SetLobChunkSize(lobChunkSize)
	testDB := sql.OpenDB(ctr)
	defer testDB.Close()

	// Insert initial data

	insertData1 := newRandomDataBytesLob(lobChunkSize * 2)
	insertData2 := newRandomDataBytesLob(lobChunkSize * 3)

	tx, err := testDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values (?, ?, ?)", table))
	if err != nil {
		t.Fatal(err)
	}

	result, err := stmt.Exec(1, insertData1, insertData2)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	checkRowsAffected(result)

	// Update
	updateData1 := newRandomDataBytesLob(lobChunkSize * 2)
	updateData2 := newRandomDataBytesLob(lobChunkSize * 3)

	tx, err = testDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err = tx.Prepare(fmt.Sprintf("update %s set b1 = ?, b2 = ? where id = ?", table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	result, err = stmt.Exec(updateData1, updateData2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	checkRowsAffected(result)

	// Verify
	var readData1 bytesLob
	var readData2 bytesLob
	if err := db.QueryRow(fmt.Sprintf("select b1, b2 from %s where id = 1", table)).Scan(&readData1, &readData2); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(updateData1, readData1) {
		t.Fatalf("data1 mismatch")
	}
	if !bytes.Equal(updateData2, readData2) {
		t.Fatalf("data2 mismatch")
	}
}

func TestLob(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T, db *sql.DB)
	}{
		{"insert", testLobInsert},
		{"pipe", testLobPipe},
		{"delayedScan", testLobDelayedScan},
		{"nilPlusBigLob", testLobNilPlusBig},
		{"affectedRows", testLobAffectedRows},
	}

	db := MT.DB()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t, db)
		})
	}
}
