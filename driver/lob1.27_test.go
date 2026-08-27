//go:build !unit && go1.27

package driver

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/rand/alphanum"
)

func newRandomData(size int) []byte {
	b := make([]byte, size)
	rand.Read(b) // starting with go1.24 rand.Read is never returning an error.
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

	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (i integer, n nclob, b blob)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	// use transactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values (?,?,?)", table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// insert as string and byte
	for i, s := range testData {
		if _, err := stmt.ExecContext(t.Context(), i, s, []byte(s)); err != nil {
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
		s string
		b []byte
	)

	for rows.Next() {
		if err := rows.Scan(&i, &s, &b); err != nil {
			t.Fatal(err)
		}
		if s != testData[i] {
			t.Fatalf("idx %d got %s - expected %s", i, s, testData[i])
		}
		if string(b) != testData[i] {
			t.Fatalf("idx %d got %s - expected %s", i, string(b), testData[i])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

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

	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (id integer primary key, b1 blob, b2 blob)", table)); err != nil {
		t.Fatal(err)
	}

	// Create a custom connector with small LOB chunk size
	ctr := MT.NewConnector()
	ctr.SetLobChunkSize(lobChunkSize)
	testDB := sql.OpenDB(ctr)
	defer testDB.Close()

	// Insert initial data

	insertData1 := newRandomData(lobChunkSize * 2)
	insertData2 := newRandomData(lobChunkSize * 3)

	tx, err := testDB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values (?, ?, ?)", table))
	if err != nil {
		t.Fatal(err)
	}

	result, err := stmt.ExecContext(t.Context(), 1, insertData1, insertData2)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	checkRowsAffected(result)

	// Update
	updateData1 := newRandomData(lobChunkSize * 2)
	updateData2 := newRandomData(lobChunkSize * 3)

	tx, err = testDB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err = tx.PrepareContext(t.Context(), fmt.Sprintf("update %s set b1 = ?, b2 = ? where id = ?", table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	result, err = stmt.ExecContext(t.Context(), updateData1, updateData2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	checkRowsAffected(result)

	// Verify
	var readData1 []byte
	var readData2 []byte
	if err := db.QueryRowContext(t.Context(), fmt.Sprintf("select b1, b2 from %s where id = 1", table)).Scan(&readData1, &readData2); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(updateData1, readData1) {
		t.Fatalf("data1 mismatch")
	}
	if !bytes.Equal(updateData2, readData2) {
		t.Fatalf("data2 mismatch")
	}
}
