//go:build !unit

package driver

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

type stringLob string

// Scan implements the database/sql/Scanner interface.
func (s *stringLob) Scan(src any) error { return ScanLobString(src, (*string)(s)) }

type bytesLob []byte

func (b *bytesLob) Scan(src any) error { return ScanLobBytes(src, (*[]byte)(b)) }

func testLobInsert(db *sql.DB, t *testing.T) {

	const (
		numRec   = 100
		blobSize = 1000
	)
	testData := make([]string, numRec)

	for i := 0; i < numRec; i++ {
		testData[i] = randAlphanumString(blobSize)
	}

	table := RandomIdentifier("lob_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, n nclob, b blob)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	// use trancactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values (?,?,?)", table))
	if err != nil {
		t.Fatal(err)
	}

	// insert as string and byte
	for i, s := range testData {
		if _, err := stmt.Exec(i, s, []byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("select * from %s", table))
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

type randReader struct{}

func (randReader) Read(b []byte) (n int, err error) {
	return rand.Read(b)
}

func testLobPipe(db *sql.DB, t *testing.T) {
	const lobSize = 10000

	table := RandomIdentifier("lobPipe")

	lrd := io.LimitReader(randReader{}, lobSize)

	wrBuf := &bytes.Buffer{}
	if _, err := wrBuf.ReadFrom(lrd); err != nil {
		t.Fatal(err)
	}

	cmpBuf := &bytes.Buffer{}

	// use trancactions:
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
	wg.Add(1)

	go func() {
		defer wg.Done()
		if _, err := stmt.Exec(lob); err != nil {
			t.Error(err)
			return
		}
		t.Log("exec finalized")
	}()

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

	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := db.QueryRow(fmt.Sprintf("select * from %s", table)).Scan(lob); err != nil {
			t.Error(err)
			return
		}
		t.Log("scan finalized")
	}()

	rdBuf := &bytes.Buffer{}
	if _, err := rdBuf.ReadFrom(rd); err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	if !bytes.Equal(rdBuf.Bytes(), cmpBuf.Bytes()) {
		t.Fatalf("read buffer is not equal to write buffer")
	}

}

func testLobDelayedScan(db *sql.DB, t *testing.T) {
	const lobSize = 10000

	table := RandomIdentifier("lobPipe")

	rd := io.LimitReader(randReader{}, lobSize)

	// use trancactions:
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

	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	wr := &bytes.Buffer{}
	lob.SetWriter(wr)

	ctx := context.Background()

	conn, err := db.Conn(ctx) // guarantee that same connection is used
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	row := conn.QueryRowContext(ctx, fmt.Sprintf("select * from %s", table))

	if err = conn.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	err = row.Scan(lob)
	switch {
	case err == nil:
		t.Fatalf("got error: <nil> - expected: %s", ErrNestedQuery)
	case !errors.Is(err, ErrNestedQuery):
		t.Fatalf("got error: %s - expected: %s", err, ErrNestedQuery)
	}
}

func TestLob(t *testing.T) {
	tests := []struct {
		name string
		fct  func(db *sql.DB, t *testing.T)
	}{
		{"insert", testLobInsert},
		{"pipe", testLobPipe},
		{"delayedScan", testLobDelayedScan},
	}

	db := DefaultTestDB()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(db, t)
		})
	}
}
