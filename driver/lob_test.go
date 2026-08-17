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
