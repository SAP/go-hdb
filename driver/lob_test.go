/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	//"bytes"
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"sync"
	"testing"
)

func testLobInsert(db *sql.DB, t *testing.T) {

	table := RandomIdentifier("lobInsert")

	if _, err := db.Exec(fmt.Sprintf("create table %s (i1 integer, b1 blob, i2 integer, b2 blob)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := db.Prepare(fmt.Sprintf("insert into %s values (?,?,?,?)", table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

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
	wrBuf.ReadFrom(lrd)

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
			t.Fatal(err)
		}
		t.Log("exec finalized")
	}()

	mwr := io.MultiWriter(wr, cmpBuf)

	wrBuf.WriteTo(mwr)
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
			t.Fatal(err)
		}
		t.Log("scan finalized")
	}()

	rdBuf := &bytes.Buffer{}
	rdBuf.ReadFrom(rd)

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

	row := conn.QueryRowContext(ctx, fmt.Sprintf("select * from %s", table))

	err = conn.PingContext(ctx)
	if err != ErrNestedQuery {
		t.Fatalf("got error %s - expected %s", err, ErrNestedQuery)
	}

	if err := row.Scan(lob); err != nil {
		t.Fatal(err)
	}

	// if !bytes.Equal(rdBuf.Bytes(), cmpBuf.Bytes()) {
	// 	t.Fatalf("read buffer is not equal to write buffer")
	// }

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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(TestDB, t)
		})
	}
}
