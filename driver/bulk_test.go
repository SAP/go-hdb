// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/drivertest"
)

// TestBulkFrame
func testBulkFrame(conn *sql.Conn, samples int, cmd string, insertFct func(stmt *sql.Stmt), t *testing.T) {
	// 1. prepare
	tmpTableName := driver.RandomIdentifier("#tmpTable")

	//keep connection / hdb session for using local temporary tables
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //cleanup

	if _, err := tx.Exec(fmt.Sprintf("create local temporary table %s (i integer)", tmpTableName)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("%s %s values (?)", cmd, tmpTableName))
	if err != nil {
		t.Fatalf("prepare bulk insert failed: %s", err)
	}
	defer stmt.Close()

	// 2. call insert function
	insertFct(stmt)

	// 3. check
	i := 0
	err = tx.QueryRow(fmt.Sprintf("select count(*) from %s", tmpTableName)).Scan(&i)
	if err != nil {
		t.Fatalf("select count failed: %s", err)
	}

	if i != samples {
		t.Fatalf("invalid number of records %d - %d expected", i, samples)
	}

	rows, err := tx.Query(fmt.Sprintf("select * from %s order by i", tmpTableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i = 0
	for rows.Next() {

		var j int

		if err := rows.Scan(&j); err != nil {
			t.Fatal(err)
		}

		if j != i {
			t.Fatalf("value %d - expected %d", j, i)
		}

		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestBulkInsertDuplicates
func testBulkInsertDuplicates(conn *sql.Conn, t *testing.T) {
	ctx := context.Background()

	table := driver.RandomIdentifier("bulkInsertDuplicates")

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("create table %s (k integer primary key, v integer)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("bulk insert into %s values (?,?)", table))
	if err != nil {
		t.Fatalf("prepare bulk insert failed: %s", err)
	}
	defer stmt.Close()

	for i := 1; i < 4; i++ {
		if _, err := stmt.Exec(i, i); err != nil {
			t.Fatalf("insert failed: %s", err)
		}
	}
	if _, err := stmt.Exec(); err != nil {
		t.Fatalf("final insert (flush) failed: %s", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := stmt.Exec(i, i); err != nil {
			t.Fatalf("insert failed: %s", err)
		}
	}
	_, err = stmt.Exec()
	if err == nil {
		t.Fatal("error duplicate key expected")
	}

	dbError, ok := err.(driver.Error)
	if !ok {
		t.Fatal("driver.Error expected")
	}

	// expect 3 errors for statement 1,2 and 3
	if dbError.NumError() != 3 {
		t.Fatalf("number of errors: %d - %d expected", dbError.NumError(), 3)
	}

	stmtNo := []int{1, 2, 3}

	for i := 0; i < dbError.NumError(); i++ {
		dbError.SetIdx(i)
		if dbError.StmtNo() != stmtNo[i] {
			t.Fatalf("statement number: %d - %d expected", dbError.StmtNo(), stmtNo[i])
		}
	}
}

func testBulk(conn *sql.Conn, t *testing.T) {
	const samples = 1000

	tests := []struct {
		name      string
		cmd       string
		insertFct func(stmt *sql.Stmt)
	}{
		{
			"bulkInsertViaCommand",
			"bulk insert into",
			func(stmt *sql.Stmt) {
				for i := 0; i < samples; i++ {
					if _, err := stmt.Exec(i); err != nil {
						t.Fatalf("insert failed: %s", err)
					}
				}
				// final flush
				if _, err := stmt.Exec(); err != nil {
					t.Fatalf("final insert (flush) failed: %s", err)
				}
			},
		},
		{
			"bulkInsertViaParameter",
			"insert into",
			func(stmt *sql.Stmt) {
				prm := driver.NoFlush
				for i := 0; i < samples; i++ {
					if i == (samples - 1) {
						prm = driver.Flush
					}
					if _, err := stmt.Exec(i, prm); err != nil {
						t.Fatalf("insert failed: %s", err)
					}
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testBulkFrame(conn, samples, test.cmd, test.insertFct, t)
		})
	}
}

// TestBulkBlob
func testBulkBlob(conn *sql.Conn, t *testing.T) {

	samples := 100
	lobData := func(i int) string {
		return fmt.Sprintf("%s-%d", "Go rocks", i)
	}

	// 1. prepare
	tmpTableName := driver.RandomIdentifier("#tmpTable")

	//keep connection / hdb session for using local temporary tables
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //cleanup

	if _, err := tx.Exec(fmt.Sprintf("create local temporary table %s (i integer, b blob)", tmpTableName)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("bulk insert into %s values (?, ?)", tmpTableName))
	if err != nil {
		t.Fatalf("prepare bulk insert failed: %s", err)
	}
	defer stmt.Close()

	// 2. call insert function
	for i := 0; i < samples; i++ {
		lob := new(driver.Lob).SetReader(strings.NewReader(lobData(i)))

		if _, err := stmt.Exec(i, lob); err != nil {
			t.Fatalf("insert failed: %s", err)
		}
	}
	// final flush
	if _, err := stmt.Exec(); err != nil {
		t.Fatalf("final insert (flush) failed: %s", err)
	}

	// 3. check
	i := 0
	err = tx.QueryRow(fmt.Sprintf("select count(*) from %s", tmpTableName)).Scan(&i)
	if err != nil {
		t.Fatalf("select count failed: %s", err)
	}

	if i != samples {
		t.Fatalf("invalid number of records %d - %d expected", i, samples)
	}

	rows, err := tx.Query(fmt.Sprintf("select * from %s order by i", tmpTableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i = 0
	for rows.Next() {

		var j int
		builder := new(strings.Builder)

		lob := new(driver.Lob).SetWriter(builder)

		if err := rows.Scan(&j, lob); err != nil {
			t.Fatal(err)
		}

		if j != i {
			t.Fatalf("value %d - expected %d", j, i)
		}
		if builder.String() != lobData(i) {
			t.Fatalf("value %s - expected %s", builder.String(), lobData(i))
		}

		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestBulk(t *testing.T) {
	tests := []struct {
		name string
		fct  func(conn *sql.Conn, t *testing.T)
	}{
		{"testBulk", testBulk},
		{"testBulkInsertDuplicates", testBulkInsertDuplicates},
		{"testBulkBlob", testBulkBlob},
	}

	connector, err := driver.NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(conn, t) // run bulk tests on conn
		})
	}
}
