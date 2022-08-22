//go:build !unit
// +build !unit

// SPDX-FileCopyrightText: 2014-2022 SAP SE
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
)

func testConnection(db *sql.DB, t *testing.T) {
	var dummy string
	err := db.QueryRow("select * from dummy").Scan(&dummy)
	switch {
	case err == sql.ErrNoRows:
		t.Fatal(err)
	case err != nil:
		t.Fatal(err)
	}
	if dummy != "X" {
		t.Fatalf("dummy is %s - expected %s", dummy, "X")
	}
}

func testPing(db *sql.DB, t *testing.T) {
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func testInsertByQuery(db *sql.DB, t *testing.T) {
	table := driver.RandomIdentifier("insertByQuery_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer)", table)); err != nil {
		t.Fatal(err)
	}

	// insert value via Query
	if err := db.QueryRow(fmt.Sprintf("insert into %s values (?)", table), 42).Scan(); err != sql.ErrNoRows {
		t.Fatal(err)
	}

	// check value
	var i int
	if err := db.QueryRow(fmt.Sprintf("select * from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 42 {
		t.Fatalf("value %d - expected %d", i, 42)
	}
}

func testHDBError(db *sql.DB, t *testing.T) {
	//select from not existing table with different table name length
	//to check if padding, etc works (see hint in protocol.error.Read(...))
	for i := 0; i < 9; i++ {
		_, err := db.Query(fmt.Sprintf("select * from %s", strings.Repeat("x", i+1)))
		if err == nil {
			t.Fatal("hdb error expected")
		}
		dbError, ok := err.(driver.Error)
		if !ok {
			t.Fatalf("hdb error expected got %v", err)
		}
		if dbError.Code() != 259 {
			t.Fatalf("hdb error code: %d - expected: %d", dbError.Code(), 259)
		}
	}
}

func testHDBWarning(db *sql.DB, t *testing.T) {
	// procedure gives warning:
	// 	SQL HdbWarning 1347 - Not recommended feature: DDL statement is used in Dynamic SQL (current dynamic_sql_ddl_error_level = 1)
	const procOut = `create procedure %[1]s ()
language SQLSCRIPT as
begin
	exec 'create table %[2]s(id int)';
	exec 'drop table %[2]s';
end
`
	procedure := driver.RandomIdentifier("proc_")
	tableName := driver.RandomIdentifier("table_")

	if _, err := db.Exec(fmt.Sprintf(procOut, procedure, tableName)); err != nil { // Create stored procedure.
		t.Fatal(err)
	}

	if _, err := db.Exec(fmt.Sprintf("call %s", procedure)); err != nil {
		t.Fatal(err)
	}
}

func testQueryAttributeAlias(db *sql.DB, t *testing.T) {
	table := driver.RandomIdentifier("queryAttributeAlias_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, j integer)", table)); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select i as x, j from %s", table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}

	if columns[0] != "X" {
		t.Fatalf("value %s - expected %s", columns[0], "X")
	}

	if columns[1] != "J" {
		t.Fatalf("value %s - expected %s", columns[1], "J")
	}
}

func testRowsAffected(db *sql.DB, t *testing.T) {
	const maxRows = 10

	table := driver.RandomIdentifier("rowsAffected_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer)", table)); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare(fmt.Sprintf("insert into %s values(?)", table))
	if err != nil {
		t.Fatal(err)
	}

	// insert
	for i := 0; i < maxRows; i++ {
		result, err := stmt.Exec(i)
		if err != nil {
			t.Fatal(err)
		}
		checkAffectedRows(t, result, 1)
	}

	// update
	result, err := db.Exec(fmt.Sprintf("update %s set i = %d where i <> %d", table, maxRows, maxRows))
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, maxRows)
}

func testUpsert(db *sql.DB, t *testing.T) {
	table := driver.RandomIdentifier("upsert_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (key int primary key, val int)", table)); err != nil {
		t.Fatal(err)
	}

	result, err := db.Exec(fmt.Sprintf("upsert %s values (1, 1)", table))
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, 1)

	result, err = db.Exec(fmt.Sprintf("upsert %s values (:1, :1) where key = :2", table), 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, 1)

	result, err = db.Exec(fmt.Sprintf("upsert %s values (?, ?) where key = ?", table), 1, 9, 1)
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, 1)

	result, err = db.Exec(fmt.Sprintf("upsert %s values (?, ?) with primary key", table), 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, 1)

	result, err = db.Exec(fmt.Sprintf("upsert %[1]s select key + ?, val from %[1]s", table), 2)
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, 2)

}

func checkAffectedRows(t *testing.T, result sql.Result, rowsExpected int64) {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if rowsAffected != rowsExpected {
		t.Fatalf("rows affected %d - expected %d", rowsAffected, rowsExpected)
	}
}

func TestDriver(t *testing.T) {
	tests := []struct {
		name string
		fct  func(db *sql.DB, t *testing.T)
	}{
		{"connection", testConnection},
		{"ping", testPing},
		{"insertByQuery", testInsertByQuery},
		{"hdbError", testHDBError},
		{"hdbWarning", testHDBWarning},
		{"queryAttributeAlias", testQueryAttributeAlias},
		{"rowsAffected", testRowsAffected},
		{"upsert", testUpsert},
	}

	db := driver.DefaultTestDB()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(db, t)
		})
	}
}
