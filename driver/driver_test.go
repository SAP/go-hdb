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
	"context"
	"database/sql"
	"fmt"
	"testing"
)

func TestCheckBulkInsert(t *testing.T) {

	var data = []struct {
		bulkSql    string
		sql        string
		bulkInsert bool
	}{
		{"bulk insert", "insert", true},
		{"   bulk   insert  ", "insert  ", true},
		{"BuLk iNsErT", "iNsErT", true},
		{"   bUlK   InSeRt  ", "InSeRt  ", true},
		{"  bulkinsert  ", "  bulkinsert  ", false},
		{"bulk", "bulk", false},
		{"insert", "insert", false},
	}

	for i, d := range data {
		sql, bulkInsert := checkBulkInsert(d.bulkSql)
		if sql != d.sql {
			t.Fatalf("test %d failed: bulk insert flag %t - %t expected", i, bulkInsert, d.bulkInsert)
		}
		if sql != d.sql {
			t.Fatalf("test %d failed: sql %s - %s expected", i, sql, d.sql)
		}
	}
}

func TestPing(t *testing.T) {

	db, err := sql.Open(DriverName, TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}

}

func TestInsertByQuery(t *testing.T) {

	db, err := sql.Open(DriverName, TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	table := RandomIdentifier("insertByQuery_")
	if _, err := db.Exec(fmt.Sprintf("create table %s.%s (i integer)", TestSchema, table)); err != nil {
		t.Fatal(err)
	}

	// insert value via Query
	if err := db.QueryRow(fmt.Sprintf("insert into %s.%s values (?)", TestSchema, table), 42).Scan(); err != sql.ErrNoRows {
		t.Fatal(err)
	}

	// check value
	var i int
	if err := db.QueryRow(fmt.Sprintf("select * from %s.%s", TestSchema, table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 42 {
		t.Fatalf("value %d - expected %d", i, 42)
	}
}

func TestHDBWarning(t *testing.T) {
	// procedure gives warning:
	// 	SQL HdbWarning 1347 - Not recommended feature: DDL statement is used in Dynamic SQL (current dynamic_sql_ddl_error_level = 1)
	const procOut = `create procedure %[1]s.%[2]s ()
language SQLSCRIPT as
begin
	exec 'create table %[3]s(id int)';
	exec 'drop table %[3]s';
end
`

	db, err := sql.Open(DriverName, TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	procedure := RandomIdentifier("proc_")
	tableName := RandomIdentifier("table_")

	if _, err := db.Exec(fmt.Sprintf(procOut, TestSchema, procedure, tableName)); err != nil { // Create stored procedure.
		t.Fatal(err)
	}

	if _, err := db.Exec(fmt.Sprintf("call %s.%s", TestSchema, procedure)); err != nil {
		t.Fatal(err)
	}
}

func TestQueryAttributeAlias(t *testing.T) {

	db, err := sql.Open(DriverName, TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	table := RandomIdentifier("queryAttributeAlias_")
	if _, err := db.Exec(fmt.Sprintf("create table %s.%s (i integer, j integer)", TestSchema, table)); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select i as x, j from %s.%s", TestSchema, table))
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

func TestRowsAffected(t *testing.T) {
	const maxRows = 10

	db, err := sql.Open(DriverName, TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	table := RandomIdentifier("rowsAffected_")
	if _, err := db.Exec(fmt.Sprintf("create table %s.%s (i integer)", TestSchema, table)); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare(fmt.Sprintf("insert into %s.%s values(?)", TestSchema, table))
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
	result, err := db.Exec(fmt.Sprintf("update %s.%s set i = %d where i <> %d", TestSchema, table, maxRows, maxRows))
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, maxRows)
}

func TestUpsert(t *testing.T) {
	db, err := sql.Open(DriverName, TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	table := RandomIdentifier("upsert_")
	if _, err := db.Exec(fmt.Sprintf("create table %s.%s (key int primary key, val int)", TestSchema, table)); err != nil {
		t.Fatal(err)
	}

	result, err := db.Exec(fmt.Sprintf("upsert %s.%s values (1, 1)", TestSchema, table))
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, 1)

	result, err = db.Exec(fmt.Sprintf("upsert %s.%s values (:1, :1) where key = :2", TestSchema, table), 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, 1)

	result, err = db.Exec(fmt.Sprintf("upsert %s.%s values (?, ?) where key = ?", TestSchema, table), 1, 9, 1)
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, 1)

	result, err = db.Exec(fmt.Sprintf("upsert %s.%s values (?, ?) with primary key", TestSchema, table), 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	checkAffectedRows(t, result, 1)

	result, err = db.Exec(fmt.Sprintf("upsert %[1]s.%[2]s select key + ?, val from %[1]s.%[2]s", TestSchema, table), 2)
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
