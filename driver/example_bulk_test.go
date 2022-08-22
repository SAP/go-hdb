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
	"log"

	"github.com/SAP/go-hdb/driver"
)

// ExampleBulkInsert inserts 1000 rows into a database table.
//
// The insert SQL command is "bulk insert" instead of "insert".
// After the insertion of the values a final stmt.Exec() without parameters must be executed.
//
// Caution:
// Bulk statements need to be executed in the context of a transaction or connection
// to guarantee that that all statement operations are done within the same connection.
func Example_bulkInsert() {
	db := sql.OpenDB(driver.DefaultTestConnector())
	defer db.Close()

	tableName := driver.RandomIdentifier("table_")

	// Create table.
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, f float)", tableName)); err != nil {
		log.Fatal(err)
	}

	// Get connection for bulk insert.
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Prepare statement on basis of connection (bulk insert).
	stmt, err := conn.PrepareContext(context.Background(), fmt.Sprintf("bulk insert into %s values (?,?)", tableName))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// Bulk insert.
	for i := 0; i < 1000; i++ {
		if _, err := stmt.Exec(i, float64(i)); err != nil {
			log.Fatal(err)
		}
	}
	// Call final stmt.Exec().
	if _, err := stmt.Exec(); err != nil {
		log.Fatal(err)
	}

	// Select number of inserted rows.
	var numRow int
	if err := db.QueryRow(fmt.Sprintf("select count(*) from %s", tableName)).Scan(&numRow); err != nil {
		log.Fatal(err)
	}
	fmt.Print(numRow)

	// Drop table.
	if _, err := db.Exec(fmt.Sprintf("drop table %s", tableName)); err != nil {
		log.Fatal(err)
	}

	// output: 1000
}
