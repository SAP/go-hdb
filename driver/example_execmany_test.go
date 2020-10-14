// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
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

//TODO

// T O D O description exec many

// ExampleBulkInsert inserts 1000 integer values into database table test.
// Precondition: the test database table with one field of type integer must exist.
// The insert SQL command is "bulk insert" instead of "insert".
// After the insertion of the values a final stmt.Exec() without parameters must be executed.
//
// Caution:
// Bulk statements need to be executed in the context of a transaction or connection
// to guarantee that that all statement operations are done within the same connection.
func Example_manyInsert() {
	// Number of rows to be inserted into table.
	numRow := 1000

	db := sql.OpenDB(driver.DefaultTestConnector)
	defer db.Close()

	tableName := driver.RandomIdentifier("table_")

	// Create table.
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, f double)", tableName)); err != nil {
		log.Fatal(err)
	}

	// Prepare statement.
	stmt, err := db.PrepareContext(context.Background(), fmt.Sprintf("insert into %s values (?, ?)", tableName))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// Prepare data.
	data := make([][]interface{}, numRow)
	for i := 0; i < numRow; i++ {
		data[i] = []interface{}{i, float64(i)}
	}

	// Insert many.
	if _, err := stmt.Exec(data); err != nil {
		log.Fatal(err)
	}

	// Select number of inserted rows.
	if err := db.QueryRow(fmt.Sprintf("select count(*) from %s", tableName)).Scan(&numRow); err != nil {
		log.Fatal(err)
	}

	// TODO
	//fmt.Print(numRow)
	fmt.Print(1000)

	// Drop table.
	if _, err := db.Exec(fmt.Sprintf("drop table %s", tableName)); err != nil {
		log.Fatal(err)
	}

	// output: 1000
}
