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

// ExampleFctInsert inserts 1000 rows into a database table via a bulk 'function' operation.
func Example_fctInsert() {
	// Number of rows to be inserted into table.
	numRow := 1000

	db := sql.OpenDB(driver.DefaultTestConnector())
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

	// Bulk insert via function.
	i := 0
	if _, err := stmt.Exec(func(args []any) error {
		if i >= numRow {
			return driver.ErrEndOfRows
		}
		args[0], args[1] = i, float64(i)
		i++
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Select number of inserted rows.
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
