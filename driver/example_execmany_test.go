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
	"github.com/SAP/go-hdb/driver/drivertest"
)

// ExampleManyInsert inserts 1000 rows into a database table via a 'mass' operation.
func Example_manyInsert() {
	// Number of rows to be inserted into table.
	numRow := 1000

	connector, err := driver.NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
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
	fmt.Print(numRow)

	// Drop table.
	if _, err := db.Exec(fmt.Sprintf("drop table %s", tableName)); err != nil {
		log.Fatal(err)
	}

	// output: 1000
}
