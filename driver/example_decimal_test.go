// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"database/sql"
	"fmt"
	"log"
	"math/big"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/drivertest"
)

/*
ExampleDecimal creates a table with a single decimal attribute, insert a record into it and select the entry afterwards.
This demonstrates the usage of the type Decimal to write and scan decimal database attributes.
For variables TestDSN and TestSchema see main_test.go.
*/
func ExampleDecimal() {
	connector, err := driver.NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	tableName := driver.RandomIdentifier("table_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (x decimal)", tableName)); err != nil { // Create table with decimal attribute.
		log.Fatal(err)
	}

	// Decimal values are represented in Go as big.Rat.
	in := (*driver.Decimal)(big.NewRat(1, 1)) // Create *big.Rat and cast to Decimal.

	if _, err := db.Exec(fmt.Sprintf("insert into %s values(?)", tableName), in); err != nil { // Insert record.
		log.Fatal(err)
	}

	var out driver.Decimal // Declare scan variable.

	if err := db.QueryRow(fmt.Sprintf("select * from %s", tableName)).Scan(&out); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Decimal value: %s", (*big.Rat)(&out).String()) // Cast scan variable to *big.Rat to use *big.Rat methods.

	// output: Decimal value: 1/1
}
