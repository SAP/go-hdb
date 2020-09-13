// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"database/sql"
	"fmt"
	"log"
	"math/big"
)

/*
ExampleDecimal creates a table with a single decimal attribute, insert a record into it and select the entry afterwards.
This demonstrates the usage of the type Decimal to write and scan decimal database attributes.
For variables TestDSN and TestSchema see main_test.go.
*/
func ExampleDecimal() {

	db, err := sql.Open(DriverName, TestDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tableName := RandomIdentifier("table_")

	if _, err := db.Exec(fmt.Sprintf("create table %s.%s (x decimal)", TestSchema, tableName)); err != nil { // Create table with decimal attribute.
		log.Fatal(err)
	}

	// Decimal values are represented in Go as big.Rat.
	in := (*Decimal)(big.NewRat(1, 1)) // Create *big.Rat and cast to Decimal.

	if _, err := db.Exec(fmt.Sprintf("insert into %s.%s values(?)", TestSchema, tableName), in); err != nil { // Insert record.
		log.Fatal(err)
	}

	var out Decimal // Declare scan variable.

	if err := db.QueryRow(fmt.Sprintf("select * from %s.%s", TestSchema, tableName)).Scan(&out); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Decimal value: %s", (*big.Rat)(&out).String()) // Cast scan variable to *big.Rat to use *big.Rat methods.

	// output: Decimal value: 1/1
}
