// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"database/sql"
	"fmt"
	"log"
)

/*
ExampleCallSimpleOut creates a stored procedure with one output parameter and executes it.
Stored procedures with output parameters must be executed by sql.Query or sql.QueryRow.
For variables TestDSN and TestSchema see main_test.go.
*/

// ExampleQuery: tbd
func Example_query() {
	db, err := sql.Open(DriverName, TestDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	table := RandomIdentifier("testNamedArg_")
	if _, err := db.Exec(fmt.Sprintf("create table %s.%s (i integer, j integer)", TestSchema, table)); err != nil {
		log.Fatal(err)
	}

	var i = 0
	if err := db.QueryRow(fmt.Sprintf("select count(*) from %s.%s where i = :1 and j = :1", TestSchema, table), 1).Scan(&i); err != nil {
		log.Fatal(err)
	}

	if err := db.QueryRow(fmt.Sprintf("select count(*) from %s.%s where i = ? and j = :3", TestSchema, table), 1, "soso", 2).Scan(&i); err != nil {
		log.Fatal(err)
	}

	fmt.Print(i)
	// output: 0
}
