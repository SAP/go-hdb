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
	"database/sql"
	"fmt"
	"log"
)

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
