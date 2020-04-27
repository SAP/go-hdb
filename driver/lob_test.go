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
	"testing"
)

func testLobInsert(db *sql.DB, t *testing.T) {

	table := RandomIdentifier("lobInsert")

	if _, err := db.Exec(fmt.Sprintf("create table %s (i1 integer, b1 blob, i2 integer, b2 blob)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := db.Prepare(fmt.Sprintf("insert into %s values (?,?,?,?)", table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

}

func TestLob(t *testing.T) {
	tests := []struct {
		name string
		fct  func(db *sql.DB, t *testing.T)
	}{
		{"insert", testLobInsert},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(TestDB, t)
		})
	}
}
