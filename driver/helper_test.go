// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"bytes"
	"database/sql"
	"testing"
)

func testHelper(db *sql.DB, t *testing.T) {
	rows, err := db.Query("select definition from procedures order by procedure_name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {

		b := new(bytes.Buffer)
		lob := new(Lob)
		lob.SetWriter(b) // SetWriter sets the io.Writer object, to which the database content of the lob field is written.

		if err := rows.Scan(lob); err != nil {
			t.Fatal(err)
		}

		t.Log(b.String())

	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

}

func TestHelper(t *testing.T) {
	tests := []struct {
		name string
		fct  func(db *sql.DB, t *testing.T)
	}{
		{"helper", testHelper},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(TestDB, t)
		})
	}
}
