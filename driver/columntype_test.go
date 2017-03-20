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
	"bytes"
	"database/sql"
	"fmt"
	"testing"
)

type testColumnType struct {
	sqlType string
	length  int64

	varLength bool
	sizeable  bool

	typeName  string
	precision int64
	scale     int64
	nullable  bool
}

func TestColumnType(t *testing.T) {

	var testColumnTypeData = []testColumnType{
		{"tinyint", 0, false, false, "TINYINT", 0, 0, true},
		{"smallint", 0, false, false, "SMALLINT", 0, 0, true},
		{"integer", 0, false, false, "INTEGER", 0, 0, true},
		{"bigint", 0, false, false, "BIGINT", 0, 0, true},
		{"decimal", 0, false, false, "DECIMAL", 0, 0, true}, //TODO sizeable
		{"real", 0, false, false, "REAL", 0, 0, true},
		{"double", 0, false, false, "DOUBLE", 0, 0, true},
		{"char", 0, false, false, "CHAR", 0, 0, true},
		{"varchar", 30, true, false, "VARCHAR", 0, 0, true},
		{"nchar", 0, false, false, "NCHAR", 0, 0, true},
		{"nvarchar", 30, true, false, "NVARCHAR", 0, 0, true},
		{"binary", 0, false, false, "BINARY", 0, 0, true},
		{"varbinary", 10, true, false, "VARBINARY", 0, 0, true},
		{"date", 0, false, false, "DATE", 0, 0, true},
		{"time", 0, false, false, "TIME", 0, 0, true},
		{"timestamp", 0, false, false, "TIMESTAMP", 0, 0, true},
		{"clob", 0, false, false, "CLOB", 0, 0, true},
		{"nclob", 0, false, false, "NCLOB", 0, 0, true},
		{"blob", 0, false, false, "BLOB", 0, 0, true},
		{"boolean", 0, false, false, "TINYINT", 0, 0, true},      // hdb gives TINYINT back - not BOOLEAN
		{"smalldecimal", 0, false, false, "DECIMAL", 0, 0, true}, // hdb gives DECIMAL back - not SMALLDECIMAL
		{"text", 0, false, false, "NCLOB", 0, 0, true},           // hdb gives NCLOB back - not TEXT
		{"shorttext", 15, true, false, "NVARCHAR", 0, 0, true},   // hdb gives NVARCHAR back - not SHORTTEXT
		{"bintext", 0, false, false, "NCLOB", 0, 0, true},        // hdb gives NCLOB back - not BINTEXT
		{"alphanum", 12, true, false, "NVARCHAR", 0, 0, true},    // hdb gives NVARCHAR back - not ALPHANUM
		{"longdate", 0, false, false, "TIMESTAMP", 0, 0, true},   // hdb gives TIMESTAMP back - not LONGDATE
		{"seconddate", 0, false, false, "TIMESTAMP", 0, 0, true}, // hdb gives TIMESTAMP back - not SECONDDATE
		{"daydate", 0, false, false, "DATE", 0, 0, true},         // hdb gives DATE back - not DAYDATE
		{"secondtime", 0, false, false, "TIME", 0, 0, true},      // hdb gives TIME back - not SECONDTIME

		// not nullable
		{"tinyint", 0, false, false, "TINYINT", 0, 0, false},
		{"nvarchar", 25, true, false, "NVARCHAR", 0, 0, false},
	}

	// text, st_geometry, st_point is only supported for column table

	var createSql bytes.Buffer

	createSql.WriteString("create column table %s.%s (") // some data types are only valid for column tables
	for i, td := range testColumnTypeData {

		if i != 0 {
			createSql.WriteString(",")
		}

		createSql.WriteString(fmt.Sprintf("X%d %s", i, td.sqlType))
		if td.length != 0 {
			createSql.WriteString(fmt.Sprintf("(%d)", td.length))
		}
		if !td.nullable {
			createSql.WriteString(" not null")
		}
	}
	createSql.WriteString(")")

	t.Log(createSql.String())

	db, err := sql.Open(DriverName, TestDsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	table := RandomIdentifier("testColumnType_")
	if _, err := db.Exec(fmt.Sprintf(createSql.String(), TestSchema, table)); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s.%s", TestSchema, table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	for i, td := range testColumnTypeData {

		ct := cts[i]

		if td.typeName != ct.DatabaseTypeName() {
			t.Fatalf("index %d sql type %s type name %s - expected %s", i, td.sqlType, ct.DatabaseTypeName(), td.typeName)
		}

		length, ok := ct.Length()
		if td.varLength != ok {
			t.Fatalf("index %d sql type %s variable length %t - expected %t", i, td.sqlType, ok, td.varLength)
		}
		if td.length != length {
			t.Fatalf("index %d sql type %s length %d - expected %d", i, td.sqlType, length, td.length)
		}

		nullable, ok := ct.Nullable()
		if !ok {
			t.Fatalf("index %d sql type %s - nullable info is expected to be provided", i, td.sqlType)
		}
		if td.nullable != nullable {
			t.Fatalf("index %d sql type %s nullable %t - expected %t", i, td.sqlType, nullable, td.nullable)
		}

		//precision, scale, ok := ct.DecimalSize()
		//if td.

		/*

					sqlType string
			length  int64

			varLength bool
			sizeable  bool

			typeName  string
			precision int64
			scale     int64
			nullable  bool
		*/

	}

	/*
		for i, ct := range cts {
			t.Log(ct.DatabaseTypeName())
			precision, scale, ok := ct.DecimalSize()
			t.Logf("precision %d scale %d ok %t", precision, scale, ok)
			length, ok := ct.Length()
			t.Logf("lenght %d ok %t", length, ok)
			t.Log(ct.Name())
			t.Log(ct.ScanType().String())

		}
	*/
}
