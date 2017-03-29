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
	"math/big"
	"strings"
	"testing"
	"time"
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

	value interface{}
}

var (
	testTime    = time.Now()
	testDecimal = (*Decimal)(big.NewRat(1, 1))
	testString  = "HDB column type"
	testBinary  = []byte{0x00, 0x01, 0x02}
	testBuffer  = bytes.NewBuffer(testBinary)
	testLob     = new(Lob)
)

func init() {
	testLob.SetReader(testBuffer)
}

func TestColumnType(t *testing.T) {

	var testColumnTypeData = []testColumnType{
		{"tinyint", 0, false, false, "TINYINT", 0, 0, true, 1},
		{"smallint", 0, false, false, "SMALLINT", 0, 0, true, 42},
		{"integer", 0, false, false, "INTEGER", 0, 0, true, 4711},
		{"bigint", 0, false, false, "BIGINT", 0, 0, true, 68000},
		{"decimal", 0, false, false, "DECIMAL", 0, 0, true, testDecimal}, //TODO sizeable
		{"real", 0, false, false, "REAL", 0, 0, true, 1.0},
		{"double", 0, false, false, "DOUBLE", 0, 0, true, 3.14},
		{"char", 0, false, false, "CHAR", 0, 0, true, "A"},
		{"varchar", 30, true, false, "VARCHAR", 0, 0, true, testString},
		{"nchar", 0, false, false, "NCHAR", 0, 0, true, "Z"},
		{"nvarchar", 30, true, false, "NVARCHAR", 0, 0, true, testString},
		{"binary", 0, false, false, "BINARY", 0, 0, true, testBinary},
		{"varbinary", 10, true, false, "VARBINARY", 0, 0, true, testBinary},
		{"date", 0, false, false, "DATE", 0, 0, true, testTime},
		{"time", 0, false, false, "TIME", 0, 0, true, testTime},
		{"timestamp", 0, false, false, "TIMESTAMP", 0, 0, true, testTime},
		{"clob", 0, false, false, "CLOB", 0, 0, true, testLob},
		{"nclob", 0, false, false, "NCLOB", 0, 0, true, testLob},
		{"blob", 0, false, false, "BLOB", 0, 0, true, testLob},
		{"boolean", 0, false, false, "TINYINT", 0, 0, true, false},            // hdb gives TINYINT back - not BOOLEAN
		{"smalldecimal", 0, false, false, "DECIMAL", 0, 0, true, testDecimal}, // hdb gives DECIMAL back - not SMALLDECIMAL
		{"text", 0, false, false, "NCLOB", 0, 0, true, testLob},               // hdb gives NCLOB back - not TEXT
		{"shorttext", 15, true, false, "NVARCHAR", 0, 0, true, testString},    // hdb gives NVARCHAR back - not SHORTTEXT
		{"bintext", 0, false, false, "NCLOB", 0, 0, true, testLob},            // hdb gives NCLOB back - not BINTEXT
		{"alphanum", 12, true, false, "NVARCHAR", 0, 0, true, testString},     // hdb gives NVARCHAR back - not ALPHANUM
		{"longdate", 0, false, false, "TIMESTAMP", 0, 0, true, testTime},      // hdb gives TIMESTAMP back - not LONGDATE
		{"seconddate", 0, false, false, "TIMESTAMP", 0, 0, true, testTime},    // hdb gives TIMESTAMP back - not SECONDDATE
		{"daydate", 0, false, false, "DATE", 0, 0, true, testTime},            // hdb gives DATE back - not DAYDATE
		{"secondtime", 0, false, false, "TIME", 0, 0, true, testTime},         // hdb gives TIME back - not SECONDTIME

		// not nullable
		{"tinyint", 0, false, false, "TINYINT", 0, 0, false, 42},
		{"nvarchar", 25, true, false, "NVARCHAR", 0, 0, false, testString},
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

	args := make([]interface{}, len(testColumnTypeData))
	for i, td := range testColumnTypeData {
		args[i] = td.value
	}

	prms := strings.Repeat("?,", len(testColumnTypeData)-1) + "?"

	if _, err := db.Exec(fmt.Sprintf("insert into %s.%s values (%s)", TestSchema, table, prms), args...); err != nil {
		t.Fatal(err)
	}

	//	INSERT INTO T VALUES (1, 1, 'The first');

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
