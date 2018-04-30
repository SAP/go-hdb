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
	"reflect"
	"strings"
	"testing"
	"time"
)

type testColumnType struct {
	sqlType string
	length  int64

	varLength   bool
	sizeable    bool
	decimalType bool

	typeName  string
	precision int64
	scale     int64
	nullable  bool
	scanType  reflect.Type

	value interface{}
}

func dataType(dt string) string {
	if driverDataFormatVersion == 1 {
		switch dt {
		case "DAYDATE":
			return "DATE"
		case "SECONDTIME":
			return "TIME"
		case "LONGDATE", "SECONDDATE":
			return "TIMESTAMP"
		}
	}
	return dt
}

func TestColumnType(t *testing.T) {

	var (
		testTime    = time.Now()
		testDecimal = (*Decimal)(big.NewRat(1, 1))
		testString  = "HDB column type"
		testBinary  = []byte{0x00, 0x01, 0x02}
		testBuffer  = bytes.NewBuffer(testBinary)
		testLob     = new(Lob)
	)

	testLob.SetReader(testBuffer)

	var testColumnTypeData = []testColumnType{
		{"tinyint", 0, false, false, false, "TINYINT", 0, 0, true, scanTypeTinyint, 1},
		{"smallint", 0, false, false, false, "SMALLINT", 0, 0, true, scanTypeSmallint, 42},
		{"integer", 0, false, false, false, "INTEGER", 0, 0, true, scanTypeInteger, 4711},
		{"bigint", 0, false, false, false, "BIGINT", 0, 0, true, scanTypeBigint, 68000},
		{"decimal", 0, false, false, true, "DECIMAL", 34, 32767, true, scanTypeDecimal, testDecimal},
		{"real", 0, false, false, false, "REAL", 0, 0, true, scanTypeReal, 1.0},
		{"double", 0, false, false, false, "DOUBLE", 0, 0, true, scanTypeDouble, 3.14},
		{"char", 30, true, false, false, "CHAR", 0, 0, true, scanTypeString, testString},
		{"varchar", 30, true, false, false, "VARCHAR", 0, 0, true, scanTypeString, testString},
		{"nchar", 20, true, false, false, "NCHAR", 0, 0, true, scanTypeString, testString},
		{"nvarchar", 20, true, false, false, "NVARCHAR", 0, 0, true, scanTypeString, testString},
		{"binary", 10, true, false, false, "BINARY", 0, 0, true, scanTypeBytes, testBinary},
		{"varbinary", 10, true, false, false, "VARBINARY", 0, 0, true, scanTypeBytes, testBinary},
		{"date", 0, false, false, false, dataType("DAYDATE"), 0, 0, true, scanTypeTime, testTime},
		{"time", 0, false, false, false, dataType("SECONDTIME"), 0, 0, true, scanTypeTime, testTime},
		{"timestamp", 0, false, false, false, dataType("LONGDATE"), 0, 0, true, scanTypeTime, testTime},
		{"clob", 0, false, false, false, "CLOB", 0, 0, true, scanTypeLob, testLob},
		{"nclob", 0, false, false, false, "NCLOB", 0, 0, true, scanTypeLob, testLob},
		{"blob", 0, false, false, false, "BLOB", 0, 0, true, scanTypeLob, testLob},
		{"boolean", 0, false, false, false, "TINYINT", 0, 0, true, scanTypeTinyint, false},                // hdb gives TINYINT back - not BOOLEAN
		{"smalldecimal", 0, false, false, true, "DECIMAL", 16, 32767, true, scanTypeDecimal, testDecimal}, // hdb gives DECIMAL back - not SMALLDECIMAL
		//{"text", 0, false, false, "NCLOB", 0, 0, true, testLob},             // hdb gives NCLOB back - not TEXT
		//{"shorttext", 15, true, false, "NVARCHAR", 0, 0, true, testString},  // hdb gives NVARCHAR back - not SHORTTEXT
		//{"alphanum", 15, true, false, "NVARCHAR", 0, 0, true, testString},   // hdb gives NVARCHAR back - not ALPHANUM
		{"longdate", 0, false, false, false, dataType("LONGDATE"), 0, 0, true, scanTypeTime, testTime},
		{"seconddate", 0, false, false, false, dataType("SECONDDATE"), 0, 0, true, scanTypeTime, testTime},
		{"daydate", 0, false, false, false, dataType("DAYDATE"), 0, 0, true, scanTypeTime, testTime},
		{"secondtime", 0, false, false, false, dataType("SECONDTIME"), 0, 0, true, scanTypeTime, testTime},

		// not nullable
		{"tinyint", 0, false, false, false, "TINYINT", 0, 0, false, scanTypeTinyint, 42},
		{"nvarchar", 25, true, false, false, "NVARCHAR", 0, 0, false, scanTypeString, testString},
	}

	// text is only supported for column table

	var createSQL bytes.Buffer
	table := RandomIdentifier("testColumnType_")

	createSQL.WriteString(fmt.Sprintf("create column table %s.%s (", TestSchema, table)) // some data types are only valid for column tables
	for i, td := range testColumnTypeData {

		if i != 0 {
			createSQL.WriteString(",")
		}

		createSQL.WriteString(fmt.Sprintf("X%d %s", i, td.sqlType))
		if td.length != 0 {
			createSQL.WriteString(fmt.Sprintf("(%d)", td.length))
		}
		if !td.nullable {
			createSQL.WriteString(" not null")
		}
	}
	createSQL.WriteString(")")

	db, err := sql.Open(DriverName, TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(createSQL.String()); err != nil {
		t.Fatal(err)
	}

	args := make([]interface{}, len(testColumnTypeData))
	for i, td := range testColumnTypeData {
		args[i] = td.value
	}

	prms := strings.Repeat("?,", len(testColumnTypeData)-1) + "?"

	// use trancactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(fmt.Sprintf("insert into %s.%s values (%s)", TestSchema, table, prms), args...); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
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

		precision, scale, ok := ct.DecimalSize()
		if td.decimalType != ok {
			t.Fatalf("index %d sql type %s decimal %t - expected %t", i, td.sqlType, ok, td.decimalType)
		}
		if td.precision != precision {
			t.Fatalf("index %d sql type %s precision %d - expected %d", i, td.sqlType, precision, td.precision)
		}
		if td.scale != scale {
			t.Fatalf("index %d sql type %s scale %d - expected %d", i, td.sqlType, scale, td.scale)
		}

		nullable, ok := ct.Nullable()
		if !ok {
			t.Fatalf("index %d sql type %s - nullable info is expected to be provided", i, td.sqlType)
		}
		if td.nullable != nullable {
			t.Fatalf("index %d sql type %s nullable %t - expected %t", i, td.sqlType, nullable, td.nullable)
		}

		if ct.ScanType() != td.scanType {
			t.Fatalf("index %d sql type %s scan type %v - expected %v", i, td.sqlType, ct.ScanType(), td.scanType)
		}
	}
}
