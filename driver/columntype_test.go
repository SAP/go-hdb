// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

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

	"github.com/SAP/go-hdb/driver/drivertest"
	p "github.com/SAP/go-hdb/internal/protocol"
)

func testColumnType(connector drivertest.Connector, dataType func(string, int) string, scanType func(reflect.Type, int) reflect.Type, dfv int, t *testing.T) {
	var (
		testTime    = time.Now()
		testDecimal = (*Decimal)(big.NewRat(1, 1))
		testString  = "HDB column type"
		testBinary  = []byte{0x00, 0x01, 0x02}
	)

	testColumnTypeData := []struct {
		sqlType string
		length  int64

		varLength   bool
		decimalType bool

		typeName  string
		precision int64
		scale     int64
		nullable  bool
		scanType  reflect.Type

		value interface{}
	}{
		{"tinyint", 0, false, false, "TINYINT", 0, 0, true, p.DtTinyint.ScanType(), 1},
		{"smallint", 0, false, false, "SMALLINT", 0, 0, true, p.DtSmallint.ScanType(), 42},
		{"integer", 0, false, false, "INTEGER", 0, 0, true, p.DtInteger.ScanType(), 4711},
		{"bigint", 0, false, false, "BIGINT", 0, 0, true, p.DtBigint.ScanType(), 68000},
		{"decimal", 0, false, true, "DECIMAL", 34, 32767, true, p.DtDecimal.ScanType(), testDecimal},
		{"real", 0, false, false, "REAL", 0, 0, true, p.DtReal.ScanType(), 1.0},
		{"double", 0, false, false, "DOUBLE", 0, 0, true, p.DtDouble.ScanType(), 3.14},
		{"char", 30, true, false, "CHAR", 0, 0, true, p.DtString.ScanType(), testString},
		{"varchar", 30, true, false, "VARCHAR", 0, 0, true, p.DtString.ScanType(), testString},
		{"nchar", 20, true, false, "NCHAR", 0, 0, true, p.DtString.ScanType(), testString},
		{"nvarchar", 20, true, false, "NVARCHAR", 0, 0, true, p.DtString.ScanType(), testString},
		{"binary", 10, true, false, "BINARY", 0, 0, true, p.DtBytes.ScanType(), testBinary},
		{"varbinary", 10, true, false, "VARBINARY", 0, 0, true, p.DtBytes.ScanType(), testBinary},
		{"date", 0, false, false, dataType("DAYDATE", dfv), 0, 0, true, p.DtTime.ScanType(), testTime},
		{"time", 0, false, false, dataType("SECONDTIME", dfv), 0, 0, true, p.DtTime.ScanType(), testTime},
		{"timestamp", 0, false, false, dataType("LONGDATE", dfv), 0, 0, true, p.DtTime.ScanType(), testTime},
		{"clob", 0, false, false, "CLOB", 0, 0, true, p.DtLob.ScanType(), new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{"nclob", 0, false, false, "NCLOB", 0, 0, true, p.DtLob.ScanType(), new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{"blob", 0, false, false, "BLOB", 0, 0, true, p.DtLob.ScanType(), new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{"boolean", 0, false, false, dataType("BOOLEAN", dfv), 0, 0, true, scanType(p.DtBoolean.ScanType(), dfv), false},
		{"smalldecimal", 0, false, true, "DECIMAL", 16, 32767, true, p.DtDecimal.ScanType(), testDecimal}, // hdb gives DECIMAL back - not SMALLDECIMAL
		//{"text", 0, false, false, "NCLOB", 0, 0, true, testLob},             // hdb gives NCLOB back - not TEXT
		{"shorttext", 15, true, false, dataType("SHORTTEXT", dfv), 0, 0, true, p.DtString.ScanType(), testString},
		{"alphanum", 15, true, false, dataType("ALPHANUM", dfv), 0, 0, true, p.DtString.ScanType(), testString},
		{"longdate", 0, false, false, dataType("LONGDATE", dfv), 0, 0, true, p.DtTime.ScanType(), testTime},
		{"seconddate", 0, false, false, dataType("SECONDDATE", dfv), 0, 0, true, p.DtTime.ScanType(), testTime},
		{"daydate", 0, false, false, dataType("DAYDATE", dfv), 0, 0, true, p.DtTime.ScanType(), testTime},
		{"secondtime", 0, false, false, dataType("SECONDTIME", dfv), 0, 0, true, p.DtTime.ScanType(), testTime},

		// not nullable
		{"tinyint", 0, false, false, "TINYINT", 0, 0, false, p.DtTinyint.ScanType(), 42},
		{"nvarchar", 25, true, false, "NVARCHAR", 0, 0, false, p.DtString.ScanType(), testString},
	}

	// text is only supported for column table
	var createSQL bytes.Buffer
	table := RandomIdentifier("testColumnType_")

	createSQL.WriteString(fmt.Sprintf("create column table %s (", table)) // some data types are only valid for column tables
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

	db := sql.OpenDB(connector)
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

	if _, err := tx.Exec(fmt.Sprintf("insert into %s values (%s)", table, prms), args...); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s", table))
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

func TestColumnType(t *testing.T) {
	dataType := func(dt string, dfv int) string {
		switch {
		case dfv < DfvLevel3:
			switch dt {
			case "DAYDATE":
				return "DATE"
			case "SECONDTIME":
				return "TIME"
			case "LONGDATE", "SECONDDATE":
				return "TIMESTAMP"
			case "SHORTTEXT", "ALPHANUM":
				return "NVARCHAR"
			}
			fallthrough
		case dfv < DfvLevel7:
			switch dt {
			case "BOOLEAN":
				return "TINYINT"
			}
			// fallthrough
		}
		return dt
	}

	scanType := func(rt reflect.Type, dfv int) reflect.Type {
		switch {
		case dfv < DfvLevel7:
			switch rt {
			case p.DtBoolean.ScanType():
				return p.DtTinyint.ScanType()
			}
			// fallthrough
		}
		return rt
	}

	var testSet map[int]bool
	if testing.Short() {
		testSet = map[int]bool{DefaultDfv: true}
	} else {
		testSet = supportedDfvs
	}

	connector, err := drivertest.DefaultConnector(NewConnector())
	if err != nil {
		t.Fatal(err)
	}

	for dfv := range testSet {
		name := fmt.Sprintf("dfv_%d", dfv)
		t.Run(name, func(t *testing.T) {
			connector.SetDfv(dfv)
			testColumnType(connector, dataType, scanType, dfv, t)
		})
	}
}
