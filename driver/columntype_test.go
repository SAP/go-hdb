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
	"testing"
	"time"

	dt "github.com/SAP/go-hdb/driver/drivertest"
	p "github.com/SAP/go-hdb/internal/protocol"
)

type columnTypeTest struct {
	dfv  int
	cond int

	sqlType          string
	length, fraction int64

	varLength   bool
	decimalType bool

	typeName  string
	precision int64
	scale     int64
	nullable  bool
	scanType  reflect.Type

	value interface{}
}

func (t *columnTypeTest) checkRun(dfv int) bool {
	switch t.cond {
	default:
		return true
	case dt.CondEQ:
		return dfv == t.dfv
	case dt.CondGE:
		return dfv >= t.dfv
	case dt.CondLT:
		return dfv < t.dfv
	}
}

func (t *columnTypeTest) name() string {
	var nullable string
	if !t.nullable {
		nullable = "_notNull"
	}
	switch {
	case t.length != 0 && t.fraction != 0:
		return fmt.Sprintf("%s_%d_%d%s", t.sqlType, t.length, t.fraction, nullable)
	case t.length != 0:
		return fmt.Sprintf("%s_%d%s", t.sqlType, t.length, nullable)
	default:
		return fmt.Sprintf("%s%s", t.sqlType, nullable)
	}
}

func (t *columnTypeTest) column() string {
	var notNull string
	if !t.nullable {
		notNull = " not null"
	}
	switch {
	case t.length != 0 && t.fraction != 0:
		return fmt.Sprintf("%s(%d, %d)%s", t.sqlType, t.length, t.fraction, notNull)
	case t.length != 0:
		return fmt.Sprintf("%s(%d)%s", t.sqlType, t.length, notNull)
	default:
		return fmt.Sprintf("%s%s", t.sqlType, notNull)
	}
}

func TestColumnType(t *testing.T) {

	testColumnType := func(db *sql.DB, test *columnTypeTest, t *testing.T) {
		table := RandomIdentifier(fmt.Sprintf("%s_", test.name()))

		// some data types are only valid for column tables
		// e.g. text
		if _, err := db.Exec(fmt.Sprintf("create column table %s (x %s)", table, test.column())); err != nil {
			t.Fatal(err)
		}

		// use trancactions:
		// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Exec(fmt.Sprintf("insert into %s values (?)", table), test.value); err != nil {
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
		ct := cts[0]

		if test.typeName != ct.DatabaseTypeName() {
			t.Fatalf("sql type %s type name %s - expected %s", test.sqlType, ct.DatabaseTypeName(), test.typeName)
		}

		length, ok := ct.Length()
		if test.varLength != ok {
			t.Fatalf("sql type %s variable length %t - expected %t", test.sqlType, ok, test.varLength)
		}
		if test.varLength {
			if test.length != length {
				t.Fatalf("sql type %s length %d - expected %d", test.sqlType, length, test.length)
			}
		}

		precision, scale, ok := ct.DecimalSize()
		if test.decimalType != ok {
			t.Fatalf("sql type %s decimal %t - expected %t", test.sqlType, ok, test.decimalType)
		}
		if test.decimalType {
			if test.precision != precision {
				t.Fatalf("sql type %s precision %d - expected %d", test.sqlType, precision, test.precision)
			}
			if test.scale != scale {
				t.Fatalf("sql type %s scale %d - expected %d", test.sqlType, scale, test.scale)
			}
		}

		nullable, ok := ct.Nullable()
		if !ok {
			t.Fatalf("sql type %s - nullable info is expected to be provided", test.sqlType)
		}
		if test.nullable != nullable {
			t.Fatalf("sql type %s nullable %t - expected %t", test.sqlType, nullable, test.nullable)
		}

		if ct.ScanType() != test.scanType {
			t.Fatalf("sql type %s scan type %v - expected %v", test.sqlType, ct.ScanType(), test.scanType)
		}
	}

	var (
		testTime    = time.Now()
		testDecimal = (*Decimal)(big.NewRat(1, 1))
		testString  = "HDB column type"
		testBinary  = []byte{0x00, 0x01, 0x02}
	)

	tests := []*columnTypeTest{
		{DfvLevel1, dt.CondGE, "tinyint", 0, 0, false, false, "TINYINT", 0, 0, true, p.DtTinyint.ScanType(), 1},
		{DfvLevel1, dt.CondGE, "smallint", 0, 0, false, false, "SMALLINT", 0, 0, true, p.DtSmallint.ScanType(), 42},
		{DfvLevel1, dt.CondGE, "integer", 0, 0, false, false, "INTEGER", 0, 0, true, p.DtInteger.ScanType(), 4711},
		{DfvLevel1, dt.CondGE, "bigint", 0, 0, false, false, "BIGINT", 0, 0, true, p.DtBigint.ScanType(), 68000},
		{DfvLevel1, dt.CondGE, "decimal", 0, 0, false, true, "DECIMAL", 34, 32767, true, p.DtDecimal.ScanType(), testDecimal}, // decimal

		{DfvLevel8, dt.CondLT, "decimal", 18, 2, false, true, "DECIMAL", 18, 2, true, p.DtDecimal.ScanType(), testDecimal}, // decimal(p,q)
		{DfvLevel8, dt.CondLT, "decimal", 28, 4, false, true, "DECIMAL", 28, 4, true, p.DtDecimal.ScanType(), testDecimal}, // decimal(p,q)
		{DfvLevel8, dt.CondLT, "decimal", 38, 8, false, true, "DECIMAL", 38, 8, true, p.DtDecimal.ScanType(), testDecimal}, // decimal(p,q)

		{DfvLevel8, dt.CondGE, "decimal", 18, 2, false, true, "FIXED8", 18, 2, true, p.DtDecimal.ScanType(), testDecimal},  // decimal(p,q) - fixed8
		{DfvLevel8, dt.CondGE, "decimal", 28, 4, false, true, "FIXED12", 28, 4, true, p.DtDecimal.ScanType(), testDecimal}, // decimal(p,q) - fixed12
		{DfvLevel8, dt.CondGE, "decimal", 38, 8, false, true, "FIXED16", 38, 8, true, p.DtDecimal.ScanType(), testDecimal}, // decimal(p,q) - fixed16

		{DfvLevel1, dt.CondGE, "real", 0, 0, false, false, "REAL", 0, 0, true, p.DtReal.ScanType(), 1.0},
		{DfvLevel1, dt.CondGE, "double", 0, 0, false, false, "DOUBLE", 0, 0, true, p.DtDouble.ScanType(), 3.14},
		{DfvLevel1, dt.CondGE, "char", 30, 0, true, false, "CHAR", 0, 0, true, p.DtString.ScanType(), testString},
		{DfvLevel1, dt.CondGE, "varchar", 30, 0, true, false, "VARCHAR", 0, 0, true, p.DtString.ScanType(), testString},
		{DfvLevel1, dt.CondGE, "nchar", 20, 0, true, false, "NCHAR", 0, 0, true, p.DtString.ScanType(), testString},
		{DfvLevel1, dt.CondGE, "nvarchar", 20, 0, true, false, "NVARCHAR", 0, 0, true, p.DtString.ScanType(), testString},
		{DfvLevel1, dt.CondGE, "binary", 10, 0, true, false, "BINARY", 0, 0, true, p.DtBytes.ScanType(), testBinary},
		{DfvLevel1, dt.CondGE, "varbinary", 10, 0, true, false, "VARBINARY", 0, 0, true, p.DtBytes.ScanType(), testBinary},

		{DfvLevel3, dt.CondLT, "date", 0, 0, false, false, "DATE", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondLT, "time", 0, 0, false, false, "TIME", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondLT, "timestamp", 0, 0, false, false, "TIMESTAMP", 0, 0, true, p.DtTime.ScanType(), testTime},

		{DfvLevel3, dt.CondGE, "date", 0, 0, false, false, "DAYDATE", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondGE, "time", 0, 0, false, false, "SECONDTIME", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondGE, "timestamp", 0, 0, false, false, "LONGDATE", 0, 0, true, p.DtTime.ScanType(), testTime},

		{DfvLevel3, dt.CondLT, "longdate", 0, 0, false, false, "TIMESTAMP", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondLT, "seconddate", 0, 0, false, false, "TIMESTAMP", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondLT, "daydate", 0, 0, false, false, "DATE", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondLT, "secondtime", 0, 0, false, false, "TIME", 0, 0, true, p.DtTime.ScanType(), testTime},

		{DfvLevel3, dt.CondGE, "longdate", 0, 0, false, false, "LONGDATE", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondGE, "seconddate", 0, 0, false, false, "SECONDDATE", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondGE, "daydate", 0, 0, false, false, "DAYDATE", 0, 0, true, p.DtTime.ScanType(), testTime},
		{DfvLevel3, dt.CondGE, "secondtime", 0, 0, false, false, "SECONDTIME", 0, 0, true, p.DtTime.ScanType(), testTime},

		{DfvLevel1, dt.CondGE, "clob", 0, 0, false, false, "CLOB", 0, 0, true, p.DtLob.ScanType(), new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{DfvLevel1, dt.CondGE, "nclob", 0, 0, false, false, "NCLOB", 0, 0, true, p.DtLob.ScanType(), new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{DfvLevel1, dt.CondGE, "blob", 0, 0, false, false, "BLOB", 0, 0, true, p.DtLob.ScanType(), new(Lob).SetReader(bytes.NewBuffer(testBinary))},

		{DfvLevel7, dt.CondLT, "boolean", 0, 0, false, false, "TINYINT", 0, 0, true, p.DtTinyint.ScanType(), false},

		{DfvLevel7, dt.CondGE, "boolean", 0, 0, false, false, "BOOLEAN", 0, 0, true, p.DtBoolean.ScanType(), false},

		{DfvLevel1, dt.CondGE, "smalldecimal", 0, 0, false, true, "DECIMAL", 16, 32767, true, p.DtDecimal.ScanType(), testDecimal}, // hdb gives DECIMAL back - not SMALLDECIMAL
		//{"text", 0, false, false, "NCLOB", 0, 0, true, testLob},             // hdb gives NCLOB back - not TEXT

		{DfvLevel3, dt.CondLT, "shorttext", 15, 0, true, false, "NVARCHAR", 0, 0, true, p.DtString.ScanType(), testString},
		{DfvLevel3, dt.CondLT, "alphanum", 15, 0, true, false, "NVARCHAR", 0, 0, true, p.DtString.ScanType(), testString},

		{DfvLevel3, dt.CondGE, "shorttext", 15, 0, true, false, "SHORTTEXT", 0, 0, true, p.DtString.ScanType(), testString},
		{DfvLevel3, dt.CondGE, "alphanum", 15, 0, true, false, "ALPHANUM", 0, 0, true, p.DtString.ScanType(), testString},

		// not nullable
		{DfvLevel1, dt.CondGE, "tinyint", 0, 0, false, false, "TINYINT", 0, 0, false, p.DtTinyint.ScanType(), 42},
		{DfvLevel1, dt.CondGE, "nvarchar", 25, 0, true, false, "NVARCHAR", 0, 0, false, p.DtString.ScanType(), testString},
	}

	var testSet map[int]bool
	if testing.Short() {
		testSet = map[int]bool{DefaultDfv: true}
	} else {
		testSet = supportedDfvs
	}

	connector, err := NewConnector(dt.DefaultAttrs())
	if err != nil {
		t.Fatal(err)
	}

	for dfv := range testSet {
		name := fmt.Sprintf("dfv %d", dfv)
		t.Run(name, func(t *testing.T) {
			t.Parallel() // run in parallel to speed up
			connector.SetDfv(dfv)
			db := sql.OpenDB(connector)
			defer db.Close()
			for _, test := range tests {
				if test.checkRun(dfv) {
					t.Run(test.name(), func(t *testing.T) {
						testColumnType(db, test, t)
					})
				}
			}
		})
	}
}
