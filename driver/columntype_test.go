// +build !unit

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver/common"
	drvtst "github.com/SAP/go-hdb/driver/drivertest"
)

func TestColumnType(t *testing.T) {

	fields := func(types []drvtst.ColumnType) string {
		if len(types) == 0 {
			return ""
		}

		b := strings.Builder{}
		b.WriteString(fmt.Sprintf("x0 %s", types[0].Column()))
		for i := 1; i < len(types); i++ {
			b.WriteString(", ")
			b.WriteString(fmt.Sprintf("x%s %s", strconv.Itoa(i), types[i].Column()))
		}
		return b.String()
	}

	vars := func(size int) string {
		if size == 0 {
			return ""
		}
		return strings.Repeat("?, ", size-1) + "?"
	}

	testColumnType := func(db *sql.DB, version *common.HDBVersion, dfv int, types []drvtst.ColumnType, values []interface{}, t *testing.T) {

		tableName := RandomIdentifier(fmt.Sprintf("%s_", t.Name()))

		// some data types are only valid for column tables
		// e.g. text
		if _, err := db.Exec(fmt.Sprintf("create column table %s (%s)", tableName, fields(types))); err != nil {
			t.Fatal(err)
		}

		// use trancactions:
		// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Exec(fmt.Sprintf("insert into %s values (%s)", tableName, vars(len(types))), values...); err != nil {
			t.Fatal(err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		rows, err := db.Query(fmt.Sprintf("select * from %s", tableName))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		cmpTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}

		for i, cmpType := range cmpTypes {

			if types[i].DatabaseTypeName(version, dfv) != cmpType.DatabaseTypeName() {
				t.Fatalf("sql type %s type name %s - expected %s", types[i].TypeName(), cmpType.DatabaseTypeName(), types[i].DatabaseTypeName(version, dfv))
			}

			cmpLength, cmpOk := cmpType.Length()
			length, ok := types[i].Length()
			if cmpLength != length || cmpOk != ok {
				t.Fatalf("sql type %s variable length %t length %d - expected %t %d", types[i].TypeName(), cmpOk, cmpLength, ok, length)
			}

			cmpPrecision, cmpScale, cmpOk := cmpType.DecimalSize()
			precision, scale, ok := types[i].DecimalSize()
			if cmpPrecision != precision || cmpScale != scale || cmpOk != ok {
				t.Fatalf("sql type %s decimal %t precision %d scale %d - expected %t %d %d", types[i].TypeName(), cmpOk, cmpPrecision, cmpScale, ok, precision, scale)

			}

			cmpNullable, cmpOk := cmpType.Nullable()
			nullable, ok := types[i].Nullable()
			if cmpNullable != nullable || cmpOk != ok {
				t.Fatalf("sql type %s hasProperty %t nullable %t - expected %t %t", types[i].TypeName(), cmpOk, cmpNullable, ok, nullable)
			}

			if cmpType.ScanType() != types[i].ScanType(dfv) {
				t.Fatalf("sql type %s scan type %v - expected %v", types[i].TypeName(), cmpType.ScanType(), types[i].ScanType(dfv))
			}
		}
	}

	var (
		testDecimal = (*Decimal)(big.NewRat(1, 1))
		testString  = "HDB column type"
		testBinary  = []byte{0x00, 0x01, 0x02}
		testTime    = time.Now()
	)

	testFields := []struct {
		typ   drvtst.ColumnType
		value interface{}
	}{
		{typ: drvtst.NewStdColumn(drvtst.DtTinyint), value: 1},
		{typ: drvtst.NewStdColumn(drvtst.DtSmallint), value: 42},
		{typ: drvtst.NewStdColumn(drvtst.DtInteger), value: 4711},
		{typ: drvtst.NewStdColumn(drvtst.DtBigint), value: 68000},

		{typ: drvtst.NewDecimalColumn(drvtst.DtDecimal, 0, 0), value: testDecimal},  // decimal
		{typ: drvtst.NewDecimalColumn(drvtst.DtDecimal, 18, 2), value: testDecimal}, // decimal(p,q) - fixed8  (beginning with dfv 8)
		{typ: drvtst.NewDecimalColumn(drvtst.DtDecimal, 28, 4), value: testDecimal}, // decimal(p,q) - fixed12 (beginning with dfv 8)
		{typ: drvtst.NewDecimalColumn(drvtst.DtDecimal, 38, 8), value: testDecimal}, // decimal(p,q) - fixed16 (beginning with dfv 8)

		{typ: drvtst.NewDecimalColumn(drvtst.DtSmalldecimal, 0, 0), value: testDecimal}, // smalldecimal

		{typ: drvtst.NewStdColumn(drvtst.DtReal), value: 1.0},
		{typ: drvtst.NewStdColumn(drvtst.DtDouble), value: 3.14},

		{typ: drvtst.NewVarColumn(drvtst.DtChar, 30), value: testString},
		{typ: drvtst.NewVarColumn(drvtst.DtVarchar, 30), value: testString},
		{typ: drvtst.NewVarColumn(drvtst.DtNChar, 20), value: testString},
		{typ: drvtst.NewVarColumn(drvtst.DtNVarchar, 20), value: testString},

		{typ: drvtst.NewVarColumn(drvtst.DtShorttext, 15), value: testString},
		{typ: drvtst.NewVarColumn(drvtst.DtAlphanum, 15), value: testString},

		{typ: drvtst.NewVarColumn(drvtst.DtBinary, 10), value: testBinary},
		{typ: drvtst.NewVarColumn(drvtst.DtVarbinary, 10), value: testBinary},

		{typ: drvtst.NewStdColumn(drvtst.DtDate), value: testTime},
		{typ: drvtst.NewStdColumn(drvtst.DtTime), value: testTime},
		{typ: drvtst.NewStdColumn(drvtst.DtTimestamp), value: testTime},
		{typ: drvtst.NewStdColumn(drvtst.DtLongdate), value: testTime},
		{typ: drvtst.NewStdColumn(drvtst.DtSeconddate), value: testTime},
		{typ: drvtst.NewStdColumn(drvtst.DtDaydate), value: testTime},
		{typ: drvtst.NewStdColumn(drvtst.DtSecondtime), value: testTime},

		{typ: drvtst.NewStdColumn(drvtst.DtClob), value: new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{typ: drvtst.NewStdColumn(drvtst.DtNClob), value: new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{typ: drvtst.NewStdColumn(drvtst.DtBlob), value: new(Lob).SetReader(bytes.NewBuffer(testBinary))},

		{typ: drvtst.NewStdColumn(drvtst.DtText), value: new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{typ: drvtst.NewStdColumn(drvtst.DtBintext), value: new(Lob).SetReader(bytes.NewBuffer(testBinary))},

		{typ: drvtst.NewStdColumn(drvtst.DtBoolean), value: false},

		// TODO: insert with function (e.g. st_geomfromewkb(?))
		// {typ: drvtst.NewSpatialColumn(drvtst.DtSTPoint, 0), value: ""},
		// {typ: drvtst.NewSpatialColumn(drvtst.DtSTGeometry, 0), value: ""},

		// not nullable
		{typ: drvtst.NewStdColumn(drvtst.DtTinyint).SetNullable(false), value: 42},
		{typ: drvtst.NewVarColumn(drvtst.DtVarchar, 25).SetNullable(false), value: testString},
	}

	dfvs := []int{DefaultDfv}
	if !testing.Short() {
		dfvs = common.SupportedDfvs
	}

	for _, dfv := range dfvs {
		func(dfv int) { // new dfv to run in parallel
			name := fmt.Sprintf("dfv %d", dfv)
			t.Run(name, func(t *testing.T) {
				t.Parallel() // run in parallel to speed up

				connector, err := NewConnector(drvtst.DefaultAttrs())
				if err != nil {
					t.Fatal(err)
				}
				connector.SetDfv(int(dfv))
				db := sql.OpenDB(connector)
				defer db.Close()

				version, err := drvtst.HDBVersion(db)
				if err != nil {
					t.Fatal(err)
				}

				types := make([]drvtst.ColumnType, 0, len(testFields))
				values := make([]interface{}, 0, len(testFields))
				for _, field := range testFields {
					if field.typ.IsSupportedHDBVersion(version) && field.typ.IsSupportedDfv(dfv) {
						types = append(types, field.typ)
						values = append(values, field.value)
					}
				}
				testColumnType(db, version, dfv, types, values, t)
			})
		}(dfv)
	}
}
