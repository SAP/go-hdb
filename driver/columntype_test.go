//go:build !unit
// +build !unit

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

func TestColumnType(t *testing.T) {

	fields := func(types []columnType) string {
		if len(types) == 0 {
			return ""
		}

		b := strings.Builder{}
		b.WriteString(fmt.Sprintf("x0 %s", types[0].column()))
		for i := 1; i < len(types); i++ {
			b.WriteString(", ")
			b.WriteString(fmt.Sprintf("x%s %s", strconv.Itoa(i), types[i].column()))
		}
		return b.String()
	}

	vars := func(size int) string {
		if size == 0 {
			return ""
		}
		return strings.Repeat("?, ", size-1) + "?"
	}

	testColumnType := func(db *sql.DB, types []columnType, values []any, t *testing.T) {

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

			if types[i].databaseTypeName() != cmpType.DatabaseTypeName() {
				t.Fatalf("sql type %s type name %s - expected %s", types[i].typeName(), cmpType.DatabaseTypeName(), types[i].databaseTypeName())
			}

			cmpLength, cmpOk := cmpType.Length()
			length, ok := types[i].length()
			if cmpLength != length || cmpOk != ok {
				t.Fatalf("sql type %s variable length %t length %d - expected %t %d", types[i].typeName(), cmpOk, cmpLength, ok, length)
			}

			cmpPrecision, cmpScale, cmpOk := cmpType.DecimalSize()
			precision, scale, ok := types[i].precisionScale()
			if cmpPrecision != precision || cmpScale != scale || cmpOk != ok {
				t.Fatalf("sql type %s decimal %t precision %d scale %d - expected %t %d %d", types[i].typeName(), cmpOk, cmpPrecision, cmpScale, ok, precision, scale)

			}

			cmpNullable, cmpOk := cmpType.Nullable()
			nullable, ok := types[i].nullable()
			if cmpNullable != nullable || cmpOk != ok {
				t.Fatalf("sql type %s hasProperty %t nullable %t - expected %t %t", types[i].typeName(), cmpOk, cmpNullable, ok, nullable)
			}

			if cmpType.ScanType() != types[i].scanType() {
				t.Fatalf("sql type %s scan type %v - expected %v", types[i].typeName(), cmpType.ScanType(), types[i].scanType())
			}
		}
	}

	var (
		testDecimal = (*Decimal)(big.NewRat(1, 1))
		testString  = "HDB column type"
		testBinary  = []byte{0x00, 0x01, 0x02}
		testTime    = time.Now()
	)

	type testFields []struct {
		typ   columnType
		value any
	}

	getTestFields := func(version *Version, dfv int) testFields {
		return testFields{
			{&basicColumn{version, dfv, basicType[dtTinyint], true}, 1},
			{&basicColumn{version, dfv, basicType[dtSmallint], true}, 42},
			{&basicColumn{version, dfv, basicType[dtInteger], true}, 4711},
			{&basicColumn{version, dfv, basicType[dtBigint], true}, 68000},

			{&basicColumn{version, dfv, basicType[dtReal], true}, 1.0},
			{&basicColumn{version, dfv, basicType[dtDouble], true}, 3.14},

			{&basicColumn{version, dfv, basicType[dtDate], true}, testTime},
			{&basicColumn{version, dfv, basicType[dtTime], true}, testTime},
			{&basicColumn{version, dfv, basicType[dtTimestamp], true}, testTime},
			{&basicColumn{version, dfv, basicType[dtLongdate], true}, testTime},
			{&basicColumn{version, dfv, basicType[dtSeconddate], true}, testTime},
			{&basicColumn{version, dfv, basicType[dtDaydate], true}, testTime},
			{&basicColumn{version, dfv, basicType[dtSecondtime], true}, testTime},

			{&basicColumn{version, dfv, basicType[dtClob], true}, new(Lob).SetReader(bytes.NewBuffer(testBinary))},
			{&basicColumn{version, dfv, basicType[dtNClob], true}, new(Lob).SetReader(bytes.NewBuffer(testBinary))},
			{&basicColumn{version, dfv, basicType[dtBlob], true}, new(Lob).SetReader(bytes.NewBuffer(testBinary))},

			{&basicColumn{version, dfv, basicType[dtText], true}, new(Lob).SetReader(bytes.NewBuffer(testBinary))},
			{&basicColumn{version, dfv, basicType[dtBintext], true}, new(Lob).SetReader(bytes.NewBuffer(testBinary))},

			{&basicColumn{version, dfv, basicType[dtBoolean], true}, false},

			{&varColumn{version, dfv, varType[dtChar], true, 30}, testString},
			{&varColumn{version, dfv, varType[dtVarchar], true, 30}, testString},
			{&varColumn{version, dfv, varType[dtNChar], true, 20}, testString},
			{&varColumn{version, dfv, varType[dtNVarchar], true, 20}, testString},

			{&varColumn{version, dfv, varType[dtShorttext], true, 15}, testString},
			{&varColumn{version, dfv, varType[dtAlphanum], true, 15}, testString},

			{&varColumn{version, dfv, varType[dtBinary], true, 10}, testBinary},
			{&varColumn{version, dfv, varType[dtVarbinary], true, 10}, testBinary},

			{&decimalColumn{version, dfv, decimalType[dtDecimal], true, 0, 0}, testDecimal},  // decimal
			{&decimalColumn{version, dfv, decimalType[dtDecimal], true, 18, 2}, testDecimal}, // decimal(p,q) - fixed8  (beginning with dfv 8)
			{&decimalColumn{version, dfv, decimalType[dtDecimal], true, 28, 4}, testDecimal}, // decimal(p,q) - fixed12 (beginning with dfv 8)
			{&decimalColumn{version, dfv, decimalType[dtDecimal], true, 38, 8}, testDecimal}, // decimal(p,q) - fixed16 (beginning with dfv 8)

			{&decimalColumn{version, dfv, decimalType[dtSmalldecimal], true, 0, 0}, testDecimal}, // smalldecimal

			// TODO: insert with function (e.g. st_geomfromewkb(?))
			// {typ: datatypes.NewSpatialColumn(datatypes.DtSTPoint, 0), value: ""},
			// {typ: datatypes.NewSpatialColumn(datatypes.DtSTGeometry, 0), value: ""},

			// not nullable
			{&basicColumn{version, dfv, basicType[dtTinyint], false}, 42},
			{&varColumn{version, dfv, varType[dtVarchar], false, 25}, testString},
		}
	}

	for _, dfv := range p.SupportedDfvs(testing.Short()) {
		func(dfv int) { // new dfv to run in parallel
			name := fmt.Sprintf("dfv %d", dfv)
			t.Run(name, func(t *testing.T) {
				t.Parallel() // run in parallel to speed up

				connector := NewTestConnector()
				connector.SetDfv(int(dfv))
				db := sql.OpenDB(connector)
				defer db.Close()

				var version *Version
				// Grab connection to detect hdb version.
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				conn.Raw(func(driverConn any) error {
					version = driverConn.(Conn).HDBVersion()
					return nil
				})

				testFields := getTestFields(version, dfv)
				types := make([]columnType, 0, len(testFields))
				values := make([]any, 0, len(testFields))
				for _, field := range testFields {
					if field.typ.isSupported() {
						types = append(types, field.typ)
						values = append(values, field.value)
					}
				}
				testColumnType(db, types, values, t)
			})
		}(dfv)
	}
}
