//go:build !unit

package driver

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"testing"
	"time"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/internal/types"
)

func TestColumnType(t *testing.T) {
	t.Parallel()

	columnDefs := func(types []types.Column) string {
		if len(types) == 0 {
			return ""
		}
		buf := []byte{'('}
		buf = append(buf, fmt.Sprintf("x0 %s", types[0].DataType())...)
		for i := 1; i < len(types); i++ {
			buf = append(buf, ',')
			buf = append(buf, fmt.Sprintf("x%s %s", strconv.Itoa(i), types[i].DataType())...)
		}
		buf = append(buf, ')')
		return string(buf)
	}

	placeholders := func(size int) string {
		if size == 0 {
			return ""
		}
		buf := []byte{'(', '?'}
		for i := 1; i < size; i++ {
			buf = append(buf, ",?"...)
		}
		buf = append(buf, ')')
		return string(buf)
	}

	testColumnType := func(t *testing.T, db *sql.DB, version, dfv int, types []types.Column, values []any) {

		tableName := RandomIdentifier(fmt.Sprintf("%s_", t.Name()))

		// some data types are only valid for column tables
		// e.g. text
		if _, err := db.Exec(fmt.Sprintf("create column table %s %s", tableName, columnDefs(types))); err != nil {
			t.Fatal(err)
		}

		// use trancactions:
		// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Exec(fmt.Sprintf("insert into %s values %s", tableName, placeholders(len(types))), values...); err != nil {
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

			if cmpType.DatabaseTypeName() != types[i].DatabaseTypeName(version, dfv) {
				t.Fatalf("sql type %s type name %s - expected %s", types[i].TypeName(), cmpType.DatabaseTypeName(), types[i].DatabaseTypeName(version, dfv))
			}

			cmpLength, cmpOk := cmpType.Length()
			length, ok := types[i].Length()
			if cmpLength != length || cmpOk != ok {
				t.Fatalf("sql type %s variable length %t length %d - expected %t %d", types[i].TypeName(), cmpOk, cmpLength, ok, length)
			}

			cmpPrecision, cmpScale, cmpOk := cmpType.DecimalSize()
			precision, scale, ok := types[i].PrecisionScale()
			if cmpPrecision != precision || cmpScale != scale || cmpOk != ok {
				t.Fatalf("sql type %s decimal %t precision %d scale %d - expected %t %d %d", types[i].TypeName(), cmpOk, cmpPrecision, cmpScale, ok, precision, scale)
			}

			cmpNullable, cmpOk := cmpType.Nullable()
			nullable, ok := types[i].Nullable()
			if cmpNullable != nullable || cmpOk != ok {
				t.Fatalf("sql type %s hasProperty %t nullable %t - expected %t %t", types[i].TypeName(), cmpOk, cmpNullable, ok, nullable)
			}

			if cmpType.ScanType() != types[i].ScanType(version, dfv) {
				t.Fatalf("sql type %s scan type %v - expected %v", types[i].TypeName(), cmpType.ScanType(), types[i].ScanType(version, dfv))
			}
		}
	}

	var (
		testDecimal = (*Decimal)(big.NewRat(1, 1))
		testString  = "HDB column type"
		testBinary  = []byte{0x00, 0x01, 0x02}
		testTime    = time.Now()
	)

	var testFields = []struct {
		typ   types.Column
		value any
	}{
		{types.NullTinyint, 1},
		{types.NullSmallint, 42},
		{types.NullInteger, 4711},
		{types.NullBigint, 68000},

		{types.NullReal, 1.0},
		{types.NullDouble, 3.14},

		{types.NullDate, testTime},
		{types.NullTime, testTime},
		{types.NullTimestamp, testTime},
		{types.NullLongdate, testTime},
		{types.NullSeconddate, testTime},
		{types.NullDaydate, testTime},
		{types.NullSecondtime, testTime},

		{types.NullClob, new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{types.NullNClob, new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{types.NullBlob, new(Lob).SetReader(bytes.NewBuffer(testBinary))},

		{types.NullText, new(Lob).SetReader(bytes.NewBuffer(testBinary))},
		{types.NullBintext, new(Lob).SetReader(bytes.NewBuffer(testBinary))},

		{types.Boolean, false},

		{types.NewNullChar(30), testString},
		{types.NewNullVarchar(30), testString},
		{types.NewNullNChar(20), testString},
		{types.NewNullNVarchar(20), testString},

		{types.NewNullShorttext(15), testString},
		{types.NewNullAlphanum(15), testString},

		{types.NewNullBinary(10), testBinary},
		{types.NewNullVarbinary(10), testBinary},

		{types.NewNullDecimal(0, 0), testDecimal},  // decimal
		{types.NewNullDecimal(18, 2), testDecimal}, // decimal(p,q) - fixed8  (beginning with dfv 8)
		{types.NewNullDecimal(28, 4), testDecimal}, // decimal(p,q) - fixed12 (beginning with dfv 8)
		{types.NewNullDecimal(38, 8), testDecimal}, // decimal(p,q) - fixed16 (beginning with dfv 8)

		{types.NewNullSmalldecimal(0, 0), testDecimal}, // smalldecimal

		// TODO: insert with function (e.g. st_geomfromewkb(?))
		// {typ: datatypes.NewSpatialColumn(datatypes.DtSTPoint, 0), value: ""},
		// {typ: datatypes.NewSpatialColumn(datatypes.DtSTGeometry, 0), value: ""},

		// not nullable
		{types.Tinyint, 42},
		{types.NewVarchar(25), testString},
	}

	version := int(MT.Version().Major())

	for _, dfv := range p.SupportedDfvs(testing.Short()) {
		dfv := dfv // new dfv to run in parallel

		name := fmt.Sprintf("dfv %d", dfv)
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			connector := MT.NewConnector()
			connector.SetDfv(dfv)
			db := sql.OpenDB(connector)
			defer db.Close()

			types := make([]types.Column, 0, len(testFields))
			values := make([]any, 0, len(testFields))
			for _, field := range testFields {
				if field.typ.IsSupported(version, dfv) {
					types = append(types, field.typ)
					values = append(values, field.value)
				}
			}
			testColumnType(t, db, version, dfv, types, values)
		})
	}
}
