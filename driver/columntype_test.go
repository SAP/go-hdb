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

	"github.com/SAP/go-hdb/driver/internal/coltest"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

func TestColumnType(t *testing.T) {
	t.Parallel()

	columnDefs := func(types []coltest.Type) string {
		if len(types) == 0 {
			return ""
		}
		buf := []byte{'('}
		buf = append(buf, "x0 "+types[0].DataType()...)
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

	compareColumnTypes := func(ct ColumnType, c coltest.Type, version uint64, dfv int) error {
		if ct.DatabaseTypeName() != c.DatabaseTypeName(version, dfv) {
			return fmt.Errorf("sql type %s type name %s - expected %s", c.TypeName(), ct.DatabaseTypeName(), c.DatabaseTypeName(version, dfv))
		}

		ctLength, ctOk := ct.Length()
		length, ok := c.Length()
		if length != ctLength || ok != ctOk {
			return fmt.Errorf("sql type %s variable length %t length %d - expected %t %d", c.TypeName(), ctOk, ctLength, ok, length)
		}

		ctPrecision, ctScale, ctOk := ct.DecimalSize()
		precision, scale, ok := c.PrecisionScale()
		if ctPrecision != precision || ctScale != scale || ctOk != ok {
			return fmt.Errorf("sql type %s decimal %t precision %d scale %d - expected %t %d %d", c.TypeName(), ctOk, ctPrecision, ctScale, ok, precision, scale)
		}

		ctNullable, ctOk := ct.Nullable()
		nullable, ok := c.Nullable()
		if ctNullable != nullable || ctOk != ok {
			return fmt.Errorf("sql type %s hasProperty %t nullable %t - expected %t %t", c.TypeName(), ctOk, ctNullable, ok, nullable)
		}

		if ct.ScanType() != c.ScanType(version, dfv) {
			return fmt.Errorf("sql type %s scan type %v - expected %v", c.TypeName(), ct.ScanType(), c.ScanType(version, dfv))
		}

		return nil
	}

	testColumnType := func(t *testing.T, db *sql.DB, version uint64, dfv int, types []coltest.Type, values []any) {

		tableName := RandomIdentifier(t.Name() + "_")

		// some data types are only valid for column tables
		// e.g. text
		if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create column table %s %s", tableName, columnDefs(types))); err != nil {
			t.Fatal(err)
		}

		// use transactions:
		// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.ExecContext(t.Context(), fmt.Sprintf("insert into %s values %s", tableName, placeholders(len(types))), values...); err != nil {
			t.Fatal(err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		// retrieve statement metadata
		var stmtMetadata StmtMetadata
		ctx := WithStmtMetadata(t.Context(), &stmtMetadata)

		stmt, err := db.PrepareContext(ctx, fmt.Sprintf("select * from %s", tableName))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()

		rows, err := stmt.QueryContext(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		// compare with rows metadata
		cts, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}
		for i, ct := range cts {
			if err := compareColumnTypes(ct, types[i], version, dfv); err != nil {
				t.Fatal(err)
			}
		}
		// compare with statement metadata
		for i, ct := range stmtMetadata.ColumnTypes() {
			if err := compareColumnTypes(ct, types[i], version, dfv); err != nil {
				t.Fatal(err)
			}
		}
	}

	var (
		testDecimal = (*Decimal)(big.NewRat(1, 1))
		testString  = "HDB column type"
		testBinary  = []byte{0x00, 0x01, 0x02}
		testTime    = time.Now()
	)

	type testField struct {
		typ   coltest.Type
		value any
	}

	testFields := func() []testField {
		// create new set of fields to avoid race condition on bytes.Buffer.
		return []testField{
			{coltest.NullTinyint, 1},
			{coltest.NullSmallint, 42},
			{coltest.NullInteger, 4711},
			{coltest.NullBigint, 68000},

			{coltest.NullReal, 1.0},
			{coltest.NullDouble, 3.14},

			{coltest.NullDate, testTime},
			{coltest.NullTime, testTime},
			{coltest.NullTimestamp, testTime},
			{coltest.NullLongdate, testTime},
			{coltest.NullSeconddate, testTime},
			{coltest.NullDaydate, testTime},
			{coltest.NullSecondtime, testTime},

			{coltest.NullClob, new(Lob).SetReader(bytes.NewBuffer(testBinary))},
			{coltest.NullNClob, new(Lob).SetReader(bytes.NewBuffer(testBinary))},
			{coltest.NullBlob, new(Lob).SetReader(bytes.NewBuffer(testBinary))},

			{coltest.NullText, new(Lob).SetReader(bytes.NewBuffer(testBinary))},
			{coltest.NullBintext, new(Lob).SetReader(bytes.NewBuffer(testBinary))},

			{coltest.Boolean, false},

			{coltest.NewNullChar(30), testString},
			{coltest.NewNullVarchar(30), testString},
			{coltest.NewNullNChar(20), testString},
			{coltest.NewNullNVarchar(20), testString},

			{coltest.NewNullShorttext(15), testString},
			{coltest.NewNullAlphanum(15), testString},

			{coltest.NewNullBinary(10), testBinary},
			{coltest.NewNullVarbinary(10), testBinary},

			{coltest.NewNullDecimal(0, 0), testDecimal},  // decimal
			{coltest.NewNullDecimal(18, 2), testDecimal}, // decimal(p,q) - fixed8  (beginning with dfv 8)
			{coltest.NewNullDecimal(28, 4), testDecimal}, // decimal(p,q) - fixed12 (beginning with dfv 8)
			{coltest.NewNullDecimal(38, 8), testDecimal}, // decimal(p,q) - fixed16 (beginning with dfv 8)

			{coltest.NewNullSmalldecimal(0, 0), testDecimal}, // smalldecimal

			// TODO: insert with function (e.g. st_geomfromewkb(?))
			// {typ: datatypes.NewSpatialColumn(datatypes.DtSTPoint, 0), value: ""},
			// {typ: datatypes.NewSpatialColumn(datatypes.DtSTGeometry, 0), value: ""},

			// not nullable
			{coltest.Tinyint, 42},
			{coltest.NewVarchar(25), testString},
		}
	}

	version := MT.Version().Major()

	for _, dfv := range p.SupportedDfvs(testing.Short()) {
		name := fmt.Sprintf("dfv %d", dfv)
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			connector := MT.NewConnector()
			connector.SetDfv(dfv)
			db := sql.OpenDB(connector)
			defer db.Close()

			testFields := testFields()
			types := make([]coltest.Type, 0, len(testFields))
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
