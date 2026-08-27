//go:build !unit

package driver

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/coltest"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// testNegative asserts that every value fails to insert. Run across all dfvs because
// nchar/nvarchar dispatch to different encode paths per dfv - the invalid input must be
// rejected on every one of them.
func testNegative(t *testing.T, db *sql.DB, column coltest.Type, testData []any) {
	tableName := RandomIdentifier(column.DataType() + "_")
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (x %s, i integer)", tableName, column.DataType())); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values(?, ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i, in := range testData {
		if _, err := stmt.ExecContext(t.Context(), in, i); err == nil { // error expected
			t.Fatalf("type: %s - %d - error expected", column.TypeName(), i)
		} else {
			t.Logf("type: %[1]s - %[2]d - %[3]T - %[3]s", column.TypeName(), i, err)
		}
	}
}

func TestDataTypeNegative(t *testing.T) {
	t.Parallel()

	// invalidUnicodeTestData holds byte sequences that are not valid unicode / CESU-8 (e.g. an
	// encoded high surrogate). HANA must reject these on insert into nchar/nvarchar columns.
	invalidUnicodeTestData := []any{
		string([]byte{0xed, 0xa2, 0xa8}),
		string([]byte{43, 48, 28, 57, 237, 162, 168, 17, 50, 48, 96, 51}),
	}

	type test struct {
		column   coltest.Type
		testData []any
	}
	tests := []test{
		{coltest.NewNullNChar(20), invalidUnicodeTestData},
		{coltest.NewNullNVarchar(20), invalidUnicodeTestData},
	}

	version := MT.Version().Major()

	for _, dfv := range p.SupportedDfvs(testing.Short()) {
		t.Run(fmt.Sprintf("dfv %d", dfv), func(t *testing.T) {
			t.Parallel()

			connector := MT.NewConnector()
			connector.SetDfv(dfv)
			db := sql.OpenDB(connector)
			db.SetMaxIdleConns(10)
			t.Cleanup(func() { db.Close() })

			for i, test := range tests {
				if test.column.IsSupported(version, dfv) {
					t.Run(fmt.Sprintf("%s %d", test.column.DataType(), i), func(t *testing.T) {
						t.Parallel()
						testNegative(t, db, test.column, test.testData)
					})
				}
			}
		})
	}
}
