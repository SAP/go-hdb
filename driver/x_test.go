//go:build !unit

package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
	"golang.org/x/text/transform"
)

// testInvalidCESU8 extracts invalid CESU-8 data out of a dedicated test database, schema and table
func testInvalidCESU8(t *testing.T) {
	const fieldName = "xref1_hd"
	var schemaName = driver.Identifier("SXSLTPBC")
	var tableName = driver.Identifier("BKPF")

	decoder := cesu8.NewDecoder(nil)

	connector := driver.NewTestConnector()
	// register nop decoder to receive 'raw' undecoded data
	connector.SetCESU8Decoder(func() transform.Transformer { return transform.Nop })

	db := sql.OpenDB(connector)
	defer db.Close()

	numRow := 0
	err := db.QueryRow(fmt.Sprintf("select count(*) from %[2]s.%[3]s where %[1]s<>''", fieldName, schemaName, tableName)).Scan(&numRow)
	switch {
	case err == sql.ErrNoRows:
		t.Logf("table %s.%s is empty", schemaName, tableName)
	case err != nil:
		t.Fatal(err)
	}
	t.Logf("number of rows: %d", numRow)

	rows, err := db.Query(fmt.Sprintf("select %[1]s from %[2]s.%[3]s where %[1]s<>''", fieldName, schemaName, tableName))
	if err != nil {
		t.Fatal(err)
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	for i, typ := range types {
		precision, scale, _ := typ.DecimalSize()
		length, _ := typ.Length()
		nullable, _ := typ.Nullable()

		t.Logf("field %d database type name %s name %s scan type %v precision %d scale %d length %d nullable %t", i, typ.DatabaseTypeName(), typ.Name(), typ.ScanType(), precision, scale, length, nullable)
	}

	i := 0
	for rows.Next() {
		var content string
		if err := rows.Scan(&content); err != nil {
			t.Fatal(err)
		}

		source := []byte(content)
		dest := make([]byte, len(source))
		_, _, err := decoder.Transform(dest, []byte(content), true)
		if err != nil {
			i++
			t.Logf("%[1]s:%[2]v", err, source)
		}

	}
	t.Logf("number of issues found: %d", i)

	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func testIncorrectDate(t *testing.T) {
	connector := driver.NewTestConnector()
	db := sql.OpenDB(connector)
	defer db.Close()

	tableName := driver.RandomIdentifier("table_")
	// fmt.Println(tableName)
	// Create table.
	if _, err := db.Exec(fmt.Sprintf("create table %s (a date)", tableName)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s values('0000-00-00')", tableName)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s values('0001-01-01')", tableName)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s values(NULL)", tableName)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s values('2020-10-10')", tableName)); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var date interface{}
	for rows.Next() {
		rows.Scan(&date)
		t.Log(date)
	}

	cnt := 0
	db.QueryRow(fmt.Sprintf("select count(*) from %s where A is NULL", tableName)).Scan(&cnt)
	t.Logf("number of NULL records %d\n", cnt)
}

// TestX has extended tests for specific systems
func TestX(t *testing.T) {
	tests := []struct {
		name    string
		fct     func(t *testing.T)
		enabled bool
	}{
		{"invalid cesu-8", testInvalidCESU8, false},
		{"test incorrect date", testIncorrectDate, false},
	}

	anyTestEnabled := func() bool {
		for _, test := range tests {
			if test.enabled {
				return true
			}
		}
		return false
	}()

	if len(tests) == 0 || !anyTestEnabled {
		t.Skip("skipping tests - no test available or enabled")
	}

	for _, test := range tests {
		if test.enabled {
			t.Run(test.name, func(t *testing.T) {
				test.fct(t)
			})
		}
	}
}
