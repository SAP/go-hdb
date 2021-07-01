// +build !unit

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/drivertest"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
	"golang.org/x/text/transform"
)

func setupEncodingTestTable(testData []string, t *testing.T) driver.Identifier {
	connector, err := driver.NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	tableName := driver.RandomIdentifier("cesuerror_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, s nvarchar(20))", tableName)); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare(fmt.Sprintf("insert into %s values(?, bintostr(?))", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i, s := range testData {
		if _, err := stmt.Exec(i, s); err != nil {
			t.Fatal(err)
		}
	}
	return tableName
}

func testDecodeError(tableName driver.Identifier, testData []string, t *testing.T) {
	connector, err := driver.NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("select * from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() { // will fail
		// ...
	}
	switch err := rows.Err(); err {
	case nil:
		t.Fatal("invalid cesu-8 error expected")
	default:
		t.Log(err) // just print the (expected) error
	}

}

func testDecodeErrorHandler(tableName driver.Identifier, testData []string, t *testing.T) {
	connector, err := driver.NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		t.Fatal(err)
	}

	// register decoder with replace error handler
	decoder := cesu8.NewDecoder(cesu8.ReplaceErrorHandler)
	connector.SetCESU8Decoder(func() transform.Transformer { return decoder })

	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("select * from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var (
		i int
		s string
	)

	resultData := []string{
		string([]byte{0x2b, 0x30, 0x1c, 0x39, 0xef, 0xbf, 0xbd, 0x32, 0x30, 0x60, 0x33}), // invalid sequence "eda2a811" gets replaces by replacement char "fffd" -> UTF-8 "efbfbd"
	}

	for rows.Next() {
		if err := rows.Scan(&i, &s); err != nil {
			t.Fatal(err)
		}
		if s != resultData[i] {
			t.Fatalf("record %d: invalid result %x - expected %x", i, s, resultData[i])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

}

func testDecodeRaw(tableName driver.Identifier, testData []string, t *testing.T) {
	connector, err := driver.NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		t.Fatal(err)
	}

	// register nop decoder to receive 'raw' undecoded data
	connector.SetCESU8Decoder(func() transform.Transformer { return transform.Nop })

	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("select * from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var (
		i int
		s string
	)

	for rows.Next() {
		if err := rows.Scan(&i, &s); err != nil {
			t.Fatal(err)
		}
		cmp, err := hex.DecodeString(testData[i])
		if err != nil {
			t.Fatal(err)
		}
		if s != string(cmp) {
			t.Fatalf("record %d: invalid result %x - expected %x", i, s, string(cmp))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

}

func TestEncoding(t *testing.T) {
	testData := []string{
		"2b301c39eda2a81132306033",
	}

	tableName := setupEncodingTestTable(testData, t)

	tests := []struct {
		name string
		fct  func(tableName driver.Identifier, testData []string, t *testing.T)
	}{
		{"testDecodeError", testDecodeError},
		{"testDecodeErrorHandler", testDecodeErrorHandler},
		{"testDecodeRaw", testDecodeRaw},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(tableName, testData, t)
		})
	}
}
