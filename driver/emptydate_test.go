//go:build !unit

package driver

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

func testEmptyDate(t *testing.T, tableName Identifier, dfv int, emptyDateAsNull bool) {
	var nt sql.NullTime
	var emptyDate = time.Date(0, time.December, 31, 0, 0, 0, 0, time.UTC)

	connector := NewTestConnector()
	connector.SetDfv(dfv)
	connector.SetEmptyDateAsNull(emptyDateAsNull)
	db := sql.OpenDB(connector)
	defer db.Close()

	// Query db.
	rows, err := db.Query(fmt.Sprintf("select * from %s", tableName))
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&nt); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	// dfv == 1 -> empty date equals NULL, else depends on build tag
	if dfv == p.DfvLevel1 || emptyDateAsNull {
		if nt.Valid != false {
			t.Fatalf("dfv %d time %v columnType %v", dfv, nt, columnTypes[0].DatabaseTypeName())
		}
	} else {
		if !nt.Time.Equal(emptyDate) {
			t.Fatalf("dfv %d time %v columnType %v", dfv, nt, columnTypes[0].DatabaseTypeName())
		}
	}
}

func TestEmptyDate(t *testing.T) {

	tableName := RandomIdentifier("table_")

	db := sql.OpenDB(DefaultTestConnector())
	defer db.Close()

	// Create table.
	if _, err := db.Exec(fmt.Sprintf("create table %s (d date)", tableName)); err != nil {
		log.Fatal(err)
	}
	// Insert empty date value.
	if _, err := db.Exec(fmt.Sprintf("insert into %s values ('0000-00-00')", tableName)); err != nil {
		log.Fatal(err)
	}

	for _, dfv := range p.SupportedDfvs(testing.Short()) {
		func(dfv int) { // new dfv to run in parallel
			t.Run(fmt.Sprintf("dfv %d emptyDateAsNull %t", dfv, false), func(t *testing.T) {
				t.Parallel() // run in parallel to speed up
				testEmptyDate(t, tableName, dfv, false)
			})
			t.Run(fmt.Sprintf("dfv %d emptyDateAsNull %t", dfv, true), func(t *testing.T) {
				t.Parallel() // run in parallel to speed up
				testEmptyDate(t, tableName, dfv, true)
			})
		}(dfv)
	}
}
