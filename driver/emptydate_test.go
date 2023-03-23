//go:build !unit

package driver

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver/internal/build"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

func testEmptyDate(t *testing.T, tableName Identifier) {
	var nt sql.NullTime
	var emptyDate = time.Date(0, time.December, 31, 0, 0, 0, 0, time.UTC)

	for _, dfv := range p.SupportedDfvs(testing.Short()) {
		connector := NewTestConnector()
		connector.SetDfv(dfv)
		db := sql.OpenDB(connector)

		// Query db.
		rows, err := db.Query(fmt.Sprintf("select * from %s", tableName))
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			if err := rows.Scan(&nt); err != nil {
				t.Fatal(err)
			}
			break
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}

		// dfv == 1 -> empty date equals NULL, else depends on build tag
		if dfv == p.DfvLevel1 || build.EmptyDateAsNull {
			if nt.Valid != false {
				t.Fatalf("dfv %d time %v columnType %v", dfv, nt, columnTypes[0].DatabaseTypeName())
			}
		} else {
			if !nt.Time.Equal(emptyDate) {
				t.Fatalf("dfv %d time %v columnType %v", dfv, nt, columnTypes[0].DatabaseTypeName())
			}
		}
		db.Close()
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

	testEmptyDate(t, tableName)
}
