//go:build !unit
// +build !unit

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// ExampleBulkInsert inserts 2000 rows into a database table:
// - 1000 rows are inserted via an extended argument list and
// - 1000 rows are inserted with the help of a argument function
func Example_bulkInsert() {
	// Number of rows to be inserted into table.
	numRow := 1000

	db := sql.OpenDB(driver.DefaultTestConnector())
	defer db.Close()

	tableName := driver.RandomIdentifier("table_")

	// Create table.
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, f double)", tableName)); err != nil {
		log.Fatal(err)
	}

	// Prepare statement.
	stmt, err := db.PrepareContext(context.Background(), fmt.Sprintf("insert into %s values (?, ?)", tableName))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// Bulk insert via 'extended' argument list.
	args := make([]any, numRow*2)
	for i := 0; i < numRow; i++ {
		args[i*2], args[i*2+1] = i, float64(i)
	}
	if _, err := stmt.Exec(args...); err != nil {
		log.Fatal(err)
	}

	// Bulk insert via function.
	i := 0
	if _, err := stmt.Exec(func(args []any) error {
		if i >= numRow {
			return driver.ErrEndOfRows
		}
		args[0], args[1] = i, float64(i)
		i++
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Select number of inserted rows.
	if err := db.QueryRow(fmt.Sprintf("select count(*) from %s", tableName)).Scan(&numRow); err != nil {
		log.Fatal(err)
	}
	fmt.Print(numRow)

	// Drop table.
	if _, err := db.Exec(fmt.Sprintf("drop table %s", tableName)); err != nil {
		log.Fatal(err)
	}

	// output: 2000
}
