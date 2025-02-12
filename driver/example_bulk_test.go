//go:build !unit

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"log"
	"slices"

	"github.com/SAP/go-hdb/driver"
)

/*
ExampleBulkInsert inserts 2000 rows into a database table:

	1000 rows are inserted via an extended argument list and
	1000 rows are inserted with the help of a argument function
*/
func Example_bulkInsert() {
	const numRow = 1000 // Number of rows to be inserted into table.

	db := sql.OpenDB(driver.MT.Connector())
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
	for i := range numRow {
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
	var count int
	if err := db.QueryRow(fmt.Sprintf("select count(*) from %s", tableName)).Scan(&count); err != nil {
		log.Fatal(err)
	}
	fmt.Print(count)

	// Drop table.
	if _, err := db.Exec(fmt.Sprintf("drop table %s", tableName)); err != nil {
		log.Fatal(err)
	}

	// output: 2000
}

/*
ExampleBulkInsert inserts 3000 rows into a database table:

	1000 rows are inserted via a slices chunc iterator
	1000 rows are inserted via a custom iterator
*/
func Example_bulkInsertViaIterator() {
	const numRow = 1000 // Number of rows to be inserted into table.

	db := sql.OpenDB(driver.MT.Connector())
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

	// Bulk insert via slices chunc iterator.
	args := make([]any, numRow*2)
	for i := range numRow {
		args[i*2], args[i*2+1] = i, float64(i)
	}

	if _, err := stmt.Exec(slices.Chunk(args, 2)); err != nil {
		log.Fatal(err)
	}

	// Bulk insert via custom iterator.
	var myIter iter.Seq[[]any] = func(yield func([]any) bool) {
		for i := range numRow {
			if !yield([]any{i, float64(i)}) {
				return
			}
		}
	}

	if _, err := stmt.Exec(myIter); err != nil {
		log.Fatal(err)
	}

	// Select number of inserted rows.
	var count int
	if err := db.QueryRow(fmt.Sprintf("select count(*) from %s", tableName)).Scan(&count); err != nil {
		log.Fatal(err)
	}
	fmt.Print(count)

	// Drop table.
	if _, err := db.Exec(fmt.Sprintf("drop table %s", tableName)); err != nil {
		log.Fatal(err)
	}

	// output: 2000
}
