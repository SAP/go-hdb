//go:build !unit

package driver_test

import (
	"database/sql"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// ExampleDB shows hot to print extended database statistics with the help of
// function driver.OpenDB and a driver.DB object.
func ExampleDB() {
	// print default sql database statistics.
	db1 := sql.OpenDB(driver.DefaultTestConnector())
	log.Printf("waitDuration: %d", db1.Stats().WaitDuration) // print field waitDuration of default database statistics.
	db1.Close()

	// print extended go-hdb driver db statistics.
	db2 := driver.OpenDB(driver.DefaultTestConnector())
	log.Printf("waitDuration: %d", db2.Stats().WaitDuration)   // print field waitDuration of default database statistics.
	log.Printf("bytesWritten: %d", db2.ExStats().WrittenBytes) // print field bytesWritten of extended driver database statistics.
	db2.Close()
	// output:
}
