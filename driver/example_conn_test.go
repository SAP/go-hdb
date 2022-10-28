//go:build !unit
// +build !unit

package driver_test

import (
	"context"
	"database/sql"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// ExampleConn-HDBVersion shows how to retrieve hdb server info with the help of sql.Conn.Raw().
func ExampleConn_HDBVersion() {
	db := sql.OpenDB(driver.DefaultTestConnector())
	defer db.Close()

	// Grab connection.
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	conn.Raw(func(driverConn any) error {
		// Access driver.Conn methods.
		log.Printf("hdb version: %s", driverConn.(driver.Conn).HDBVersion())
		return nil
	})
	// output:
}

// ExampleConn-DBConnectInfo shows how to retrieve hdb DBConnectInfo with the help of sql.Conn.Raw().
func ExampleConn_DBConnectInfo() {
	db := sql.OpenDB(driver.DefaultTestConnector())
	defer db.Close()

	// Grab connection.
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Raw(func(driverConn any) error {
		// Access driver.Conn methods.
		ci, err := driverConn.(driver.Conn).DBConnectInfo(context.Background(), driverConn.(driver.Conn).DatabaseName())
		if err != nil {
			return err
		}
		log.Printf("db connect info: %s", ci)
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	// output:
}
