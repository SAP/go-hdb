// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// ExampleConnector shows how to open a database with the help of a connector.
func ExampleConn_raw() {
	db, err := sql.Open(driver.DriverName, driver.TestDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Grab connection.
	conn, err := db.Conn(context.Background())

	if err := conn.Raw(func(driverConn interface{}) error {
		conn, ok := driverConn.(*driver.Conn)
		if !ok {
			log.Fatal("connection does not implement *driver.Conn")
		}
		// Access driver.Conn methods.
		log.Printf("hdb version: %s", conn.ServerInfo().Version)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Make sure that the example is executed during test runs.
	fmt.Print("ok")

	// output: ok
}
