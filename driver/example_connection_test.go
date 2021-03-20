// +build !unit

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/drivertest"
)

// ExampleConn-ServerInfo shows how to retrieve hdb server info with the help of sql.Conn.Raw().
func ExampleConn_ServerInfo() {
	connector, err := driver.NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	// Grab connection.
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err)
	}

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
