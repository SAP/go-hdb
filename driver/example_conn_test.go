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

// ExampleConn-HDBVersion shows how to retrieve hdb server info with the help of sql.Conn.Raw().
func ExampleConn_HDBVersion() {
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

	conn.Raw(func(driverConn interface{}) error {
		// Access driver.Conn methods.
		log.Printf("hdb version: %s", driverConn.(driver.Conn).HDBVersion())
		return nil
	})

	// Make sure that the example is executed during test runs.
	fmt.Print("ok")

	// output: ok
}
