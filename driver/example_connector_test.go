// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"database/sql"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// ExampleConnector shows how to open a database with the help of a connector.
func ExampleConnector() {
	connector := driver.NewBasicAuthConnector("host:port", "username", "password")
	connector.SetTimeout(60)
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
}
