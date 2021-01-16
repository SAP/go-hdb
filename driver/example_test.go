// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"database/sql"
	"log"

	// Register hdb driver.
	_ "github.com/SAP/go-hdb/driver"
)

const (
	driverName = "hdb"
	hdbDsn     = "hdb://user:password@host:port"
)

func Example() {
	db, err := sql.Open(driverName, hdbDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
}
