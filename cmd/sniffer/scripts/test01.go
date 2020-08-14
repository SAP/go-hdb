// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/SAP/go-hdb/driver"
)

func main() {
	dsn := os.Getenv("GOHDBDSN")
	db, err := sql.Open(driver.DriverName, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Query("select * from T1")
	if err != nil {
		log.Fatal(err)
	}
}
