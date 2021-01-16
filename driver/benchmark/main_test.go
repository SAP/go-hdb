// +build !unit

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package benchmark

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"testing"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/drivertest"
)

func TestMain(m *testing.M) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	if !flag.Parsed() {
		flag.Parse()
	}

	connector, err := driver.NewDSNConnector(drivertest.DSN())
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	// TestDB.SetMaxIdleConns(0)

	drivertest.Setup(db)
	exitCode := m.Run()
	drivertest.Teardown(db, exitCode == 0)
	db.Close()
	os.Exit(exitCode)
}
