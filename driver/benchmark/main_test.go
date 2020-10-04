// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package benchmark

import (
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/drivertest"
)

// globals
var (
	DefaultTestConnector *driver.Connector
	NewTestConnector     func() *driver.Connector
)

func TestMain(m *testing.M) {
	dbTest := drivertest.NewDBTest()

	NewTestConnector = func() *driver.Connector {
		connector, err := driver.NewDSNConnector(dbTest.DSN())
		if err != nil {
			log.Fatal(err)
		}
		connector.SetDefaultSchema(driver.Identifier(dbTest.Schema()))
		connector.SetPingInterval(time.Duration(dbTest.PingInt()) * time.Millisecond)
		return connector
	}

	DefaultTestConnector = NewTestConnector()

	connector, err := driver.NewDSNConnector(dbTest.DSN())
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	//TestDB.SetMaxIdleConns(0)

	dbTest.Setup(db)
	exitCode := m.Run()
	dbTest.Teardown(db, exitCode == 0)
	os.Exit(exitCode)
}
