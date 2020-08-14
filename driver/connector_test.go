// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	goHdbDriver "github.com/SAP/go-hdb/driver"
)

func testConnector(connector driver.Connector, t *testing.T) {
	db := sql.OpenDB(connector)
	defer db.Close()

	var dummy string
	err := db.QueryRow("select * from dummy").Scan(&dummy)
	switch {
	case err == sql.ErrNoRows:
		t.Fatal(err)
	case err != nil:
		t.Fatal(err)
	}
	if dummy != "X" {
		t.Fatalf("dummy is %s - expected %s", dummy, "X")
	}
}

func testSessionVariables(connector driver.Connector, sv goHdbDriver.SessionVariables, t *testing.T) {
	// check session variables
	db := sql.OpenDB(connector)
	defer db.Close()

	var val string
	for k, v := range sv {
		err := db.QueryRow(fmt.Sprintf("select session_context('%s') from dummy", k)).Scan(&val)
		switch {
		case err == sql.ErrNoRows:
			t.Fatal(err)
		case err != nil:
			t.Fatal(err)
		}
		if val != v {
			t.Fatalf("session variable value for %s is %s - expected %s", k, val, v)
		}
	}
}

func TestConnector(t *testing.T) {
	dsnConnector, err := goHdbDriver.NewDSNConnector(goHdbDriver.TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("dsnConnector", func(t *testing.T) {
		testConnector(dsnConnector, t)
	})

	basicAuthConnector := goHdbDriver.NewBasicAuthConnector(dsnConnector.Host(), dsnConnector.Username(), dsnConnector.Password())
	t.Run("basicAuthConnector", func(t *testing.T) {
		testConnector(basicAuthConnector, t)
	})

	// set session variables
	sv := goHdbDriver.SessionVariables{"k1": "v1", "k2": "v2", "k3": "v3"}
	if err := dsnConnector.SetSessionVariables(sv); err != nil {
		t.Fatal(err)
	}
	t.Run("sessionVariables", func(t *testing.T) {
		testSessionVariables(dsnConnector, sv, t)
	})
}
