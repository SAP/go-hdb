// +build !unit

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	goHdbDriver "github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/drivertest"
)

func testConnector(connector driver.Connector, t *testing.T) {
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := drivertest.DummySelect(db); err != nil {
		t.Fatal(err)
	}
}

func testExistSessionVariables(sv1, sv2 map[string]string, t *testing.T) {
	for k1, v1 := range sv1 {
		v2, ok := sv2[k1]
		if !ok {
			t.Fatalf("session variable value for %s does not exist", k1)
		}
		if v2 != v1 {
			t.Fatalf("session variable value for %s is %s - expected %s", k1, v2, v1)
		}
	}
}

func testNotExistSessionVariables(keys []string, sv2 map[string]string, t *testing.T) {
	for _, k1 := range keys {
		v2, ok := sv2[k1]
		// deleted session variable shouldn't be found, but like clientInfo does not allow (check TODO in internal/protocol/protocol.go)
		// deletions, the value of a 'deleted' session variable is set to <space>.
		if ok && v2 != "" {
			t.Fatalf("session variable value for %s is %s - should be empty", k1, v2)
		}
	}
}

func testSessionVariables(connector *goHdbDriver.Connector, t *testing.T) {
	// set session variables
	sv1 := goHdbDriver.SessionVariables{"k1": "v1", "k2": "v2", "k3": "v3"}
	if err := connector.SetSessionVariables(sv1); err != nil {
		t.Fatal(err)
	}

	// check session variables
	db := sql.OpenDB(connector)
	defer db.Close()

	// retrieve session variables
	sv2, err := drivertest.SessionVariables(db)
	if err != nil {
		t.Fatal(err)
	}

	// check if session variables are set after connect to db.
	testExistSessionVariables(sv1, sv2, t)

	// update, delete, insert session variables
	sv1 = goHdbDriver.SessionVariables{"k1": "v1new", "k2": "v2", "k4": "v4"}
	if err := connector.SetSessionVariables(sv1); err != nil {
		t.Fatal(err)
	}

	// execute statement to update session variables.
	if err := drivertest.DummySelect(db); err != nil {
		t.Fatal(err)
	}

	// retrieve session variables
	if sv2, err = drivertest.SessionVariables(db); err != nil {
		t.Fatal(err)
	}

	t.Log(sv2)

	// check session variables again.
	testExistSessionVariables(sv1, sv2, t)
	testNotExistSessionVariables([]string{"k3"}, sv2, t)

}

func TestConnector(t *testing.T) {
	dsnConnector, err := goHdbDriver.NewDSNConnector(drivertest.DSN())
	if err != nil {
		t.Fatal(err)
	}

	t.Run("dsnConnector", func(t *testing.T) {
		testConnector(dsnConnector, t)
	})

	if dsnConnector.TLSConfig() == nil { // in case of TLS the following test will fail.
		basicAuthConnector := goHdbDriver.NewBasicAuthConnector(dsnConnector.Host(), dsnConnector.Username(), dsnConnector.Password())
		t.Run("basicAuthConnector", func(t *testing.T) {
			testConnector(basicAuthConnector, t)
		})
	}

	t.Run("sessionVariables", func(t *testing.T) {
		testSessionVariables(dsnConnector, t)
	})
}
