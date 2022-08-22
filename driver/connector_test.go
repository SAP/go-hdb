//go:build !unit
// +build !unit

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"database/sql"
	"testing"
)

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
		if ok && v2 != "" {
			t.Fatalf("session variable value for %s is %s - should be empty", k1, v2)
		}
	}
}

func testSessionVariables(t *testing.T) {
	connector := NewTestConnector()

	// set session variables
	sv1 := SessionVariables{"k1": "v1", "k2": "v2", "k3": "v3"}
	connector.SetSessionVariables(sv1)

	// check session variables
	db := sql.OpenDB(connector)
	defer db.Close()

	// retrieve session variables
	sv2, err := querySessionVariables(db)
	if err != nil {
		t.Fatal(err)
	}

	// check if session variables are set after connect to db.
	testExistSessionVariables(sv1, sv2, t)
	testNotExistSessionVariables([]string{"k4"}, sv2, t)
}

func printInvalidConnectAttempts(t *testing.T, username string) {
	db := DefaultTestDB()

	if invalidConnectAttempts, err := queryInvalidConnectAttempts(db, username); err != nil {
		t.Logf("error in selecting invalid connect attempts: %s", err)
	} else {
		t.Logf("number of invalid connect attempts: %d", invalidConnectAttempts)
	}
}

func testRetryConnect(t *testing.T) {
	const invalidPassword = "invalid_password"

	connector := NewTestConnector()

	password := connector.Password() // safe password
	refreshPassword := func() (string, bool) {
		printInvalidConnectAttempts(t, connector.Username())
		return password, true
	}
	connector.SetPassword(invalidPassword) // set invalid password
	connector.SetRefreshPassword(refreshPassword)
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestConnector(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testSessionVariables", testSessionVariables},
		{"testRetryConnect", testRetryConnect},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t)
		})
	}
}
