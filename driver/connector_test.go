// +build go1.10

/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver_test

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	goHdbDriver "github.com/SAP/go-hdb/driver"
)

func TestConnector(t *testing.T) {
	dsnConnector, err := goHdbDriver.NewDSNConnector(goHdbDriver.TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	testConnector(t, dsnConnector)

	basicAuthConnector := goHdbDriver.NewBasicAuthConnector(dsnConnector.Host(), dsnConnector.Username(), dsnConnector.Password())
	testConnector(t, basicAuthConnector)
}

func testConnector(t *testing.T, connector driver.Connector) {
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
