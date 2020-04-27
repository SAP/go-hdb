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
	"errors"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

const (
	errCodeInvalidTableName = 259
)

func ExampleError() {
	db, err := sql.Open(driver.DriverName, driver.TestDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	invalidTableName := driver.RandomIdentifier("table_")
	stmt, err := db.Query(fmt.Sprintf("select * from %s", invalidTableName))
	if err == nil {
		defer stmt.Close()
	}

	var dbError driver.Error
	if err != nil {
		// Check if error is driver.Error.
		if errors.As(err, &dbError) {
			switch dbError.Code() {
			case errCodeInvalidTableName:
				fmt.Print("invalid table name")
			default:
				log.Fatalf("code %d text %s", dbError.Code(), dbError.Text())
			}
		}
	}
	// output: invalid table name
}
