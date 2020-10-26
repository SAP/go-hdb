// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/drivertest"
)

const (
	errCodeInvalidTableName = 259
)

func ExampleError() {
	connector, err := driver.NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
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
