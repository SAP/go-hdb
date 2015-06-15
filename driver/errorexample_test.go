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
	"log"

	"github.com/SAP/go-hdb/driver"
)

func ExampleError() {
	db, err := sql.Open("hdb", "hdb://user:password@host:port")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Query("select * from dummy"); err != nil {
		// type assertion of database server error message
		if dbError, ok := err.(driver.Error); ok {
			log.Printf("code %d text %s", dbError.Code(), dbError.Text())
		}
	}

	/*// Output:*/
}
