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

package driver

import (
	"database/sql"
	"fmt"
	"log"
)

const procOut = `create procedure %s.%s (out message nvarchar(1024))
language SQLSCRIPT as
begin
    message := 'Hello World!';
end
`

// ExampleCallOut creates a stored procedure with one output parameter and executes it.
// Stored procedures with output parameters must be executed by sql.Query or sql.QueryRow.
// For variables TestDsn and TestSchema see main_test.go.
func Example_callOut() {
	db, err := sql.Open(DriverName, TestDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	procedure := RandomIdentifier("procOut_")

	if _, err := db.Exec(fmt.Sprintf(procOut, TestSchema, procedure)); err != nil { // Create stored procedure.
		log.Fatal(err)
	}

	var out string

	if err := db.QueryRow(fmt.Sprintf("call %s.%s(?)", TestSchema, procedure)).Scan(&out); err != nil {
		log.Fatal(err)
	}

	fmt.Print(out)

	// output: Hello World!
}
