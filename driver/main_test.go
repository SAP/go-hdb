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
	"crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"testing"
)

// globals
var (
	dsn        = flag.String("dsn", "hdb://user:password@ip_address:port", "database dsn")
	dropSchema = flag.Bool("dropSchema", true, "drop test schema after test ran successfully")
	tSchema    Identifier
)

func TestMain(m *testing.M) {

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if !flag.Parsed() {
		flag.Parse()
	}

	// init driver
	db, err := sql.Open(DriverName, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// create schema
	tSchema = testRandomIdentifier("test_")
	if _, err := db.Exec(fmt.Sprintf("create schema %s", tSchema)); err != nil {
		log.Fatal(err)
	}
	log.Printf("created schema %s", tSchema)

	exitCode := m.Run()

	if exitCode == 0 && *dropSchema {
		if _, err := db.Exec(fmt.Sprintf("drop schema %s cascade", tSchema)); err != nil {
			log.Fatal(err)
		}
		log.Printf("dropped schema %s", tSchema)
	}

	os.Exit(exitCode)
}

func testRandomIdentifier(prefix string) Identifier {
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err.Error()) // rand should never fail
	}
	return Identifier(fmt.Sprintf(fmt.Sprintf("%s%x", prefix, b)))
}
