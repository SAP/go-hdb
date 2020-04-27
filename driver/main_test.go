/*
Copyright 2020 SAP SE

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
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
)

const (
	/*
		support of environment variables to
		- set e.g. DSN via env variable and
		- e.g. execute tests via go test -v ./...
	*/
	envDSN = "GOHDBDSN"
)

const (
	TestGoHDBSchemaPrefix = "goHdbTest_"
)

// flags
var (
	// TestDSN (data source name for testing) has to be provided by calling go test with dsn parameter.
	TestDSN string
	// TestDropSchema could be provided by calling go test with dropSchema parameter.
	// If set to true (default), the test schema will be dropped after successful test execution.
	// If set to false, the test schema will remain on database after test execution.
	TestDropSchema bool
	// TestDropAllSchema will drop all schemas with GoHDBTestSchemaPrefix prefix to clean-up all not yet deleted
	// test schemas created by go-hdb unit tests.
	TestDropAllSchemas bool
)

func init() {
	// check env variables
	dsn, ok := os.LookupEnv(envDSN)
	if !ok {
		dsn = "hdb://user:password@host:port"
	}
	flag.StringVar(&TestDSN, "dsn", dsn, "database dsn")
	flag.BoolVar(&TestDropSchema, "dropSchema", true, "drop test schema if test ran successfully")
	flag.BoolVar(&TestDropAllSchemas, "dropAllSchemas", false, "drop all existing test schemas if test ran successfully")
}

// globals
var (
	// TestSchema will be used as test schema name and created on the database by TestMain.
	// The schema name consists of the prefix "test_" and a random Identifier.
	TestSchema Identifier
	// TestDB is instantiated by TestMain and should be used by tests.
	// TestDB uses TestDSN to connect to database.
	// Each TestDB connection will set TestSchema as default database schema.
	TestDB *sql.DB
	// // TestRec enables test recording.
	// TestRec bool
	// // TestRecFname is the filename used for test recording.
	// TestRecFname string
	// // TestRpl enables replaying test recordings.
	// TestRpl bool
	// // TestRplFname is the filename used for replaying the test recording.
	// TestRplFname string
)

func init() {
	TestSchema = RandomIdentifier(TestGoHDBSchemaPrefix)
}

func testExit(err error) {
	func() {
		prefix := ""
		for err != nil {
			log.Printf("%s%s", prefix, err.Error())
			prefix += "."
			err = errors.Unwrap(err)
		}
	}()
	os.Exit(1)
}

func TestMain(m *testing.M) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	if !flag.Parsed() {
		flag.Parse()
	}
	conn := testSetup()
	exitCode := m.Run()
	testTeardown(exitCode, conn)
	os.Exit(exitCode)
}

func testSetup() *sql.Conn {
	connector, err := NewDSNConnector(TestDSN)
	if err != nil {
		testExit(err)
	}
	TestDB = sql.OpenDB(connector)

	ctx := context.Background()

	// create schema in own connection (no reuse of conn as DefaultSchema is not set)
	conn, err := TestDB.Conn(ctx)
	if err != nil {
		testExit(err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("create schema %s", TestSchema)); err != nil {
		testExit(err)
	}
	log.Printf("created schema %s", TestSchema)

	// now: set TestSchema in connector, so that all further connections are going to use it
	connector.SetDefaultSchema(TestSchema)

	return conn
}

func testTeardown(exitCode int, conn *sql.Conn) {
	ctx := context.Background()

	//schema := string(TestSchema) + "'"

	numTables, numProcs := 0, 0
	if err := conn.QueryRowContext(ctx, fmt.Sprintf("select count(*) from sys.tables where schema_name = '%s'", string(TestSchema))).Scan(&numTables); err != nil {
		testExit(err)
	}
	if err := conn.QueryRowContext(ctx, fmt.Sprintf("select count(*) from sys.procedures where schema_name = '%s'", string(TestSchema))).Scan(&numProcs); err != nil {
		testExit(err)
	}
	log.Printf("schema %s - #tables created: %d #procedures created: %d", TestSchema, numTables, numProcs)

	if exitCode == 0 {
		switch {
		case TestDropAllSchemas:
			dropAllSchemas(ctx, conn)
		case TestDropSchema:
			dropSchema(ctx, conn, TestSchema)
		}
	}
}

func dropAllSchemas(ctx context.Context, conn *sql.Conn) {
	schemas := make([]string, 0)

	rows, err := conn.QueryContext(ctx, fmt.Sprintf("select schema_name from sys.schemas where schema_name like '%s_%%'", TestGoHDBSchemaPrefix))
	if err != nil {
		testExit(err)
	}
	var schema string
	for rows.Next() {
		if err := rows.Scan(&schema); err != nil {
			testExit(err)
		}

		// cannot delete schemas in select loop (SQL Error 150 - statement cancelled or snapshot timestamp already invalidated)
		// --> collect them and delete outside of select
		schemas = append(schemas, schema) // cannot drop schemas in loop (
	}
	if err := rows.Err(); err != nil {
		testExit(err)
	}
	rows.Close()

	for _, schema := range schemas {
		dropSchema(ctx, conn, Identifier(schema))
	}
	log.Printf("number of dropped schemas: %d", len(schemas))
}

func dropSchema(ctx context.Context, conn *sql.Conn, schema Identifier) {
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("drop schema %s cascade", schema)); err != nil {
		testExit(err)
	}
	log.Printf("dropped schema %s", schema)
}
