// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package drivertest

import (
	"database/sql"
	"errors"
	"flag"
	"log"
	"os"

	"github.com/SAP/go-hdb/internal/rand"
)

const (
	/*
		support of environment variables to
		- set e.g. DSN via env variable and
		- e.g. execute tests via go test -v ./...
	*/
	envDSN = "GOHDBDSN"

	testGoHDBSchemaPrefix = "goHdbTest_"
)

type dbFlags struct {
	// dsn (data source name for testing) has to be provided by calling go test with dsn parameter.
	dsn string
	// schema defines the database schema where test tables are going to be created.
	schema string
	// dropSchema:
	//  if set to true (default), the test schema will be dropped after successful test execution.
	//  if set to false, the test schema will remain on database after test execution.
	dropSchema bool
	// dropSchemas will drop all schemas with GoHDBTestSchemaPrefix prefix to clean-up all not yet deleted
	// test schemas created by go-hdb unit tests.
	dropSchemas bool
	// pingInt sets the connection ping interval in milliseconds.
	// If zero, the connection ping is deactivated.
	pingInt int
}

func newDBFlags() *dbFlags {
	f := new(dbFlags)

	flag.StringVar(&f.dsn, "dsn", os.Getenv(envDSN), "database dsn")
	flag.StringVar(&f.schema, "schema", testGoHDBSchemaPrefix+rand.RandomString(16), "database schema")
	flag.BoolVar(&f.dropSchema, "dropschema", true, "drop test schema if test ran successfully")
	flag.BoolVar(&f.dropSchemas, "dropschemas", false, "drop all existing test schemas if test ran successfully")
	flag.IntVar(&f.pingInt, "pingint", 0, "sets the connection ping interval (if zero, the connection ping is deactivated)")

	return f
}

// DBTest provides setup and teardown methods for unit tests using the database.
type DBTest struct {
	flags *dbFlags
}

// NewDBTest creates a new database test object.
func NewDBTest() *DBTest {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	flags := newDBFlags()
	if !flag.Parsed() {
		flag.Parse()
	}
	return &DBTest{flags: flags}
}

// DSN returns the dsn parameter.
func (t *DBTest) DSN() string { return t.flags.dsn }

// Schema returns the database schema.
func (t *DBTest) Schema() string { return t.flags.schema }

// PingInt returns the ping interval.
func (t *DBTest) PingInt() int { return t.flags.pingInt }

// Setup creates the database schema.
func (t *DBTest) Setup(db *sql.DB) {
	if err := CreateSchema(db, t.flags.schema); err != nil {
		t.exit(err)
	}
	log.Printf("created schema %s", t.flags.schema)
}

// Teardown deletes the database schema(s).
func (t *DBTest) Teardown(db *sql.DB, drop bool) {
	schema := t.flags.schema

	numTables, err := NumTablesInSchema(db, schema)
	if err != nil {
		t.exit(err)
	}
	numProcs, err := NumProcsInSchema(db, schema)
	if err != nil {
		t.exit(err)
	}
	log.Printf("schema %s - #tables created: %d #procedures created: %d", schema, numTables, numProcs)

	if !drop {
		return
	}

	switch {
	case t.flags.dropSchemas:
		schemas, err := QuerySchemasPrefix(db, testGoHDBSchemaPrefix)
		if err != nil {
			t.exit(err)
		}
		for _, schema := range schemas {
			DropSchema(db, schema)
			log.Printf("dropped schema %s", schema)
		}
		log.Printf("number of dropped schemas: %d", len(schemas))
	case t.flags.dropSchema:
		DropSchema(db, schema)
		log.Printf("dropped schema %s", schema)
	}
}

func (t *DBTest) exit(err error) {
	prefix := ""
	for err != nil {
		log.Printf("%s%s", prefix, err.Error())
		prefix += "."
		err = errors.Unwrap(err)
	}
	os.Exit(1)
}
