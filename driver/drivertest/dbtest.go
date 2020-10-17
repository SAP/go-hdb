// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package drivertest

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"flag"
	"log"
	"os"
	"time"

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

var stdDBFlags = newDBFlags()

// DSN returns the dsn parameter.
func DSN() string { return stdDBFlags.dsn }

// // Schema returns the database schema.
// func Schema() string { return stdDBFlags.schema }

// // PingInt returns the ping interval.
// func PingInt() int { return stdDBFlags.pingInt }

// Connector interface.
type Connector interface {
	driver.Connector
	SetDSN(dsn string) error
	SetDefaultSchema(schema string) error
	SetPingInterval(d time.Duration) error
	SetDfv(dfv int) error
	SetLegacy(b bool) error
	SetBulkSize(bulkSize int) error
}

// DefaultConnector set test default values to ctr and returns it ('fluent').
// ctr needs to implement drivertest.connector interface which is 'fullfilled' by driver.Connector.
// driver.Connector cannot be used directly because of cyclic reference between
// go-hdb/driver and go-hdb/driver.drivertest.
func DefaultConnector(ctr Connector) (Connector, error) {
	if err := ctr.SetDSN(stdDBFlags.dsn); err != nil {
		return nil, err
	}
	if err := ctr.SetDefaultSchema(stdDBFlags.schema); err != nil {
		return nil, err
	}
	if err := ctr.SetPingInterval(time.Duration(stdDBFlags.pingInt) * time.Millisecond); err != nil {
		return nil, err
	}
	return ctr, nil
}

// dbTest provides setup and teardown methods for unit tests using the database.
type dbTest struct{}

// setup creates the database schema.
func (t *dbTest) setup(db *sql.DB) {
	if err := CreateSchema(db, stdDBFlags.schema); err != nil {
		t.exit(err)
	}
	log.Printf("created schema %s", stdDBFlags.schema)
}

// teardown deletes the database schema(s).
func (t *dbTest) teardown(db *sql.DB, drop bool) {
	schema := stdDBFlags.schema

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
	case stdDBFlags.dropSchemas:
		schemas, err := QuerySchemasPrefix(db, testGoHDBSchemaPrefix)
		if err != nil {
			t.exit(err)
		}
		for _, schema := range schemas {
			DropSchema(db, schema)
			log.Printf("dropped schema %s", schema)
		}
		log.Printf("number of dropped schemas: %d", len(schemas))
	case stdDBFlags.dropSchema:
		DropSchema(db, schema)
		log.Printf("dropped schema %s", schema)
	}
}

func (t *dbTest) exit(err error) {
	prefix := ""
	for err != nil {
		log.Printf("%s%s", prefix, err.Error())
		prefix += "."
		err = errors.Unwrap(err)
	}
	os.Exit(1)
}

var stdDBTest = dbTest{}

// Setup creates the database schema.
func Setup(db *sql.DB) { stdDBTest.setup(db) }

// Teardown deletes the database schema(s).
func Teardown(db *sql.DB, drop bool) { stdDBTest.teardown(db, drop) }
