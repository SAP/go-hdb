//go:build !unit
// +build !unit

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"testing"
)

const (
	envDSN = "GOHDBDSN"
)

const testGoHDBSchemaPrefix = "goHdbTest_"

var (
	testDSNStr string
	testDSN    *DSN
)

func init() {
	var ok bool
	if testDSNStr, ok = os.LookupEnv(envDSN); !ok {
		log.Fatalf("environment variable %s not set", envDSN)
	}
	var err error
	if testDSN, err = parseDSN(testDSNStr); err != nil {
		log.Fatal(err)
	}
}

// schema defines the database schema where test tables are going to be created.
var schema = flag.String("schema", testGoHDBSchemaPrefix+randAlphanumString(16), "database schema")

// dropSchema:
// if set to true (default), the test schema will be dropped after successful test execution.
// if set to false, the test schema will remain on database after test execution.
var dropSchema = flag.Bool("dropschema", true, "drop test schema if test ran successfully")

// dropSchemas will drop all schemas with GoHDBTestSchemaPrefix prefix to clean-up all not yet deleted
// test schemas created by go-hdb unit tests.
var dropSchemas = flag.Bool("dropschemas", false, "drop all existing test schemas if test ran successfully")

// NewTestConnector return a Connector with the relevant test attributes set.
func NewTestConnector() *Connector {
	c, err := newDSNConnector(testDSN)
	if err != nil {
		log.Fatal(err)
	}
	c.connAttrs._defaultSchema = *schema // important: set test schema!
	return c
}

func TestMain(m *testing.M) {

	// setup creates the database schema.
	setup := func(db *sql.DB) {
		if err := execCreateSchema(db, *schema); err != nil {
			log.Fatal(err)
		}
		log.Printf("created schema %s", *schema)
	}

	// teardown deletes the database schema(s).
	teardown := func(db *sql.DB, drop bool) {
		schema := *schema

		numTables, err := queryNumTablesInSchema(db, schema)
		if err != nil {
			log.Fatal(err)
		}
		numProcs, err := queryNumProcsInSchema(db, schema)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("schema %s - #tables created: %d #procedures created: %d", schema, numTables, numProcs)

		if !drop {
			return
		}

		switch {
		case *dropSchemas:
			schemas, err := querySchemasPrefix(db, testGoHDBSchemaPrefix)
			if err != nil {
				log.Fatal(err)
			}
			for _, schema := range schemas {
				execDropSchema(db, schema)
				log.Printf("dropped schema %s", schema)
			}
			log.Printf("number of dropped schemas: %d", len(schemas))
		case *dropSchema:
			execDropSchema(db, schema)
			log.Printf("dropped schema %s", schema)
		}
	}

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	if !flag.Parsed() {
		flag.Parse()
	}

	// do not use NewTestConnector as it does set the default schema and the schema creation in setup would be answered by a HDB error.
	connector, err := newDSNConnector(testDSN)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	// TestDB.SetMaxIdleConns(0)
	setup(db)
	exitCode := m.Run()
	teardown(db, exitCode == 0)
	db.Close()
	log.Print(connector.NativeDriver().Stats())
	os.Exit(exitCode)
}
