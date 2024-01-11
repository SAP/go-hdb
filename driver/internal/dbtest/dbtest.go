// Package dbtest implements database test functions.
package dbtest

import (
	"bytes"
	"database/sql"
	_ "embed" // embed stats template
	"flag"
	"html/template"
	"log"

	"github.com/SAP/go-hdb/driver/internal/rand"
)

const testGoHDBSchemaPrefix = "goHdbTest_"

// Schema defines the database schema where test tables are going to be created.
var Schema = flag.String("schema", testGoHDBSchemaPrefix+rand.AlphanumString(16), "database schema")

// DropSchema will drop the test schema after execution.
// if set to true (default), the test schema will be dropped after successful test execution.
// if set to false, the test schema will remain on database after test execution.
var DropSchema = flag.Bool("dropschema", true, "drop test schema if test ran successfully")

// DropSchemas will drop all schemas with GoHDBTestSchemaPrefix prefix to clean-up all not yet deleted
// test schemas created by go-hdb unit tests.
var DropSchemas = flag.Bool("dropschemas", false, "drop all existing test schemas if test ran successfully")

//go:embed stats.tmpl
var statsTemplate string

// Setup creates the database schema.
func Setup(driverName, dsn string) error {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	return createSchema(db, *Schema)
}

// Teardown deletes the database schema(s).
func Teardown(driverName, dsn string, drop bool) error {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	numTables, err := queryNumTablesInSchema(db, *Schema)
	if err != nil {
		return err
	}
	numProcs, err := queryNumProcsInSchema(db, *Schema)
	if err != nil {
		return err
	}
	log.Printf("schema %s - #tables created: %d #procedures created: %d", *Schema, numTables, numProcs)

	if !drop {
		return nil
	}

	switch {
	case *DropSchemas:
		schemas, err := querySchemasPrefix(db, testGoHDBSchemaPrefix)
		if err != nil {
			return err
		}
		for _, schema := range schemas {
			if err := dropSchema(db, schema); err != nil {
				return err
			}
			log.Printf("dropped schema %s", schema)
		}
		log.Printf("number of dropped schemas: %d", len(schemas))

	case *DropSchema:
		if err := dropSchema(db, *Schema); err != nil {
			return err
		}
		log.Printf("dropped schema %s", *Schema)
	}
	return nil
}

// LogDriverStats outputs the driver statistics.
func LogDriverStats(driverStats any) error {
	t := template.Must(template.New("stats").Parse(statsTemplate))
	b := new(bytes.Buffer)
	if err := t.Execute(b, driverStats); err != nil {
		return err
	}
	log.Printf("\n%s", b.String())
	return nil
}
