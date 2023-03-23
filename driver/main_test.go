//go:build !unit

package driver

import (
	"bytes"
	"database/sql"
	_ "embed" // embed stats template
	"flag"
	"html/template"
	"log"
	"os"
	"testing"
	"time"
)

const (
	envDSN = "GOHDBDSN"
)

const testGoHDBSchemaPrefix = "goHdbTest_"

//go:embed stats.tmpl
var statsTemplate string

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

// NewTestConnector returns a Connector with the relevant test attributes set.
func NewTestConnector() *Connector {
	c, err := newDSNConnector(testDSN)
	if err != nil {
		log.Fatal(err)
	}
	c._defaultSchema = *schema        // important: set test schema!
	c._pingInterval = 1 * time.Second // turn on connection validity check while resetting
	return c
}

var defaultTestConnector *Connector
var defaultTestDB *sql.DB

// DefaultTestConnector returns the default Test Connector with the relevant test attributes set.
func DefaultTestConnector() *Connector { return defaultTestConnector }

// DefaultTestDB return the default database with the relevant test attributes set.
func DefaultTestDB() *sql.DB { return defaultTestDB }

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

	// init default DB and default connector
	defaultTestConnector = NewTestConnector()
	defaultTestDB = sql.OpenDB(defaultTestConnector)

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
	defaultTestDB.Close()

	t := template.Must(template.New("stats").Parse(statsTemplate))
	b := new(bytes.Buffer)
	if err := t.Execute(b, connector.NativeDriver().Stats()); err != nil {
		log.Fatal(err)
	}
	log.Printf("\n%s", b.String())
	os.Exit(exitCode)
}
