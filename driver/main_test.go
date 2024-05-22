//go:build !unit

package driver

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed" // embed stats template
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"testing"
	"text/template"
	"time"
)

//go:embed stats.tmpl
var statsTemplate string

const (
	envDSN                = "GOHDBDSN"
	testGoHDBSchemaPrefix = "goHdbTest_"
)

var MT = MainTest{}

type MainTest struct {
	ctr     *Connector
	db      *sql.DB
	version *Version
}

// NewConnector returns a Connector with the relevant test attributes set.
func (mt *MainTest) NewConnector() *Connector { return mt.ctr.clone() }

// Connector returns the default Test Connector with the relevant test attributes set.
func (mt *MainTest) Connector() *Connector { return mt.ctr }

// DB return the default test database with the relevant test attributes set.
func (mt *MainTest) DB() *sql.DB { return mt.db }

// Version returns the database version of the test database.
func (mt *MainTest) Version() *Version { return mt.version }

type dropKind int

func (dk dropKind) String() string {
	if dk < 0 || dk > dkSchemas {
		return fmt.Sprintf("%d: invalid drop kind", dk)
	}
	return fmt.Sprintf("%d: %s", dk, dropKindStr[dk])
}

const (
	dkNone dropKind = iota
	dkSchema
	dkSchemas
)

var dropKindStr = []string{
	"don't drop schema",
	"drop schema if test ran successfully",
	"drop all existing test schemas if test ran successfully",
}

func (mt *MainTest) run(m *testing.M, schema string, dk dropKind) (int, error) {
	dsnStr, ok := os.LookupEnv(envDSN)
	if !ok {
		return 0, fmt.Errorf("environment variable %s not set", envDSN)
	}

	var err error
	if mt.ctr, err = NewDSNConnector(dsnStr); err != nil {
		return 0, err
	}

	db := sql.OpenDB(mt.ctr) // use own db as 'drop schema' sometimes doesn't work for connections where the same schema is set
	defer db.Close()
	mt.version, err = mt.detectVersion(db)
	if err != nil {
		return 0, err
	}
	if err := mt.setup(db, schema); err != nil {
		return 0, err
	}

	// init default DB and default connector
	mt.ctr.SetDefaultSchema(schema)         // important: set test schema! but after create schema
	mt.ctr.SetPingInterval(1 * time.Second) // turn on connection validity check while resetting
	mt.db = sql.OpenDB(mt.ctr)
	defer mt.db.Close()
	mt.db.SetMaxIdleConns(25) // let's keep some more connections in the pool

	exitCode := m.Run()

	mt.db.Close() // close before teardown, so that schema can be dropped.

	if exitCode != 0 {
		dk = dkNone // do not drop schema in case of test execution error
	}
	if err := mt.teardown(db, schema, dk); err != nil {
		return exitCode, err
	}

	db.Close() // close before printing stats

	stdHdbDriver.metrics.close() // wait for all pending metrics

	t := template.Must(template.New("stats").Parse(statsTemplate))
	b := new(bytes.Buffer)
	if err := t.Execute(b, stdHdbDriver.Stats()); err != nil {
		return exitCode, err
	}
	log.Printf("\n%s", b.String())

	return exitCode, nil
}

func (mt *MainTest) setup(db *sql.DB, schema string) error {
	return mt.createSchema(db, schema)
}

func (mt *MainTest) teardown(db *sql.DB, schema string, dk dropKind) error {
	numTables, err := mt.queryNumTablesInSchema(db, schema)
	if err != nil {
		return err
	}
	numProcs, err := mt.queryNumProcsInSchema(db, schema)
	if err != nil {
		return err
	}
	log.Printf("schema %s - #tables created: %d #procedures created: %d", schema, numTables, numProcs)

	switch dk {
	case dkNone:
		// nothing to do
	case dkSchema:
		if err := mt.dropSchema(db, schema); err != nil {
			return err
		}
		log.Printf("dropped schema %s", schema)
	case dkSchemas:
		schemas, err := mt.querySchemasPrefix(db, testGoHDBSchemaPrefix)
		if err != nil {
			return err
		}
		for _, schema := range schemas {
			if err := mt.dropSchema(db, schema); err != nil {
				return err
			}
			log.Printf("dropped schema %s", schema)
		}
		log.Printf("number of dropped schemas: %d", len(schemas))
	}
	return nil
}
func (mt *MainTest) detectVersion(db *sql.DB) (*Version, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var version *Version
	if err := conn.Raw(func(driverConn any) error {
		version = driverConn.(Conn).HDBVersion()
		return nil
	}); err != nil {
		return nil, err
	}
	return version, nil
}

func (mt *MainTest) createSchema(db *sql.DB, schema string) error {
	_, err := db.Exec("create schema " + strconv.Quote(schema))
	return err
}

func (mt *MainTest) dropSchema(db *sql.DB, schema string) error {
	_, err := db.Exec(fmt.Sprintf("drop schema %s cascade", strconv.Quote(schema)))
	return err
}

func (mt *MainTest) queryNumTablesInSchema(db *sql.DB, schema string) (int, error) {
	numTables := 0
	if err := db.QueryRow(fmt.Sprintf("select count(*) from sys.tables where schema_name = '%s'", schema)).Scan(&numTables); err != nil {
		return 0, err
	}
	return numTables, nil
}

func (mt *MainTest) queryNumProcsInSchema(db *sql.DB, schema string) (int, error) {
	numProcs := 0
	if err := db.QueryRow(fmt.Sprintf("select count(*) from sys.procedures where schema_name = '%s'", schema)).Scan(&numProcs); err != nil {
		return 0, err
	}
	return numProcs, nil
}

func (mt *MainTest) querySchemasPrefix(db *sql.DB, prefix string) ([]string, error) {
	names := make([]string, 0)

	rows, err := db.Query(fmt.Sprintf("select schema_name from sys.schemas where schema_name like '%s_%%'", prefix))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var name string
	for rows.Next() {
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

const (
	cpuProfileName = "test.cpuprofile"
)

// copied from runtime/debug.
func stack() []byte {
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, true) // all stacks
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}

func TestMain(m *testing.M) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	schema := flag.String("schema", string(RandomIdentifier(testGoHDBSchemaPrefix)), "database schema")
	dk := dkSchema
	flag.Func("drop", fmt.Sprintf("%s %s %s (default %d)", dkNone, dkSchema, dkSchemas, dkSchema), func(s string) error {
		i, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		if i < 0 || i > int(dkSchemas) {
			return fmt.Errorf("invalid dropKind %d", i)
		}
		dk = dropKind(i)
		return nil
	})

	if !flag.Parsed() {
		flag.Parse()
	}

	flag.Visit(func(f *flag.Flag) {
		if f.Name == cpuProfileName {
			cpuProfile = true
		}
	})

	exitCode, err := MT.run(m, *schema, dk)
	if err != nil {
		log.Fatal(err)
	}

	// cleanup go-hdb driver.
	Unregister() //nolint: errcheck

	// detect go routine leaks.
	stack := stack()
	numLeaking := bytes.Count(stack, []byte{'\n', '\n'}) // count newlines.
	if numLeaking > 0 {
		log.Printf("\nnumber of leaking go routines: %d\n%s\n", numLeaking, stack)
	}

	os.Exit(exitCode)
}
