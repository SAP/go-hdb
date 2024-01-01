//go:build !unit

package driver

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver/internal/dbtest"
)

const envDSN = "GOHDBDSN"

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

func (mt *MainTest) detectVersion(db *sql.DB) (*Version, error) {
	var version *Version
	// Grab connection to detect hdb version.
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := conn.Raw(func(driverConn any) error {
		version = driverConn.(Conn).HDBVersion()
		return nil
	}); err != nil {
		return nil, err
	}
	return version, nil
}

func (mt *MainTest) run(m *testing.M) (int, error) {
	dsnStr, ok := os.LookupEnv(envDSN)
	if !ok {
		return 0, fmt.Errorf("environment variable %s not set", envDSN)
	}

	// init default DB and default connector
	var err error
	if mt.ctr, err = NewDSNConnector(dsnStr); err != nil {
		return 0, err
	}
	mt.db = sql.OpenDB(mt.ctr)
	defer mt.db.Close()

	mt.version, err = mt.detectVersion(mt.db)
	if err != nil {
		return 0, err
	}

	if err := dbtest.Setup(mt.db); err != nil {
		return 0, err
	}

	mt.ctr.SetDefaultSchema(*dbtest.Schema) // important: set test schema! but after create schema
	mt.ctr.SetPingInterval(1 * time.Second) // turn on connection validity check while resetting

	exitCode := m.Run()

	if err := dbtest.Teardown(mt.db, exitCode == 0); err != nil {
		return 0, err
	}

	mt.db.Close() // close before printing stats
	if err := dbtest.LogDriverStats(stdHdbDriver.Stats()); err != nil {
		return 0, err
	}

	return exitCode, nil
}

func TestMain(m *testing.M) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	if !flag.Parsed() {
		flag.Parse()
	}
	exitCode, err := MT.run(m)
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(exitCode)
}
