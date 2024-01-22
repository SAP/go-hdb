package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/SAP/go-hdb/driver"
)

// dbResult is the structure used to provide db command result response.
type dbResult struct {
	Command string
	Name    string
	NumRow  int64
	Err     error
}

func (r *dbResult) String() string {
	switch {
	case r.Err != nil:
		return fmt.Sprintf("command: %s name: %s error: %s", r.Command, r.Name, r.Err)
	case r.NumRow != -1:
		return fmt.Sprintf("command: %s name: %s: %d rows", r.Command, r.Name, r.NumRow)
	default:
		return fmt.Sprintf("command: %s name: %s: ok", r.Command, r.Name)
	}
}

const (
	schemaPrefix = "goHdbTest_"
	tablePrefix  = "table_"
)

type dba struct {
	schemaName    string
	tableName     string
	fullTableName string
	db            *sql.DB
}

func newDBA(dsn string) (*dba, error) {
	ctr, err := driver.NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}
	schemaName := string(driver.RandomIdentifier(schemaPrefix))
	tableName := string(driver.RandomIdentifier(tablePrefix))
	return &dba{
		schemaName:    schemaName,
		tableName:     tableName,
		fullTableName: strings.Join([]string{schemaName, tableName}, "."),
		db:            sql.OpenDB(ctr),
	}, nil
}

func (dba *dba) close() error {
	err1 := dropTable(dba.db, dba.schemaName, dba.tableName)
	err2 := dba.db.Close()
	return errors.Join(err1, err2)
}

// hdbVersion returns the hdb version.
func (dba *dba) hdbVersion() string {
	conn, err := dba.db.Conn(context.Background())
	if err != nil {
		return err.Error()
	}
	var hdbVersion string
	if err := conn.Raw(func(driverConn any) error {
		hdbVersion = driverConn.(driver.Conn).HDBVersion().String()
		return nil
	}); err != nil {
		return err.Error()
	}
	return hdbVersion
}

func (dba *dba) dropTable() {
	_ = dropTable(dba.db, dba.schemaName, dba.tableName) // ignore error
}

func (dba *dba) ensureSchemaTable() error {
	if err := ensureSchema(dba.db, dba.schemaName, drop, true); err != nil {
		return err
	}
	if err := ensureTable(dba.db, dba.schemaName, dba.tableName, drop); err != nil {
		return err
	}
	return nil
}

func (dba *dba) executeCommand(command string) *dbResult {
	result := &dbResult{Command: command}

	switch command {
	case cmdCreateTable:
		result.Name = dba.fullTableName
		result.Err = createTable(dba.db, dba.schemaName, dba.tableName)
	case cmdDropTable:
		result.Name = dba.fullTableName
		result.Err = dropTable(dba.db, dba.schemaName, dba.tableName)
	case cmdDeleteRows:
		result.Name = dba.fullTableName
		result.NumRow, result.Err = deleteRows(dba.db, dba.schemaName, dba.tableName)
	case cmdCountRows:
		result.Name = dba.fullTableName
		result.NumRow, result.Err = countRows(dba.db, dba.schemaName, dba.tableName)
	case cmdCreateSchema:
		result.Name = dba.schemaName
		result.Err = createSchema(dba.db, dba.schemaName)
	case cmdDropSchema:
		result.Name = dba.schemaName
		result.Err = dropSchema(dba.db, dba.schemaName, true)
	default:
		result.Err = fmt.Errorf("invalid command: %s", command)
	}
	return result
}

// createSchema creates a schema on the database.
func createSchema(db *sql.DB, name string) error {
	_, err := db.Exec(fmt.Sprintf("create schema %s", driver.Identifier(name)))
	return err
}

// dropSchema drops a schema from the database even if the schema is not empty.
func dropSchema(db *sql.DB, name string, cascade bool) error {
	var stmt string
	if cascade {
		stmt = fmt.Sprintf("drop schema %s cascade", driver.Identifier(name))
	} else {
		stmt = fmt.Sprintf("drop schema %s", driver.Identifier(name))
	}
	_, err := db.Exec(stmt)
	return err
}

// existSchema returns true if the schema exists.
func existSchema(db *sql.DB, name string) (bool, error) {
	numSchemas := 0
	if err := db.QueryRow(fmt.Sprintf("select count(*) from sys.schemas where schema_name = '%s'", name)).Scan(&numSchemas); err != nil {
		return false, err
	}
	return numSchemas != 0, nil
}

// ensureSchema creates a schema if it does not exist. If drop is set, an existing schema would be dropped before recreated.
func ensureSchema(db *sql.DB, name string, drop, cascade bool) error {
	exist, err := existSchema(db, name)
	if err != nil {
		return err
	}

	switch {
	case exist && drop:
		if err := dropSchema(db, name, cascade); err != nil {
			return err
		}
		if err := createSchema(db, name); err != nil {
			return err
		}
	case !exist:
		if err := createSchema(db, name); err != nil {
			return err
		}
	}
	return nil
}

const columns = "id integer, field double"

// createTable creates a table on the databases.
func createTable(db *sql.DB, schemaName, tableName string) error {
	_, err := db.Exec(fmt.Sprintf("create column table %s.%s (%s)", driver.Identifier(schemaName), driver.Identifier(tableName), columns))
	return err
}

// dropTable drops a table from the databases.
func dropTable(db *sql.DB, schemaName, tableName string) error {
	_, err := db.Exec(fmt.Sprintf("drop table %s.%s", driver.Identifier(schemaName), driver.Identifier(tableName)))
	return err
}

// existTable returns true if the table exists in schema.
func existTable(db *sql.DB, schemaName, tableName string) (bool, error) {
	numTables := 0
	if err := db.QueryRow(fmt.Sprintf("select count(*) from sys.tables where schema_name = '%s' and table_name = '%s'", schemaName, tableName)).Scan(&numTables); err != nil {
		return false, err
	}
	return numTables != 0, nil
}

// ensureTable creates a table if it does not exist. If drop is set, an existing table would be dropped before recreated.
func ensureTable(db *sql.DB, schemaName, tableName string, drop bool) error {
	exist, err := existTable(db, schemaName, tableName)
	if err != nil {
		return err
	}

	switch {
	case exist && drop:
		if err := dropTable(db, schemaName, tableName); err != nil {
			return err
		}
		if err := createTable(db, schemaName, tableName); err != nil {
			return err
		}
	case !exist:
		if err := createTable(db, schemaName, tableName); err != nil {
			return err
		}
	}
	return nil
}

// deleteRows deletes all records in the database table.
func deleteRows(db *sql.DB, schemaName, tableName string) (int64, error) {
	result, err := db.Exec(fmt.Sprintf("delete from %s.%s", driver.Identifier(schemaName), driver.Identifier(tableName)))
	if err != nil {
		return 0, err
	}
	numRow, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return numRow, nil
}

// countRows returns the number of rows in the database table.
func countRows(db *sql.DB, schemaName, tableName string) (int64, error) {
	var numRow int64

	err := db.QueryRow(fmt.Sprintf("select count(*) from %s.%s", driver.Identifier(schemaName), driver.Identifier(tableName))).Scan(&numRow)
	if err != nil {
		return 0, err
	}
	return numRow, nil
}
