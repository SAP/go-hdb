package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"

	"github.com/SAP/go-hdb/driver"
)

// Database operation URL paths.
const (
	cmdCountRows    = "countRows"
	cmdDeleteRows   = "deleteRows"
	cmdCreateTable  = "createTable"
	cmdDropTable    = "dropTable"
	cmdCreateSchema = "createSchema"
	cmdDropSchema   = "dropSchema"
)

// dbHandler implements the http.Handler interface for database operations.
type dbHandler struct {
	tmpl *template.Template
	dba  *dba
}

// newDBHandler returns a new DBHandler instance.
func newDBHandler(dba *dba, templateFS fs.FS) (*dbHandler, error) {
	tmpl, err := template.ParseFS(templateFS, tmplDBResult)
	if err != nil {
		return nil, err
	}
	return &dbHandler{tmpl: tmpl, dba: dba}, nil
}

func (h *dbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := newURLQuery(r)

	command := q.getString(urlQueryCommand, "")

	result := h.dba.executeCommand(command)

	log.Printf("%s", result)
	h.tmpl.Execute(w, result) //nolint: errcheck
}

// dbResult is the structure used to provide db command result response.
type dbResult struct {
	Command string
	NumRow  int64
	Err     error
}

func (r *dbResult) String() string {
	switch {
	case r.Err != nil:
		return fmt.Sprintf("command: %s error: %s", r.Command, r.Err)
	case r.NumRow != -1:
		return fmt.Sprintf("command: %s: %d rows", r.Command, r.NumRow)
	default:
		return fmt.Sprintf("command: %s: ok", r.Command)
	}
}

const (
	schemaPrefix = "goHdbTest_"
	tablePrefix  = "table_"
)

type dba struct {
	schemaName driver.Identifier
	tableName  driver.Identifier
	db         *sql.DB
}

func newDBA(dsn string) (*dba, error) {
	ctr, err := driver.NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}
	return &dba{
		schemaName: driver.RandomIdentifier(schemaPrefix),
		tableName:  driver.RandomIdentifier(tablePrefix),
		db:         sql.OpenDB(ctr),
	}, nil
}

func (dba *dba) close() error {
	err1 := dropSchema(dba.db, dba.schemaName, true)
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
		result.Err = createTable(dba.db, dba.schemaName, dba.tableName)
	case cmdDropTable:
		result.Err = dropTable(dba.db, dba.schemaName, dba.tableName)
	case cmdDeleteRows:
		result.NumRow, result.Err = deleteRows(dba.db, dba.schemaName, dba.tableName)
	case cmdCountRows:
		result.NumRow, result.Err = countRows(dba.db, dba.schemaName, dba.tableName)
	case cmdCreateSchema:
		result.Err = createSchema(dba.db, dba.schemaName)
	case cmdDropSchema:
		result.Err = dropSchema(dba.db, dba.schemaName, true)
	default:
		result.Err = fmt.Errorf("invalid command: %s", command)
	}
	return result
}

// createSchema creates a schema on the database.
func createSchema(db *sql.DB, name driver.Identifier) error {
	_, err := db.Exec(fmt.Sprintf("create schema %s", name))
	return err
}

// dropSchema drops a schema from the database even if the schema is not empty.
func dropSchema(db *sql.DB, name driver.Identifier, cascade bool) error {
	var stmt string
	if cascade {
		stmt = fmt.Sprintf("drop schema %s cascade", name)
	} else {
		stmt = fmt.Sprintf("drop schema %s", name)
	}
	_, err := db.Exec(stmt)
	return err
}

// existSchema returns true if the schema exists.
func existSchema(db *sql.DB, name driver.Identifier) (bool, error) {
	numSchemas := 0
	if err := db.QueryRow(fmt.Sprintf("select count(*) from sys.schemas where schema_name = '%s'", string(name))).Scan(&numSchemas); err != nil {
		return false, err
	}
	return numSchemas != 0, nil
}

// ensureSchema creates a schema if it does not exist. If drop is set, an existing schema would be dropped before recreated.
func ensureSchema(db *sql.DB, name driver.Identifier, drop, cascade bool) error {
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
func createTable(db *sql.DB, schemaName, tableName driver.Identifier) error {
	_, err := db.Exec(fmt.Sprintf("create column table %s.%s (%s)", schemaName, tableName, columns))
	return err
}

// dropTable drops a table from the databases.
func dropTable(db *sql.DB, schemaName, tableName driver.Identifier) error {
	_, err := db.Exec(fmt.Sprintf("drop table %s.%s", schemaName, tableName))
	return err
}

// existTable returns true if the table exists in schema.
func existTable(db *sql.DB, schemaName, tableName driver.Identifier) (bool, error) {
	numTables := 0
	if err := db.QueryRow(fmt.Sprintf("select count(*) from sys.tables where schema_name = '%s' and table_name = '%s'", string(schemaName), string(tableName))).Scan(&numTables); err != nil {
		return false, err
	}
	return numTables != 0, nil
}

// ensureTable creates a table if it does not exist. If drop is set, an existing table would be dropped before recreated.
func ensureTable(db *sql.DB, schemaName, tableName driver.Identifier, drop bool) error {
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
func deleteRows(db *sql.DB, schemaName, tableName driver.Identifier) (int64, error) {
	result, err := db.Exec(fmt.Sprintf("delete from %s.%s", schemaName, tableName))
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
func countRows(db *sql.DB, schemaName, tableName driver.Identifier) (int64, error) {
	var numRow int64

	err := db.QueryRow(fmt.Sprintf("select count(*) from %s.%s", schemaName, tableName)).Scan(&numRow)
	if err != nil {
		return 0, err
	}
	return numRow, nil
}
