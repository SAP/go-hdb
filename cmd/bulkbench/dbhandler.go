package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/SAP/go-hdb/driver"
)

const columns = "id integer, field double"

var (
	schemaName   = string(driver.RandomIdentifier(schemaPrefix))
	tableName    = string(driver.RandomIdentifier(tablePrefix))
	prepareQuery = fmt.Sprintf("insert into %s.%s values (?, ?)", driver.Identifier(schemaName), driver.Identifier(tableName))
)

func ensureSchemaTable(db *sql.DB) error {
	if err := ensureSchema(db, schemaName, drop, true); err != nil {
		return err
	}
	if err := ensureTable(db, schemaName, tableName, drop); err != nil {
		return err
	}
	return nil
}

// Database operation URL paths.
const (
	cmdCountRows    = "/db/countRows"
	cmdDeleteRows   = "/db/deleteRows"
	cmdCreateTable  = "/db/createTable"
	cmdDropTable    = "/db/dropTable"
	cmdCreateSchema = "/db/createSchema"
	cmdDropSchema   = "/db/dropSchema"
)

const (
	objTable = iota
	objSchema
)

var dbObjText = map[dbObj]string{objTable: "table", objSchema: "schema"}

type dbObj int

func (o dbObj) String() string { return dbObjText[o] }

const (
	opCountRows = iota
	opDeleteRows
	opCreate
	opDrop
)

var dbOpText = map[dbOp]string{opCountRows: "Count rows", opDeleteRows: "Delete rows", opCreate: "Create", opDrop: "Drop"}

type dbOp int

func (o dbOp) String() string { return dbOpText[o] }

// dbResult is the structure used to provide the JSON based cb command result response.
type dbResult struct {
	Command string
	DBObj   dbObj
	DBOp    dbOp
	ObjName string
	NumRow  int64
	Error   string
}

func (r *dbResult) String() string {
	switch {
	case r.Error != "":
		return fmt.Sprintf("%s %s %s error: %s", r.DBOp, r.DBObj, r.ObjName, r.Error)
	case r.NumRow != -1:
		return fmt.Sprintf("%s %s %s: %d rows", r.DBOp, r.DBObj, r.ObjName, r.NumRow)
	default:
		return fmt.Sprintf("%s %s %s: ok", r.DBOp, r.DBObj, r.ObjName)
	}
}

type dbFunc struct {
	Command string
	Obj     dbObj
	Op      dbOp
	f       func(q *urlQuery, r *dbResult) error
}

// dbHandler implements the http.Handler interface for database operations.
type dbHandler struct {
	log     func(format string, v ...any)
	db      *sql.DB
	dbFuncs map[string]*dbFunc
}

// newDBHandler returns a new DBHandler instance.
func newDBHandler(log func(format string, v ...any)) (*dbHandler, error) {
	connector, err := driver.NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}
	h := &dbHandler{
		log: log,
		db:  sql.OpenDB(connector),
	}
	h.dbFuncs = map[string]*dbFunc{
		cmdCountRows:    {Command: cmdCountRows, Obj: objTable, Op: opCountRows, f: h.countRows},
		cmdDeleteRows:   {Command: cmdDeleteRows, Obj: objTable, Op: opDeleteRows, f: h.deleteRows},
		cmdCreateTable:  {Command: cmdCreateTable, Obj: objTable, Op: opCreate, f: h.createTable},
		cmdDropTable:    {Command: cmdDropTable, Obj: objTable, Op: opDrop, f: h.dropTable},
		cmdCreateSchema: {Command: cmdCreateSchema, Obj: objSchema, Op: opCreate, f: h.createSchema},
		cmdDropSchema:   {Command: cmdDropSchema, Obj: objSchema, Op: opDrop, f: h.dropSchema},
	}
	return h, nil
}

// driverVersion returns the go-hdb driver version.
func (h dbHandler) driverVersion() string { return driver.DriverVersion }

// hdbVersion returns the hdb version.
func (h dbHandler) hdbVersion() string {
	conn, err := h.db.Conn(context.Background())
	if err != nil {
		return err.Error()
	}
	var hdbVersion string
	conn.Raw(func(driverConn any) error {
		hdbVersion = driverConn.(driver.Conn).HDBVersion().String()
		return nil
	})
	return hdbVersion
}

func (h dbHandler) schemaFuncs() []*dbFunc {
	return []*dbFunc{
		h.dbFuncs[cmdCreateSchema],
		h.dbFuncs[cmdDropSchema],
	}
}

func (h dbHandler) tableFuncs() []*dbFunc {
	return []*dbFunc{
		h.dbFuncs[cmdCreateTable],
		h.dbFuncs[cmdDropTable],
		h.dbFuncs[cmdDeleteRows],
		h.dbFuncs[cmdCountRows],
	}
}

func (h *dbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	command := r.URL.Path

	result := &dbResult{Command: command}

	defer func() {
		h.log("%s", result)
		e := json.NewEncoder(w)
		e.Encode(result) // ignore error
	}()

	var err error

	dbFunc, ok := h.dbFuncs[command]
	if ok {
		result.DBObj = dbFunc.Obj
		result.DBOp = dbFunc.Op
		err = dbFunc.f(newURLQuery(r), result)
	} else {
		err = fmt.Errorf("invalid command %s", command)
	}
	if err != nil {
		result.Error = err.Error()
	}
}

func (h *dbHandler) countRows(q *urlQuery, r *dbResult) error {
	r.ObjName = strings.Join([]string{schemaName, tableName}, ".")
	numRow, err := countRows(h.db, schemaName, tableName)
	if err != nil {
		return err
	}
	r.NumRow = numRow
	return nil
}

func (h *dbHandler) deleteRows(q *urlQuery, r *dbResult) error {
	r.ObjName = strings.Join([]string{schemaName, tableName}, ".")
	numRow, err := deleteRows(h.db, schemaName, tableName)
	if err != nil {
		return err
	}
	r.NumRow = numRow
	return nil
}

func (h *dbHandler) createTable(q *urlQuery, r *dbResult) error {
	r.ObjName = strings.Join([]string{schemaName, tableName}, ".")
	if err := createTable(h.db, schemaName, tableName); err != nil {
		return err
	}
	return nil
}

func (h *dbHandler) dropTable(q *urlQuery, r *dbResult) error {
	r.ObjName = strings.Join([]string{schemaName, tableName}, ".")
	if err := dropTable(h.db, schemaName, tableName); err != nil {
		return err
	}
	return nil
}

func (h *dbHandler) createSchema(q *urlQuery, r *dbResult) error {
	if err := createSchema(h.db, schemaName); err != nil {
		return err
	}
	return nil
}

func (h *dbHandler) dropSchema(q *urlQuery, r *dbResult) error {
	r.ObjName = schemaName
	if err := dropSchema(h.db, schemaName, true); err != nil {
		return err
	}
	return nil
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
