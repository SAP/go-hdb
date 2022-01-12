// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package drivertest

import (
	"database/sql"
	"fmt"
	"strconv"
)

// ConnectionID returns the hdb connection id.
func ConnectionID(db *sql.DB) (int, error) {
	var connectionID int

	err := db.QueryRow("select current_connection \"current connection\" from dummy").Scan(&connectionID)
	if err != nil {
		return 0, err
	}
	return connectionID, nil
}

// DummySelect executes a select dummy on hdb.
func DummySelect(db *sql.DB) error {
	var dummy string
	err := db.QueryRow("select * from dummy").Scan(&dummy)
	if err != nil {
		return err
	}
	if dummy != "X" {
		return fmt.Errorf("dummy is %s - expected %s", dummy, "X")
	}
	return nil
}

// mSessionContext represents the hdb M_SESSION_CONTEXT system view.
type mSessionContext struct {
	host         string
	port         int
	connectionID int
	key          string
	value        string
	section      string
	ddlEnabled   sql.NullInt64 // not always popuated (see HANA docu for m_session_context for reference).
}

func sessionContext(db *sql.DB) ([]mSessionContext, error) {
	rows, err := db.Query("select host, port, connection_id, key, value, section from m_session_context where connection_id=current_connection")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	mscs := []mSessionContext{}
	var msc mSessionContext

	for rows.Next() {
		if err := rows.Scan(&msc.host, &msc.port, &msc.connectionID, &msc.key, &msc.value, &msc.section); err != nil {
			return nil, err
		}
		mscs = append(mscs, msc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return mscs, nil
}

// SessionVariable returns the value of a session variable.
func SessionVariable(k string, db *sql.DB) (string, error) {
	var v string
	if err := db.QueryRow(fmt.Sprintf("select session_context('%s') from dummy", k)).Scan(&v); err != nil {
		return "", err
	}
	return v, nil
}

// SessionVariables returns a map of current session variables.
func SessionVariables(db *sql.DB) (map[string]string, error) {
	mscs, err := sessionContext(db)
	if err != nil {
		return nil, err
	}
	sv := make(map[string]string, len(mscs))
	for _, v := range mscs {
		sv[v.key] = v.value
	}
	return sv, nil
}

// CreateSchema creates a schema on the database.
func CreateSchema(db *sql.DB, schema string) error {
	_, err := db.Exec(fmt.Sprintf("create schema %s", strconv.Quote(schema)))
	return err
}

// DropSchema drops a schema from the database.
func DropSchema(db *sql.DB, schema string) error {
	_, err := db.Exec(fmt.Sprintf("drop schema %s cascade", strconv.Quote(schema)))
	return err
}

// DropTable drops a table from the database.
func DropTable(db *sql.DB, schema, table string) error {
	_, err := db.Exec(fmt.Sprintf("drop table %s.%s", strconv.Quote(schema), strconv.Quote(table)))
	return err
}

// NumTablesInSchema returns the number of tables in a database schema.
func NumTablesInSchema(db *sql.DB, schema string) (int, error) {
	numTables := 0
	if err := db.QueryRow(fmt.Sprintf("select count(*) from sys.tables where schema_name = '%s'", schema)).Scan(&numTables); err != nil {
		return 0, err
	}
	return numTables, nil
}

// NumProcsInSchema returns the number of stored procedures in a database schema.
func NumProcsInSchema(db *sql.DB, schema string) (int, error) {
	numProcs := 0
	if err := db.QueryRow(fmt.Sprintf("select count(*) from sys.procedures where schema_name = '%s'", schema)).Scan(&numProcs); err != nil {
		return 0, err
	}
	return numProcs, nil
}

// QuerySchemasPrefix returns all schemas of a database starting with prefix in name.
func QuerySchemasPrefix(db *sql.DB, prefix string) ([]string, error) {
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

// QueryTablesPrefix returns all tables of a database starting with prefix in name.
func QueryTablesPrefix(db *sql.DB, prefix string) ([]string, error) {
	names := make([]string, 0)

	rows, err := db.Query(fmt.Sprintf("select table_name from sys.tables where table_name like '%s_%%'", prefix))
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
