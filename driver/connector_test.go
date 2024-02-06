//go:build !unit

package driver

import (
	"database/sql"
	"fmt"
	"testing"
)

func testExistSessionVariables(t *testing.T, sv1, sv2 map[string]string) {
	for k1, v1 := range sv1 {
		v2, ok := sv2[k1]
		if !ok {
			t.Fatalf("session variable value for %s does not exist", k1)
		}
		if v2 != v1 {
			t.Fatalf("session variable value for %s is %s - expected %s", k1, v2, v1)
		}
	}
}

func testNotExistSessionVariables(t *testing.T, keys []string, sv2 map[string]string) {
	for _, k1 := range keys {
		v2, ok := sv2[k1]
		if ok && v2 != "" {
			t.Fatalf("session variable value for %s is %s - should be empty", k1, v2)
		}
	}
}

func testSessionVariables(t *testing.T) {
	// mSessionContext represents the hdb M_SESSION_CONTEXT system view.
	type mSessionContext struct {
		host         string
		port         int
		connectionID int
		key          string
		value        string
		section      string
		// ddlEnabled   sql.NullInt64 // not always popuated (see HANA docu for m_session_context for reference).
	}

	sessionContext := func(db *sql.DB) ([]mSessionContext, error) {
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

	querySessionVariables := func(db *sql.DB) (map[string]string, error) {
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

	connector := MT.NewConnector()

	// set session variables
	sv1 := SessionVariables{"k1": "v1", "k2": "v2", "k3": "v3"}
	connector.SetSessionVariables(sv1)

	// check session variables
	db := sql.OpenDB(connector)
	defer db.Close()

	// retrieve session variables
	sv2, err := querySessionVariables(db)
	if err != nil {
		t.Fatal(err)
	}

	// check if session variables are set after connect to db.
	testExistSessionVariables(t, sv1, sv2)
	testNotExistSessionVariables(t, []string{"k4"}, sv2)
}

func printInvalidConnectAttempts(t *testing.T, username string) {
	db := MT.DB()
	invalidConnectAttempts := int64(0)
	// ignore error (entry not found)
	db.QueryRow(fmt.Sprintf("select invalid_connect_attempts from sys.invalid_connect_attempts where user_name = '%s'", username)).Scan(&invalidConnectAttempts) //nolint:errcheck
	t.Logf("number of invalid connect attempts: %d", invalidConnectAttempts)
}

func testRetryConnect(t *testing.T) {
	const invalidPassword = "invalid_password"

	connector := MT.NewConnector()

	password := connector.Password() // safe password
	refreshPassword := func() (string, bool) {
		printInvalidConnectAttempts(t, connector.Username())
		return password, true
	}
	connector.SetPassword(invalidPassword) // set invalid password
	connector.SetRefreshPassword(refreshPassword)
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestConnector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testSessionVariables", testSessionVariables},
		{"testRetryConnect", testRetryConnect},
	}

	for _, test := range tests {
		test := test // new test to run in parallel
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t)
		})
	}
}
