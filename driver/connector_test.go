//go:build !unit

package driver

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
)

func TestConnector(t *testing.T) {

	testExistSessionVariables := func(t *testing.T, sv1, sv2 map[string]string) {
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

	testNotExistSessionVariables := func(t *testing.T, keys []string, sv2 map[string]string) {
		for _, k1 := range keys {
			v2, ok := sv2[k1]
			if ok && v2 != "" {
				t.Fatalf("session variable value for %s is %s - should be empty", k1, v2)
			}
		}
	}

	testSessionVariables := func(t *testing.T) {
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

		ctr := MT.NewConnector()

		// set session variables
		sv1 := SessionVariables{"k1": "v1", "k2": "v2", "k3": "v3"}
		ctr.SetSessionVariables(sv1)

		// check session variables
		db := sql.OpenDB(ctr)
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

	printInvalidConnectAttempts := func(t *testing.T, username string) {
		db := MT.DB()
		invalidConnectAttempts := int64(0)
		// ignore error (entry not found)
		db.QueryRow(fmt.Sprintf("select invalid_connect_attempts from sys.invalid_connect_attempts where user_name = '%s'", username)).Scan(&invalidConnectAttempts) //nolint:errcheck
		t.Logf("number of invalid connect attempts: %d", invalidConnectAttempts)
	}

	testRetryConnect := func(t *testing.T) {
		const invalidPassword = "invalid_password"

		ctr := MT.NewConnector()

		password := ctr.Password() // safe password
		refreshPassword := func() (string, bool) {
			printInvalidConnectAttempts(t, ctr.Username())
			return password, true
		}
		ctr.SetPassword(invalidPassword) // set invalid password
		ctr.SetRefreshPassword(refreshPassword)
		db := sql.OpenDB(ctr)
		defer db.Close()

		if err := db.Ping(); err != nil {
			t.Fatal(err)
		}
	}

	// test if concurrent auth refresh would deadlock.
	testAuthRefreshDeadlock := func(t *testing.T) {
		const numConcurrent = 100

		ctr := MT.NewConnector()

		ctr.SetRefreshPassword(func() (string, bool) { return "", true })
		ctr.SetRefreshToken(func() (string, bool) { return "", true })
		ctr.SetRefreshClientCert(func() ([]byte, []byte, bool) { return nil, nil, true })

		wg := new(sync.WaitGroup)
		start := make(chan struct{})
		for range numConcurrent {
			wg.Go(func() {
				<-start
				ctr.refresh() //nolint:errcheck
			})
		}
		// start refresh concurrently
		close(start)
		// wait for all go routines to end
		wg.Wait()
	}

	// test if auth refresh would work for getting connections cuncurrently.
	testAuthRefresh := func(t *testing.T) {
		const numConcurrent = 5 // limit to 5 as after 5 invalid attempts user is locked

		ctr := MT.NewConnector()

		if ctr._databaseName != "" {
			// test does not work in case of redirectCache.Load() is successful, as connect is called twice,
			// so that the password is most probably refreshed already on second call
			t.Skip("to execute test, don't use database redirection")
		}

		password := ctr.Password()
		ctr.SetPassword("invalid password")
		passwordRefreshed := false
		ctr.SetRefreshPassword(func() (string, bool) {
			if passwordRefreshed {
				return "", false
			}
			passwordRefreshed = true
			return password, true
		})
		db := sql.OpenDB(ctr)
		defer db.Close()

		wg := new(sync.WaitGroup)
		start := make(chan struct{})
		connCh := make(chan *sql.Conn, numConcurrent)
		errCh := make(chan error, numConcurrent)
		for range numConcurrent {
			wg.Go(func() {
				<-start
				conn, err := db.Conn(t.Context())
				if err != nil {
					errCh <- err
				} else {
					connCh <- conn
				}
			})
		}
		// start connections concurrently
		close(start)
		// wait for all go routines to end
		wg.Wait()
		close(connCh)
		close(errCh)
		// close connections
		for conn := range connCh {
			conn.Close()
		}
		// check errors (especially authentication errors in case the password refresh didn't work for any connection)
		for err := range errCh {
			t.Fatal(err)
		}
	}

	t.Parallel()

	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testSessionVariables", testSessionVariables},
		{"testRetryConnect", testRetryConnect},
		{"testAuthRefreshDeadlock", testAuthRefreshDeadlock},
		{"testAuthRefresh", testAuthRefresh},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t)
		})
	}
}
