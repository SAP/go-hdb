//go:build !unit

package driver

import (
	"context"
	"database/sql"
	"sync"
	"testing"
)

// test if concurrent refresh would deadlock.
func testRefreshDeadlock(t *testing.T) {
	const numConcurrent = 100

	attrs := &authAttrs{}
	attrs.SetRefreshPassword(func() (string, bool) { return "", true })
	attrs.SetRefreshToken(func() (string, bool) { return "", true })
	attrs.SetRefreshClientCert(func() ([]byte, []byte, bool) { return nil, nil, true })

	wg := new(sync.WaitGroup)
	wg.Add(numConcurrent)
	start := make(chan struct{})
	for i := 0; i < numConcurrent; i++ {
		go func(start <-chan struct{}, wg *sync.WaitGroup) {
			defer wg.Done()
			<-start
			attrs.refresh() //nolint:errcheck
		}(start, wg)
	}
	// start refresh concurrently
	close(start)
	// wait for all go routines to end
	wg.Wait()
}

// test if refresh would work for getting connections cuncurrently.
func testRefresh(t *testing.T) {
	const numConcurrent = 5 // limit to 5 as after 5 invalid attempts user is locked

	connector := MT.NewConnector()

	if connector._databaseName != "" {
		// test does not work in case of redirectCache.Load() is successful, as connect is called twice,
		// so that the password is most probably refreshed already on second call
		t.Skip("to execute test, don't use database redirection")
	}

	password := connector.Password()
	connector.SetPassword("invalid password")
	passwordRefreshed := false
	connector.SetRefreshPassword(func() (string, bool) {
		if passwordRefreshed {
			return "", false
		}
		passwordRefreshed = true
		return password, true
	})
	db := sql.OpenDB(connector)
	defer db.Close()

	wg := new(sync.WaitGroup)
	wg.Add(numConcurrent)
	start := make(chan struct{})
	connCh := make(chan *sql.Conn, numConcurrent)
	errCh := make(chan error, numConcurrent)
	for i := 0; i < numConcurrent; i++ {
		go func(start <-chan struct{}, connCh chan *sql.Conn, errCh chan error, wg *sync.WaitGroup) {
			defer wg.Done()
			<-start
			conn, err := db.Conn(context.Background())
			if err != nil {
				errCh <- err
			} else {
				connCh <- conn
			}
		}(start, connCh, errCh, wg)
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

func TestAuthAttrs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testRefreshDeadlock", testRefreshDeadlock},
		{"testRefresh", testRefresh},
	}

	for _, test := range tests {
		test := test // new test to run in parallel

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t)
		})
	}
}
