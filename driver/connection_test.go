//go:build !unit

package driver

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func testCancelContext(t *testing.T, db *sql.DB) {
	stmt, err := db.Prepare("select * from dummy")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// create cancel context
	ctx, cancel := context.WithCancel(context.Background())
	// callback function to cancel context
	cancelCtx := func(c *conn, op int) {
		if op == choStmtExec {
			cancel()
		}
	}
	// set hook
	connHook = cancelCtx
	/*
		should return with err == context.Cancelled
		- works only if stmt.Exec does evaluate ctx and call the callback function
		  provided by context.WithValue
	*/
	if _, err := stmt.ExecContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// reset hook
	connHook = nil

	// use statement again
	// . should work even first stmt.Exec got cancelled
	for i := 0; i < 5; i++ {
		if _, err := stmt.Exec(); err != nil {
			t.Fatal(err)
		}
	}
}

func testCheckCallStmt(t *testing.T, db *sql.DB) {
	testData := []struct {
		stmt  string
		match bool
	}{
		{"call", false},
		{"call ", true},
		{"CALL ", true},
		{"caller", false},
		{"call function", true},
		{" call function", true},
		{"my call", false},
	}

	for _, data := range testData {
		if match := callStmt.MatchString(data.stmt); match != data.match {
			t.Fatalf("stmt %s regex match gives %t - expected %t", data.stmt, match, data.match)
		}
	}
}

func TestConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fct  func(t *testing.T, db *sql.DB)
	}{
		{"cancelContext", testCancelContext},
		{"checkCallStmt", testCheckCallStmt},
	}

	db := MT.DB()
	for _, test := range tests {
		test := test // new dfv to run in parallel

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t, db)
		})
	}
}
