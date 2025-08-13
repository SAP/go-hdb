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
	ctx, cancel := context.WithCancel(t.Context())

	// callback function to cancel context
	cancelCtx := func(op int) {
		if op == choStmtExec {
			cancel()
		}
	}
	// set hook context.
	hookCtx := withConnHook(ctx, cancelCtx)
	// exec - should return with error context.Cancelled.
	if _, err := stmt.ExecContext(hookCtx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// use statement again - should work even first stmt.Exec got cancelled.
	for range 5 {
		if _, err := stmt.Exec(); err != nil {
			t.Fatal(err)
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
	}

	db := MT.DB()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t, db)
		})
	}
}
