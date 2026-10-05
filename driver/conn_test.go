//go:build !unit

package driver

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

func testCancelSleep(t *testing.T, _ *sql.DB) {
	// Dedicated connector with a short cancel budget: the SLEEP victim
	// provably outlives it, so the cancel misses deterministically.
	cfg := MT.Connector().Config()
	cfg.CancelTimeout = 300 * time.Millisecond
	ctr, err := NewConfigConnector(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(ctr)
	defer db.Close()

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(ctx, "DO BEGIN USING SQLSCRIPT_SYNC AS SYNCLIB; CALL SYNCLIB:SLEEP_SECONDS(1); END")
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond) // victim inside SLEEP.
	cancel()
	// Drained abort reports the server truth (HANA error 139).
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		if hdbErrs, ok := errors.AsType[*p.HdbErrors](err); !ok || hdbErrs.Code() != 139 {
			t.Fatal(err)
		}
	}

	// The session was severed; the statement must work on a fresh one.
	if _, err := db.ExecContext(t.Context(), "select * from dummy"); err != nil {
		t.Fatal(err)
	}
}

func testCancelContext(t *testing.T, db *sql.DB) {
	stmt, err := db.PrepareContext(t.Context(), "select * from dummy")
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
	// exec - the instant statement finishes inside the gate: reply-first
	// keep reports its success despite the entry cancel.
	if _, err := stmt.ExecContext(hookCtx); err != nil {
		t.Fatal(err)
	}

	// use statement again - should work even first stmt.Exec got canceled.
	if _, err := stmt.ExecContext(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func testCancelLegacy(t *testing.T, _ *sql.DB) {
	// Dedicated legacy connector: no CANCEL attempted, the sever is
	// immediate, so the caller deterministically gets context.Canceled.
	cfg := MT.Connector().Config()
	cfg.CancelTimeout = 0
	ctr, err := NewConfigConnector(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(ctr)
	defer db.Close()

	stmt, err := db.PrepareContext(t.Context(), "select * from dummy")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancelCtx := func(op int) {
		if op == choStmtExec {
			cancel()
		}
	}
	hookCtx := withConnHook(ctx, cancelCtx)
	if _, err := stmt.ExecContext(hookCtx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// The session was severed; the statement must work on a fresh one.
	if _, err := stmt.ExecContext(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, db *sql.DB)
	}{
		{"cancelContext", testCancelContext},
		{"cancelSleep", testCancelSleep},
		{"cancelLegacy", testCancelLegacy},
	}

	db := MT.DB()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fn(t, db)
		})
	}
}
