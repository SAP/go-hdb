//go:build !unit

package driver

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// createTableOutProc creates a table type and a procedure with a single table output parameter.
func createTableOutProc(t *testing.T, db *sql.DB) string {
	ctx := t.Context()

	tableType := string(RandomIdentifier("tableTypeOut_"))
	if _, err := db.ExecContext(ctx, fmt.Sprintf("create type %s as table (i integer, x varchar(10))", tableType)); err != nil {
		t.Fatal(err)
	}

	const procTableOut = `create procedure %[1]s (in i integer, out t1 %[2]s)
	language SQLSCRIPT as
	begin
	  create local temporary table #test like %[2]s;
	  insert into #test values(0, 'A');
	  insert into #test values(1, 'B');
	  t1 = select * from #test;
	  drop table #test;
	end
	`
	proc := string(RandomIdentifier("procTableOut_"))
	if _, err := db.ExecContext(ctx, fmt.Sprintf(procTableOut, proc, tableType)); err != nil {
		t.Fatal(err)
	}
	return proc
}

// execTableOut runs the procedure on a pinned conn and returns the open table
// rows. The statement is closed here: it fires pre-release and stands down on
// the attached tracker, leaving the single close to the worker.
func execTableOut(t *testing.T, conn *sql.Conn, proc string) *sql.Rows {
	ctx := t.Context()

	stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("call %s(?, ?)", proc))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	var resultRows sql.Rows
	if _, err := stmt.ExecContext(ctx, 1, sql.Named("T1", sql.Out{Dest: &resultRows})); err != nil {
		t.Fatal(err)
	}
	return &resultRows
}

// queueTableOut runs one table-out call on a pinned conn, releases it with rows
// open, and returns the queued tracker state. Rows stay open: the caller owns
// them and must close to let the worker finalize.
func queueTableOut(ctx context.Context, t *testing.T, db *sql.DB, lc *connLifecycle, proc string) (*sql.Rows, chan struct{}) {
	pinned, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows := execTableOut(t, pinned, proc)
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.pending) != 1 {
		t.Fatalf("pending holds %d trackers, want 1", len(lc.pending))
	}
	return rows, lc.pending[0].done
}

func waitFinalized(t *testing.T, done chan struct{}) {
	select {
	case <-done: // worker finalized: pooled or closed
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finalize tracker in time")
	}
}

// closeRows closes the table rows; the last close wakes the worker.
func closeRows(t *testing.T, rows *sql.Rows) {
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}

// assertPool asserts the pool depth; when want is 1 it returns the parked conn.
func assertPool(t *testing.T, lc *connLifecycle, want int) *conn {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.pool) != want {
		t.Fatalf("pool holds %d conns, want %d", len(lc.pool), want)
	}
	if want == 1 {
		return lc.pool[0].conn
	}
	return nil
}

// testLifecycleReuse releases with rows open, then checks the next checkout
// hands back the identical pooled conn: db/sql holds nothing itself, so the
// checkout must come from the pool.
func testLifecycleReuse(t *testing.T, db *sql.DB, ctr *Connector, proc string) {
	ctx := t.Context()
	lc := ctr.lifecycle

	rows, done := queueTableOut(ctx, t, db, lc, proc)
	closeRows(t, rows)
	waitFinalized(t, done)
	parked := assertPool(t, lc, 1)

	reused, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reused.Close()

	var got *conn
	if err := reused.Raw(func(dc any) error {
		var ok bool
		if got, ok = dc.(*conn); !ok {
			t.Fatalf("reused conn has type %T, want *conn", dc)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != parked {
		t.Fatalf("checkout did not reuse the pooled conn")
	}
}

// testLifecycleDoubleCloseDrops tears the driver conn down directly while the
// tracker is still queued (db/sql itself calls Close once): the worker must
// drop the conn, never pool it.
func testLifecycleDoubleCloseDrops(t *testing.T, db *sql.DB, ctr *Connector, proc string) {
	ctx := t.Context()
	lc := ctr.lifecycle

	rows, done := queueTableOut(ctx, t, db, lc, proc)
	lc.mu.Lock()
	conn := lc.pending[0].stmt.conn
	lc.mu.Unlock()

	_ = conn.close() // second Close: real teardown under the queued tracker
	closeRows(t, rows)
	waitFinalized(t, done)
	assertPool(t, lc, 0)
}

// testLifecycleInTxRefused forces in-tx before the last rows close (synthetic:
// unreachable via the public API, tx close clears it before release). The worker
// must close the conn instead of pooling tx-dirty state.
func testLifecycleInTxRefused(t *testing.T, db *sql.DB, ctr *Connector, proc string) {
	ctx := t.Context()
	lc := ctr.lifecycle

	rows, done := queueTableOut(ctx, t, db, lc, proc)
	lc.mu.Lock()
	lc.pending[0].stmt.conn.session.inTx.Store(true)
	lc.mu.Unlock()

	closeRows(t, rows)
	waitFinalized(t, done)
	assertPool(t, lc, 0)
}

// testLifecycleBadRejected cancels the pooled session before checkout: get must
// reject it (close + dial fresh) instead of handing back a dead conn.
func testLifecycleBadRejected(t *testing.T, db *sql.DB, ctr *Connector, proc string) {
	ctx := t.Context()
	lc := ctr.lifecycle

	rows, done := queueTableOut(ctx, t, db, lc, proc)
	closeRows(t, rows)
	waitFinalized(t, done)
	parked := assertPool(t, lc, 1)
	parked.session.cancel() // dead while pooled: resetSession must refuse it

	reused, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reused.Close()

	var got *conn
	if err := reused.Raw(func(dc any) error {
		var ok bool
		if got, ok = dc.(*conn); !ok {
			t.Fatalf("reused conn has type %T, want *conn", dc)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got == parked {
		t.Fatalf("checkout reused a canceled pooled conn")
	}
}
func TestConnLifecycle(t *testing.T) {
	t.Parallel()

	// Shared table-out procedure, created once before the subtests run:
	// subtests only call it, which is session-safe.
	proc := createTableOutProc(t, MT.DB())

	tests := []struct {
		name string
		fct  func(t *testing.T, db *sql.DB, ctr *Connector, proc string)
	}{
		{"lifecycleReuse", testLifecycleReuse},
		{"lifecycleDoubleCloseDrops", testLifecycleDoubleCloseDrops},
		{"lifecycleInTxRefused", testLifecycleInTxRefused},
		{"lifecycleBadRejected", testLifecycleBadRejected},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctr := MT.NewConnector() // isolated lifecycle
			db := sql.OpenDB(ctr)
			defer db.Close()

			test.fct(t, db, ctr, proc)
		})
	}
}
