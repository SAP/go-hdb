//go:build !unit

package driver

import (
	"database/sql"
	"fmt"
	"testing"
)

func testTransactionCommit(t *testing.T, db *sql.DB) {
	table := RandomIdentifier("testTxCommit_")
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (i tinyint)", table)); err != nil {
		t.Fatal(err)
	}

	tx1, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	tx2, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback() //nolint:errcheck

	// insert record in transaction 1
	if _, err := tx1.ExecContext(t.Context(), fmt.Sprintf("insert into %s values(42)", table)); err != nil {
		t.Fatal(err)
	}

	// count records in transaction 1
	i := 0
	if err := tx1.QueryRowContext(t.Context(), fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatal(fmt.Errorf("tx1: invalid number of records %d - 1 expected", i))
	}

	// count records in transaction 2 - isolation level 'read committed' (default) expected, so no record should be there
	if err := tx2.QueryRowContext(t.Context(), fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 0 {
		t.Fatal(fmt.Errorf("tx2: invalid number of records %d - 0 expected", i))
	}

	// commit insert
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	// in isolation level 'read committed' (default) record should be visible now
	if err := tx2.QueryRowContext(t.Context(), fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatal(fmt.Errorf("tx2: invalid number of records %d - 1 expected", i))
	}
}

func testTransactionRollback(t *testing.T, db *sql.DB) {
	table := RandomIdentifier("testTxRollback_")
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (i tinyint)", table)); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// insert record
	if _, err := tx.ExecContext(t.Context(), fmt.Sprintf("insert into %s values(42)", table)); err != nil {
		t.Fatal(err)
	}

	// count records
	i := 0
	if err := tx.QueryRowContext(t.Context(), fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatal(fmt.Errorf("tx: invalid number of records %d - 1 expected", i))
	}

	// rollback insert
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// new transaction
	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck

	// rollback - no record expected
	if err := tx.QueryRowContext(t.Context(), fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 0 {
		t.Fatal(fmt.Errorf("tx: invalid number of records %d - 0 expected", i))
	}
}

// testTransactionAccessModeRestore proves a read-only transaction does not leave
// the connection in read-only mode: a following autocommit write must succeed.
// Before the restore was added this failed with HANA SQL error 129.
func testTransactionAccessModeRestore(t *testing.T, db *sql.DB) {
	ctx := t.Context()
	conn, err := db.Conn(ctx) // pin one physical connection
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	table := RandomIdentifier("testTxAccessRestore_")
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("create table %s (i integer)", table)); err != nil {
		t.Fatal(err)
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// autocommit write on the same connection after the read-only tx committed.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("insert into %s values (1)", table)); err != nil {
		t.Fatalf("autocommit write after read-only tx failed (access mode leaked): %s", err)
	}
}

// testTransactionStateTracking checks that the cached session transaction state
// follows what BeginTx sets and is restored to the pre-transaction value on
// commit and rollback. It reads the driver's own txState (not the server), so
// it verifies the driver's bookkeeping directly.
func testTransactionStateTracking(t *testing.T, db *sql.DB) {
	ctx := t.Context()
	sqlConn, err := db.Conn(ctx) // pin one physical connection
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()

	// readState returns the pinned connection's cached transaction state.
	readState := func() txState {
		var state txState
		if err := sqlConn.Raw(func(driverConn any) error {
			state = driverConn.(*conn).session.txState
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return state
	}

	if state := readState(); state != defaultTxState {
		t.Fatalf("initial txState %+v - %+v expected", state, defaultTxState)
	}

	run := func(name string, endTx func(*sql.Tx) error) {
		want := txState{isolationLevel: sql.LevelSerializable, readOnly: true}
		tx, err := sqlConn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		if state := readState(); state != want {
			_ = tx.Rollback()
			t.Fatalf("%s: txState in transaction %+v - %+v expected", name, state, want)
		}
		if err := endTx(tx); err != nil {
			t.Fatal(err)
		}
		if state := readState(); state != defaultTxState {
			t.Fatalf("%s: txState after end %+v - %+v expected (not restored)", name, state, defaultTxState)
		}
	}

	run("commit", (*sql.Tx).Commit)
	run("rollback", (*sql.Tx).Rollback)
}

func TestTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, db *sql.DB)
	}{
		{"transactionCommit", testTransactionCommit},
		{"transactionRollback", testTransactionRollback},
		{"transactionAccessModeRestore", testTransactionAccessModeRestore},
		{"transactionStateTracking", testTransactionStateTracking},
	}

	db := MT.DB()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fn(t, db)
		})
	}
}
