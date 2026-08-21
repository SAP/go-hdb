//go:build !unit

package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func testTransactionCommit(t *testing.T, db *sql.DB) {
	table := driver.RandomIdentifier("testTxCommit_")
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
	table := driver.RandomIdentifier("testTxRollback_")
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

func TestTransaction(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T, db *sql.DB)
	}{
		{"transactionCommit", testTransactionCommit},
		{"transactionRollback", testTransactionRollback},
	}

	db := driver.MT.DB()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t, db)
		})
	}
}
