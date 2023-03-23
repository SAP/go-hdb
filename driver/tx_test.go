//go:build !unit

package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func testTransactionCommit(db *sql.DB, t *testing.T) {
	table := driver.RandomIdentifier("testTxCommit_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (i tinyint)", table)); err != nil {
		t.Fatal(err)
	}

	tx1, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	//insert record in transaction 1
	if _, err := tx1.Exec(fmt.Sprintf("insert into %s values(42)", table)); err != nil {
		t.Fatal(err)
	}

	//count records in transaction 1
	i := 0
	if err := tx1.QueryRow(fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatal(fmt.Errorf("tx1: invalid number of records %d - 1 expected", i))
	}

	//count records in transaction 2 - isolation level 'read committed'' (default) expected, so no record should be there
	if err := tx2.QueryRow(fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 0 {
		t.Fatal(fmt.Errorf("tx2: invalid number of records %d - 0 expected", i))
	}

	//commit insert
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	//in isolation level 'read commited' (default) record should be visible now
	if err := tx2.QueryRow(fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatal(fmt.Errorf("tx2: invalid number of records %d - 1 expected", i))
	}
}

func testTransactionRollback(db *sql.DB, t *testing.T) {
	table := driver.RandomIdentifier("testTxRollback_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (i tinyint)", table)); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	//insert record
	if _, err := tx.Exec(fmt.Sprintf("insert into %s values(42)", table)); err != nil {
		t.Fatal(err)
	}

	//count records
	i := 0
	if err := tx.QueryRow(fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatal(fmt.Errorf("tx: invalid number of records %d - 1 expected", i))
	}

	//rollback insert
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	//new transaction
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	//rollback - no record expected
	if err := tx.QueryRow(fmt.Sprintf("select count(*) from %s", table)).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 0 {
		t.Fatal(fmt.Errorf("tx: invalid number of records %d - 0 expected", i))
	}
}

func TestTransaction(t *testing.T) {
	tests := []struct {
		name string
		fct  func(db *sql.DB, t *testing.T)
	}{
		{"transactionCommit", testTransactionCommit},
		{"transactionRollback", testTransactionRollback},
	}

	db := driver.DefaultTestDB()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(db, t)
		})
	}
}
