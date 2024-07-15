//go:build !unit

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func TestUserSwitch(t *testing.T) {
	t.Parallel()

	ctr := driver.MT.Connector()
	tableName := driver.RandomIdentifier("table_")

	sessionUser := &driver.SessionUser{Username: ctr.Username(), Password: ctr.Password()}
	ctx := driver.WithUserSwitch(context.Background(), sessionUser)

	secondSessionUser := &driver.SessionUser{Username: "secondUser", Password: "secondPassword"}
	secondCtx := driver.WithUserSwitch(context.Background(), secondSessionUser)

	createTable := func() {
		db := sql.OpenDB(ctr)
		defer db.Close()

		// Create table.
		if _, err := db.Exec(fmt.Sprintf("create table %s (i integer)", tableName)); err != nil {
			t.Fatal(err)
		}
		// Insert record.
		if _, err := db.Exec(fmt.Sprintf("insert into %s values (?)", tableName), 42); err != nil {
			t.Fatal(err)
		}
	}

	testUserSwitchOnNew := func() {
		db := sql.OpenDB(ctr)
		defer db.Close()

		i := 0
		// switch user on new connection.
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := conn.QueryRowContext(ctx, fmt.Sprintf("select * from %s", tableName)).Scan(&i); err != nil {
			t.Fatal(err)
		}
	}

	testUserSwitchOnExisting := func() {
		db := sql.OpenDB(ctr)
		defer db.Close()

		i := 0
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		// switch user on existing connection.
		if err := conn.QueryRowContext(ctx, fmt.Sprintf("select * from %s", tableName)).Scan(&i); err != nil {
			t.Fatal(err)
		}
	}

	testUserSwitchOnStmt := func() {
		db := sql.OpenDB(ctr)
		defer db.Close()

		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		// switch user.
		stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("select * from %s", tableName))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
		// switch user on statement context should throw an error.
		_, err = stmt.QueryContext(secondCtx) //nolint:sqlclosecheck
		switch err {
		// expected error.
		case driver.ErrSwitchUser: //nolint:errorlint
		case nil:
			t.Fatalf("expected error %s", driver.ErrSwitchUser)
		default:
			t.Fatalf("expected error %s - got %s", driver.ErrSwitchUser, err)
		}
	}

	testUserSwitchOnTx := func() {
		db := sql.OpenDB(ctr)
		defer db.Close()

		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		// switch user.
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback() //nolint:errcheck
		// switch user on transaction context should throw an error.
		_, err = tx.PrepareContext(secondCtx, fmt.Sprintf("select * from %s", tableName)) //nolint:sqlclosecheck
		switch err {
		// expected error.
		case driver.ErrSwitchUser: //nolint:errorlint
		case nil:
			t.Fatalf("expected error %s", driver.ErrSwitchUser)
		default:
			t.Fatalf("expected error %s - got %s", driver.ErrSwitchUser, err)
		}
	}

	tests := []struct {
		name string
		fn   func()
	}{
		{"testUserSwitchOnNew", testUserSwitchOnNew},
		{"testUserSwitchOnExisting", testUserSwitchOnExisting},
		{"testUserSwitchOnStmt", testUserSwitchOnStmt},
		{"testUserSwitchOnTx", testUserSwitchOnTx},
	}

	createTable()

	for _, test := range tests {
		test := test // new test to run in parallel

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fn()
		})
	}
}
