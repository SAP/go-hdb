// +build !unit

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/SAP/go-hdb/driver/drivertest"
)

func testCancelContext(db *sql.DB, t *testing.T) {
	stmt, err := db.Prepare("select * from dummy")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// create cancel context
	ctx, cancel := context.WithCancel(context.Background())
	// callback function to cancel context
	cancelCtx := func(c *Conn, op int) {
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
	if _, err := stmt.ExecContext(ctx); err != context.Canceled {
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

func TestConnection(t *testing.T) {
	tests := []struct {
		name string
		fct  func(db *sql.DB, t *testing.T)
	}{
		{"cancelContext", testCancelContext},
	}

	connector, err := NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(db, t)
		})
	}
}
