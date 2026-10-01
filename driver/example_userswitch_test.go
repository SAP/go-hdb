//go:build !unit

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

// ExampleWithUserSwitch demonstrates switching users on new or existing connections.
func ExampleWithUserSwitch() {
	ctrCfg := driver.MT.Connector().Config()
	ctr, err := driver.NewConfigConnector(&ctrCfg)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(ctr)
	defer db.Close()

	tableName := driver.RandomIdentifier("table_")

	// Own connector, freshly built: no refresh can have run, so the
	// config copy is current.
	sessionUser := &driver.SessionUser{Username: ctrCfg.Username, Password: ctrCfg.Password}
	ctx := driver.WithUserSwitch(context.Background(), sessionUser)

	// Create table.
	if _, err := db.ExecContext(context.Background(), fmt.Sprintf("create table %s (i integer)", tableName)); err != nil {
		log.Fatal(err)
	}

	// Switch user via context.
	stmt, err := db.PrepareContext(ctx, fmt.Sprintf("insert into %s values (?)", tableName))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	// Switch to different user not possible in context of statement and transactions, but
	// former context can be used as long as the session user data are not changed.
	if _, err := stmt.ExecContext(ctx, 42); err != nil {
		log.Fatal(err)
	}

	// Drop table.
	if _, err := db.ExecContext(context.Background(), fmt.Sprintf("drop table %s", tableName)); err != nil {
		log.Fatal(err)
	}

	// output:
}
