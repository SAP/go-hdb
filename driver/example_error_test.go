//go:build !unit

package driver_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

const (
	errCodeInvalidTableName = 259
)

func ExampleError() {
	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	invalidTableName := driver.RandomIdentifier("table_")
	stmt, err := db.QueryContext(context.Background(), fmt.Sprintf("select * from %s", invalidTableName))
	if err == nil {
		defer stmt.Close()
	}

	// Check if error is driver.Error.
	if dbError, ok := errors.AsType[driver.Error](err); ok {
		switch dbError.Code() {
		case errCodeInvalidTableName:
			fmt.Print("invalid table name")
		default:
			log.Fatalf("code %d text %s", dbError.Code(), dbError.Text())
		}
	}
	// output: invalid table name
}
