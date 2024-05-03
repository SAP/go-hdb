//go:build !unit

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"

	"github.com/SAP/go-hdb/driver"
)

// ExampleStmtMetadata demonstrates the use of statement metadata provided by PrepareContext.
func ExampleStmtMetadata() {
	const procOut = `create procedure %s (out message nvarchar(1024))
language SQLSCRIPT as
begin
    message := 'Hello World!';
end
`

	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	procedure := driver.RandomIdentifier("procOut_")

	if _, err := db.Exec(fmt.Sprintf(procOut, procedure)); err != nil { // Create stored procedure.
		log.Panic(err)
	}

	// Call PrepareContext with statement metadata context value.
	var stmtMetadata driver.StmtMetadata
	ctx := driver.WithStmtMetadata(context.Background(), &stmtMetadata)

	stmt, err := db.PrepareContext(ctx, fmt.Sprintf("call %s(?)", procedure))
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	// Create Exec args based on statement metadata columns...
	columnTypes := stmtMetadata.ColumnTypes()
	numColumnType := len(columnTypes)
	args := make([]any, numColumnType)

	for i, columnType := range columnTypes {
		out := reflect.New(columnType.ScanType()).Interface()
		args[i] = sql.Named(columnType.Name(), sql.Out{Dest: out})
	}

	// .. and execute Exec.
	if _, err := stmt.Exec(args...); err != nil {
		log.Panic(err)
	}

	// Finally print the values.
	for _, arg := range args {
		namedArg, ok := arg.(sql.NamedArg)
		if !ok {
			panic("should never happen")
		}
		sqlOut, ok := namedArg.Value.(sql.Out)
		if !ok {
			panic("should never happen")
		}

		switch dest := sqlOut.Dest.(type) {
		case *sql.NullString:
			fmt.Printf("%s: %s", namedArg.Name, dest.String)
		default:
			panic("should never happen")
		}
	}

	// output: MESSAGE: Hello World!
}
