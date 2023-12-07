//go:build !unit

package sqlscript_test

import (
	"bufio"
	"database/sql"
	"log"
	"os"
	"strings"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/sqlscript"
)

// Example demonstrates the usage of go-hdb sqlscript scanning functions.
func Example() {
	ddlScript := `
-- create local temporary table
CREATE LOCAL TEMPORARY TABLE #my_local_temp_table (
	Column1 INTEGER,
	Column2 VARCHAR(10)
);
--- insert some records
INSERT INTO #MY_LOCAL_TEMP_TABLE VALUES (1,'A');
INSERT INTO #MY_LOCAL_TEMP_TABLE VALUES (2,'B');
--- and drop the table
DROP TABLE #my_local_temp_table
`

	const envDSN = "GOHDBDSN"

	dsn := os.Getenv(envDSN)
	// exit if dsn is missing.
	if dsn == "" {
		return
	}

	connector, err := driver.NewDSNConnector(dsn)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	scanner := bufio.NewScanner(strings.NewReader(ddlScript))
	// Include comments as part of the sql statements.
	scanner.Split(sqlscript.ScanFunc(sqlscript.DefaultSeparator, true))

	for scanner.Scan() {
		if _, err := db.Exec(scanner.Text()); err != nil {
			log.Panic(err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Panic(err)
	}

	// output:
}
