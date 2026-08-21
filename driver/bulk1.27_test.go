//go:build !unit && go1.27

package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// TestBulkBlob.
func testBulkBlob(t *testing.T, ctr *Connector, db *sql.DB) {
	const numRows = 100
	chunkSize := ctr.LobChunkSize()
	bigData := strings.Repeat("a", chunkSize)

	smallLobData := func(i int) string {
		return fmt.Sprintf("%s-%d", "Go rocks", i)
	}

	bigLobData := func(i int) string {
		return fmt.Sprintf("%s-%d", bigData, i)
	}

	tmpTableName := RandomIdentifier("#tmpTable")

	// keep connection / hdb session for using local temporary tables
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// cleanup
	defer tx.Rollback() //nolint: errcheck

	if _, err := tx.ExecContext(t.Context(), fmt.Sprintf("create local temporary table %s (i integer, b1 blob, b2 blob)", tmpTableName)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := tx.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values (?, ?, ?)", tmpTableName))
	if err != nil {
		t.Fatalf("prepare bulk insert failed: %s", err)
	}
	defer stmt.Close()

	// call insert function
	i := 0
	if _, err := stmt.ExecContext(t.Context(), func(args []any) error {
		if i >= numRows {
			return ErrEndOfRows
		}
		args[0], args[1], args[2] = i, bigLobData(i), smallLobData(i)
		i++
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// check
	err = tx.QueryRowContext(t.Context(), fmt.Sprintf("select count(*) from %s", tmpTableName)).Scan(&i)
	if err != nil {
		t.Fatalf("select count failed: %s", err)
	}

	if i != numRows {
		t.Fatalf("invalid number of records %d - %d expected", i, numRows)
	}

	rows, err := tx.QueryContext(t.Context(), fmt.Sprintf("select * from %s order by i", tmpTableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var lob1, lob2 string
	i = 0
	for rows.Next() {
		var j int
		if err := rows.Scan(&j, &lob1, &lob2); err != nil {
			t.Fatal(err)
		}
		if j != i {
			t.Fatalf("value %d - expected %d", j, i)
		}
		if lob1 != bigLobData(i) {
			t.Fatalf("value %s - expected %s", lob1, bigLobData(i))
		}
		if lob2 != smallLobData(i) {
			t.Fatalf("value %s - expected %s", lob2, smallLobData(i))
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func testBulkBlob106(t *testing.T, ctr *Connector, db *sql.DB) {
	/*
		issue https://github.com/SAP/go-hdb/issues/106
		precondition:
			- bulk insert of blob data
			- most of the blob content does fit into lob chunk size
			- only some of the blob content did exceed lob chunk size
	*/

	tableName := RandomIdentifier("bulkBlob106")

	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (i integer, b nclob)", tableName)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	const (
		numRecs           = 1000
		numRecsPerCall    = numRecs / 10
		bigChunkSizeRecNo = 77 // record exceeding lob chunk size
	)

	chunkSize := MT.Connector().LobChunkSize()

	testData := [numRecsPerCall]string{}

	for i := range numRecsPerCall {
		if i == bigChunkSizeRecNo {
			testData[i] = strings.Repeat("a", chunkSize+1)
		} else {
			testData[i] = "b"
		}
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values (?, ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	args := make([]any, 2*numRecsPerCall)

	for i := range numRecs / numRecsPerCall {
		for j := range numRecsPerCall {
			args[j*2] = i*numRecsPerCall + j
			args[j*2+1] = testData[j]
		}
		if _, err := stmt.ExecContext(t.Context(), args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// check
	i := 0
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("select count(*) from %s", tableName)).Scan(&i)
	if err != nil {
		t.Fatalf("select count failed: %s", err)
	}

	if i != numRecs {
		t.Fatalf("invalid number of records %d - %d expected", i, numRecs)
	}

	rows, err := db.QueryContext(t.Context(), fmt.Sprintf("select * from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var lob string
	i = 0
	for rows.Next() {
		var j int
		if err := rows.Scan(&j, &lob); err != nil {
			t.Fatal(err)
		}
		if j != i {
			t.Fatalf("value %d - expected %d", j, i)
		}
		if lob != testData[j%numRecsPerCall] {
			t.Fatalf("value %s len %d - expected %s len %d",
				lob,
				len(lob),
				testData[j%numRecsPerCall],
				len(testData[j%numRecsPerCall]))
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
