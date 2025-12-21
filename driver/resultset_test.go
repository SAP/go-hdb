//go:build !unit

package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func testMultipleResultSetsDirect(t *testing.T, db *sql.DB) {
	// Test multiple result sets using a stored procedure with direct query execution.
	const procMultiRS = `create procedure %[1]s
language SQLSCRIPT as
begin
	select 1 as col1, 'first' as col2 from dummy;
	select 2 as col1, 'second' as col2 from dummy;
	select 3 as col1, 'third' as col2 from dummy;
end
`
	// use same connection
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// create procedure
	proc := driver.RandomIdentifier("procMultiRS_")
	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(procMultiRS, proc)); err != nil {
		t.Fatal(err)
	}

	// call procedure and iterate over result sets
	rows, err := conn.QueryContext(t.Context(), fmt.Sprintf("call %s", proc))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	expectedResults := []struct {
		col1 int
		col2 string
	}{
		{1, "first"},
		{2, "second"},
		{3, "third"},
	}

	resultSetIdx := 0
	for {
		// Process current result set
		for rows.Next() {
			var col1 int
			var col2 string
			if err := rows.Scan(&col1, &col2); err != nil {
				t.Fatal(err)
			}
			if resultSetIdx >= len(expectedResults) {
				t.Fatalf("unexpected result set at index %d", resultSetIdx)
			}
			if col1 != expectedResults[resultSetIdx].col1 {
				t.Fatalf("result set %d: col1 = %d, expected %d", resultSetIdx, col1, expectedResults[resultSetIdx].col1)
			}
			if col2 != expectedResults[resultSetIdx].col2 {
				t.Fatalf("result set %d: col2 = %s, expected %s", resultSetIdx, col2, expectedResults[resultSetIdx].col2)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		resultSetIdx++

		// Try to advance to next result set
		if !rows.NextResultSet() {
			break
		}
	}

	if resultSetIdx != len(expectedResults) {
		t.Fatalf("expected %d result sets, got %d", len(expectedResults), resultSetIdx)
	}
}

func testMultipleResultSetsPrepared(t *testing.T, db *sql.DB) {
	// Test multiple result sets using a prepared statement.
	const procMultiRSPrepared = `create procedure %[1]s (in val integer)
language SQLSCRIPT as
begin
	select val as col1, 'a' as col2 from dummy;
	select val + 1 as col1, 'b' as col2 from dummy;
end
`
	// use same connection
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// create procedure
	proc := driver.RandomIdentifier("procMultiRSPrepared_")
	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(procMultiRSPrepared, proc)); err != nil {
		t.Fatal(err)
	}

	// prepare and execute
	stmt, err := conn.PrepareContext(t.Context(), fmt.Sprintf("call %s(?)", proc))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	inputVal := 10
	rows, err := stmt.QueryContext(t.Context(), inputVal)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// First result set
	if !rows.Next() {
		t.Fatal("expected first row in first result set")
	}
	var col1 int
	var col2 string
	if err := rows.Scan(&col1, &col2); err != nil {
		t.Fatal(err)
	}
	if col1 != inputVal || col2 != "a" {
		t.Fatalf("first result set: got (%d, %s), expected (%d, %s)", col1, col2, inputVal, "a")
	}
	if rows.Next() {
		t.Fatal("expected only one row in first result set")
	}

	// Advance to second result set
	if !rows.NextResultSet() {
		t.Fatal("expected second result set")
	}

	// Second result set
	if !rows.Next() {
		t.Fatal("expected first row in second result set")
	}
	if err := rows.Scan(&col1, &col2); err != nil {
		t.Fatal(err)
	}
	if col1 != inputVal+1 || col2 != "b" {
		t.Fatalf("second result set: got (%d, %s), expected (%d, %s)", col1, col2, inputVal+1, "b")
	}
	if rows.Next() {
		t.Fatal("expected only one row in second result set")
	}

	// Should have no more result sets
	if rows.NextResultSet() {
		t.Fatal("expected no more result sets")
	}
}

func testMultipleResultSetsWithMultipleRows(t *testing.T, db *sql.DB) {
	// Test multiple result sets where each result set has multiple rows.
	const procMultiRowRS = `create procedure %[1]s
language SQLSCRIPT as
begin
	select * from (select 1 as val from dummy union all select 2 as val from dummy union all select 3 as val from dummy);
	select * from (select 'a' as letter from dummy union all select 'b' as letter from dummy);
end
`
	// use same connection
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// create procedure
	proc := driver.RandomIdentifier("procMultiRowRS_")
	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(procMultiRowRS, proc)); err != nil {
		t.Fatal(err)
	}

	rows, err := conn.QueryContext(t.Context(), fmt.Sprintf("call %s", proc))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// First result set: 3 integer rows
	expectedInts := []int{1, 2, 3}
	idx := 0
	for rows.Next() {
		var val int
		if err := rows.Scan(&val); err != nil {
			t.Fatal(err)
		}
		if idx >= len(expectedInts) {
			t.Fatalf("too many rows in first result set")
		}
		if val != expectedInts[idx] {
			t.Fatalf("first result set row %d: got %d, expected %d", idx, val, expectedInts[idx])
		}
		idx++
	}
	if idx != len(expectedInts) {
		t.Fatalf("first result set: got %d rows, expected %d", idx, len(expectedInts))
	}

	// Advance to second result set
	if !rows.NextResultSet() {
		t.Fatal("expected second result set")
	}

	// Second result set: 2 string rows
	expectedStrings := []string{"a", "b"}
	idx = 0
	for rows.Next() {
		var letter string
		if err := rows.Scan(&letter); err != nil {
			t.Fatal(err)
		}
		if idx >= len(expectedStrings) {
			t.Fatalf("too many rows in second result set")
		}
		if letter != expectedStrings[idx] {
			t.Fatalf("second result set row %d: got %s, expected %s", idx, letter, expectedStrings[idx])
		}
		idx++
	}
	if idx != len(expectedStrings) {
		t.Fatalf("second result set: got %d rows, expected %d", idx, len(expectedStrings))
	}

	// Should have no more result sets
	if rows.NextResultSet() {
		t.Fatal("expected no more result sets")
	}
}

func testSingleResultSet(t *testing.T, db *sql.DB) {
	// Verify that single result set queries still work correctly with the new code.
	var dummy string
	if err := db.QueryRow("select * from dummy").Scan(&dummy); err != nil {
		t.Fatal(err)
	}
	if dummy != "X" {
		t.Fatalf("dummy = %s, expected X", dummy)
	}

	// Also verify that NextResultSet returns false for single result sets
	rows, err := db.Query("select * from dummy")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected one row")
	}
	if err := rows.Scan(&dummy); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("expected only one row")
	}
	if rows.NextResultSet() {
		t.Fatal("expected no more result sets for single result query")
	}
}

func testMultipleResultSetsDifferentColumns(t *testing.T, db *sql.DB) {
	// Test multiple result sets with different column structures.
	const procDiffCols = `create procedure %[1]s
language SQLSCRIPT as
begin
	select 1 as a, 2 as b, 3 as c from dummy;
	select 'x' as single_col from dummy;
	select 100 as num1, 200 as num2 from dummy;
end
`
	// use same connection
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// create procedure
	proc := driver.RandomIdentifier("procDiffCols_")
	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(procDiffCols, proc)); err != nil {
		t.Fatal(err)
	}

	rows, err := conn.QueryContext(t.Context(), fmt.Sprintf("call %s", proc))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// First result set: 3 columns
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 3 {
		t.Fatalf("first result set: expected 3 columns, got %d", len(cols))
	}
	if !rows.Next() {
		t.Fatal("expected row in first result set")
	}
	var a, b, c int
	if err := rows.Scan(&a, &b, &c); err != nil {
		t.Fatal(err)
	}
	if a != 1 || b != 2 || c != 3 {
		t.Fatalf("first result set: got (%d, %d, %d), expected (1, 2, 3)", a, b, c)
	}

	// Advance to second result set
	if !rows.NextResultSet() {
		t.Fatal("expected second result set")
	}

	// Second result set: 1 column
	cols, err = rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 1 {
		t.Fatalf("second result set: expected 1 column, got %d", len(cols))
	}
	if !rows.Next() {
		t.Fatal("expected row in second result set")
	}
	var singleCol string
	if err := rows.Scan(&singleCol); err != nil {
		t.Fatal(err)
	}
	if singleCol != "x" {
		t.Fatalf("second result set: got %s, expected x", singleCol)
	}

	// Advance to third result set
	if !rows.NextResultSet() {
		t.Fatal("expected third result set")
	}

	// Third result set: 2 columns
	cols, err = rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 {
		t.Fatalf("third result set: expected 2 columns, got %d", len(cols))
	}
	if !rows.Next() {
		t.Fatal("expected row in third result set")
	}
	var num1, num2 int
	if err := rows.Scan(&num1, &num2); err != nil {
		t.Fatal(err)
	}
	if num1 != 100 || num2 != 200 {
		t.Fatalf("third result set: got (%d, %d), expected (100, 200)", num1, num2)
	}

	// Should have no more result sets
	if rows.NextResultSet() {
		t.Fatal("expected no more result sets")
	}
}

func TestResultSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fct  func(t *testing.T, db *sql.DB)
	}{
		{"singleResultSet", testSingleResultSet},
		{"multipleResultSetsDirect", testMultipleResultSetsDirect},
		{"multipleResultSetsPrepared", testMultipleResultSetsPrepared},
		{"multipleResultSetsWithMultipleRows", testMultipleResultSetsWithMultipleRows},
		{"multipleResultSetsDifferentColumns", testMultipleResultSetsDifferentColumns},
	}

	db := driver.MT.DB()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t, db)
		})
	}
}
