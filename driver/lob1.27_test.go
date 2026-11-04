//go:build !unit && go1.27

package driver

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/rand/alphanum"
)

func newRandomData(size int) []byte {
	b := make([]byte, size)
	rand.Read(b) // starting with go1.24 rand.Read never returns an error.
	return b
}

func testLobInsert(t *testing.T, db *sql.DB) {

	const (
		numRec   = 100
		blobSize = 1000
	)
	testData := make([]string, numRec)

	for i := range numRec {
		testData[i] = alphanum.ReadString(blobSize)
	}

	table := RandomIdentifier("lob_")

	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (i integer, n nclob, b blob)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	// use transactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values (?,?,?)", table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// insert as string and byte
	for i, s := range testData {
		if _, err := stmt.ExecContext(t.Context(), i, s, []byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(t.Context(), fmt.Sprintf("select * from %s", table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var (
		i int
		s string
		b []byte
	)

	for rows.Next() {
		if err := rows.Scan(&i, &s, &b); err != nil {
			t.Fatal(err)
		}
		if s != testData[i] {
			t.Fatalf("idx %d got %s - expected %s", i, s, testData[i])
		}
		if string(b) != testData[i] {
			t.Fatalf("idx %d got %s - expected %s", i, string(b), testData[i])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func testLobAffectedRows(t *testing.T, db *sql.DB) {

	checkRowsAffected := func(result sql.Result) {
		// Check rowsAffected - MUST be 1, not 0 (HANA 4 fix)
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("RowsAffected() failed: %s", err)
		}
		if rowsAffected != 1 {
			t.Fatalf("got rowsAffected %d - expected 1", rowsAffected)
		}
	}

	const lobChunkSize = 1024 // 1KB chunks for testing

	table := RandomIdentifier("lobAffectedRows_")

	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (id integer primary key, b1 blob, b2 blob)", table)); err != nil {
		t.Fatal(err)
	}

	// Create a custom connector with small LOB chunk size
	cfg := MT.Connector().Config()
	cfg.LobChunkSize = lobChunkSize
	ctr, err := NewConfigConnector(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	testDB := sql.OpenDB(ctr)
	defer testDB.Close()

	// Insert initial data

	insertData1 := newRandomData(lobChunkSize * 2)
	insertData2 := newRandomData(lobChunkSize * 3)

	tx, err := testDB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values (?, ?, ?)", table))
	if err != nil {
		t.Fatal(err)
	}

	result, err := stmt.ExecContext(t.Context(), 1, insertData1, insertData2)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	checkRowsAffected(result)

	// Update
	updateData1 := newRandomData(lobChunkSize * 2)
	updateData2 := newRandomData(lobChunkSize * 3)

	tx, err = testDB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err = tx.PrepareContext(t.Context(), fmt.Sprintf("update %s set b1 = ?, b2 = ? where id = ?", table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	result, err = stmt.ExecContext(t.Context(), updateData1, updateData2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	checkRowsAffected(result)

	// Verify
	var readData1 []byte
	var readData2 []byte
	if err := db.QueryRowContext(t.Context(), fmt.Sprintf("select b1, b2 from %s where id = 1", table)).Scan(&readData1, &readData2); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(updateData1, readData1) {
		t.Fatalf("data1 mismatch")
	}
	if !bytes.Equal(updateData2, readData2) {
		t.Fatalf("data2 mismatch")
	}
}

// testLobStringRetention verifies that retained strings survive reuse of the
// scan destination because strings are immutable. []byte aliases share mutable
// backing storage, which subsequent LOB scans can overwrite.
func testLobStringRetention(t *testing.T, db *sql.DB) {
	const (
		firstValue = "first value"
		otherValue = "other value"
		query      = `select to_nclob(s) from (
		select 1 as n, '` + firstValue + `' as s from dummy
		union all
		select 2 as n, '` + otherValue + `' as s from dummy
	) order by n`
	)
	rows, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var value string
	var values []string
	for rows.Next() {
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != firstValue || values[1] != otherValue {
		t.Fatalf("got %q - expected [%q %q]", values, firstValue, otherValue)
	}
}

// testLobRescan verifies that scanning the same row twice returns the same LOB.
// LobOutDescr.Bytes converts CESU-8 in place and reuses the result on later scans.
// The emoji changes from six CESU-8 bytes to four UTF-8 bytes, exposing attempts
// to decode the descriptor's already-converted bytes again.
func testLobRescan(t *testing.T, db *sql.DB) {
	for _, want := range []string{
		"Hello world",
		"Hello 😀 world",
	} {
		rows, err := db.QueryContext(t.Context(), "select to_nclob(cast(? as nvarchar(5000))) from dummy", want)
		if err != nil {
			t.Fatalf("%q: query failed: %v", want, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("%q: no row: %v", want, rows.Err())
		}
		for i := range 2 {
			var value string
			if err := rows.Scan(&value); err != nil {
				t.Fatalf("%q: scan %d failed: %v", want, i+1, err)
			}
			if value != want {
				t.Fatalf("scan %d: got %q, want %q", i+1, value, want)
			}
		}
		if rows.Next() {
			t.Fatalf("%q: unexpected second row", want)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%q: %v", want, err)
		}
	}
}

// testLobWriter checks empty, chunked, and NULL BLOB and NCLOB values for each
// writer destination type. NULL must clear nullable values and pointer destinations
// while preserving the Lob inside an embedded NullLob.
func testLobWriter(t *testing.T) {
	const chunkSize = 1024
	// Use chunkSize+1 bytes to exercise at least two chunk writes.
	want := alphanum.ReadString(chunkSize + 1)
	cfg := MT.Connector().Config()
	cfg.LobChunkSize = chunkSize
	ctr, err := NewConfigConnector(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(ctr)
	defer db.Close()

	table := RandomIdentifier("lobWriter_")
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (i integer, b blob, n nclob)", table)); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	values := []any{"", want, nil}
	for i, value := range values {
		if _, err := tx.ExecContext(t.Context(), fmt.Sprintf("insert into %s values (?, ?, ?)", table), i, value, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	type embeddedLob struct{ *Lob }
	type nestedLob struct{ *embeddedLob }
	type embeddedNullLob struct{ *NullLob }
	type nestedNullLob struct{ *embeddedNullLob }
	var blob, nclob bytes.Buffer
	lobs := [2]*Lob{NewLob(nil, &blob), NewLob(nil, &nclob)}
	pointers := lobs
	for _, tt := range []struct {
		name      string
		blobDest  any
		nclobDest any
		nullable  bool
	}{
		{"writer", &blob, &nclob, false},
		{
			"nullable writer",
			&sql.Null[*bytes.Buffer]{V: &blob},
			&sql.Null[*bytes.Buffer]{V: &nclob},
			true,
		},
		{
			"embedded Lob",
			&nestedLob{&embeddedLob{lobs[0]}},
			&nestedLob{&embeddedLob{lobs[1]}},
			false,
		},
		{
			"embedded NullLob",
			&nestedNullLob{&embeddedNullLob{&NullLob{Lob: lobs[0]}}},
			&nestedNullLob{&embeddedNullLob{&NullLob{Lob: lobs[1]}}},
			true,
		},
		{
			"nullable Lob",
			&sql.Null[*Lob]{V: lobs[0]},
			&sql.Null[*Lob]{V: lobs[1]},
			true,
		},
		{"Lob pointer", &pointers[0], &pointers[1], true},
	} {
		for i, value := range values {
			valid := value != nil
			if !valid && !tt.nullable {
				continue
			}
			blob, nclob = bytes.Buffer{}, bytes.Buffer{}
			if err := db.QueryRowContext(t.Context(), fmt.Sprintf("select b, n from %s where i = ?", table), i).Scan(tt.blobDest, tt.nclobDest); err != nil {
				t.Fatalf("%s row %d: %v", tt.name, i, err)
			}
			for column, dest := range []any{tt.blobDest, tt.nclobDest} {
				switch dest := dest.(type) {
				case *sql.Null[*bytes.Buffer]:
					if dest.Valid != valid || (dest.V != nil) != valid {
						t.Fatalf("%s row %d column %d: Valid %t, V %p, want valid %t", tt.name, i, column, dest.Valid, dest.V, valid)
					}
				case *nestedNullLob:
					if dest.Valid != valid || dest.Lob != lobs[column] {
						t.Fatalf("%s row %d column %d: Valid %t, want %t with the original Lob retained", tt.name, i, column, dest.Valid, valid)
					}
				case *sql.Null[*Lob]:
					if dest.Valid != valid || (dest.V != nil) != valid {
						t.Fatalf("%s row %d column %d: Valid %t, V %p, want valid %t", tt.name, i, column, dest.Valid, dest.V, valid)
					}
				case **Lob:
					if (*dest != nil) != valid {
						t.Fatalf("%s row %d column %d: Lob %p, want valid %t", tt.name, i, column, *dest, valid)
					}
				}
			}
			if valid {
				if got := blob.String(); got != value {
					t.Fatalf("b/%s: got %q, want %q", tt.name, got, value)
				}
				if got := nclob.String(); got != value {
					t.Fatalf("n/%s: got %q, want %q", tt.name, got, value)
				}
			}
		}
	}
}

type lobScanner struct {
	Lob
	called bool
	src    any
}

func (l *lobScanner) Scan(src any) error {
	l.called = true
	l.src = src
	return nil
}

// testLobScanner verifies that the wrapper's custom Scan method is called with
// the expected value and the embedded Lob's writer is not used.
func testLobScanner(t *testing.T, db *sql.DB) {
	for _, tt := range []struct {
		query string
		want  any
	}{
		{"select to_blob('value') from dummy", []byte("value")},
		{"select to_nclob('value') from dummy", []byte("value")},
		{"select cast('value' as nvarchar(20)) from dummy", []byte("value")},
		{"select cast(null as nclob) from dummy", nil},
	} {
		var wr bytes.Buffer
		dest := lobScanner{Lob: *NewLob(nil, &wr)}
		if err := db.QueryRowContext(t.Context(), tt.query).Scan(&dest); err != nil {
			t.Fatalf("%s: scan failed: %v", tt.query, err)
		}
		if !dest.called {
			t.Fatalf("%s: custom Scan was not called", tt.query)
		}
		if !reflect.DeepEqual(dest.src, tt.want) {
			t.Fatalf("%s: Scan received %T(%v), want %T(%v)", tt.query, dest.src, dest.src, tt.want, tt.want)
		}
		if wr.Len() != 0 {
			t.Fatalf("%s: embedded Lob writer received %d bytes", tt.query, wr.Len())
		}
	}
}

func TestLob(t *testing.T) {
	t.Parallel()

	testsWithDB := []struct {
		name string
		fn   func(t *testing.T, db *sql.DB)
	}{
		{"insert", testLobInsert},
		{"delayedScan", testLobDelayedScan},
		{"nilPlusBigLob", testLobNilPlusBig},
		{"affectedRows", testLobAffectedRows},
		{"embeddedLob", testEmbeddedLob},
		{"stringRetention", testLobStringRetention},
		{"rescan", testLobRescan},
		{"scanner", testLobScanner},
	}

	testsWithoutDB := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"pipe", testLobPipe},
		{"writer", testLobWriter},
	}

	db := MT.DB()
	for _, test := range testsWithDB {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fn(t, db)
		})
	}

	for _, test := range testsWithoutDB {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fn(t)
		})
	}
}
