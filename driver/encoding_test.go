//go:build !unit

package driver_test

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
	"golang.org/x/text/transform"
)

func setupEncodingTestTable(t *testing.T, testData []struct{ s, r string }) driver.Identifier {
	db := driver.MT.DB()

	tableName := driver.RandomIdentifier("cesuerror_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, s nvarchar(20))", tableName)); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare(fmt.Sprintf("insert into %s values(?, bintostr(?))", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i, td := range testData {
		if _, err := stmt.Exec(i, td.s); err != nil {
			t.Fatal(err)
		}
	}
	return tableName
}

func testDecodeError(t *testing.T, tableName driver.Identifier, testData []struct{ s, r string }) {
	db := driver.MT.DB()

	rows, err := db.Query(fmt.Sprintf("select * from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() { // will fail
		// ...
	}
	switch err := rows.Err(); err {
	case nil:
		t.Fatal("invalid cesu-8 error expected")
	default:
		t.Log(err) // just print the (expected) error
	}
}

func testDecodeErrorHandler(t *testing.T, tableName driver.Identifier, testData []struct{ s, r string }) {
	connector := driver.MT.NewConnector()

	// register decoder with replace error handler
	decoder := cesu8.NewDecoder(cesu8.ReplaceErrorHandler)
	connector.SetCESU8Decoder(func() transform.Transformer { return decoder })

	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("select * from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var (
		i int
		s string
	)

	for rows.Next() {
		if err := rows.Scan(&i, &s); err != nil {
			t.Fatal(err)
		}

		r, err := hex.DecodeString(testData[i].r)
		if err != nil {
			t.Fatal(err)
		}

		if s != string(r) {
			t.Fatalf("record %d: invalid result %x - expected %x", i, s, string(r))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func testDecodeRaw(t *testing.T, tableName driver.Identifier, testData []struct{ s, r string }) {
	connector := driver.MT.NewConnector()

	// register nop decoder to receive 'raw' undecoded data
	connector.SetCESU8Decoder(func() transform.Transformer { return transform.Nop })

	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("select * from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var (
		i int
		s string
	)

	for rows.Next() {
		if err := rows.Scan(&i, &s); err != nil {
			t.Fatal(err)
		}
		cmp, err := hex.DecodeString(testData[i].s)
		if err != nil {
			t.Fatal(err)
		}
		if s != string(cmp) {
			t.Fatalf("record %d: invalid result %x - expected %x", i, s, string(cmp))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestEncoding(t *testing.T) {
	testData := []struct{ s, r string }{
		// invalid sequence "eda2a8" (only high surrogate pair) gets replaced by replacement char "fffd" -> UTF-8 "efbfbd"
		{"2b301c39eda2a81132306033", "2b301c39efbfbd1132306033"},
		{"243036301dedb1a060ceb63714", "243036301defbfbd60ceb63714"},
		{"24302837323245edb1a9146c", "24302837323245efbfbd146c"},
		{"2443306eedbaae382e19517c", "2443306eefbfbd382e19517c"},
		{"243035027f03353245edac89", "243035027f03353245efbfbd"},
		{"246a3066ed828f5d303054eda6bf", "246a3066ed828f5d303054efbfbd"},
		{"245d66301d5a383435edb1ba", "245d66301d5a383435efbfbd"},
		{"24d9973048287342edb8945078", "24d9973048287342efbfbd5078"},
		{"24306a36edb1ab3039393738", "24306a36efbfbd3039393738"},
		{"30393feda48f30312c391936", "30393fefbfbd30312c391936"},
		{"303735eda898613425256135", "303735efbfbd613425256135"},
		{"24edb1a93033334d374c3736", "24efbfbd3033334d374c3736"},
		{"1130691932593303edaf9f4c", "1130691932593303efbfbd4c"},
		{"30154301326b133334edae8e", "30154301326b133334efbfbd"},
		{"24eda6b43033611037370a38", "24efbfbd3033611037370a38"},
		{"24eda3ab3061223433390674", "24efbfbd3061223433390674"},
		{"2443307f61313aedbeac3734", "2443307f61313aefbfbd3734"},
		{"08303438013624edbc963133", "08303438013624efbfbd3133"},
		{"24350730345f1a373fedb8ae", "24350730345f1a373fefbfbd"},
		{"240d30edbdbb1e7738044132", "240d30efbfbd1e7738044132"},
		{"24301bde964cedbeac36357229", "24301bde964cefbfbd36357229"},
		{"306e3631324eedaf85303036", "306e3631324eefbfbd303036"},
		{"243b303434613742eda9910c", "243b303434613742efbfbd0c"},
		{"2430772b4533360164eda69d", "2430772b4533360164efbfbd"},
		{"2430d28763eda2996d3f333830", "2430d28763efbfbd6d3f333830"},
		{"303535376bd8a80936edb19134", "303535376bd8a80936efbfbd34"},
	}

	tableName := setupEncodingTestTable(t, testData)

	tests := []struct {
		name string
		fct  func(t *testing.T, tableName driver.Identifier, testData []struct{ s, r string })
	}{
		{"testDecodeError", testDecodeError},
		{"testDecodeErrorHandler", testDecodeErrorHandler},
		{"testDecodeRaw", testDecodeRaw},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t, tableName, testData)
		})
	}
}
