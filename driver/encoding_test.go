//go:build !unit
// +build !unit

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

func setupEncodingTestTable(testData []string, t *testing.T) driver.Identifier {
	db := driver.DefaultTestDB()

	tableName := driver.RandomIdentifier("cesuerror_")
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer, s nvarchar(20))", tableName)); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare(fmt.Sprintf("insert into %s values(?, bintostr(?))", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i, s := range testData {
		if _, err := stmt.Exec(i, s); err != nil {
			t.Fatal(err)
		}
	}
	return tableName
}

func testDecodeError(tableName driver.Identifier, testData []string, t *testing.T) {
	db := driver.DefaultTestDB()

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

func testDecodeErrorHandler(tableName driver.Identifier, testData []string, t *testing.T) {
	connector := driver.NewTestConnector()

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

	resultData := []string{
		"2b301c39efbfbd32306033", // invalid sequence "eda2a811" gets replaces by replacement char "fffd" -> UTF-8 "efbfbd"
		"243036301defbfbdceb63714",
		"24302837323245efbfbd6c",
		"2443306eefbfbd2e19517c",
		"243035027f03353245efbfbd",
		"246a3066ed828f5d303054efbfbd",
		"245d66301d5a383435efbfbd",
		"24d9973048287342efbfbd78",
		"24306a36efbfbd39393738",
		"30393fefbfbd312c391936",
		"303735efbfbd3425256135",
		"24efbfbd33334d374c3736",
		"1130691932593303efbfbd",
		"30154301326b133334efbfbd",
		"24efbfbd33611037370a38",
		"24efbfbd61223433390674",
		"2443307f61313aefbfbd34",
		"08303438013624efbfbd33",
		"24350730345f1a373fefbfbd",
		"240d30efbfbd7738044132",
		"24301bde964cefbfbd357229",
		"306e3631324eefbfbd3036",
		"243b303434613742efbfbd",
		"2430772b4533360164efbfbd",
		"2430d28763efbfbd3f333830",
		"303535376bd8a80936efbfbd",
	}

	for rows.Next() {
		if err := rows.Scan(&i, &s); err != nil {
			t.Fatal(err)
		}

		r, err := hex.DecodeString(resultData[i])
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

func testDecodeRaw(tableName driver.Identifier, testData []string, t *testing.T) {
	connector := driver.NewTestConnector()

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
		cmp, err := hex.DecodeString(testData[i])
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
	testData := []string{
		"2b301c39eda2a81132306033",
		"243036301dedb1a060ceb63714",
		"24302837323245edb1a9146c",
		"2443306eedbaae382e19517c",
		"243035027f03353245edac89",
		"246a3066ed828f5d303054eda6bf",
		"245d66301d5a383435edb1ba",
		"24d9973048287342edb8945078",
		"24306a36edb1ab3039393738",
		"30393feda48f30312c391936",
		"303735eda898613425256135",
		"24edb1a93033334d374c3736",
		"1130691932593303edaf9f4c",
		"30154301326b133334edae8e",
		"24eda6b43033611037370a38",
		"24eda3ab3061223433390674",
		"2443307f61313aedbeac3734",
		"08303438013624edbc963133",
		"24350730345f1a373fedb8ae",
		"240d30edbdbb1e7738044132",
		"24301bde964cedbeac36357229",
		"306e3631324eedaf85303036",
		"243b303434613742eda9910c",
		"2430772b4533360164eda69d",
		"2430d28763eda2996d3f333830",
		"303535376bd8a80936edb19134",
	}

	tableName := setupEncodingTestTable(testData, t)

	tests := []struct {
		name string
		fct  func(tableName driver.Identifier, testData []string, t *testing.T)
	}{
		{"testDecodeError", testDecodeError},
		{"testDecodeErrorHandler", testDecodeErrorHandler},
		{"testDecodeRaw", testDecodeRaw},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(tableName, testData, t)
		})
	}
}
