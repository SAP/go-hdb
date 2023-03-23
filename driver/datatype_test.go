//go:build !unit

package driver

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/spatial"
)

func dttCreateTable(db *sql.DB, column string) (Identifier, error) {
	tableName := RandomIdentifier(fmt.Sprintf("%s_", column))
	if _, err := db.Exec(fmt.Sprintf("create table %s (x %s, i integer)", tableName, column)); err != nil {
		return tableName, err
	}
	return tableName, nil
}

type dttNeg struct { // negative test
	_columnType columnType
	checkFn     func(ct columnType, in, out any) (bool, error)
	testData    []any
}

func (dtt *dttNeg) columnType() columnType { return dtt._columnType }

func (dtt *dttNeg) insert(t *testing.T, db *sql.DB, tableName Identifier) {
	stmt, err := db.Prepare(fmt.Sprintf("insert into %s values(?, ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for _, in := range dtt.testData {
		if _, err := stmt.Exec(in, i); err == nil { // error expected
			t.Fatalf("type: %s - %d - error expected", dtt._columnType.typeName(), i)
		} else {
			t.Logf("type: %s - %d - %s - %T", dtt._columnType.typeName(), i, err, err)
		}
	}
}

func (dtt *dttNeg) run(t *testing.T, db *sql.DB) {
	tableName, err := dttCreateTable(db, dtt._columnType.column())
	if err != nil {
		t.Fatal(err)
	}
	dtt.insert(t, db, tableName)
}

type dttDef struct {
	_columnType columnType
	checkFn     func(ct columnType, in, out any) (bool, error)
	testData    []any
}

func (dtt *dttDef) columnType() columnType { return dtt._columnType }

func (dtt *dttDef) insert(t *testing.T, db *sql.DB, tableName Identifier) int {
	stmt, err := db.Prepare(fmt.Sprintf("insert into %s values(?, ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for _, in := range dtt.testData {
		if _, err := stmt.Exec(in, i); err != nil {
			t.Fatalf("type: %s - %d - %v - %s", dtt._columnType.typeName(), i, in, err)
		}
		i++
	}
	return i
}

func (dtt *dttDef) check(t *testing.T, db *sql.DB, tableName Identifier, numRecs int) {
	rows, err := db.Query(fmt.Sprintf("select * from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		in := dtt.testData[i]
		outRef := reflect.New(reflect.TypeOf(in)).Interface()

		if outRef, ok := outRef.(*NullLob); ok {
			outRef.Lob = new(Lob)
		}

		if err := rows.Scan(outRef, &i); err != nil {
			t.Fatal(err)
		}
		outVal := reflect.ValueOf(outRef).Elem().Interface()

		ok, err := dtt.checkFn(dtt._columnType, in, outVal)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("%d value %v - expected %v", i, outVal, in)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != numRecs {
		t.Fatalf("rows %d - expected %d", i, numRecs)
	}
}

func (dtt *dttDef) run(t *testing.T, db *sql.DB) {
	tableName, err := dttCreateTable(db, dtt._columnType.column())
	if err != nil {
		t.Fatal(err)
	}
	numRecs := dtt.insert(t, db, tableName)
	dtt.check(t, db, tableName, numRecs)
}

type dttTX struct {
	dttDef
}

func (dtt *dttTX) insert(t *testing.T, db *sql.DB, tableName Identifier) int { // override insert
	// use trancactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values(?, ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for _, in := range dtt.testData {
		if _, err := stmt.Exec(in, i); err != nil {
			t.Fatalf("%d - %s", i, err)
		}
		i++
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return i
}

func (dtt *dttTX) run(t *testing.T, db *sql.DB) { // override run, so that dttTX insert is called
	tableName, err := dttCreateTable(db, dtt._columnType.column())
	if err != nil {
		t.Fatal(err)
	}
	numRec := dtt.insert(t, db, tableName)
	dtt.check(t, db, tableName, numRec)
}

type dttSpatial struct {
	_columnType columnType
	testData    []spatial.Geometry
}

func (dtt *dttSpatial) columnType() columnType { return dtt._columnType }

func (dtt *dttSpatial) withTx(t *testing.T, db *sql.DB, tableName Identifier, fn func(func(value any))) int {
	// use trancactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values(st_geomfromewkb(?), ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	fn(func(value any) {
		if _, err := stmt.Exec(value, i); err != nil {
			t.Fatalf("%d - %s", i, err)
		}
		i++
	})

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return i
}

func (dtt *dttSpatial) withRows(t *testing.T, db *sql.DB, tableName Identifier, numRecs int, fn func(i int), dest ...any) {
	rows, err := db.Query(fmt.Sprintf("select x, i, x.st_aswkb(), x.st_asewkb(), x.st_aswkt(), x.st_asewkt(), x.st_asgeojson() from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		fn(i)
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != numRecs {
		t.Fatalf("rows %d - expected %d", i, numRecs)
	}
}

func (dtt *dttSpatial) run(t *testing.T, db *sql.DB) {
	tableName, err := dttCreateTable(db, dtt._columnType.column())
	if err != nil {
		t.Fatal(err)
	}

	srid := dtt._columnType.(spatialColumnType).srid()

	numRec := dtt.withTx(t, db, tableName, func(exec func(value any)) {
		for _, g := range dtt.testData {
			ewkb, err := spatial.EncodeEWKB(g, false, srid)
			if err != nil {
				t.Fatal(err)
			}
			exec(new(Lob).SetReader(bytes.NewReader(ewkb)))
		}
	})

	var i int
	var x string

	asWKBBuffer := new(bytes.Buffer)
	asWKBLob := &Lob{wr: asWKBBuffer}

	asEWKBBuffer := new(bytes.Buffer)
	asEWKBLob := &Lob{wr: asEWKBBuffer}

	asWKTBuffer := new(bytes.Buffer)
	asWKTLob := &Lob{wr: asWKTBuffer}

	asEWKTBuffer := new(bytes.Buffer)
	asEWKTLob := &Lob{wr: asEWKTBuffer}

	asGeoJSONBuffer := new(bytes.Buffer)
	asGeoJSONLob := &Lob{wr: asGeoJSONBuffer}

	dtt.withRows(t, db, tableName, numRec, func(i int) {
		wkb, err := spatial.EncodeWKB(dtt.testData[i], false)
		if err != nil {
			t.Fatal(err)
		}

		if string(wkb) != x {
			t.Fatalf("test %d: x value %v - expected %v", i, x, string(wkb))
		}

		ewkb, err := spatial.EncodeEWKB(dtt.testData[i], false, srid)
		if err != nil {
			t.Fatal(err)
		}

		wkt, err := spatial.EncodeWKT(dtt.testData[i])
		if err != nil {
			t.Fatal(err)
		}

		ewkt, err := spatial.EncodeEWKT(dtt.testData[i], srid)
		if err != nil {
			t.Fatal(err)
		}

		geoJSON, err := spatial.EncodeGeoJSON(dtt.testData[i])
		if err != nil {
			t.Fatal(err)
		}

		asWKB := hex.EncodeToString(asWKBBuffer.Bytes())
		if string(wkb) != asWKB {
			t.Fatalf("test %d: wkb value %v - expected %v", i, asWKB, string(wkb))
		}

		asEWKB := hex.EncodeToString(asEWKBBuffer.Bytes())
		if string(ewkb) != asEWKB {
			t.Fatalf("test %d: ewkb value %v - expected %v", i, asEWKB, string(ewkb))
		}

		asWKT := asWKTBuffer.Bytes()
		if !bytes.Equal(wkt, asWKT) {
			t.Fatalf("test %d: wkt value %s - expected %s", i, asWKT, wkt)
		}

		asEWKT := asEWKTBuffer.Bytes()
		if !bytes.Equal(ewkt, asEWKT) {
			t.Fatalf("test %d: wkt value %s - expected %s", i, asEWKT, ewkt)
		}

		asGeoJSON := asGeoJSONBuffer.Bytes()
		ok, err := equalJSON(geoJSON, asGeoJSON)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Logf("test %d: geoJSON value %s - expected %s", i, asGeoJSON, geoJSON)
		}

		// reset buffers
		asWKBBuffer.Reset()
		asEWKBBuffer.Reset()
		asWKTBuffer.Reset()
		asEWKTBuffer.Reset()
		asGeoJSONBuffer.Reset()

	}, &x, &i, asWKBLob, asEWKBLob, asWKTLob, asEWKTLob, asGeoJSONLob)
}

const (
	minTinyint  = 0
	maxTinyint  = math.MaxUint8
	minSmallint = math.MinInt16
	maxSmallint = math.MaxInt16
	minInteger  = math.MinInt32
	maxInteger  = math.MaxInt32
	minBigint   = math.MinInt64
	maxBigint   = math.MaxInt64
	maxReal     = math.MaxFloat32
	maxDouble   = math.MaxFloat64
)

var tinyintTestData = []any{
	uint8(minTinyint),
	uint8(maxTinyint),
	sql.NullInt64{Valid: false, Int64: minTinyint},
	sql.NullInt64{Valid: true, Int64: maxTinyint},
}

var smallintTestData = []any{
	int16(minSmallint),
	int16(maxSmallint),
	sql.NullInt64{Valid: false, Int64: minSmallint},
	sql.NullInt64{Valid: true, Int64: maxSmallint},
}

var integerTestData = []any{
	int32(minInteger),
	int32(maxInteger),
	sql.NullInt64{Valid: false, Int64: minInteger},
	sql.NullInt64{Valid: true, Int64: maxInteger},
}

var bigintTestData = []any{
	int64(minBigint),
	int64(maxBigint),
	sql.NullInt64{Valid: false, Int64: minBigint},
	sql.NullInt64{Valid: true, Int64: maxBigint},
}

var realTestData = []any{
	float32(-maxReal),
	float32(maxReal),
	sql.NullFloat64{Valid: false, Float64: -maxReal},
	sql.NullFloat64{Valid: true, Float64: maxReal},
}

var doubleTestData = []any{
	float64(-maxDouble),
	float64(maxDouble),
	sql.NullFloat64{Valid: false, Float64: -maxDouble},
	sql.NullFloat64{Valid: true, Float64: maxDouble},
}

var timeTestData = []any{
	time.Now(),
	time.Date(2000, 12, 31, 23, 59, 59, 999999999, time.UTC),
	sql.NullTime{Valid: false, Time: time.Now()},
	sql.NullTime{Valid: true, Time: time.Now()},
}

var (
	natOne     = big.NewRat(1, 1)
	natTen     = big.NewInt(10)
	natHundret = big.NewRat(100, 1)
)

func exp10(n int) *big.Int {
	r := big.NewInt(int64(n))
	return r.Exp(natTen, r, nil)
}

func maxValue(prec int) *big.Rat {
	r := new(big.Rat).SetInt(exp10(prec))
	r.Sub(r, natOne)
	r.Quo(r, natHundret)
	return r
}

func minValue(prec int) *big.Rat {
	max := maxValue(prec)
	return max.Neg(max)
}

var (
	fixed8MinValue  = (*Decimal)(minValue(18)) // min value Dec(18,2)
	fixed8MaxValue  = (*Decimal)(maxValue(18)) // max value Dec(18,2)
	fixed12MinValue = (*Decimal)(minValue(28)) // min value Dec(18,2)
	fixed12MaxValue = (*Decimal)(maxValue(28)) // max value Dec(18,2)
	fixed16MinValue = (*Decimal)(minValue(38)) // min value Dec(18,2)
	fixed16MaxValue = (*Decimal)(maxValue(38)) // max value Dec(18,2)
)

var (
	decimalTestData = []any{
		(*Decimal)(big.NewRat(0, 1)),
		(*Decimal)(big.NewRat(1, 1)),
		(*Decimal)(big.NewRat(-1, 1)),
		(*Decimal)(big.NewRat(10, 1)),
		(*Decimal)(big.NewRat(1000, 1)),
		(*Decimal)(big.NewRat(1, 10)),
		(*Decimal)(big.NewRat(-1, 10)),
		(*Decimal)(big.NewRat(1, 100)),
		(*Decimal)(big.NewRat(15, 1)),
		(*Decimal)(big.NewRat(4, 5)),
		(*Decimal)(big.NewRat(34, 10)),
		fixed8MinValue,
		fixed8MaxValue,

		NullDecimal{Valid: false, Decimal: (*Decimal)(big.NewRat(1, 1))},
		NullDecimal{Valid: true, Decimal: (*Decimal)(big.NewRat(1, 1))},
	}
	decimalFixed12TestData = append(decimalTestData, fixed12MinValue, fixed12MaxValue)
	decimalFixed16TestData = append(decimalFixed12TestData, fixed16MinValue, fixed16MaxValue)
)

var booleanTestData = []any{
	true,
	false,
	sql.NullBool{Valid: false, Bool: true},
	sql.NullBool{Valid: true, Bool: false},
}

var asciiStringTestData = []any{
	"Hello HDB",
	"aaaaaaaaaa",
	sql.NullString{Valid: false, String: "Hello HDB"},
	sql.NullString{Valid: true, String: "Hello HDB"},
}

var stringTestData = []any{
	"Hello HDB",
	// varchar: UTF-8 4 bytes per char -> size 40 bytes
	// nvarchar: CESU-8 6 bytes per char -> hdb counts 2 chars per 6 byte encoding -> size 20 bytes
	"𝄞𝄞𝄞𝄞𝄞𝄞𝄞𝄞𝄞𝄞",
	"𝄞𝄞aa",
	"€€",
	"𝄞𝄞€€",
	"𝄞𝄞𝄞€€",
	"aaaaaaaaaa",
	sql.NullString{Valid: false, String: "Hello HDB"},
	sql.NullString{Valid: true, String: "Hello HDB"},
}

var invalidUnicodeTestData = []any{
	string([]byte{43, 48, 28, 57, 237, 162, 168, 17, 50, 48, 96, 51}), // invalid unicode
}

var binaryTestData = []any{
	[]byte("Hello HDB"),
	[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
	[]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0xff},
	NullBytes{Valid: false, Bytes: []byte("Hello HDB")},
	NullBytes{Valid: true, Bytes: []byte("Hello HDB")},
}

var alphanumTestData = []any{
	"0123456789",
	"1234567890",
	"abc",
	"123",
	"-abc",
	"-123",
	"0a1b2c",
	"12345678901234567890",
	sql.NullString{Valid: false, String: "42"},
	sql.NullString{Valid: true, String: "42"},
}

type testLobFile struct {
	content []byte
	isASCII bool
}

var testLobFiles = make([]*testLobFile, 0)

var initLobFilesOnce sync.Once

func testInitLobFiles(t *testing.T) {
	initLobFilesOnce.Do(func() { // lazy (lob file test might not be executed at allss)

		isASCII := func(content []byte) bool {
			for _, b := range content {
				if b >= utf8.RuneSelf {
					return false
				}
			}
			return true
		}

		filter := func(name string) bool {
			for _, ext := range []string{".go"} {
				if filepath.Ext(name) == ext {
					return true
				}
			}
			return false
		}

		walk := func(p string, info os.FileInfo, err error) error {
			if !info.IsDir() && filter(info.Name()) {
				content, err := os.ReadFile(p)
				if err != nil {
					return err
				}
				testLobFiles = append(testLobFiles, &testLobFile{isASCII: isASCII(content), content: content})
			}
			return nil
		}

		root, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		if err := filepath.Walk(root, walk); err != nil {
			t.Fatal(err)
		}

		// add random
		// Lob size 1MB
		b := make([]byte, 1e6)
		if _, err := randAlphanumReader.Read(b); err != nil {
			t.Fatal(err)
		}
		testLobFiles = append(testLobFiles, &testLobFile{isASCII: true, content: b})
	})
}

func lobTestData(ascii bool, t *testing.T) []any {
	testInitLobFiles(t)
	testData := make([]any, 0, len(testLobFiles))
	first := true
	for _, f := range testLobFiles {
		if !ascii || f.isASCII {
			if first {
				testData = append(
					testData,
					NullLob{Valid: false, Lob: &Lob{rd: bytes.NewReader(f.content)}},
					NullLob{Valid: true, Lob: &Lob{rd: bytes.NewReader(f.content)}},
				)
				first = false
			}
			testData = append(testData, Lob{rd: bytes.NewReader(f.content)})
		}
	}
	return testData
}

var stPointTestData = []spatial.Geometry{
	spatial.Point{},
	spatial.Point{X: 2.5, Y: 3.0},
	spatial.Point{X: -3.0, Y: -4.5},
}

var stGeometryTestData = []spatial.Geometry{
	spatial.Point{X: 2.5, Y: 3.0},
	spatial.Point{X: -3.0, Y: -4.5},
	spatial.PointZ{X: -3.0, Y: -4.5, Z: 5.0},
	spatial.PointM{X: -3.0, Y: -4.5, M: 6.0},
	spatial.PointM{X: -3.0, Y: -4.5, M: spatial.NaN()},
	spatial.PointZM{X: -3.0, Y: -4.5, Z: 5.0, M: 6.0},
	spatial.PointZM{X: -3.0, Y: -4.5, Z: 5.0, M: spatial.NaN()},

	spatial.LineString{},
	spatial.LineString{{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}, {X: 6.0, Y: 3.0}},
	spatial.LineString{{X: 4.0, Y: 4.0}, {X: 6.0, Y: 5.0}, {X: 7.0, Y: 4.0}},
	spatial.LineString{{X: 7.0, Y: 5.0}, {X: 9.0, Y: 7.0}},
	spatial.LineString{{X: 7.0, Y: 3.0}, {X: 8.0, Y: 5.0}},

	spatial.CircularString{},
	spatial.CircularString{{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}, {X: 6.0, Y: 3.0}},

	spatial.Polygon{},
	spatial.Polygon{{{X: 6.0, Y: 7.0}, {X: 10.0, Y: 3.0}, {X: 10.0, Y: 10.0}, {X: 6.0, Y: 7.0}}},
	// hdb permutates ring points?
	// same call with
	// spatial.Polygon{{{6.0, 7.0}, {10.0, 3.0}, {10.0, 10.0}, {6.0, 7.0}}, {{6.0, 7.0}, {10.0, 3.0}, {10.0, 10.0}, {6.0, 7.0}}}
	// would give errors as hdb changes 'middle' coordinates for included ring
	spatial.Polygon{{{X: 6.0, Y: 7.0}, {X: 10.0, Y: 3.0}, {X: 10.0, Y: 10.0}, {X: 6.0, Y: 7.0}}, {{X: 6.0, Y: 7.0}, {X: 10.0, Y: 10.0}, {X: 10.0, Y: 3.0}, {X: 6.0, Y: 7.0}}},

	spatial.MultiPoint{},
	spatial.MultiPoint{{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}},

	spatial.MultiLineString{},
	spatial.MultiLineString{{{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}, {X: 6.0, Y: 3.0}}, {{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}, {X: 6.0, Y: 3.0}}},

	spatial.MultiPolygon{},
	spatial.MultiPolygon{
		{{{X: 6.0, Y: 7.0}, {X: 10.0, Y: 3.0}, {X: 10.0, Y: 10.0}, {X: 6.0, Y: 7.0}}, {{X: 6.0, Y: 7.0}, {X: 10.0, Y: 10.0}, {X: 10.0, Y: 3.0}, {X: 6.0, Y: 7.0}}},
		{{{X: 6.0, Y: 7.0}, {X: 10.0, Y: 3.0}, {X: 10.0, Y: 10.0}, {X: 6.0, Y: 7.0}}, {{X: 6.0, Y: 7.0}, {X: 10.0, Y: 10.0}, {X: 10.0, Y: 3.0}, {X: 6.0, Y: 7.0}}},
	},

	spatial.GeometryCollection{},
	spatial.GeometryCollection{spatial.Point{X: 1, Y: 1}, spatial.LineString{{X: 1, Y: 1}, {X: 2, Y: 2}}},
}

func checkInt(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(sql.NullInt64); ok {
		in := in.(sql.NullInt64)
		return in.Valid == out.Valid && (!in.Valid || in.Int64 == out.Int64), nil
	}
	return in == out, nil
}

func checkFloat(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(sql.NullFloat64); ok {
		in := in.(sql.NullFloat64)
		return in.Valid == out.Valid && (!in.Valid || in.Float64 == out.Float64), nil
	}
	return in == out, nil
}

func checkDecimal(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(NullDecimal); ok {
		in := in.(NullDecimal)
		return in.Valid == out.Valid && (!in.Valid || ((*big.Rat)(in.Decimal)).Cmp((*big.Rat)(out.Decimal)) == 0), nil
	}
	return ((*big.Rat)(in.(*Decimal))).Cmp((*big.Rat)(out.(*Decimal))) == 0, nil
}

func checkBoolean(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(sql.NullBool); ok {
		in := in.(sql.NullBool)
		return in.Valid == out.Valid && (!in.Valid || in.Bool == out.Bool), nil
	}
	return in == out, nil
}

func equalDate(t1, t2 time.Time) bool {
	return t1.Year() == t2.Year() && t1.Month() == t2.Month() && t1.Day() == t2.Day()
}

func equalTime(t1, t2 time.Time) bool {
	return t1.Hour() == t2.Hour() && t1.Minute() == t2.Minute() && t1.Second() == t2.Second()
}

func equalDateTime(t1, t2 time.Time) bool {
	return equalDate(t1, t2) && equalTime(t1, t2)
}

func equalMillisecond(t1, t2 time.Time) bool {
	return t1.Nanosecond()/1000000*1000000 == t2.Nanosecond()/1000000*1000000
}

func equalTimestamp(t1, t2 time.Time) bool {
	return equalDate(t1, t2) && equalTime(t1, t2) && equalMillisecond(t1, t2)
}

func equalLongdate(t1, t2 time.Time) bool {
	//HDB: nanosecond 7-digit precision
	return equalDate(t1, t2) && equalTime(t1, t2) && (t1.Nanosecond()/100) == (t2.Nanosecond()/100)
}

func checkDate(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalDate(in.Time.UTC(), out.Time)), nil
	}
	return equalDate(in.(time.Time).UTC(), out.(time.Time)), nil
}

func checkTime(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalTime(in.Time.UTC(), out.Time)), nil
	}
	return equalTime(in.(time.Time).UTC(), out.(time.Time)), nil
}

func checkDateTime(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalDateTime(in.Time.UTC(), out.Time)), nil
	}
	return equalDateTime(in.(time.Time).UTC(), out.(time.Time)), nil
}

func _checkTimestamp(in, out any) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalTimestamp(in.Time.UTC(), out.Time)), nil
	}
	return equalTimestamp(in.(time.Time).UTC(), out.(time.Time)), nil
}

func _checkLongdate(in, out any) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalLongdate(in.Time.UTC(), out.Time)), nil
	}
	return equalLongdate(in.(time.Time).UTC(), out.(time.Time)), nil
}

func checkTimestamp(ct columnType, in, out any) (bool, error) {
	if ct.dfv() == 1 {
		return _checkTimestamp(in, out)
	}
	return _checkLongdate(in, out)
}

func compareStringFixSize(in, out string) bool {
	if in != out[:len(in)] {
		return false
	}
	for _, r := range out[len(in):] {
		if r != rune(' ') {
			return false
		}
	}
	return true
}

func checkFixString(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(sql.NullString); ok {
		in := in.(sql.NullString)
		return in.Valid == out.Valid && (!in.Valid || compareStringFixSize(in.String, out.String)), nil
	}
	return compareStringFixSize(in.(string), out.(string)), nil
}

func checkString(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(sql.NullString); ok {
		in := in.(sql.NullString)
		return in.Valid == out.Valid && (!in.Valid || in.String == out.String), nil
	}
	return in == out, nil
}

func compareBytesFixSize(in, out []byte) bool {
	if !bytes.Equal(in, out[:len(in)]) {
		return false
	}
	for _, r := range out[len(in):] {
		if r != 0 {
			return false
		}
	}
	return true
}

func checkFixBytes(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(NullBytes); ok {
		in := in.(NullBytes)
		return in.Valid == out.Valid && (!in.Valid || compareBytesFixSize(in.Bytes, out.Bytes)), nil
	}
	return compareBytesFixSize(in.([]byte), out.([]byte)), nil
}

func checkBytes(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(NullBytes); ok {
		in := in.(NullBytes)
		return in.Valid == out.Valid && (!in.Valid || bytes.Equal(in.Bytes, out.Bytes)), nil
	}
	return bytes.Equal(in.([]byte), out.([]byte)), nil
}

// baseline: alphanum is varchar
func formatAlphanumVarchar(s string, fieldSize int) string {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil { // non numeric
		return s
	}
	// numeric (pad with leading zeroes)
	return fmt.Sprintf("%0"+strconv.Itoa(fieldSize)+"d", i)
}

func formatAlphanum(s string) string {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil { // non numeric
		return s
	}
	// numeric (return number as string with no leading zeroes)
	return strconv.FormatUint(i, 10)
}

func _checkAlphanumVarchar(in, out any, length int) (bool, error) {
	if out, ok := out.(sql.NullString); ok {
		in := in.(sql.NullString)
		return in.Valid == out.Valid && (!in.Valid || formatAlphanumVarchar(in.String, length) == out.String), nil
	}
	return formatAlphanumVarchar(in.(string), length) == out.(string), nil
}

func _checkAlphanum(in, out any) (bool, error) {
	if out, ok := out.(sql.NullString); ok {
		in := in.(sql.NullString)
		return in.Valid == out.Valid && (!in.Valid || formatAlphanum(in.String) == out.String), nil
	}
	return formatAlphanum(in.(string)) == out.(string), nil
}

func checkAlphanum(ct columnType, in, out any) (bool, error) {
	if ct.dfv() == 1 {
		length, ok := ct.length()
		if !ok {
			return false, fmt.Errorf("cannot detect fieldlength of %v", ct)
		}
		return _checkAlphanumVarchar(in, out, int(length))
	}
	return _checkAlphanum(in, out)
}

func compareLob(in, out Lob) (bool, error) {
	if _, err := in.rd.(*bytes.Reader).Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	content, err := io.ReadAll(in.rd)
	if err != nil {
		return false, err
	}
	// println(len(content))
	return bytes.Equal(content, out.wr.(*bytes.Buffer).Bytes()), nil
}

func checkLob(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(NullLob); ok {
		in := in.(NullLob)
		if in.Valid != out.Valid {
			return false, nil
		}
		if !in.Valid { // null value - skip comparing values
			return true, nil
		}
		return compareLob(*in.Lob, *out.Lob)
	}
	return compareLob(in.(Lob), out.(Lob))
}

// for text and bintext do not check content as we have seen examples for bintext
// where the content was slightly modified by hdb (e.g. elimination of spaces)
func checkText(ct columnType, in, out any) (bool, error) {
	if out, ok := out.(NullLob); ok {
		in := in.(NullLob)
		return in.Valid == out.Valid, nil
	}
	return true, nil
}

func equalJSON(b1, b2 []byte) (bool, error) {
	var j1, j2 any

	if err := json.Unmarshal(b1, &j1); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b2, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j1, j2), nil
}

func TestDataType(t *testing.T) {
	type tester interface {
		columnType() columnType
		run(t *testing.T, db *sql.DB)
	}

	getTests := func(version *Version, dfv int) []tester {
		return []tester{
			&dttDef{&basicColumn{version, dfv, basicType[dtTinyint], true}, checkInt, tinyintTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtSmallint], true}, checkInt, smallintTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtInteger], true}, checkInt, integerTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtBigint], true}, checkInt, bigintTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtReal], true}, checkFloat, realTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtDouble], true}, checkFloat, doubleTestData},

			&dttDef{&basicColumn{version, dfv, basicType[dtDate], true}, checkDate, timeTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtTime], true}, checkTime, timeTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtSeconddate], true}, checkDateTime, timeTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtDaydate], true}, checkDate, timeTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtSecondtime], true}, checkTime, timeTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtTimestamp], true}, checkTimestamp, timeTestData},
			&dttDef{&basicColumn{version, dfv, basicType[dtLongdate], true}, checkTimestamp, timeTestData},

			&dttTX{dttDef{&basicColumn{version, dfv, basicType[dtClob], true}, checkLob, lobTestData(true, t)}},   // tests executed in parallel -> do not reuse lobs
			&dttTX{dttDef{&basicColumn{version, dfv, basicType[dtNClob], true}, checkLob, lobTestData(false, t)}}, // tests executed in parallel -> do not reuse lobs
			&dttTX{dttDef{&basicColumn{version, dfv, basicType[dtBlob], true}, checkLob, lobTestData(false, t)}},  // tests executed in parallel -> do not reuse lobs

			&dttDef{&basicColumn{version, dfv, basicType[dtBoolean], true}, checkBoolean, booleanTestData},

			&dttTX{dttDef{&basicColumn{version, dfv, basicType[dtText], true}, checkText, lobTestData(false, t)}},   // tests executed in parallel -> do not reuse lobs
			&dttTX{dttDef{&basicColumn{version, dfv, basicType[dtBintext], true}, checkText, lobTestData(true, t)}}, // tests executed in parallel -> do not reuse lobs
			/*
			 using unicode (CESU-8) data for char HDB
			 - successful insert into table
			 - but query table returns
			   SQL HdbError 7 - feature not supported: invalid character encoding: ...
			 --> use ASCII test data only
			 surprisingly: varchar works with unicode characters
			*/
			&dttDef{&varColumn{version, dfv, varType[dtChar], true, 40}, checkFixString, asciiStringTestData},
			&dttDef{&varColumn{version, dfv, varType[dtVarchar], true, 40}, checkString, stringTestData},
			&dttDef{&varColumn{version, dfv, varType[dtNChar], true, 20}, checkFixString, stringTestData},
			&dttDef{&varColumn{version, dfv, varType[dtNVarchar], true, 20}, checkString, stringTestData},
			&dttDef{&varColumn{version, dfv, varType[dtAlphanum], true, 20}, checkAlphanum, alphanumTestData},
			&dttDef{&varColumn{version, dfv, varType[dtBinary], true, 20}, checkFixBytes, binaryTestData},
			&dttDef{&varColumn{version, dfv, varType[dtVarbinary], true, 20}, checkBytes, binaryTestData},

			// negative test
			&dttNeg{&varColumn{version, dfv, varType[dtNChar], true, 20}, nil, invalidUnicodeTestData},
			&dttNeg{&varColumn{version, dfv, varType[dtNVarchar], true, 20}, nil, invalidUnicodeTestData},

			&dttDef{&decimalColumn{version, dfv, decimalType[dtDecimal], true, 0, 0}, checkDecimal, decimalTestData}, // floating point decimal number

			&dttDef{&decimalColumn{version, dfv, decimalType[dtDecimal], true, 18, 2}, checkDecimal, decimalTestData},        // precision, scale decimal number -fixed8
			&dttDef{&decimalColumn{version, dfv, decimalType[dtDecimal], true, 28, 2}, checkDecimal, decimalFixed12TestData}, // precision, scale decimal number -fixed12
			&dttDef{&decimalColumn{version, dfv, decimalType[dtDecimal], true, 38, 2}, checkDecimal, decimalFixed16TestData}, // precision, scale decimal number -fixed16

			&dttSpatial{&spatialColumn{version, dfv, spatialType[dtSTPoint], true, 0}, stPointTestData},
			&dttSpatial{&spatialColumn{version, dfv, spatialType[dtSTPoint], true, 3857}, stPointTestData},

			&dttSpatial{&spatialColumn{version, dfv, spatialType[dtSTGeometry], true, 0}, stGeometryTestData},
			&dttSpatial{&spatialColumn{version, dfv, spatialType[dtSTGeometry], true, 3857}, stGeometryTestData},
		}
	}

	for _, dfv := range p.SupportedDfvs(testing.Short()) {
		func(dfv int) { // new dfv to run in parallel
			name := fmt.Sprintf("dfv %d", dfv)
			t.Run(name, func(t *testing.T) {
				t.Parallel() // run in parallel to speed up

				connector := NewTestConnector()
				connector.SetDfv(dfv)
				db := sql.OpenDB(connector)
				t.Cleanup(func() { db.Close() }) // close only when all parallel subtests are completed

				var version *Version
				// Grab connection to detect hdb version.
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				conn.Raw(func(driverConn any) error {
					version = driverConn.(Conn).HDBVersion()
					return nil
				})

				for i, test := range getTests(version, dfv) {
					func(i int, test tester, db *sql.DB) { // save i, test to run in parallel
						if test.columnType().isSupported() {
							t.Run(fmt.Sprintf("%s %d", test.columnType().column(), i), func(t *testing.T) {
								t.Parallel() // run in parallel to speed up
								test.run(t, db)
							})
						}
					}(i, test, db)
				}
			})
		}(dfv)
	}
}
