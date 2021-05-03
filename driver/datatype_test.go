// +build !unit

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

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

	"github.com/SAP/go-hdb/driver/common"
	dt "github.com/SAP/go-hdb/driver/drivertest"
	"github.com/SAP/go-hdb/driver/spatial"
)

type dttDef struct {
	*testing.T
	db               *sql.DB
	sqlType          string
	length, fraction int
	checkFn          func(in, out interface{}) bool
	testData         []interface{}

	tableName Identifier
	numRecs   int
}

func newDttDef(sqlType string, checkFn func(in, out interface{}) bool, testData []interface{}, t *testing.T) *dttDef {
	return &dttDef{sqlType: sqlType, checkFn: checkFn, testData: testData, T: t}
}

func newDttDefL(sqlType string, length int, checkFn func(in, out interface{}) bool, testData []interface{}, t *testing.T) *dttDef {
	return &dttDef{sqlType: sqlType, length: length, checkFn: checkFn, testData: testData, T: t}
}

func newDttDefLF(sqlType string, length, fraction int, checkFn func(in, out interface{}) bool, testData []interface{}, t *testing.T) *dttDef {
	return &dttDef{sqlType: sqlType, length: length, fraction: fraction, checkFn: checkFn, testData: testData, T: t}
}

func (t *dttDef) setDB(db *sql.DB) { t.db = db }

func (t *dttDef) name() string {
	switch {
	case t.length != 0 && t.fraction != 0:
		return fmt.Sprintf("%s_%d_%d", t.sqlType, t.length, t.fraction)
	case t.length != 0:
		return fmt.Sprintf("%s_%d", t.sqlType, t.length)
	default:
		return t.sqlType
	}
}

func (t *dttDef) column() string {
	switch {
	case t.length != 0 && t.fraction != 0:
		return fmt.Sprintf("%s(%d, %d)", t.sqlType, t.length, t.fraction)
	case t.length != 0:
		return fmt.Sprintf("%s(%d)", t.sqlType, t.length)
	default:
		return t.sqlType
	}
}

func (t *dttDef) createTable() {
	t.tableName = RandomIdentifier(fmt.Sprintf("%s_", t.name()))
	if _, err := t.db.Exec(fmt.Sprintf("create table %s (x %s, i integer)", t.tableName, t.column())); err != nil {
		t.Fatal(err)
	}
}

func (t *dttDef) insert() {
	stmt, err := t.db.Prepare(fmt.Sprintf("insert into %s values(?, ?)", t.tableName))
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for _, in := range t.testData {
		if _, err := stmt.Exec(in, i); err != nil {
			t.Fatalf("type: %s - %d - %s", t.sqlType, i, err)
		}
		i++
	}
	t.numRecs = i
}

func (t *dttDef) check() {
	rows, err := t.db.Query(fmt.Sprintf("select * from %s order by i", t.tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		in := t.testData[i]
		outRef := reflect.New(reflect.TypeOf(in)).Interface()

		switch outRef := outRef.(type) {
		case *NullDecimal:
			outRef.Decimal = (*Decimal)(new(big.Rat))
		case *Lob:
			outRef.SetWriter(new(bytes.Buffer))
		case *NullLob:
			outRef.Lob = new(Lob).SetWriter(new(bytes.Buffer))
		}

		if err := rows.Scan(outRef, &i); err != nil {
			t.Fatal(err)
		}
		outVal := reflect.ValueOf(outRef).Elem().Interface()

		if !t.checkFn(in, outVal) {
			t.Fatalf("%d value %v - expected %v", i, outVal, in)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != t.numRecs {
		t.Fatalf("rows %d - expected %d", i, t.numRecs)
	}
}

func (t *dttDef) run() {
	t.createTable()
	t.insert()
	t.check()
}

type dttTX struct {
	dttDef
}

func newDttTX(sqlType string, checkFn func(in, out interface{}) bool, testData []interface{}, t *testing.T) *dttTX {
	return &dttTX{dttDef: dttDef{sqlType: sqlType, checkFn: checkFn, testData: testData, T: t}}
}

func (t *dttTX) insert() { // override insert
	// use trancactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := t.db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values(?, ?)", t.tableName))
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for _, in := range t.testData {
		if _, err := stmt.Exec(in, i); err != nil {
			t.Fatalf("%d - %s", i, err)
		}
		i++
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	t.numRecs = i
}

func (t *dttTX) run() { // override run, so that dttTX insert is called
	t.createTable()
	t.insert()
	t.check()
}

type dttSpatial struct {
	*testing.T
	db       *sql.DB
	sqlType  string
	srid     int32
	fn       string
	testData []spatial.Geometry

	tableName Identifier
	numRecs   int
}

func newDttSpatial(sqlType string, srid int32, fn string, testData []spatial.Geometry, t *testing.T) *dttSpatial {
	return &dttSpatial{sqlType: sqlType, srid: srid, fn: fn, testData: testData, T: t}

}

func (t *dttSpatial) setDB(db *sql.DB) { t.db = db }

func (t *dttSpatial) name() string {
	switch {
	case t.srid != 0:
		return fmt.Sprintf("%s_%d", t.sqlType, t.srid)
	default:
		return t.sqlType
	}
}

func (t *dttSpatial) column() string {
	if t.srid == 0 {
		return t.sqlType
	}
	return fmt.Sprintf("%s(%d)", t.sqlType, t.srid)
}

func (t *dttSpatial) parameter() string {
	if t.fn == "" {
		return "?"
	}
	return fmt.Sprintf("%s(?)", t.fn)
}

func (t *dttSpatial) createTable() {
	t.tableName = RandomIdentifier(fmt.Sprintf("%s_", t.name()))
	if _, err := t.db.Exec(fmt.Sprintf("create table %s (x %s, i integer)", t.tableName, t.column())); err != nil {
		t.Fatal(err)
	}
}

func (t *dttSpatial) withTx(fn func(func(value interface{}))) {
	// use trancactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := t.db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values(%s, ?)", t.tableName, t.parameter()))
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	fn(func(value interface{}) {
		if _, err := stmt.Exec(value, i); err != nil {
			t.Fatalf("%d - %s", i, err)
		}
		i++
	})

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	t.numRecs = i
}

func (t *dttSpatial) withRows(fn func(i int), dest ...interface{}) {
	rows, err := t.db.Query(fmt.Sprintf("select x, i, x.st_aswkb(), x.st_asewkb(), x.st_aswkt(), x.st_asewkt(), x.st_asgeojson() from %s order by i", t.tableName))
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
	if i != t.numRecs {
		t.Fatalf("rows %d - expected %d", i, t.numRecs)
	}
}

func (t *dttSpatial) run() {
	t.createTable()

	t.withTx(func(exec func(value interface{})) {
		for _, g := range t.testData {
			ewkb, err := spatial.EncodeEWKB(g, false, t.srid)
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

	t.withRows(func(i int) {
		wkb, err := spatial.EncodeWKB(t.testData[i], false)
		if err != nil {
			t.Fatal(err)
		}

		if string(wkb) != x {
			t.Fatalf("test %d: x value %v - expected %v", i, x, string(wkb))
		}

		ewkb, err := spatial.EncodeEWKB(t.testData[i], false, t.srid)
		if err != nil {
			t.Fatal(err)
		}

		wkt, err := spatial.EncodeWKT(t.testData[i])
		if err != nil {
			t.Fatal(err)
		}

		ewkt, err := spatial.EncodeEWKT(t.testData[i], t.srid)
		if err != nil {
			t.Fatal(err)
		}

		geoJSON, err := spatial.EncodeGeoJSON(t.testData[i])
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
		if !equalJSON(geoJSON, asGeoJSON, t.T) {
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

var tinyintTestData = []interface{}{
	uint8(minTinyint),
	uint8(maxTinyint),
	sql.NullInt64{Valid: false, Int64: minTinyint},
	sql.NullInt64{Valid: true, Int64: maxTinyint},
}

var smallintTestData = []interface{}{
	int16(minSmallint),
	int16(maxSmallint),
	sql.NullInt64{Valid: false, Int64: minSmallint},
	sql.NullInt64{Valid: true, Int64: maxSmallint},
}

var integerTestData = []interface{}{
	int32(minInteger),
	int32(maxInteger),
	sql.NullInt64{Valid: false, Int64: minInteger},
	sql.NullInt64{Valid: true, Int64: maxInteger},
}

var bigintTestData = []interface{}{
	int64(minBigint),
	int64(maxBigint),
	sql.NullInt64{Valid: false, Int64: minBigint},
	sql.NullInt64{Valid: true, Int64: maxBigint},
}

var realTestData = []interface{}{
	float32(-maxReal),
	float32(maxReal),
	sql.NullFloat64{Valid: false, Float64: -maxReal},
	sql.NullFloat64{Valid: true, Float64: maxReal},
}

var doubleTestData = []interface{}{
	float64(-maxDouble),
	float64(maxDouble),
	sql.NullFloat64{Valid: false, Float64: -maxDouble},
	sql.NullFloat64{Valid: true, Float64: maxDouble},
}

var timeTestData = []interface{}{
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
	decimalTestData = []interface{}{
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

var booleanTestData = []interface{}{
	true,
	false,
	sql.NullBool{Valid: false, Bool: true},
	sql.NullBool{Valid: true, Bool: false},
}

var asciiStringTestData = []interface{}{
	"Hello HDB",
	"aaaaaaaaaa",
	sql.NullString{Valid: false, String: "Hello HDB"},
	sql.NullString{Valid: true, String: "Hello HDB"},
}

var stringTestData = []interface{}{
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

var binaryTestData = []interface{}{
	[]byte("Hello HDB"),
	[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
	[]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0xff},
	NullBytes{Valid: false, Bytes: []byte("Hello HDB")},
	NullBytes{Valid: true, Bytes: []byte("Hello HDB")},
}

var alphanumTestData = []interface{}{
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
	initLobFilesOnce.Do(func() { // lazy (lob file test might not be executed)

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

		walk := func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() && filter(info.Name()) {
				content, err := _readFile(path)
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
	})
}

func lobTestData(ascii bool, t *testing.T) []interface{} {
	testInitLobFiles(t)
	testData := make([]interface{}, 0, len(testLobFiles))
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

func checkInt(in, out interface{}) bool {
	if out, ok := out.(sql.NullInt64); ok {
		in := in.(sql.NullInt64)
		return in.Valid == out.Valid && (!in.Valid || in.Int64 == out.Int64)
	}
	return in == out
}

func checkFloat(in, out interface{}) bool {
	if out, ok := out.(sql.NullFloat64); ok {
		in := in.(sql.NullFloat64)
		return in.Valid == out.Valid && (!in.Valid || in.Float64 == out.Float64)
	}
	return in == out
}

func checkDecimal(in, out interface{}) bool {
	if out, ok := out.(NullDecimal); ok {
		in := in.(NullDecimal)
		return in.Valid == out.Valid && (!in.Valid || ((*big.Rat)(in.Decimal)).Cmp((*big.Rat)(out.Decimal)) == 0)
	}
	return ((*big.Rat)(in.(*Decimal))).Cmp((*big.Rat)(out.(*Decimal))) == 0
}

func checkBoolean(in, out interface{}) bool {
	if out, ok := out.(sql.NullBool); ok {
		in := in.(sql.NullBool)
		return in.Valid == out.Valid && (!in.Valid || in.Bool == out.Bool)
	}
	return in == out
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

func checkDate(in, out interface{}) bool {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalDate(in.Time.UTC(), out.Time))
	}
	return equalDate(in.(time.Time).UTC(), out.(time.Time))
}

func checkTime(in, out interface{}) bool {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalTime(in.Time.UTC(), out.Time))
	}
	return equalTime(in.(time.Time).UTC(), out.(time.Time))
}

func checkDateTime(in, out interface{}) bool {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalDateTime(in.Time.UTC(), out.Time))
	}
	return equalDateTime(in.(time.Time).UTC(), out.(time.Time))
}

func checkTimestamp(in, out interface{}) bool {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalTimestamp(in.Time.UTC(), out.Time))
	}
	return equalTimestamp(in.(time.Time).UTC(), out.(time.Time))
}

func checkLongdate(in, out interface{}) bool {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalLongdate(in.Time.UTC(), out.Time))
	}
	return equalLongdate(in.(time.Time).UTC(), out.(time.Time))
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

func checkFixString(in, out interface{}) bool {
	if out, ok := out.(sql.NullString); ok {
		in := in.(sql.NullString)
		return in.Valid == out.Valid && (!in.Valid || compareStringFixSize(in.String, out.String))
	}
	return compareStringFixSize(in.(string), out.(string))
}

func checkString(in, out interface{}) bool {
	if out, ok := out.(sql.NullString); ok {
		in := in.(sql.NullString)
		return in.Valid == out.Valid && (!in.Valid || in.String == out.String)
	}
	return in == out
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

func checkFixBytes(in, out interface{}) bool {
	if out, ok := out.(NullBytes); ok {
		in := in.(NullBytes)
		return in.Valid == out.Valid && (!in.Valid || compareBytesFixSize(in.Bytes, out.Bytes))
	}
	return compareBytesFixSize(in.([]byte), out.([]byte))
}

func checkBytes(in, out interface{}) bool {
	if out, ok := out.(NullBytes); ok {
		in := in.(NullBytes)
		return in.Valid == out.Valid && (!in.Valid || bytes.Equal(in.Bytes, out.Bytes))
	}
	return bytes.Equal(in.([]byte), out.([]byte))
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

func checkAlphanumVarchar(length int) func(in, out interface{}) bool {
	return func(in, out interface{}) bool {
		if out, ok := out.(sql.NullString); ok {
			in := in.(sql.NullString)
			return in.Valid == out.Valid && (!in.Valid || formatAlphanumVarchar(in.String, length) == out.String)
		}
		return formatAlphanumVarchar(in.(string), length) == out.(string)
	}
}

func checkAlphanum(in, out interface{}) bool {
	if out, ok := out.(sql.NullString); ok {
		in := in.(sql.NullString)
		return in.Valid == out.Valid && (!in.Valid || formatAlphanum(in.String) == out.String)
	}
	return formatAlphanum(in.(string)) == out.(string)
}

func compareLob(in, out Lob, t *testing.T) bool {
	if _, err := in.rd.(*bytes.Reader).Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
		return false
	}
	content, err := _readAll(in.rd)
	if err != nil {
		t.Fatal(err)
		return false
	}
	return bytes.Equal(content, out.wr.(*bytes.Buffer).Bytes())
}

func checkLob(t *testing.T) func(in, out interface{}) bool {
	return func(in, out interface{}) bool {
		if out, ok := out.(NullLob); ok {
			in := in.(NullLob)
			return in.Valid == out.Valid && (!in.Valid || compareLob(*in.Lob, *out.Lob, t))
		}
		return compareLob(in.(Lob), out.(Lob), t)
	}
}

// for text and bintext do not check content as we have seen examples for bintext
// where the content was slightly modified by hdb (e.g. elimination of spaces)
func checkText(in, out interface{}) bool {
	if out, ok := out.(NullLob); ok {
		in := in.(NullLob)
		return in.Valid == out.Valid
	}
	return true
}

func equalJSON(b1, b2 []byte, t *testing.T) bool {
	var j1, j2 interface{}

	if err := json.Unmarshal(b1, &j1); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b2, &j2); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(j1, j2)
}

func TestDataType(t *testing.T) {
	type tester interface {
		setDB(db *sql.DB)
		name() string
		run()
	}

	/*
		hdb version 4:
		- does not support alphanum
		- does not support shorttext
	*/

	hdbVersion1 := common.ParseHDBVersion("1.00.000")
	hdbVersion4 := common.ParseHDBVersion("4.00.000")

	tests := []struct {
		dfv            int
		dvfCond        int
		hdbVersion     *common.HDBVersion
		hdbVersionCond int
		tester         func() tester
	}{
		{DfvLevel1, dt.CondEQ, hdbVersion1, dt.CondGE, func() tester { return newDttDef("timestamp", checkTimestamp, timeTestData, t) }},
		{DfvLevel1, dt.CondEQ, hdbVersion1, dt.CondGE, func() tester { return newDttDef("longdate", checkTimestamp, timeTestData, t) }},
		{DfvLevel1, dt.CondEQ, hdbVersion4, dt.CondLT, func() tester { return newDttDefL("alphanum", 20, checkAlphanumVarchar(20), alphanumTestData, t) }},

		{DfvLevel2, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("timestamp", checkLongdate, timeTestData, t) }},
		{DfvLevel2, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("longdate", checkLongdate, timeTestData, t) }},
		{DfvLevel2, dt.CondGE, hdbVersion4, dt.CondLT, func() tester { return newDttDefL("alphanum", 20, checkAlphanum, alphanumTestData, t) }},

		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("tinyint", checkInt, tinyintTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("smallint", checkInt, smallintTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("integer", checkInt, integerTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("bigint", checkInt, bigintTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("real", checkFloat, realTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("double", checkFloat, doubleTestData, t) }},

		/*
		 using unicode (CESU-8) data for char HDB
		 - successful insert into table
		 - but query table returns
		   SQL HdbError 7 - feature not supported: invalid character encoding: ...
		 --> use ASCII test data only
		 surprisingly: varchar works with unicode characters
		*/
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDefL("char", 40, checkFixString, asciiStringTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDefL("varchar", 40, checkString, stringTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDefL("nchar", 20, checkFixString, stringTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDefL("nvarchar", 20, checkString, stringTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDefL("binary", 20, checkFixBytes, binaryTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDefL("varbinary", 20, checkBytes, binaryTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("date", checkDate, timeTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("time", checkTime, timeTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("seconddate", checkDateTime, timeTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("daydate", checkDate, timeTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("secondtime", checkTime, timeTestData, t) }},
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("decimal", checkDecimal, decimalTestData, t) }}, // floating point decimal number
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDef("boolean", checkBoolean, booleanTestData, t) }},

		{DfvLevel8, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDefLF("decimal", 18, 2, checkDecimal, decimalTestData, t) }},        // precision, scale decimal number -fixed8
		{DfvLevel8, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDefLF("decimal", 28, 2, checkDecimal, decimalFixed12TestData, t) }}, // precision, scale decimal number -fixed12
		{DfvLevel8, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttDefLF("decimal", 38, 2, checkDecimal, decimalFixed16TestData, t) }}, // precision, scale decimal number -fixed16

		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttTX("clob", checkLob(t), lobTestData(true, t), t) }},   // tests executed in parallel -> do not reuse lobs
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttTX("nclob", checkLob(t), lobTestData(false, t), t) }}, // tests executed in parallel -> do not reuse lobs
		{DfvLevel1, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttTX("blob", checkLob(t), lobTestData(false, t), t) }},  // tests executed in parallel -> do not reuse lobs

		{DfvLevel4, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttTX("text", checkText, lobTestData(false, t), t) }},   // tests executed in parallel -> do not reuse lobs
		{DfvLevel6, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttTX("bintext", checkText, lobTestData(true, t), t) }}, // tests executed in parallel -> do not reuse lobs

		{DfvLevel6, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttSpatial("st_point", 0, "st_geomfromewkb", stPointTestData, t) }},
		{DfvLevel6, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttSpatial("st_point", 3857, "st_geomfromewkb", stPointTestData, t) }},

		{DfvLevel6, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttSpatial("st_geometry", 0, "st_geomfromewkb", stGeometryTestData, t) }},
		{DfvLevel6, dt.CondGE, hdbVersion1, dt.CondGE, func() tester { return newDttSpatial("st_geometry", 3857, "st_geomfromewkb", stGeometryTestData, t) }},
	}

	checkDFV := func(dfv, testDfv, testCond int) bool {
		switch testCond {
		default:
			return true
		case dt.CondEQ:
			return dfv == testDfv
		case dt.CondGE:
			return dfv >= testDfv
		}
	}

	checkHDBVersion := func(hdbVersion, testHDBVersion *common.HDBVersion, testHDBVersionCond int) bool {
		cmp := hdbVersion.Compare(testHDBVersion)
		switch testHDBVersionCond {
		case dt.CondLT:
			if cmp != -1 {
				return false
			}
		}
		return true
	}

	hdbVersion := func(db *sql.DB, t *testing.T) *common.HDBVersion {
		// Grab connection.
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		var hdbVersion *common.HDBVersion

		if err := conn.Raw(func(driverConn interface{}) error {
			conn, ok := driverConn.(*Conn)
			if !ok {
				t.Fatal("connection does not implement *driver.Conn")
			}
			hdbVersion = conn.ServerInfo().Version
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return hdbVersion
	}

	var testSet map[int]bool
	if testing.Short() {
		testSet = map[int]bool{DefaultDfv: true}
	} else {
		testSet = supportedDfvs
	}

	for dfv := range testSet {
		func(dfv int) { // new dfv to run in parallel
			name := fmt.Sprintf("dfv %d", dfv)
			t.Run(name, func(t *testing.T) {
				t.Parallel() // run in parallel to speed up

				connector, err := NewConnector(dt.DefaultAttrs())
				if err != nil {
					t.Fatal(err)
				}
				connector.SetDfv(dfv)
				db := sql.OpenDB(connector)
				defer db.Close()

				hdbVersion := hdbVersion(db, t)

				for _, test := range tests {
					if checkDFV(dfv, test.dfv, test.dvfCond) && checkHDBVersion(hdbVersion, test.hdbVersion, test.hdbVersionCond) {
						tester := test.tester() // create new instance to be run in parallel
						t.Run(tester.name(), func(t *testing.T) {
							tester.setDB(db)
							tester.run()
						})
					}
				}
			})
		}(dfv)
	}
}
