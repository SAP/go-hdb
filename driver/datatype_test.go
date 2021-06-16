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

	drvtst "github.com/SAP/go-hdb/driver/drivertest"
	"github.com/SAP/go-hdb/driver/hdb"
	"github.com/SAP/go-hdb/driver/spatial"
)

type dttDef struct {
	*testing.T
	db       *sql.DB
	typ      drvtst.ColumnType
	checkFn  func(in, out interface{}) (bool, error)
	testData []interface{}

	tableName Identifier
	numRecs   int
}

func (t *dttDef) createTable() {
	column := t.typ.Column()
	t.tableName = RandomIdentifier(fmt.Sprintf("%s_", column))
	if _, err := t.db.Exec(fmt.Sprintf("create table %s (x %s, i integer)", t.tableName, column)); err != nil {
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
			t.Fatalf("type: %s - %d - %s", t.typ.TypeName(), i, err)
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

		ok, err := t.checkFn(in, outVal)
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
	if i != t.numRecs {
		t.Fatalf("rows %d - expected %d", i, t.numRecs)
	}
}

func (t *dttDef) run(db *sql.DB) {
	t.db = db
	t.createTable()
	t.insert()
	t.check()
}

type dttTX struct {
	dttDef
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

func (t *dttTX) run(db *sql.DB) { // override run, so that dttTX insert is called
	t.dttDef.db = db
	t.createTable()
	t.insert()
	t.check()
}

type dttSpatial struct {
	*testing.T
	db       *sql.DB
	typ      drvtst.ColumnType
	testData []spatial.Geometry

	tableName Identifier
	numRecs   int
}

func (t *dttSpatial) createTable() {
	column := t.typ.Column()
	t.tableName = RandomIdentifier(fmt.Sprintf("%s_", column))
	if _, err := t.db.Exec(fmt.Sprintf("create table %s (x %s, i integer)", t.tableName, column)); err != nil {
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

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values(st_geomfromewkb(?), ?)", t.tableName))
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

func (t *dttSpatial) run(db *sql.DB) {
	t.db = db
	t.createTable()

	srid := t.typ.SRID()

	t.withTx(func(exec func(value interface{})) {
		for _, g := range t.testData {
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

	t.withRows(func(i int) {
		wkb, err := spatial.EncodeWKB(t.testData[i], false)
		if err != nil {
			t.Fatal(err)
		}

		if string(wkb) != x {
			t.Fatalf("test %d: x value %v - expected %v", i, x, string(wkb))
		}

		ewkb, err := spatial.EncodeEWKB(t.testData[i], false, srid)
		if err != nil {
			t.Fatal(err)
		}

		wkt, err := spatial.EncodeWKT(t.testData[i])
		if err != nil {
			t.Fatal(err)
		}

		ewkt, err := spatial.EncodeEWKT(t.testData[i], srid)
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

func checkInt(in, out interface{}) (bool, error) {
	if out, ok := out.(sql.NullInt64); ok {
		in := in.(sql.NullInt64)
		return in.Valid == out.Valid && (!in.Valid || in.Int64 == out.Int64), nil
	}
	return in == out, nil
}

func checkFloat(in, out interface{}) (bool, error) {
	if out, ok := out.(sql.NullFloat64); ok {
		in := in.(sql.NullFloat64)
		return in.Valid == out.Valid && (!in.Valid || in.Float64 == out.Float64), nil
	}
	return in == out, nil
}

func checkDecimal(in, out interface{}) (bool, error) {
	if out, ok := out.(NullDecimal); ok {
		in := in.(NullDecimal)
		return in.Valid == out.Valid && (!in.Valid || ((*big.Rat)(in.Decimal)).Cmp((*big.Rat)(out.Decimal)) == 0), nil
	}
	return ((*big.Rat)(in.(*Decimal))).Cmp((*big.Rat)(out.(*Decimal))) == 0, nil
}

func checkBoolean(in, out interface{}) (bool, error) {
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

func checkDate(in, out interface{}) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalDate(in.Time.UTC(), out.Time)), nil
	}
	return equalDate(in.(time.Time).UTC(), out.(time.Time)), nil
}

func checkTime(in, out interface{}) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalTime(in.Time.UTC(), out.Time)), nil
	}
	return equalTime(in.(time.Time).UTC(), out.(time.Time)), nil
}

func checkDateTime(in, out interface{}) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalDateTime(in.Time.UTC(), out.Time)), nil
	}
	return equalDateTime(in.(time.Time).UTC(), out.(time.Time)), nil
}

func checkTimestamp(in, out interface{}) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalTimestamp(in.Time.UTC(), out.Time)), nil
	}
	return equalTimestamp(in.(time.Time).UTC(), out.(time.Time)), nil
}

func checkLongdate(in, out interface{}) (bool, error) {
	if out, ok := out.(sql.NullTime); ok {
		in := in.(sql.NullTime)
		return in.Valid == out.Valid && (!in.Valid || equalLongdate(in.Time.UTC(), out.Time)), nil
	}
	return equalLongdate(in.(time.Time).UTC(), out.(time.Time)), nil
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

func checkFixString(in, out interface{}) (bool, error) {
	if out, ok := out.(sql.NullString); ok {
		in := in.(sql.NullString)
		return in.Valid == out.Valid && (!in.Valid || compareStringFixSize(in.String, out.String)), nil
	}
	return compareStringFixSize(in.(string), out.(string)), nil
}

func checkString(in, out interface{}) (bool, error) {
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

func checkFixBytes(in, out interface{}) (bool, error) {
	if out, ok := out.(NullBytes); ok {
		in := in.(NullBytes)
		return in.Valid == out.Valid && (!in.Valid || compareBytesFixSize(in.Bytes, out.Bytes)), nil
	}
	return compareBytesFixSize(in.([]byte), out.([]byte)), nil
}

func checkBytes(in, out interface{}) (bool, error) {
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

func checkAlphanumVarchar(length int) func(in, out interface{}) (bool, error) {
	return func(in, out interface{}) (bool, error) {
		if out, ok := out.(sql.NullString); ok {
			in := in.(sql.NullString)
			return in.Valid == out.Valid && (!in.Valid || formatAlphanumVarchar(in.String, length) == out.String), nil
		}
		return formatAlphanumVarchar(in.(string), length) == out.(string), nil
	}
}

func checkAlphanum(in, out interface{}) (bool, error) {
	if out, ok := out.(sql.NullString); ok {
		in := in.(sql.NullString)
		return in.Valid == out.Valid && (!in.Valid || formatAlphanum(in.String) == out.String), nil
	}
	return formatAlphanum(in.(string)) == out.(string), nil
}

func compareLob(in, out Lob) (bool, error) {
	if _, err := in.rd.(*bytes.Reader).Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	content, err := _readAll(in.rd)
	if err != nil {
		return false, err
	}
	return bytes.Equal(content, out.wr.(*bytes.Buffer).Bytes()), nil
}

func checkLob(in, out interface{}) (bool, error) {
	if out, ok := out.(NullLob); ok {
		in := in.(NullLob)
		ok, err := compareLob(*in.Lob, *out.Lob)
		if err != nil {
			return ok, err
		}
		return in.Valid == out.Valid && (!in.Valid || ok), nil
	}
	return compareLob(in.(Lob), out.(Lob))
}

// for text and bintext do not check content as we have seen examples for bintext
// where the content was slightly modified by hdb (e.g. elimination of spaces)
func checkText(in, out interface{}) (bool, error) {
	if out, ok := out.(NullLob); ok {
		in := in.(NullLob)
		return in.Valid == out.Valid, nil
	}
	return true, nil
}

func equalJSON(b1, b2 []byte) (bool, error) {
	var j1, j2 interface{}

	if err := json.Unmarshal(b1, &j1); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b2, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j1, j2), nil
}

func TestDataType(t *testing.T) {
	// test run condition constants
	const (
		condNone = iota
		condEQ   // equal
		condLT   // less than
		condGE   // greater equal
	)

	checkDFV := func(dfv, testDfv, testCond int) bool {
		switch testCond {
		default:
			return true
		case condEQ:
			return dfv == testDfv
		case condGE:
			return dfv >= testDfv
		}
	}

	type tester interface {
		run(db *sql.DB)
	}

	const (
		dttDefType = iota
		dttTXType
		dttSpatialType
	)

	fnLobTestDataASCII := func() []interface{} { return lobTestData(true, t) }
	fnLobTestData := func() []interface{} { return lobTestData(false, t) }

	tests := []struct {
		dfv      int
		dvfCond  int
		testType int
		typ      drvtst.ColumnType
		checkFn  func(in, out interface{}) (bool, error)
		testData interface{}
	}{
		{DfvLevel1, condEQ, dttDefType, drvtst.NewStdColumn(drvtst.DtTimestamp), checkTimestamp, timeTestData},
		{DfvLevel1, condEQ, dttDefType, drvtst.NewStdColumn(drvtst.DtLongdate), checkTimestamp, timeTestData},
		{DfvLevel1, condEQ, dttDefType, drvtst.NewVarColumn(drvtst.DtAlphanum, 20), checkAlphanumVarchar(20), alphanumTestData},

		{DfvLevel2, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtTimestamp), checkLongdate, timeTestData},
		{DfvLevel2, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtLongdate), checkLongdate, timeTestData},
		{DfvLevel2, condGE, dttDefType, drvtst.NewVarColumn(drvtst.DtAlphanum, 20), checkAlphanum, alphanumTestData},

		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtTinyint), checkInt, tinyintTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtSmallint), checkInt, smallintTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtInteger), checkInt, integerTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtBigint), checkInt, bigintTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtReal), checkFloat, realTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtDouble), checkFloat, doubleTestData},

		/*
		 using unicode (CESU-8) data for char HDB
		 - successful insert into table
		 - but query table returns
		   SQL HdbError 7 - feature not supported: invalid character encoding: ...
		 --> use ASCII test data only
		 surprisingly: varchar works with unicode characters
		*/
		{DfvLevel1, condGE, dttDefType, drvtst.NewVarColumn(drvtst.DtChar, 40), checkFixString, asciiStringTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewVarColumn(drvtst.DtVarchar, 40), checkString, stringTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewVarColumn(drvtst.DtNChar, 20), checkFixString, stringTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewVarColumn(drvtst.DtNVarchar, 20), checkString, stringTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewVarColumn(drvtst.DtBinary, 20), checkFixBytes, binaryTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewVarColumn(drvtst.DtVarbinary, 20), checkBytes, binaryTestData},

		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtDate), checkDate, timeTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtTime), checkTime, timeTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtSeconddate), checkDateTime, timeTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtDaydate), checkDate, timeTestData},
		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtSecondtime), checkTime, timeTestData},

		{DfvLevel1, condGE, dttDefType, drvtst.NewStdColumn(drvtst.DtBoolean), checkBoolean, booleanTestData},

		{DfvLevel1, condGE, dttDefType, drvtst.NewDecimalColumn(drvtst.DtDecimal, 0, 0), checkDecimal, decimalTestData},         // floating point decimal number
		{DfvLevel8, condGE, dttDefType, drvtst.NewDecimalColumn(drvtst.DtDecimal, 18, 2), checkDecimal, decimalTestData},        // precision, scale decimal number -fixed8
		{DfvLevel8, condGE, dttDefType, drvtst.NewDecimalColumn(drvtst.DtDecimal, 28, 2), checkDecimal, decimalFixed12TestData}, // precision, scale decimal number -fixed12
		{DfvLevel8, condGE, dttDefType, drvtst.NewDecimalColumn(drvtst.DtDecimal, 38, 2), checkDecimal, decimalFixed16TestData}, // precision, scale decimal number -fixed16

		{DfvLevel1, condGE, dttTXType, drvtst.NewStdColumn(drvtst.DtClob), checkLob, fnLobTestDataASCII}, // tests executed in parallel -> do not reuse lobs
		{DfvLevel1, condGE, dttTXType, drvtst.NewStdColumn(drvtst.DtNClob), checkLob, fnLobTestData},     // tests executed in parallel -> do not reuse lobs
		{DfvLevel1, condGE, dttTXType, drvtst.NewStdColumn(drvtst.DtBlob), checkLob, fnLobTestData},      // tests executed in parallel -> do not reuse lobs

		{DfvLevel4, condGE, dttTXType, drvtst.NewStdColumn(drvtst.DtText), checkText, fnLobTestData},         // tests executed in parallel -> do not reuse lobs
		{DfvLevel6, condGE, dttTXType, drvtst.NewStdColumn(drvtst.DtBintext), checkText, fnLobTestDataASCII}, // tests executed in parallel -> do not reuse lobs

		{DfvLevel6, condGE, dttSpatialType, drvtst.NewSpatialColumn(drvtst.DtSTPoint, 0), nil, stPointTestData},
		{DfvLevel6, condGE, dttSpatialType, drvtst.NewSpatialColumn(drvtst.DtSTPoint, 3857), nil, stPointTestData},

		{DfvLevel6, condGE, dttSpatialType, drvtst.NewSpatialColumn(drvtst.DtSTGeometry, 0), nil, stGeometryTestData},
		{DfvLevel6, condGE, dttSpatialType, drvtst.NewSpatialColumn(drvtst.DtSTGeometry, 3857), nil, stGeometryTestData},
	}

	dfvs := []int{DefaultDfv}
	if !testing.Short() {
		dfvs = SupportedDfvs
	}

	convertTestDataType := func(td interface{}) []interface{} {
		switch td := td.(type) {
		case []interface{}:
			return td
		case func() []interface{}:
			return td() // create new instance (e.g. lob test data)
		default:
			t.Fatalf("invalid test data type %T", td)
			return nil
		}
	}

	for _, dfv := range dfvs {
		func(dfv int) { // new dfv to run in parallel
			name := fmt.Sprintf("dfv %d", dfv)
			t.Run(name, func(t *testing.T) {
				t.Parallel() // run in parallel to speed up

				connector, err := NewConnector(drvtst.DefaultAttrs())
				if err != nil {
					t.Fatal(err)
				}
				connector.SetDfv(dfv)
				db := sql.OpenDB(connector)
				defer db.Close()

				var version *hdb.Version
				// Grab connection to detect hdb version.
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				conn.Raw(func(driverConn interface{}) error {
					version = driverConn.(Conn).HDBVersion()
					return nil
				})

				for _, test := range tests {
					if test.typ.IsSupportedHDBVersion(version) && checkDFV(dfv, test.dfv, test.dvfCond) {
						var tester tester
						switch test.testType {
						case dttDefType:
							tester = &dttDef{typ: test.typ, checkFn: test.checkFn, testData: convertTestDataType(test.testData), T: t}
						case dttTXType:
							tester = &dttTX{dttDef: dttDef{typ: test.typ, checkFn: test.checkFn, testData: convertTestDataType(test.testData), T: t}}
						case dttSpatialType:
							tester = &dttSpatial{typ: test.typ, testData: test.testData.([]spatial.Geometry), T: t}
						default:
							t.Fatalf("invalid data type test definition type %d", test.testType)
						}
						t.Run(test.typ.Column(), func(t *testing.T) {
							tester.run(db)
						})
					}
				}
			})
		}(dfv)
	}
}
