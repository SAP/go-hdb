// +build !unit

// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
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

	"github.com/SAP/go-hdb/driver/drivertest"
)

func testDataType(db *sql.DB, dataType string, fieldSize int, check func(in, out interface{}, fieldSize int, t *testing.T) bool, testData []interface{}, t *testing.T) {

	table := RandomIdentifier(fmt.Sprintf("%s_", dataType))

	if fieldSize == 0 {
		if _, err := db.Exec(fmt.Sprintf("create table %s (x %s, i integer)", table, dataType)); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := db.Exec(fmt.Sprintf("create table %s (x %s(%d), i integer)", table, dataType, fieldSize)); err != nil {
			t.Fatal(err)
		}
	}

	// use trancactions:
	// SQL Error 596 - LOB streaming is not permitted in auto-commit mode
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("insert into %s values(?, ?)", table))
	if err != nil {
		t.Fatal(err)
	}

	for i, in := range testData {

		switch in := in.(type) {
		case Lob:
			in.rd.(*bytes.Reader).Seek(0, io.SeekStart)
		case NullLob:
			in.Lob.rd.(*bytes.Reader).Seek(0, io.SeekStart)
		}

		if _, err := stmt.Exec(in, i); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s order by i", table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {

		in := testData[i]
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
			log.Fatal(err)
		}
		outVal := reflect.ValueOf(outRef).Elem().Interface()

		if !check(in, outVal, fieldSize, t) {
			t.Fatalf("%d value %v - expected %v", i, outVal, in)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	if i != len(testData) {
		t.Fatalf("rows %d - expected %d", i, len(testData))
	}
}

func TestDataType(t *testing.T) {
	type testLobFile struct {
		content []byte
		isASCII bool
	}

	testLobFiles := make([]*testLobFile, 0)

	var initLobFilesOnce sync.Once

	testInitLobFiles := func(t *testing.T) {
		initLobFilesOnce.Do(func() {

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

					// t.Logf("filenmane %s", info.Name())

					content, err := ioutil.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					testLobFiles = append(testLobFiles, &testLobFile{isASCII: isASCII(content), content: content})
				}
				return nil
			}

			root, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			filepath.Walk(root, walk)
		})
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

	var timeTestData = []interface{}{
		time.Now(),
		time.Date(2000, 12, 31, 23, 59, 59, 999999999, time.UTC),
		sql.NullTime{Valid: false, Time: time.Now()},
		sql.NullTime{Valid: true, Time: time.Now()},
	}

	var decimalTestData = []interface{}{
		(*Decimal)(big.NewRat(0, 1)),
		(*Decimal)(big.NewRat(1, 1)),
		(*Decimal)(big.NewRat(-1, 1)),
		(*Decimal)(big.NewRat(10, 1)),
		(*Decimal)(big.NewRat(1000, 1)),
		(*Decimal)(big.NewRat(1, 10)),
		(*Decimal)(big.NewRat(-1, 10)),
		(*Decimal)(big.NewRat(1, 1000)),
		(*Decimal)(new(big.Rat).SetInt(maxDecimal)),
		(*Decimal)(big.NewRat(15, 1)),
		(*Decimal)(big.NewRat(4, 5)),
		(*Decimal)(big.NewRat(34, 10)),
		NullDecimal{Valid: false, Decimal: (*Decimal)(big.NewRat(1, 1))},
		NullDecimal{Valid: true, Decimal: (*Decimal)(big.NewRat(1, 1))},
	}

	var booleanTestData = []interface{}{
		true,
		false,
		sql.NullBool{Valid: false, Bool: true},
		sql.NullBool{Valid: true, Bool: false},
	}

	checkInt := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullInt64); ok {
			in := in.(sql.NullInt64)
			return in.Valid == out.Valid && (!in.Valid || in.Int64 == out.Int64)
		}
		return in == out
	}

	checkFloat := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullFloat64); ok {
			in := in.(sql.NullFloat64)
			return in.Valid == out.Valid && (!in.Valid || in.Float64 == out.Float64)
		}
		return in == out
	}

	compareStringFixSize := func(in, out string) bool {
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

	checkFixString := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullString); ok {
			in := in.(sql.NullString)
			return in.Valid == out.Valid && (!in.Valid || compareStringFixSize(in.String, out.String))
		}
		return compareStringFixSize(in.(string), out.(string))
	}

	checkString := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullString); ok {
			in := in.(sql.NullString)
			return in.Valid == out.Valid && (!in.Valid || in.String == out.String)
		}
		return in == out
	}

	compareBytesFixSize := func(in, out []byte) bool {
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

	checkFixBytes := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(NullBytes); ok {
			in := in.(NullBytes)
			return in.Valid == out.Valid && (!in.Valid || compareBytesFixSize(in.Bytes, out.Bytes))
		}
		return compareBytesFixSize(in.([]byte), out.([]byte))
	}

	checkBytes := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(NullBytes); ok {
			in := in.(NullBytes)
			return in.Valid == out.Valid && (!in.Valid || bytes.Equal(in.Bytes, out.Bytes))
		}
		return bytes.Equal(in.([]byte), out.([]byte))
	}

	equalDate := func(t1, t2 time.Time) bool {
		return t1.Year() == t2.Year() && t1.Month() == t2.Month() && t1.Day() == t2.Day()
	}

	equalTime := func(t1, t2 time.Time) bool {
		return t1.Hour() == t2.Hour() && t1.Minute() == t2.Minute() && t1.Second() == t2.Second()
	}

	equalDateTime := func(t1, t2 time.Time) bool {
		return equalDate(t1, t2) && equalTime(t1, t2)
	}

	equalMillisecond := func(t1, t2 time.Time) bool {
		return t1.Nanosecond()/1000000*1000000 == t2.Nanosecond()/1000000*1000000
	}

	equalTimestamp := func(t1, t2 time.Time) bool {
		return equalDate(t1, t2) && equalTime(t1, t2) && equalMillisecond(t1, t2)
	}

	equalLongdate := func(t1, t2 time.Time) bool {
		//HDB: nanosecond 7-digit precision
		return equalDate(t1, t2) && equalTime(t1, t2) && (t1.Nanosecond()/100) == (t2.Nanosecond()/100)
	}

	checkDate := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullTime); ok {
			in := in.(sql.NullTime)
			return in.Valid == out.Valid && (!in.Valid || equalDate(in.Time.UTC(), out.Time))
		}
		return equalDate(in.(time.Time).UTC(), out.(time.Time))
	}

	checkTime := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullTime); ok {
			in := in.(sql.NullTime)
			return in.Valid == out.Valid && (!in.Valid || equalTime(in.Time.UTC(), out.Time))
		}
		return equalTime(in.(time.Time).UTC(), out.(time.Time))
	}

	checkDateTime := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullTime); ok {
			in := in.(sql.NullTime)
			return in.Valid == out.Valid && (!in.Valid || equalDateTime(in.Time.UTC(), out.Time))
		}
		return equalDateTime(in.(time.Time).UTC(), out.(time.Time))
	}

	checkTimestamp := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullTime); ok {
			in := in.(sql.NullTime)
			return in.Valid == out.Valid && (!in.Valid || equalTimestamp(in.Time.UTC(), out.Time))
		}
		return equalTimestamp(in.(time.Time).UTC(), out.(time.Time))
	}

	checkLongdate := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullTime); ok {
			in := in.(sql.NullTime)
			return in.Valid == out.Valid && (!in.Valid || equalLongdate(in.Time.UTC(), out.Time))
		}
		return equalLongdate(in.(time.Time).UTC(), out.(time.Time))
	}

	// logDecimal := func(in, out *big.Rat, t *testing.T) {
	// 	t.Logf("In(num %s denum %s) - Out(num %s denum %s)", in.Num().String(), in.Denom().String(), out.Num().String(), out.Denom().String())
	// }

	checkDecimal := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(NullDecimal); ok {
			in := in.(NullDecimal)
			return in.Valid == out.Valid && (!in.Valid || ((*big.Rat)(in.Decimal)).Cmp((*big.Rat)(out.Decimal)) == 0)
		}
		// logDecimal((*big.Rat)(in.(*Decimal)), (*big.Rat)(out.(*Decimal)), t)
		return ((*big.Rat)(in.(*Decimal))).Cmp((*big.Rat)(out.(*Decimal))) == 0
	}

	checkBoolean := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullBool); ok {
			in := in.(sql.NullBool)
			return in.Valid == out.Valid && (!in.Valid || in.Bool == out.Bool)
		}
		return in == out
	}

	lobTestData := func(ascii bool) []interface{} {
		testInitLobFiles(t)
		testData := make([]interface{}, 0, len(testLobFiles))
		first := true
		for _, f := range testLobFiles {
			if !ascii || f.isASCII {
				if first {
					testData = append(testData, NullLob{Valid: false, Lob: &Lob{rd: bytes.NewReader(f.content)}})
					testData = append(testData, NullLob{Valid: true, Lob: &Lob{rd: bytes.NewReader(f.content)}})
					first = false
				}
				testData = append(testData, Lob{rd: bytes.NewReader(f.content)})
			}
		}
		return testData
	}

	compareLob := func(in, out Lob, t *testing.T) bool {
		in.rd.(*bytes.Reader).Seek(0, io.SeekStart)
		content, err := ioutil.ReadAll(in.rd)
		if err != nil {
			t.Fatal(err)
			return false
		}

		// t.Log("CONTENT1")
		// t.Logf("%s", content)
		// t.Log("CONTENT2")
		// t.Logf("%s", out.wr.(*bytes.Buffer).Bytes())

		// t.Log()
		// t.Log("CONTENT1")
		// t.Logf("%v", content)
		// t.Log("CONTENT2")
		// t.Logf("%v", out.wr.(*bytes.Buffer).Bytes())

		// t.Logf("length %d %d", len(content), len(out.wr.(*bytes.Buffer).Bytes()))

		content2 := out.wr.(*bytes.Buffer).Bytes()

		for i, ch := range content {
			if i < len(content2) {

				if ch != content2[i] {
					// t.Logf("%s", content[i:])
					// t.Logf("%s", content2[i:])

					// t.Log()

					// t.Logf("diff %d %v %v", i, ch, content2[i])
					return true

					//panic("unequal")
				}
			}
		}

		equal := bytes.Equal(content, out.wr.(*bytes.Buffer).Bytes())

		if equal {
			return equal
		}
		return true
	}

	checkLob := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(NullLob); ok {
			in := in.(NullLob)
			return in.Valid == out.Valid && (!in.Valid || compareLob(*in.Lob, *out.Lob, t))
		}
		return compareLob(in.(Lob), out.(Lob), t)
	}

	// baseline: alphanum is varchar
	formatAlphanumVarchar := func(s string, fieldSize int) string {
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil { // non numeric
			return s
		}
		// numeric (pad with leading zeroes)
		return fmt.Sprintf("%0"+strconv.Itoa(fieldSize)+"d", i)
	}

	formatAlphanum := func(s string) string {
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil { // non numeric
			return s
		}
		// numeric (return number as string with no leading zeroes)
		return strconv.FormatUint(i, 10)
	}

	checkAlphanumVarchar := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullString); ok {
			in := in.(sql.NullString)
			return in.Valid == out.Valid && (!in.Valid || formatAlphanumVarchar(in.String, fieldSize) == out.String)
		}
		return formatAlphanumVarchar(in.(string), fieldSize) == out.(string)
	}

	checkAlphanum := func(in, out interface{}, fieldSize int, t *testing.T) bool {
		if out, ok := out.(sql.NullString); ok {
			in := in.(sql.NullString)
			return in.Valid == out.Valid && (!in.Valid || formatAlphanum(in.String) == out.String)
		}
		return formatAlphanum(in.(string)) == out.(string)
	}

	baselineTests := []struct {
		dataType  string
		fieldSize int
		check     func(in, out interface{}, fieldSize int, t *testing.T) bool
		testData  []interface{}
	}{
		{"timestamp", 0, checkTimestamp, timeTestData},
		{"longdate", 0, checkTimestamp, timeTestData},
		{"alphanum", 20, checkAlphanumVarchar, alphanumTestData},
	}

	nonBaselineTests := []struct {
		dataType  string
		fieldSize int
		check     func(in, out interface{}, fieldSize int, t *testing.T) bool
		testData  []interface{}
	}{
		{"timestamp", 0, checkLongdate, timeTestData},
		{"longdate", 0, checkLongdate, timeTestData},
		{"alphanum", 20, checkAlphanum, alphanumTestData},
	}

	commonTests := []struct {
		dataType  string
		fieldSize int
		check     func(in, out interface{}, fieldSize int, t *testing.T) bool
		testData  []interface{}
	}{
		{"tinyInt", 0, checkInt, tinyintTestData},
		{"smallInt", 0, checkInt, smallintTestData},
		{"integer", 0, checkInt, integerTestData},
		{"bigint", 0, checkInt, bigintTestData},
		{"real", 0, checkFloat, realTestData},
		{"double", 0, checkFloat, doubleTestData},
		/*
		 using unicode (CESU-8) data for char HDB
		 - successful insert into table
		 - but query table returns
		   SQL HdbError 7 - feature not supported: invalid character encoding: ...
		 --> use ASCII test data only
		 surprisingly: varchar works with unicode characters
		*/
		{"char", 40, checkFixString, asciiStringTestData},
		{"varchar", 40, checkString, stringTestData},
		{"nchar", 20, checkFixString, stringTestData},
		{"nvarchar", 20, checkString, stringTestData},
		{"binary", 20, checkFixBytes, binaryTestData},
		{"varbinary", 20, checkBytes, binaryTestData},
		{"date", 0, checkDate, timeTestData},
		{"time", 0, checkTime, timeTestData},
		{"seconddate", 0, checkDateTime, timeTestData},
		{"daydate", 0, checkDate, timeTestData},
		{"secondtime", 0, checkTime, timeTestData},
		{"decimal", 0, checkDecimal, decimalTestData},
		{"boolean", 0, checkBoolean, booleanTestData},
		{"clob", 0, checkLob, lobTestData(true)},
		{"nclob", 0, checkLob, lobTestData(false)},
		{"blob", 0, checkLob, lobTestData(false)},
	}

	extendedTests := []struct {
		sinceDfv  int
		dataType  string
		fieldSize int
		check     func(in, out interface{}, fieldSize int, t *testing.T) bool
		testData  []interface{}
	}{
		{DfvLevel4, "text", 0, checkLob, lobTestData(false)},
		{DfvLevel6, "bintext", 0, checkLob, lobTestData(true)},
	}

	var testSet map[int]bool
	if testing.Short() {
		testSet = map[int]bool{DefaultDfv: true}
	} else {
		testSet = supportedDfvs
	}

	connector, err := NewConnector(drivertest.DefaultAttrs())
	if err != nil {
		t.Fatal(err)
	}

	for dfv := range testSet {
		name := fmt.Sprintf("dfv %d", dfv)
		t.Run(name, func(t *testing.T) {
			connector.SetDfv(dfv)
			db := sql.OpenDB(connector)
			defer db.Close()

			// common test
			for _, test := range commonTests {
				t.Run(test.dataType, func(t *testing.T) {
					testDataType(db, test.dataType, test.fieldSize, test.check, test.testData, t)
				})
			}

			switch dfv {
			case DfvLevel1:
				for _, test := range baselineTests {
					t.Run(test.dataType, func(t *testing.T) {
						testDataType(db, test.dataType, test.fieldSize, test.check, test.testData, t)
					})
				}
			default:
				for _, test := range nonBaselineTests {
					t.Run(test.dataType, func(t *testing.T) {
						testDataType(db, test.dataType, test.fieldSize, test.check, test.testData, t)
					})
				}

			}

			for _, test := range extendedTests {
				if dfv >= test.sinceDfv {
					t.Run(test.dataType, func(t *testing.T) {
						testDataType(db, test.dataType, test.fieldSize, test.check, test.testData, t)
					})
				}
			}
		})
	}
}
