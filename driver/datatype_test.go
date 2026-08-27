//go:build !unit

package driver

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"iter"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/SAP/go-hdb/driver/internal/coltest"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/internal/rand/alphanum"
)

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

type dttDef struct {
	_columnType coltest.Type
	testData    []any        // static test data
	testDataFn  func() []any // test data built fresh per run (lobs: stateful readers)
	tx          bool         // insert within a transaction (LOB streaming: "596 - not permitted in auto-commit mode")
}

func (dtt *dttDef) columnType() coltest.Type { return dtt._columnType }

// data returns the test data, materialising it fresh via testDataFn when set (lobs), else the
// static testData slice.
func (dtt *dttDef) data() []any {
	if dtt.testDataFn != nil {
		return dtt.testDataFn()
	}
	return dtt.testData
}

// preparer is the common subset of *sql.DB and *sql.Tx used to prepare the insert statement.
type preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

func (dtt *dttDef) insert(t *testing.T, p preparer, tableName Identifier, testData []any) int {
	stmt, err := p.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values(?, ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// bulk insert: yield one (value, index) row per test value in a single exec.
	rows := func(yield func([]any) bool) {
		for i, in := range testData {
			if !yield([]any{in, i}) {
				return
			}
		}
	}
	if _, err := stmt.ExecContext(t.Context(), iter.Seq[[]any](rows)); err != nil {
		t.Fatalf("type: %s - %s", dtt._columnType.TypeName(), err)
	}
	return len(testData)
}

func (dtt *dttDef) insertTx(t *testing.T, db *sql.DB, tableName Identifier, testData []any) int {
	// use a transaction: SQL Error 596 - LOB streaming is not permitted in auto-commit mode.
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	numRecs := dtt.insert(t, tx, tableName, testData)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return numRecs
}

// compare compares an (already null-unwrapped) input payload against the scanned output for the
// column type under test. The comparison is determined by the column type; each arm reaches for
// dfv or the column instance only where it needs them.
func (dtt *dttDef) compare(dfv int, in, out any) (bool, error) {
	switch dtt._columnType.TypeName() {
	case coltest.DbtnTinyint, coltest.DbtnSmallint, coltest.DbtnInteger, coltest.DbtnBigint,
		coltest.DbtnReal, coltest.DbtnDouble, coltest.DbtnBoolean, coltest.DbtnVarchar, coltest.DbtnNVarchar:
		return in == out, nil
	case coltest.DbtnChar, coltest.DbtnNChar:
		return compareStringFixSize(in.(string), out.(string)), nil
	case coltest.DbtnBinary:
		return compareBytesFixSize(in.([]byte), out.([]byte)), nil
	case coltest.DbtnVarbinary:
		return bytes.Equal(in.([]byte), out.([]byte)), nil
	case coltest.DbtnDecimal:
		rat := func(v any) *big.Rat {
			if d, ok := v.(*Decimal); ok {
				return (*big.Rat)(d)
			}
			d := v.(Decimal)
			return (*big.Rat)(&d)
		}
		return rat(in).Cmp(rat(out)) == 0, nil
	case coltest.DbtnDate, coltest.DbtnDaydate:
		return equalDate(in.(time.Time).UTC(), out.(time.Time)), nil
	case coltest.DbtnTime, coltest.DbtnSecondtime:
		return equalTime(in.(time.Time).UTC(), out.(time.Time)), nil
	case coltest.DbtnSeconddate:
		return equalDateTime(in.(time.Time).UTC(), out.(time.Time)), nil
	case coltest.DbtnTimestamp, coltest.DbtnLongdate:
		if dfv == p.DfvLevel1 {
			return equalTimestamp(in.(time.Time).UTC(), out.(time.Time)), nil
		}
		return equalLongdate(in.(time.Time).UTC(), out.(time.Time)), nil
	case coltest.DbtnAlphanum:
		if dfv == p.DfvLevel1 {
			length, ok := dtt._columnType.Length()
			if !ok {
				return false, fmt.Errorf("cannot detect fieldlength of %v", dtt._columnType)
			}
			return formatAlphanumVarchar(in.(string), int(length)) == out.(string), nil
		}
		return formatAlphanum(in.(string)) == out.(string), nil
	case coltest.DbtnClob, coltest.DbtnNClob, coltest.DbtnBlob:
		inLob, outLob := in.(Lob), out.(Lob)
		// expected content is the reader's retained slice - no rewind of the consumed reader.
		return bytes.Equal(inLob.rd.(*lobReader).data, outLob.wr.(*bytes.Buffer).Bytes()), nil
	case coltest.DbtnText, coltest.DbtnBintext:
		// text/bintext: content not compared - hdb may modify it (e.g. eliminate spaces).
		return true, nil
	default:
		return false, fmt.Errorf("no comparator for type %s", dtt._columnType.TypeName())
	}
}

// nullField reports whether v is one of the Go null wrappers used in the test data and, if so,
// returns its Valid flag and (dereferenced) payload. Each arm checks Valid first so an invalid
// wrapper never yields (or dereferences) a payload. A non-wrapper value returns isNull=false and
// is compared directly by checkValue.
func nullField(v any) (valid bool, value any, isNull bool) {
	switch v := v.(type) {
	case sql.NullBool:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.Bool, true
	case sql.NullInt64:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.Int64, true
	case sql.NullFloat64:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.Float64, true
	case sql.NullString:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.String, true
	case sql.NullTime:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.Time, true
	case NullBytes:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.Bytes, true
	case NullDecimal:
		if !v.Valid {
			return false, nil, true
		}
		return true, *v.Decimal, true
	case NullLob:
		if !v.Valid {
			return false, nil, true
		}
		return true, *v.Lob, true
	case sql.Null[int]:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.V, true
	case sql.Null[bool]:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.V, true
	case sql.Null[float64]:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.V, true
	case sql.Null[string]:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.V, true
	case sql.Null[time.Time]:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.V, true
	case sql.Null[[]byte]:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.V, true
	case sql.Null[Decimal]:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.V, true
	case sql.Null[Lob]:
		if !v.Valid {
			return false, nil, true
		}
		return true, v.V, true
	case sql.Null[*int]:
		if !v.Valid {
			return false, nil, true
		}
		return true, *v.V, true
	case sql.Null[*[]byte]:
		if !v.Valid {
			return false, nil, true
		}
		return true, *v.V, true
	case sql.Null[*Decimal]:
		if !v.Valid {
			return false, nil, true
		}
		return true, *v.V, true
	case sql.Null[*Lob]:
		if !v.Valid {
			return false, nil, true
		}
		return true, *v.V, true
	default: // not a null wrapper
		return false, nil, false
	}
}

// checkValue compares in against out, handling Go null wrappers generically: for null types the
// Valid flags must match and only if valid are the (unwrapped) payloads compared. Plain values
// go straight to compare. This keeps the null handling out of the type-specific comparison.
func (dtt *dttDef) checkValue(dfv int, in, out any) (bool, error) {
	inValid, inValue, isNull := nullField(in)
	if !isNull {
		return dtt.compare(dfv, in, out)
	}
	outValid, outValue, _ := nullField(out)
	if inValid != outValid {
		return false, nil
	}
	if !inValid {
		return true, nil // both null - skip payload comparison
	}
	return dtt.compare(dfv, inValue, outValue)
}

func (dtt *dttDef) check(t *testing.T, db *sql.DB, tableName Identifier, dtv int, testData []any, numRecs int) {
	rows, err := db.QueryContext(t.Context(), fmt.Sprintf("select * from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		in := testData[i]
		outRef := reflect.New(reflect.TypeOf(in)).Interface()

		// a NullLob scan target needs its inner *Lob allocated (NullLob.Scan writes through it).
		if outRef, ok := outRef.(*NullLob); ok {
			outRef.Lob = new(Lob)
		}

		var id int
		if err := rows.Scan(outRef, &id); err != nil {
			t.Fatal(err)
		}
		if id != i { // rows are inserted with column i = index and queried "order by i"
			t.Fatalf("row order mismatch: got id %d at position %d", id, i)
		}
		outVal := reflect.ValueOf(outRef).Elem().Interface()

		ok, err := dtt.checkValue(dtv, in, outVal)
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

func (dtt *dttDef) run(t *testing.T, db *sql.DB, dfv int) {
	dataType := dtt._columnType.DataType()
	tableName := RandomIdentifier(dataType + "_")
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (x %s, i integer)", tableName, dataType)); err != nil {
		t.Fatal(err)
	}

	// materialise once: insert and check share the same values (and, for lobs, reader instances).
	testData := dtt.data()

	var numRecs int
	if dtt.tx {
		numRecs = dtt.insertTx(t, db, tableName, testData)
	} else {
		numRecs = dtt.insert(t, db, tableName, testData)
	}
	dtt.check(t, db, tableName, dfv, testData, numRecs)
}

// lobReader is an io.Reader over a byte slice that retains the full slice, so the expected
// content can be recovered for comparison without rewinding a consumed reader.
type lobReader struct {
	data []byte
	pos  int
}

func newLobReader(data []byte) *lobReader { return &lobReader{data: data} }

func (r *lobReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestDataType(t *testing.T) {
	t.Parallel()

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

	type (
		testUint8 uint8
		testInt8  int8
	)

	tinyintTestData := []any{
		testUint8(minTinyint),
		testUint8(maxTinyint),
		testInt8(minTinyint),
		uint8(minTinyint),
		uint8(maxTinyint),
		sql.NullInt64{Valid: false, Int64: minTinyint},
		sql.NullInt64{Valid: true, Int64: maxTinyint},
	}

	type (
		testUint16 uint16
		testInt16  int16
	)

	smallintTestData := []any{
		testUint16(maxSmallint),
		testInt16(minSmallint),
		testInt16(maxSmallint),
		int16(minSmallint),
		int16(maxSmallint),
		sql.NullInt64{Valid: false, Int64: minSmallint},
		sql.NullInt64{Valid: true, Int64: maxSmallint},
	}

	type (
		testUint32 uint32
		testInt32  int32
	)

	minIntegerVal := int(minInteger)
	maxIntegerVal := int(maxInteger)

	integerTestData := []any{
		testUint32(maxInteger),
		testInt32(minInteger),
		testInt32(maxInteger),
		int32(minInteger),
		int32(maxInteger),
		sql.NullInt64{Valid: false, Int64: minInteger},
		sql.NullInt64{Valid: true, Int64: maxInteger},
		sql.Null[int]{Valid: false, V: minInteger},
		sql.Null[int]{Valid: true, V: maxInteger},
		sql.Null[*int]{Valid: false, V: &minIntegerVal},
		sql.Null[*int]{Valid: true, V: &maxIntegerVal},
	}

	type (
		testUint64 uint64
		testInt64  int64
	)

	bigintTestData := []any{
		testUint64(maxBigint),
		testInt64(minBigint),
		testInt64(maxBigint),
		int64(minBigint),
		int64(maxBigint),
		sql.NullInt64{Valid: false, Int64: minBigint},
		sql.NullInt64{Valid: true, Int64: maxBigint},
	}

	type testFloat32 float32

	realTestData := []any{
		testFloat32(-maxReal),
		testFloat32(maxReal),
		float32(-maxReal),
		float32(maxReal),
		sql.NullFloat64{Valid: false, Float64: -maxReal},
		sql.NullFloat64{Valid: true, Float64: maxReal},
	}

	type testFloat64 float64

	doubleTestData := []any{
		testFloat64(-maxDouble),
		testFloat64(maxDouble),
		float64(-maxDouble),
		float64(maxDouble),
		sql.NullFloat64{Valid: false, Float64: -maxDouble},
		sql.NullFloat64{Valid: true, Float64: maxDouble},
		sql.Null[float64]{Valid: false, V: -maxDouble},
		sql.Null[float64]{Valid: true, V: maxDouble},
	}

	timeTestData := []any{
		time.Now(),
		time.Date(2000, 12, 31, 23, 59, 59, 999999999, time.UTC),
		sql.NullTime{Valid: false, Time: time.Now()},
		sql.NullTime{Valid: true, Time: time.Now()},
		sql.Null[time.Time]{Valid: false, V: time.Now()},
		sql.Null[time.Time]{Valid: true, V: time.Now()},
	}

	natOne := big.NewRat(1, 1)
	natTen := big.NewInt(10)
	natHundret := big.NewRat(100, 1)

	exp10 := func(n int) *big.Int {
		r := big.NewInt(int64(n))
		return r.Exp(natTen, r, nil)
	}

	maxValue := func(prec int) *big.Rat {
		r := new(big.Rat).SetInt(exp10(prec))
		r.Sub(r, natOne)
		r.Quo(r, natHundret)
		return r
	}

	minValue := func(prec int) *big.Rat {
		v := maxValue(prec)
		return v.Neg(v)
	}

	fixed8MinValue := (*Decimal)(minValue(18))  // min value Dec(18,2)
	fixed8MaxValue := (*Decimal)(maxValue(18))  // max value Dec(18,2)
	fixed12MinValue := (*Decimal)(minValue(28)) // min value Dec(18,2)
	fixed12MaxValue := (*Decimal)(maxValue(28)) // max value Dec(18,2)
	fixed16MinValue := (*Decimal)(minValue(38)) // min value Dec(18,2)
	fixed16MaxValue := (*Decimal)(maxValue(38)) // max value Dec(18,2)

	// decimalValue is the reusable 1/1 payload; &decimalValue serves the *Decimal forms.
	var decimalValue = Decimal(*big.NewRat(1, 1))

	decimalTestData := []any{
		(*Decimal)(big.NewRat(0, 1)),
		&decimalValue,
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

		NullDecimal{Valid: false, Decimal: &decimalValue},
		NullDecimal{Valid: true, Decimal: &decimalValue},
		sql.Null[Decimal]{Valid: false, V: decimalValue},
		sql.Null[Decimal]{Valid: true, V: decimalValue},
		sql.Null[*Decimal]{Valid: false, V: &decimalValue},
		sql.Null[*Decimal]{Valid: true, V: &decimalValue},
	}
	// fixed12/fixed16 extend the base set with additional min/max values. Each concatenation
	// starts from a fresh slice so the base data's backing array is never mutated/aliased.
	decimalFixed12TestData := slices.Concat(decimalTestData, []any{fixed12MinValue, fixed12MaxValue})
	decimalFixed16TestData := slices.Concat(decimalFixed12TestData, []any{fixed16MinValue, fixed16MaxValue})

	booleanTestData := []any{
		true,
		false,
		sql.NullBool{Valid: false, Bool: true},
		sql.NullBool{Valid: true, Bool: false},
		sql.Null[bool]{Valid: false, V: true},
		sql.Null[bool]{Valid: true, V: false},
	}

	// stringValue is the reusable test payload shared by the string and bytes columns.
	stringValue := "Hello HDB"

	asciiStringTestData := []any{
		stringValue,
		"aaaaaaaaaa",
		sql.NullString{Valid: false, String: stringValue},
		sql.NullString{Valid: true, String: stringValue},
		sql.Null[string]{Valid: false, V: stringValue},
		sql.Null[string]{Valid: true, V: stringValue},
	}

	stringTestData := []any{
		stringValue,
		// varchar: UTF-8 4 bytes per char -> size 40 bytes
		// nvarchar: CESU-8 6 bytes per char -> hdb counts 2 chars per 6 byte encoding -> size 20 bytes
		"𝄞𝄞𝄞𝄞𝄞𝄞𝄞𝄞𝄞𝄞",
		"𝄞𝄞aa",
		"€€",
		"𝄞𝄞€€",
		"𝄞𝄞𝄞€€",
		"aaaaaaaaaa",
		sql.NullString{Valid: false, String: stringValue},
		sql.NullString{Valid: true, String: stringValue},
		sql.Null[string]{Valid: false, V: stringValue},
		sql.Null[string]{Valid: true, V: stringValue},
	}

	// bytesValue is stringValue as bytes, the reusable payload for the binary/varbinary columns.
	bytesValue := []byte(stringValue)

	binaryTestData := []any{
		bytesValue,
		[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
		[]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0xff},
		NullBytes{Valid: false, Bytes: bytesValue},
		NullBytes{Valid: true, Bytes: bytesValue},
		sql.Null[[]byte]{Valid: false, V: bytesValue},
		sql.Null[[]byte]{Valid: true, V: bytesValue},
		sql.Null[*[]byte]{Valid: false, V: &bytesValue},
		sql.Null[*[]byte]{Valid: true, V: &bytesValue},
	}

	alphanumTestData := []any{
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

	unicodeData := func() []byte {
		b := make([]byte, 0, utf8.MaxRune*4) // on average approx. 4 bytes per rune - avoid resizing by append.
		for r := rune(0); r <= utf8.MaxRune; r++ {
			if r != utf8.RuneError && utf8.ValidRune(r) {
				b = utf8.AppendRune(b, r)
			}
		}
		return b
	}()

	asciiData := func() []byte {
		b := make([]byte, utf8.RuneSelf)
		for r := range utf8.RuneSelf {
			b[r] = byte(r)
		}
		return b
	}()

	randAlphanumData := func() []byte {
		b := make([]byte, 1e6) // random Lob size 1MB
		if _, err := alphanum.Read(b); err != nil {
			panic(err) // should never happen
		}
		return b
	}()

	lobASCIITestData := func() []any {
		return []any{
			NullLob{Valid: false, Lob: &Lob{rd: newLobReader(asciiData)}},
			NullLob{Valid: true, Lob: &Lob{rd: newLobReader(asciiData)}},
			Lob{rd: newLobReader(asciiData)},
			Lob{rd: newLobReader(randAlphanumData)},
			sql.Null[Lob]{Valid: false, V: Lob{rd: newLobReader(asciiData)}},
			sql.Null[Lob]{Valid: true, V: Lob{rd: newLobReader(asciiData)}},
			sql.Null[*Lob]{Valid: false, V: &Lob{rd: newLobReader(asciiData)}},
			sql.Null[*Lob]{Valid: true, V: &Lob{rd: newLobReader(asciiData)}},
		}
	}

	lobTestData := func() []any {
		return []any{
			NullLob{Valid: false, Lob: &Lob{rd: newLobReader(asciiData)}},
			NullLob{Valid: true, Lob: &Lob{rd: newLobReader(asciiData)}},
			Lob{rd: newLobReader(unicodeData)},
			Lob{rd: newLobReader(asciiData)},
			Lob{rd: newLobReader(randAlphanumData)},
			sql.Null[Lob]{Valid: false, V: Lob{rd: newLobReader(asciiData)}},
			sql.Null[Lob]{Valid: true, V: Lob{rd: newLobReader(asciiData)}},
			sql.Null[*Lob]{Valid: false, V: &Lob{rd: newLobReader(asciiData)}},
			sql.Null[*Lob]{Valid: true, V: &Lob{rd: newLobReader(asciiData)}},
		}
	}

	type tester interface {
		columnType() coltest.Type
		run(t *testing.T, db *sql.DB, dfv int)
	}

	tests := []tester{
		&dttDef{_columnType: coltest.NullTinyint, testData: tinyintTestData},
		&dttDef{_columnType: coltest.NullSmallint, testData: smallintTestData},
		&dttDef{_columnType: coltest.NullInteger, testData: integerTestData},
		&dttDef{_columnType: coltest.NullBigint, testData: bigintTestData},
		&dttDef{_columnType: coltest.NullReal, testData: realTestData},
		&dttDef{_columnType: coltest.NullDouble, testData: doubleTestData},

		&dttDef{_columnType: coltest.NullDate, testData: timeTestData},
		&dttDef{_columnType: coltest.NullTime, testData: timeTestData},
		&dttDef{_columnType: coltest.NullSeconddate, testData: timeTestData},
		&dttDef{_columnType: coltest.NullDaydate, testData: timeTestData},
		&dttDef{_columnType: coltest.NullSecondtime, testData: timeTestData},
		&dttDef{_columnType: coltest.NullTimestamp, testData: timeTestData},
		&dttDef{_columnType: coltest.NullLongdate, testData: timeTestData},

		&dttDef{_columnType: coltest.NullBoolean, testData: booleanTestData},

		/*
		 using unicode (CESU-8) data for char HDB
		 - successful insert into table
		 - but query table returns
		   SQL HdbError 7 - feature not supported: invalid character encoding: ...
		 --> use ASCII test data only
		 surprisingly: varchar works with unicode characters
		*/
		&dttDef{_columnType: coltest.NewNullChar(40), testData: asciiStringTestData},
		&dttDef{_columnType: coltest.NewNullVarchar(40), testData: stringTestData},
		&dttDef{_columnType: coltest.NewNullNChar(20), testData: stringTestData},
		&dttDef{_columnType: coltest.NewNullNVarchar(20), testData: stringTestData},
		&dttDef{_columnType: coltest.NewNullAlphanum(20), testData: alphanumTestData},
		&dttDef{_columnType: coltest.NewNullBinary(20), testData: binaryTestData},
		&dttDef{_columnType: coltest.NewNullVarbinary(20), testData: binaryTestData},

		&dttDef{_columnType: coltest.NewNullDecimal(0, 0), testData: decimalTestData}, // floating point decimal number

		&dttDef{_columnType: coltest.NewNullDecimal(18, 2), testData: decimalTestData},        // precision, scale decimal number -fixed8
		&dttDef{_columnType: coltest.NewNullDecimal(28, 2), testData: decimalFixed12TestData}, // precision, scale decimal number -fixed12
		&dttDef{_columnType: coltest.NewNullDecimal(38, 2), testData: decimalFixed16TestData}, // precision, scale decimal number -fixed16

		// LOBs: transaction required (LOB streaming not permitted in auto-commit). Test data
		// is built fresh per run (testDataFn): the bytes.Readers are consumed on insert.
		&dttDef{_columnType: coltest.NullClob, testDataFn: lobASCIITestData, tx: true},
		&dttDef{_columnType: coltest.NullNClob, testDataFn: lobTestData, tx: true},
		&dttDef{_columnType: coltest.NullBlob, testDataFn: lobTestData, tx: true},
		&dttDef{_columnType: coltest.NullText, testDataFn: lobTestData, tx: true},
		&dttDef{_columnType: coltest.NullBintext, testDataFn: lobASCIITestData, tx: true},
	}

	version := MT.Version().Major()

	for _, dfv := range p.SupportedDfvs(testing.Short()) {
		name := fmt.Sprintf("dfv %d", dfv)
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			connector := MT.NewConnector()
			connector.SetDfv(dfv)
			db := sql.OpenDB(connector)
			db.SetMaxIdleConns(25) // let's keep some more connections in the pool
			t.Cleanup(func() { db.Close() })

			for i, test := range tests {
				if test.columnType().IsSupported(version, dfv) {
					t.Run(fmt.Sprintf("%s %d", test.columnType().DataType(), i), func(t *testing.T) {
						t.Parallel()
						test.run(t, db, dfv)
					})
				}
			}
		})
	}
}
