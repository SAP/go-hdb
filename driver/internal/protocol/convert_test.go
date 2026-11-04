package protocol

import (
	"bytes"
	"errors"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

func assertEqualBool(t *testing.T, v any, r bool) {
	t.Helper()
	cv, err := convertField(tcBoolean, v, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	rv := reflect.ValueOf(cv)
	switch rv.Kind() {
	case reflect.Bool:
		if rv.Bool() != r {
			t.Fatalf("assert equal bool failed %v - %t expected", cv, r)
		}
	default:
		t.Fatalf("invalid type %[1]T %[1]v", cv)
	}
}

func testConvertBool(t *testing.T) {
	type testCustomBool bool

	// bool data types

	assertEqualBool(t, true, true)
	assertEqualBool(t, false, false)

	assertEqualBool(t, uint(0), false)
	assertEqualBool(t, uint8(0), false)
	assertEqualBool(t, uint16(0), false)
	assertEqualBool(t, uint32(0), false)
	assertEqualBool(t, uint64(0), false)

	assertEqualBool(t, int(0), false)
	assertEqualBool(t, int8(0), false)
	assertEqualBool(t, int16(0), false)
	assertEqualBool(t, int32(0), false)
	assertEqualBool(t, int64(0), false)

	assertEqualBool(t, uint(1), true)
	assertEqualBool(t, uint8(1), true)
	assertEqualBool(t, uint16(1), true)
	assertEqualBool(t, uint32(1), true)
	assertEqualBool(t, uint64(1), true)

	assertEqualBool(t, int(1), true)
	assertEqualBool(t, int8(1), true)
	assertEqualBool(t, int16(1), true)
	assertEqualBool(t, int32(1), true)
	assertEqualBool(t, int64(1), true)

	assertEqualBool(t, float32(0), false)
	assertEqualBool(t, float64(0), false)

	assertEqualBool(t, float32(1), true)
	assertEqualBool(t, float64(1), true)

	// custom integer data type
	assertEqualBool(t, testCustomBool(false), false)
	assertEqualBool(t, testCustomBool(true), true)

	// boolean reference
	b := false
	assertEqualBool(t, &b, false)

	// boolean as string
	assertEqualBool(t, "false", false)
	assertEqualBool(t, "true", true)
}

func assertEqualInt(t *testing.T, tc typeCode, v any, r int64) {
	t.Helper()
	cv, err := convertField(tc, v, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	rv := reflect.ValueOf(cv)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if rv.Int() != r {
			t.Fatalf("assert equal int failed %v - %d expected", cv, r)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if int64(rv.Uint()) != r { //nolint: gosec
			t.Fatalf("assert equal int failed %v - %d expected", cv, r)
		}
	default:
		t.Fatalf("invalid type %[1]T %[1]v", cv)
	}
}

func assertEqualIntOutOfRangeError(t *testing.T, tc typeCode, v any) {
	t.Helper()
	_, err := convertField(tc, v, 0, 0)

	if !errors.Is(err, errIntegerOutOfRange) {
		t.Fatalf("assert equal out of range error failed %s %v", tc, v)
	}
}

func testConvertInteger(t *testing.T) {
	type testCustomInt int

	// integer data types
	assertEqualInt(t, tcTinyint, 42, 42)
	assertEqualInt(t, tcSmallint, 42, 42)
	assertEqualInt(t, tcInteger, 42, 42)
	assertEqualInt(t, tcBigint, 42, 42)

	// custom integer data type
	assertEqualInt(t, tcInteger, testCustomInt(42), 42)

	// integer reference
	i := 42
	assertEqualInt(t, tcBigint, &i, 42)

	// min max values
	assertEqualInt(t, tcBigint, int64(math.MaxInt64), math.MaxInt64)
	assertEqualIntOutOfRangeError(t, tcTinyint, minTinyint-1)
	assertEqualIntOutOfRangeError(t, tcTinyint, maxTinyint+1)
	assertEqualIntOutOfRangeError(t, tcSmallint, minSmallint-1)
	assertEqualIntOutOfRangeError(t, tcSmallint, maxSmallint+1)
	assertEqualIntOutOfRangeError(t, tcInteger, int64(minInteger)-1) // cast to int64 to avoid overflow in 32-bit systems
	assertEqualIntOutOfRangeError(t, tcInteger, int64(maxInteger)+1) // cast to int64 to avoid overflow in 32-bit systems

	// integer as string
	assertEqualInt(t, tcInteger, "42", 42)
}

func assertEqualFloat(t *testing.T, tc typeCode, v any, r float64) {
	t.Helper()
	cv, err := convertField(tc, v, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rv := reflect.ValueOf(cv)
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		if rv.Float() != r {
			t.Fatalf("assert equal float failed %v - %f expected", cv, r)
		}
	default:
		t.Fatalf("invalid type %[1]T %[1]v", cv)
	}
}

func assertEqualFloatOutOfRangeError(t *testing.T, tc typeCode, v any) {
	t.Helper()
	_, err := convertField(tc, v, 0, 0)

	if !errors.Is(err, errFloatOutOfRange) {
		t.Fatalf("assert equal out of range error failed %s %v", tc, v)
	}
}

func testConvertFloat(t *testing.T) {
	type testCustomFloat float32

	realValue := float32(42.42)
	doubleValue := float64(42.42)
	stringDoubleValue := "42.42"

	// float data types
	assertEqualFloat(t, tcReal, realValue, float64(realValue))
	assertEqualFloat(t, tcDouble, doubleValue, doubleValue)

	// custom float data type
	assertEqualFloat(t, tcReal, testCustomFloat(realValue), float64(realValue))

	// float reference
	assertEqualFloat(t, tcReal, &realValue, float64(realValue))

	// min max values
	assertEqualFloatOutOfRangeError(t, tcReal, math.Nextafter(maxReal, maxDouble))
	assertEqualFloatOutOfRangeError(t, tcReal, math.Nextafter(maxReal, maxDouble)*-1)

	// float as string
	assertEqualFloat(t, tcDouble, stringDoubleValue, doubleValue)
}

func assertEqualTime(t *testing.T, tc typeCode, v any, r time.Time) {
	t.Helper()
	cv, err := convertField(tc, v, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !cv.(time.Time).Equal(r) {
		t.Fatalf("assert equal time failed %v - %v expected", cv, r)
	}
}

func testConvertTime(t *testing.T) {
	type testCustomTime time.Time

	timeValue := time.Now()

	// time data type
	assertEqualTime(t, tcTimestamp, timeValue, timeValue)

	// custom time data type
	assertEqualTime(t, tcTimestamp, testCustomTime(timeValue), timeValue)

	// time reference
	assertEqualTime(t, tcTimestamp, &timeValue, timeValue)
}

func assertEqualString(t *testing.T, tc typeCode, v any, r string) {
	t.Helper()
	cv, err := convertField(tc, v, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cv.(string) != r {
		t.Fatalf("assert equal string failed %v - %s expected", cv, r)
	}
}

func testConvertString(t *testing.T) {
	type testCustomString string

	stringValue := "Hello World"

	// string data types
	assertEqualString(t, tcString, stringValue, stringValue)

	// custom string data type
	assertEqualString(t, tcString, testCustomString(stringValue), stringValue)

	// string reference
	assertEqualString(t, tcString, &stringValue, stringValue)
}

func assertEqualBytes(t *testing.T, tc typeCode, v any, r []byte) {
	t.Helper()
	cv, err := convertField(tc, v, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cv.([]byte), r) {
		t.Fatalf("assert equal bytes failed %v - %v expected", cv, r)
	}
}

func testConvertBytes(t *testing.T) {
	type testCustomBytes []byte

	bytesValue := []byte("Hello World")

	// bytes data types
	assertEqualBytes(t, tcString, bytesValue, bytesValue)
	assertEqualBytes(t, tcBinary, bytesValue, bytesValue)

	// custom bytes data type
	assertEqualBytes(t, tcString, testCustomBytes(bytesValue), bytesValue)
	assertEqualBytes(t, tcBinary, testCustomBytes(bytesValue), bytesValue)

	// bytes reference
	assertEqualBytes(t, tcString, &bytesValue, bytesValue)
	assertEqualBytes(t, tcBinary, &bytesValue, bytesValue)
}

// testConvertNil checks that pointer recursion preserves a nil interface as SQL NULL.
func testConvertNil(t *testing.T) {
	var value any
	for _, tc := range []typeCode{
		tcBoolean,   // convertBool
		tcBigint,    // convertInteger
		tcDouble,    // convertFloat
		tcTimestamp, // convertTime
		tcDecimal,   // convertDecimal
		tcFixed8,    // convertFixed
		tcString,    // convertBytes
		tcBlob,      // convertLob
	} {
		cv, err := convertField(tc, &value, 0, 0)
		if err != nil {
			t.Fatalf("%s: %v", tc, err)
		}
		if cv != nil {
			t.Fatalf("%s: got %v, want nil", tc, cv)
		}
	}
}

type valueReceiver struct {
	value byte
}

func (v valueReceiver) Reader() io.Reader {
	return strings.NewReader(strconv.Itoa(int(v.value)))
}

func (v valueReceiver) Decompose([]byte) (byte, bool, []byte, int32) {
	return 0, false, []byte{v.value}, 0
}

type nilAwareReceiver struct{}

func (*nilAwareReceiver) Reader() io.Reader {
	return strings.NewReader("3")
}

func (*nilAwareReceiver) Decompose([]byte) (byte, bool, []byte, int32) {
	return 0, false, []byte{3}, 0
}

// testConvertValueReceiver checks that nil pointers with value-receiver methods
// become SQL NULL, while nil-aware pointer-receiver methods still run. Both test
// types implement Reader and Decompose so the same inputs cover both interfaces.
func testConvertValueReceiver(t *testing.T) {
	inputs := []struct {
		value   any
		want    int64
		wantNil bool
	}{
		{(*valueReceiver)(nil), 0, true},
		{valueReceiver{1}, 1, false},
		{&valueReceiver{2}, 2, false},
		{(*nilAwareReceiver)(nil), 3, false},
	}
	for _, tc := range []typeCode{tcBlob, tcDecimal, tcFixed8} {
		for _, test := range inputs {
			cv, err := convertField(tc, test.value, 9, 2)
			if err != nil {
				t.Fatalf("%s %T: %v", tc, test.value, err)
			}
			if (cv == nil) != test.wantNil {
				t.Fatalf("%s %T: got %v, want nil: %t", tc, test.value, cv, test.wantNil)
			}
			if test.wantNil {
				continue
			}
			switch cv := cv.(type) {
			case *LobInDescr:
				got, err := io.ReadAll(cv.rd)
				if err != nil {
					t.Fatalf("%s %T: %v", tc, test.value, err)
				}
				if string(got) != strconv.FormatInt(test.want, 10) {
					t.Fatalf("%s %T: got %q, want %d", tc, test.value, got, test.want)
				}
			case encoding.Decimal:
				var got big.Rat
				cv.AsRat(&got)
				if got.Cmp(big.NewRat(test.want, 1)) != 0 {
					t.Fatalf("%s %T: got %s, want %d", tc, test.value, &got, test.want)
				}
			default:
				t.Fatalf("%s %T: unexpected result type %T", tc, test.value, cv)
			}
		}
	}
}

type valueReader struct {
	rd io.Reader
}

func (r valueReader) Read(p []byte) (int, error) {
	return r.rd.Read(p)
}

type nilAwareReader struct{}

func (*nilAwareReader) Read([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

// testConvertLobReader checks NULL conversion for nil value receivers and reads
// the first chunk to verify that working readers and nil-aware methods still run.
func testConvertLobReader(t *testing.T) {
	for _, test := range []struct {
		value   io.Reader
		want    string
		wantErr error
		wantNil bool
	}{
		{(*valueReader)(nil), "", nil, true},
		{valueReader{strings.NewReader("value content")}, "value content", nil, false},
		{&valueReader{strings.NewReader("pointer content")}, "pointer content", nil, false},
		{(*nilAwareReader)(nil), "", io.ErrClosedPipe, false},
	} {
		cv, err := convertField(tcBlob, test.value, 0, 0)
		if err != nil {
			t.Fatalf("%T: %v", test.value, err)
		}
		if (cv == nil) != test.wantNil {
			t.Fatalf("%T: got %v, want nil: %t", test.value, cv, test.wantNil)
		}
		if test.wantNil {
			continue
		}
		d := cv.(*LobInDescr)
		err = d.FetchFirst(64, nil)
		if !errors.Is(err, test.wantErr) {
			t.Fatalf("%T: got error %v, want %v", test.value, err, test.wantErr)
		}
		if got := d.buf.String(); got != test.want {
			t.Fatalf("%T: got %q, want %q", test.value, got, test.want)
		}
	}
}

func testConvertDecimalTypedNil(t *testing.T) {
	inputs := []any{
		nil,
		(*int64)(nil),
		(*big.Int)(nil),
		(*big.Rat)(nil),
		(*big.Float)(nil),
	}
	for _, tc := range []typeCode{tcDecimal, tcFixed8, tcFixed12, tcFixed16} {
		for _, in := range inputs {
			cv, err := convertField(tc, in, 0, 0)
			if err != nil {
				t.Fatalf("%s %T: %v", tc, in, err)
			}
			if cv != nil {
				t.Fatalf("%s %T: got %v, want nil", tc, in, cv)
			}
		}
	}
}

func testConvertDecimalBigFloat(t *testing.T) {
	inputs := []struct {
		value   *big.Float
		want    *big.Rat
		wantErr bool
	}{
		{big.NewFloat(1.25), big.NewRat(5, 4), false},
		{new(big.Float).SetInf(false), nil, true},
		{new(big.Float).SetInf(true), nil, true},
	}
	for _, test := range []struct {
		tc          typeCode
		prec, scale int
	}{
		{tcDecimal, 0, 0},
		{tcFixed8, 9, 2},
		{tcFixed12, 28, 2},
		{tcFixed16, 34, 2},
	} {
		for _, in := range inputs {
			cv, err := convertField(test.tc, in.value, test.prec, test.scale)
			if (err != nil) != in.wantErr {
				t.Fatalf("%s %s: got error %v, want error: %t", test.tc, in.value, err, in.wantErr)
			}
			if in.wantErr {
				continue
			}
			var got big.Rat
			cv.(encoding.Decimal).AsRat(&got)
			if got.Cmp(in.want) != 0 {
				t.Fatalf("%s %s: got %s, want %s", test.tc, in.value, &got, in.want)
			}
		}
	}
}

func testConvertBigintFloatBoundary(t *testing.T) {
	type namedFloat64 float64
	upper := math.Ldexp(1, 63)
	for _, test := range []struct {
		name    string
		value   any
		want    int64
		wantErr bool
	}{
		{"float64 upper", upper, 0, true},                                                                  // float64 path: reject the exclusive upper bound, 2^63.
		{"float32 upper", float32(upper), 0, true},                                                         // float32 path: reject the exclusive upper bound, 2^63.
		{"named float64 upper", namedFloat64(upper), 0, true},                                              // Reflection path: reject the exclusive upper bound, 2^63.
		{"float64 below upper", math.Nextafter(upper, 0), math.MaxInt64 - 1023, false},                     // float64 path: accept the nearest value below 2^63.
		{"float32 below upper", math.Nextafter32(float32(upper), 0), math.MaxInt64 - (1 << 39) + 1, false}, // float32 path: accept the nearest value below 2^63.
		{"named float64 below upper", namedFloat64(math.Nextafter(upper, 0)), math.MaxInt64 - 1023, false}, // Reflection path: accept the nearest value below 2^63.
		{"float64 lower", -upper, math.MinInt64, false},                                                    // float64 path: accept the inclusive lower bound, -2^63.
		{"float32 lower", float32(-upper), math.MinInt64, false},                                           // float32 path: accept the inclusive lower bound, -2^63.
		{"named float64 lower", namedFloat64(-upper), math.MinInt64, false},                                // Reflection path: accept the inclusive lower bound, -2^63.
		{"float64 below lower", math.Nextafter(-upper, math.Inf(-1)), 0, true},                             // float64 path: reject the nearest value below -2^63.
		{"float32 below lower", math.Nextafter32(float32(-upper), float32(math.Inf(-1))), 0, true},         // float32 path: reject the nearest value below -2^63.
		{"named float64 below lower", namedFloat64(math.Nextafter(-upper, math.Inf(-1))), 0, true},         // Reflection path: reject the nearest value below -2^63.
		{"float64 fraction", 1.5, 0, true},                                                                 // float64 path: reject fractional input.
		{"float32 fraction", float32(1.5), 0, true},                                                        // float32 path: reject fractional input.
		{"named float64 fraction", namedFloat64(1.5), 0, true},                                             // Reflection path: reject fractional input.
		{"float64 NaN", math.NaN(), 0, true},                                                               // float64 path: reject NaN, which bypasses range comparisons.
		{"float32 NaN", float32(math.NaN()), 0, true},                                                      // float32 path: reject NaN, which bypasses range comparisons.
		{"named float64 NaN", namedFloat64(math.NaN()), 0, true},                                           // Reflection path: reject NaN, which bypasses range comparisons.
	} {
		cv, err := convertField(tcBigint, test.value, 0, 0)
		if (err != nil) != test.wantErr {
			t.Fatalf("%s: got value %v, error %v; want error: %t", test.name, cv, err, test.wantErr)
		}
		if !test.wantErr && cv != test.want {
			t.Fatalf("%s: got %v, want %d", test.name, cv, test.want)
		}
	}
}

func TestConverter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"convertNil", testConvertNil},
		{"convertBool", testConvertBool},
		{"convertInteger", testConvertInteger},
		{"convertBigintFloatBoundary", testConvertBigintFloatBoundary},
		{"convertFloat", testConvertFloat},
		{"convertDecimalTypedNil", testConvertDecimalTypedNil},
		{"convertDecimalBigFloat", testConvertDecimalBigFloat},
		{"convertTime", testConvertTime},
		{"convertString", testConvertString},
		{"convertBytes", testConvertBytes},
		{"convertValueReceiver", testConvertValueReceiver},
		{"convertLobReader", testConvertLobReader},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fn(t)
		})
	}
}
