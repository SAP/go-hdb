package protocol

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func assertEqualBool(t *testing.T, v any, r bool) {
	cv, err := convertField(tcBoolean, v, nil)
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

func assertEqualInt(t *testing.T, tc typeCode, v any, r int64) { //nolint:unparam
	cv, err := convertField(tc, v, nil)
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
	_, err := convertField(tc, v, nil)

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
	cv, err := convertField(tc, v, nil)
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
	_, err := convertField(tc, v, nil)

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
	cv, err := convertField(tc, v, nil)
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
	cv, err := convertField(tc, v, nil)
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
	cv, err := convertField(tc, v, nil)
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

func TestConverter(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"convertBool", testConvertBool},
		{"convertInteger", testConvertInteger},
		{"convertFloat", testConvertFloat},
		{"convertTime", testConvertTime},
		{"convertString", testConvertString},
		{"convertBytes", testConvertBytes},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
