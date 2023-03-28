package protocol

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func assertEqualInt(t *testing.T, ftc *FieldTypeCtx, tc typeCode, v any, r int64) {
	cv, err := ftc.fieldType(tc, 0, 0).(fieldConverter).convert(v)
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
		if int64(rv.Uint()) != r {
			t.Fatalf("assert equal int failed %v - %d expected", cv, r)
		}
	default:
		t.Fatalf("invalid type %[1]T %[1]v", cv)
	}
}

func assertEqualIntOutOfRangeError(t *testing.T, ftc *FieldTypeCtx, tc typeCode, v any) {
	_, err := ftc.fieldType(tc, 0, 0).(fieldConverter).convert(v)

	if !errors.Is(err, ErrIntegerOutOfRange) {
		t.Fatalf("assert equal out of range error failed %s %v", tc, v)
	}
}

func testConvertInteger(t *testing.T, ftc *FieldTypeCtx) {
	type testCustomInt int

	// integer data types
	assertEqualInt(t, ftc, tcTinyint, 42, 42)
	assertEqualInt(t, ftc, tcSmallint, 42, 42)
	assertEqualInt(t, ftc, tcInteger, 42, 42)
	assertEqualInt(t, ftc, tcBigint, 42, 42)

	// custom integer data type
	assertEqualInt(t, ftc, tcInteger, testCustomInt(42), 42)

	// integer reference
	i := 42
	assertEqualInt(t, ftc, tcBigint, &i, 42)

	// min max values
	assertEqualIntOutOfRangeError(t, ftc, tcTinyint, minTinyint-1)
	assertEqualIntOutOfRangeError(t, ftc, tcTinyint, maxTinyint+1)
	assertEqualIntOutOfRangeError(t, ftc, tcSmallint, minSmallint-1)
	assertEqualIntOutOfRangeError(t, ftc, tcSmallint, maxSmallint+1)
	assertEqualIntOutOfRangeError(t, ftc, tcInteger, int64(minInteger)-1) // cast to int64 to avoid overflow in 32-bit systems
	assertEqualIntOutOfRangeError(t, ftc, tcInteger, int64(maxInteger)+1) // cast to int64 to avoid overflow in 32-bit systems

	// integer as string
	assertEqualInt(t, ftc, tcInteger, "42", 42)
}

func assertEqualFloat(t *testing.T, ftc *FieldTypeCtx, tc typeCode, v any, r float64) {
	cv, err := ftc.fieldType(tc, 0, 0).(fieldConverter).convert(v)
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

func assertEqualFloatOutOfRangeError(t *testing.T, ftc *FieldTypeCtx, tc typeCode, v any) {
	_, err := ftc.fieldType(tc, 0, 0).(fieldConverter).convert(v)

	if !errors.Is(err, ErrFloatOutOfRange) {
		t.Fatalf("assert equal out of range error failed %s %v", tc, v)
	}
}

func testConvertFloat(t *testing.T, ftc *FieldTypeCtx) {
	type testCustomFloat float32

	realValue := float32(42.42)
	doubleValue := float64(42.42)
	stringDoubleValue := "42.42"

	// float data types
	assertEqualFloat(t, ftc, tcReal, realValue, float64(realValue))
	assertEqualFloat(t, ftc, tcDouble, doubleValue, doubleValue)

	// custom float data type
	assertEqualFloat(t, ftc, tcReal, testCustomFloat(realValue), float64(realValue))

	// float reference
	assertEqualFloat(t, ftc, tcReal, &realValue, float64(realValue))

	// min max values
	assertEqualFloatOutOfRangeError(t, ftc, tcReal, math.Nextafter(maxReal, maxDouble))
	assertEqualFloatOutOfRangeError(t, ftc, tcReal, math.Nextafter(maxReal, maxDouble)*-1)

	// float as string
	assertEqualFloat(t, ftc, tcDouble, stringDoubleValue, doubleValue)
}

func assertEqualTime(t *testing.T, ftc *FieldTypeCtx, tc typeCode, v any, r time.Time) {
	cv, err := ftc.fieldType(tc, 0, 0).(fieldConverter).convert(v)
	if err != nil {
		t.Fatal(err)
	}
	if !cv.(time.Time).Equal(r) {
		t.Fatalf("assert equal time failed %v - %v expected", cv, r)
	}
}

func testConvertTime(t *testing.T, ftc *FieldTypeCtx) {
	type testCustomTime time.Time

	timeValue := time.Now()

	// time data type
	assertEqualTime(t, ftc, tcTimestamp, timeValue, timeValue)

	// custom time data type
	assertEqualTime(t, ftc, tcTimestamp, testCustomTime(timeValue), timeValue)

	// time reference
	assertEqualTime(t, ftc, tcTimestamp, &timeValue, timeValue)
}

func assertEqualString(t *testing.T, ftc *FieldTypeCtx, tc typeCode, v any, r string) {
	cv, err := ftc.fieldType(tc, 0, 0).(fieldConverter).convert(v)
	if err != nil {
		t.Fatal(err)
	}
	if cv.(string) != r {
		t.Fatalf("assert equal string failed %v - %s expected", cv, r)
	}
}

func testConvertString(t *testing.T, ftc *FieldTypeCtx) {
	type testCustomString string

	stringValue := "Hello World"

	// string data types
	assertEqualString(t, ftc, tcString, stringValue, stringValue)

	// custom string data type
	assertEqualString(t, ftc, tcString, testCustomString(stringValue), stringValue)

	// string reference
	assertEqualString(t, ftc, tcString, &stringValue, stringValue)
}

func assertEqualBytes(t *testing.T, ftc *FieldTypeCtx, tc typeCode, v any, r []byte) {
	cv, err := ftc.fieldType(tc, 0, 0).(fieldConverter).convert(v)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cv.([]byte), r) {
		t.Fatalf("assert equal bytes failed %v - %v expected", cv, r)
	}
}

func testConvertBytes(t *testing.T, ftc *FieldTypeCtx) {
	type testCustomBytes []byte

	bytesValue := []byte("Hello World")

	// bytes data types
	assertEqualBytes(t, ftc, tcString, bytesValue, bytesValue)
	assertEqualBytes(t, ftc, tcBinary, bytesValue, bytesValue)

	// custom bytes data type
	assertEqualBytes(t, ftc, tcString, testCustomBytes(bytesValue), bytesValue)
	assertEqualBytes(t, ftc, tcBinary, testCustomBytes(bytesValue), bytesValue)

	// bytes reference
	assertEqualBytes(t, ftc, tcString, &bytesValue, bytesValue)
	assertEqualBytes(t, ftc, tcBinary, &bytesValue, bytesValue)

}

func TestConverter(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T, ftc *FieldTypeCtx)
	}{
		{"convertInteger", testConvertInteger},
		{"convertFloat", testConvertFloat},
		{"convertTime", testConvertTime},
		{"convertString", testConvertString},
		{"convertBytes", testConvertBytes},
	}

	ftc := NewFieldTypeCtx(defaultDfv, false)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t, ftc)
		})
	}
}
