/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package protocol

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"
)

func assertEqualInt(t *testing.T, tc typeCode, v interface{}, r int64) {
	cv, err := tc.fieldType().Convert(v)
	if err != nil {
		t.Fatal(err)
	}
	if cv.(int64) != r {
		t.Fatalf("assert equal int failed %v - %d expected", cv, r)
	}
}

func assertEqualIntOutOfRangeError(t *testing.T, tc typeCode, v interface{}) {
	_, err := tc.fieldType().Convert(v)

	if !errors.Is(err, ErrIntegerOutOfRange) {
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
	assertEqualIntOutOfRangeError(t, tcInteger, minInteger-1)
	assertEqualIntOutOfRangeError(t, tcInteger, maxInteger+1)

	// integer as string
	assertEqualInt(t, tcInteger, "42", 42)
}

func assertEqualFloat(t *testing.T, tc typeCode, v interface{}, r float64) {
	cv, err := tc.fieldType().Convert(v)
	if err != nil {
		t.Fatal(err)
	}
	if cv.(float64) != r {
		t.Fatalf("assert equal float failed %v - %f expected", cv, r)
	}
}

func assertEqualFloatOutOfRangeError(t *testing.T, tc typeCode, v interface{}) {
	_, err := tc.fieldType().Convert(v)

	if !errors.Is(err, ErrFloatOutOfRange) {
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

func assertEqualTime(t *testing.T, v interface{}, r time.Time) {
	cv, err := tcTimestamp.fieldType().Convert(v)
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
	assertEqualTime(t, timeValue, timeValue)

	// custom time data type
	assertEqualTime(t, testCustomTime(timeValue), timeValue)

	// time reference
	assertEqualTime(t, &timeValue, timeValue)

}

func assertEqualString(t *testing.T, tc typeCode, v interface{}, r string) {
	cv, err := tc.fieldType().Convert(v)
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

func assertEqualBytes(t *testing.T, tc typeCode, v interface{}, r []byte) {
	cv, err := tc.fieldType().Convert(v)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(cv.([]byte), r) != 0 {
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
