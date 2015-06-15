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

package driver

import (
	"fmt"
	"testing"

	p "github.com/SAP/go-hdb/internal/protocol"
)

type testCustomInt int

func assertEqualErr(t *testing.T, e1, e2 error) {
	if e1 != e2 {
		t.Fatalf("assert equal error failed: %v - %v expected", e1, e2)
	}
}

func assertEqualIntOutOfRangeError(t *testing.T, e error) {
	if e == nil {
		t.Fatal("assert equal error failed: nil")
	}
	if _, ok := e.(*intOutOfRangeError); !ok {
		t.Fatalf("assert equal error failed %s (%T)", e, e)
	}
}

func assertEqualInt(t *testing.T, i1, i2 int64) {
	if i1 != i2 {
		t.Fatalf("assert equal int failed %d - %d expected", i1, i2)
	}
}

func convertInt(t *testing.T, dt p.DataType, v interface{}) (int64, error) {
	c := columnConverter(dt)
	cv, err := c.ConvertValue(v)
	if err != nil {
		return 0, err
	}
	i, ok := cv.(int64)
	if ok {
		return i, nil
	}
	return 0, fmt.Errorf("invalid type %T - %T expected", cv, i)
}

func TestConvertInt(t *testing.T) {
	//tinyint

	i, err := convertInt(t, p.DtTinyint, 42)
	assertEqualErr(t, err, nil)
	assertEqualInt(t, i, 42)

	i, err = convertInt(t, p.DtTinyint, minTinyint-1)
	assertEqualIntOutOfRangeError(t, err)

	i, err = convertInt(t, p.DtTinyint, maxTinyint+1)
	assertEqualIntOutOfRangeError(t, err)

	//...

	// int reference
	v := 42

	i, err = convertInt(t, p.DtTinyint, &v)
	assertEqualErr(t, err, nil)
	assertEqualInt(t, i, 42)

	// custom int type
	cv := testCustomInt(42)

	i, err = convertInt(t, p.DtTinyint, cv)
	assertEqualErr(t, err, nil)
	assertEqualInt(t, i, 42)

}
