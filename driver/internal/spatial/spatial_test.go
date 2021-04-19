// +build !unit

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package spatial

import (
	"testing"
)

func testEncode(t *testing.T) {
	testData := []struct {
		g    STGeometry
		ewkb string
		wkt  string
	}{
		{STPoint{Point: &Point{X: 2.5, Y: 3.0}}, "01010000206712000000000000000004400000000000000840", "POINT(2.5 3)"},
		{STPoint{}, "01040000206712000000000000", "POINT EMPTY"}, // initial multi point
	}

	for i, v := range testData {
		// ekwb
		ewkb, err := EncodeEWKB(v.g, false, 4711)
		if err != nil {
			t.Fatal(err)
		}
		if string(ewkb) != v.ewkb {
			t.Fatalf("ewkb test %d got %s expected %s", i, ewkb, v.ewkb)
		}

		// wkt
		wkt, err := EncodeWKT(v.g)
		if err != nil {
			t.Fatal(err)
		}
		if wkt != v.wkt {
			t.Fatalf("wkt test %d got %s expected %s", i, wkt, v.wkt)
		}
	}
}

func TestSpatial(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"encode", testEncode},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
