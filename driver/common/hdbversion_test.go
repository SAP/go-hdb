// SPDX-FileCopyrightText: 2014-2021 Stefan Miller
//
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"testing"
)

func testHDBVersionNumberParse(t *testing.T) {
	var tests = []struct {
		s string
		v hdbVersionNumber
	}{
		{"2.00.048.00", hdbVersionNumber{2, 0, 48, 0, 0}},
		{"2.00.045.00.15756393121", hdbVersionNumber{2, 0, 45, 0, 15756393121}},
	}

	for i, test := range tests {
		v := parseHDBVersionNumber(test.s)
		if v.String() != test.s {
			t.Fatalf("line: %d got: %s expected: %s", i, v, test.s)
		}
	}
}

func testHDBVersionNumberCompare(t *testing.T) {
	var tests = []struct {
		s1, s2 string
		r      int
	}{
		{"2.00.045.00.15756393121", "2.00.048.00", -1},
		{"2.00.045.00.15756393121", "2.00.045.00.15756393122", 0}, // should be equal as the buildID is not tested
	}

	for i, test := range tests {
		v1 := parseHDBVersionNumber(test.s1)
		v2 := parseHDBVersionNumber(test.s2)
		if v1.compare(v2) != test.r {
			t.Fatalf("line: %d expected: compare(%s,%s) = %d", i, v1, v2, test.r)
		}
	}
}

func testHDBVersionFeature(t *testing.T) {
	for f, cv1 := range hdbFeatureAvailability {
		for _, cv2 := range hdbFeatureAvailability {
			v1 := ParseHDBVersion(cv1.String())
			v2 := ParseHDBVersion(cv2.String())

			hasFeature := v2.Compare(v1) >= 0

			if v2.HasFeature(f) != hasFeature {
				t.Fatalf("Version %s has feature %d - got %t - expected %t", v2, f, v2.HasFeature(f), hasFeature)
			}
		}
	}
}

func TestHDBVersion(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"parse", testHDBVersionNumberParse},
		{"compare", testHDBVersionNumberCompare},
		{"feature", testHDBVersionFeature},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
