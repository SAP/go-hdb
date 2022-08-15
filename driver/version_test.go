// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"testing"
)

func testVersionNumberParse(t *testing.T) {
	var tests = []struct {
		s string
		v versionNumber
	}{
		{"2.00.048.00", versionNumber{2, 0, 48, 0, 0}},
		{"2.00.045.00.15756393121", versionNumber{2, 0, 45, 0, 15756393121}},
	}

	for i, test := range tests {
		v := parseVersion(test.s)
		if v.String() != test.s {
			t.Fatalf("line: %d got: %s expected: %s", i, v, test.s)
		}
	}
}

func testVersionNumberCompare(t *testing.T) {
	var tests = []struct {
		s1, s2 string
		r      int
	}{
		{"2.00.045.00.15756393121", "2.00.048.00", -1},
		{"2.00.045.00.15756393121", "2.00.045.00.15756393122", 0}, // should be equal as the buildID is not tested
	}

	for i, test := range tests {
		v1 := parseVersionNumber(test.s1)
		v2 := parseVersionNumber(test.s2)
		if v1.compare(v2) != test.r {
			t.Fatalf("line: %d expected: compare(%s,%s) = %d", i, v1, v2, test.r)
		}
	}
}

func testVersionFeature(t *testing.T) {
	for f, cv1 := range hdbFeatureAvailability {
		for _, cv2 := range hdbFeatureAvailability {
			v1 := parseVersion(cv1.String())
			v2 := parseVersion(cv2.String())

			hasFeature := v2.compare(v1) >= 0

			if v2.hasFeature(f) != hasFeature {
				t.Fatalf("Version %s has feature %d - got %t - expected %t", v2, f, v2.hasFeature(f), hasFeature)
			}
		}
	}
}

func TestVersion(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"parse", testVersionNumberParse},
		{"compare", testVersionNumberCompare},
		{"feature", testVersionFeature},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
