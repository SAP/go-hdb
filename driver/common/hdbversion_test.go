// SPDX-FileCopyrightText: 2014-2020 Stefan Miller
//
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"testing"
)

func testParseHDBVersion(t *testing.T) {
	var tests = []struct {
		s string
		v HDBVersion
	}{
		{"2.00.048.00", HDBVersion{2, 0, 48, 0, 0}},
		{"2.00.045.00.15756393121", HDBVersion{2, 0, 45, 0, 15756393121}},
	}

	for i, test := range tests {
		v := ParseHDBVersion(test.s)
		if v.String() != test.s {
			t.Fatalf("line: %d got: %s expected: %s", i, v, test.s)
		}
	}
}

func testCompareHDBVersion(t *testing.T) {
	var tests = []struct {
		s1, s2 string
		r      int
	}{
		{"2.00.045.00.15756393121", "2.00.048.00", -1},
		{"2.00.045.00.15756393121", "2.00.045.00.15756393122", 0}, // should be equal as the buildID is not tested
	}

	for i, test := range tests {
		v1 := ParseHDBVersion(test.s1)
		v2 := ParseHDBVersion(test.s2)
		if v1.Compare(v2) != test.r {
			t.Fatalf("line: %d expected: compare(%s,%s) = %d", i, v1, v2, test.r)
		}
	}
}

func TestHDBVersion(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"parse", testParseHDBVersion},
		{"compare", testCompareHDBVersion},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
