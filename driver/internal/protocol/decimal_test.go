// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"math/big"
	"testing"
)

func testDigits10(t *testing.T) {
	testData := []struct {
		x      *big.Int
		digits int
	}{
		{new(big.Int).SetInt64(0), 1},
		{new(big.Int).SetInt64(1), 1},
		{new(big.Int).SetInt64(9), 1},
		{new(big.Int).SetInt64(10), 2},
		{new(big.Int).SetInt64(99), 2},
		{new(big.Int).SetInt64(100), 3},
		{new(big.Int).SetInt64(999), 3},
		{new(big.Int).SetInt64(1000), 4},
		{new(big.Int).SetInt64(9999), 4},
		{new(big.Int).SetInt64(10000), 5},
		{new(big.Int).SetInt64(99999), 5},
		{new(big.Int).SetInt64(100000), 6},
		{new(big.Int).SetInt64(999999), 6},
		{new(big.Int).SetInt64(1000000), 7},
		{new(big.Int).SetInt64(9999999), 7},
		{new(big.Int).SetInt64(10000000), 8},
		{new(big.Int).SetInt64(99999999), 8},
		{new(big.Int).SetInt64(100000000), 9},
		{new(big.Int).SetInt64(999999999), 9},
		{new(big.Int).SetInt64(1000000000), 10},
		{new(big.Int).SetInt64(9999999999), 10},
	}

	for i, d := range testData {
		digits := digits10(d.x)
		if d.digits != digits {
			t.Fatalf("value %d int %s digits %d - expected digits %d", i, d.x, digits, d.digits)
		}
	}
}

func testConvertRatToDecimal(t *testing.T) {
	testData := []struct {
		// in
		x      *big.Rat
		digits int
		minExp int
		maxExp int
		// out
		cmp *big.Int
		exp int
		df  byte
	}{
		{new(big.Rat).SetFrac64(0, 1), 3, -2, 2, new(big.Int).SetInt64(0), 0, 0},                              // convert 0
		{new(big.Rat).SetFrac64(1, 1), 3, -2, 2, new(big.Int).SetInt64(1), 0, 0},                              // convert 1
		{new(big.Rat).SetFrac64(1, 10), 3, -2, 2, new(big.Int).SetInt64(1), -1, 0},                            // convert 1/10
		{new(big.Rat).SetFrac64(1, 99), 3, -2, 2, new(big.Int).SetInt64(1), -2, dfNotExact},                   // convert 1/99
		{new(big.Rat).SetFrac64(1, 100), 3, -2, 2, new(big.Int).SetInt64(1), -2, 0},                           // convert 1/100
		{new(big.Rat).SetFrac64(1, 1000), 3, -2, 2, new(big.Int).SetInt64(1), -3, dfUnderflow},                // convert 1/1000
		{new(big.Rat).SetFrac64(10, 1), 3, -2, 2, new(big.Int).SetInt64(1), 1, 0},                             // convert 10
		{new(big.Rat).SetFrac64(100, 1), 3, -2, 2, new(big.Int).SetInt64(1), 2, 0},                            // convert 100
		{new(big.Rat).SetFrac64(1000, 1), 3, -2, 2, new(big.Int).SetInt64(10), 2, 0},                          // convert 1000
		{new(big.Rat).SetFrac64(10000, 1), 3, -2, 2, new(big.Int).SetInt64(100), 2, 0},                        // convert 10000
		{new(big.Rat).SetFrac64(100000, 1), 3, -2, 2, new(big.Int).SetInt64(100), 3, dfOverflow},              // convert 100000
		{new(big.Rat).SetFrac64(999999, 1), 3, -2, 2, new(big.Int).SetInt64(100), 4, dfNotExact | dfOverflow}, // convert 999999
		{new(big.Rat).SetFrac64(99999, 1), 3, -2, 2, new(big.Int).SetInt64(100), 3, dfNotExact | dfOverflow},  // convert 99999
		{new(big.Rat).SetFrac64(9999, 1), 3, -2, 2, new(big.Int).SetInt64(100), 2, dfNotExact},                // convert 9999
		{new(big.Rat).SetFrac64(99950, 1), 3, -2, 2, new(big.Int).SetInt64(100), 3, dfNotExact | dfOverflow},  // convert 99950
		{new(big.Rat).SetFrac64(99949, 1), 3, -2, 2, new(big.Int).SetInt64(999), 2, dfNotExact},               // convert 99949

		{new(big.Rat).SetFrac64(1, 3), 5, -5, 5, new(big.Int).SetInt64(33333), -5, dfNotExact}, // convert 1/3
		{new(big.Rat).SetFrac64(2, 3), 5, -5, 5, new(big.Int).SetInt64(66667), -5, dfNotExact}, // convert 2/3
		{new(big.Rat).SetFrac64(11, 2), 5, -5, 5, new(big.Int).SetInt64(55), -1, 0},            // convert 11/2

		{new(big.Rat).SetFrac64(3, 2), 1, 0, 1, new(big.Int).SetInt64(2), 0, dfNotExact},         // round 1.5 to 2
		{new(big.Rat).SetFrac64(14999, 10000), 1, 0, 1, new(big.Int).SetInt64(1), 0, dfNotExact}, // round 1.4999 to 1

	}

	m := new(big.Int)

	for i := 0; i < 1; i++ { // use for performance tests
		for j, d := range testData {
			exp, df := convertRatToDecimal(d.x, m, d.digits, d.minExp, d.maxExp)
			if m.Cmp(d.cmp) != 0 || exp != d.exp || df != d.df {
				t.Fatalf("converted %d value m %s exp %d df %b - expected m %s exp %d df %b", j, m, exp, df, d.cmp, d.exp, d.df)
			}
		}
	}
}

func testConvertRatToFixed(t *testing.T) {
	testData := []struct {
		// in
		x     *big.Rat
		prec  int
		scale int
		// out
		cmp *big.Int
		df  byte
	}{
		{new(big.Rat).SetFrac64(0, 1), 1, 0, new(big.Int).SetInt64(0), 0},    // convert 0
		{new(big.Rat).SetFrac64(1, 1), 1, 0, new(big.Int).SetInt64(1), 0},    // convert 1
		{new(big.Rat).SetFrac64(-1, 1), 1, 0, new(big.Int).SetInt64(-1), 0},  // convert -1
		{new(big.Rat).SetFrac64(1, -10), 2, 1, new(big.Int).SetInt64(-1), 0}, // convert -1/10

		{new(big.Rat).SetFrac64(1, 2), 1, 0, new(big.Int).SetInt64(1), dfNotExact},        // convert 1/2 - should round to 1
		{new(big.Rat).SetFrac64(4999, 10000), 1, 0, new(big.Int).SetInt64(0), dfNotExact}, // convert 0,4999 - should round to 0

		{new(big.Rat).SetFrac64(1000, 1), 3, 0, new(big.Int).SetInt64(1000), dfOverflow}, // convert 1000 - prec 3 - should overflow
		{new(big.Rat).SetFrac64(10, 1), 3, 2, new(big.Int).SetInt64(1000), dfOverflow},   // convert 10 - prec 3, scale 2 - should overflow
	}

	m := new(big.Int)

	for i := 0; i < 1; i++ { // use for performance tests
		for j, d := range testData {
			df := convertRatToFixed(d.x, m, d.prec, d.scale)
			if m.Cmp(d.cmp) != 0 || df != d.df {
				t.Fatalf("converted %d value m %s df %b - expected m %s df %b (prec %d scale %d)", j, m, df, d.cmp, d.df, d.prec, d.scale)
			}
		}
	}
}

func TestDecimal(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"digits10", testDigits10},
		{"convertRatToDecimal", testConvertRatToDecimal},
		{"convertRatToFixed", testConvertRatToFixed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
