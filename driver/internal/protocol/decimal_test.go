package protocol

import (
	"math/big"
	"testing"
)

func testDigits10(t *testing.T) {
	natNine := big.NewInt(9)

	// test zero
	digits := digits10(natZero)
	if digits != 1 {
		t.Fatalf("int %s digits %d - expected digits %d", natZero, digits, 1)
	}

	// test entire range
	v1 := new(big.Int).SetInt64(1) // 1, 10, 100, ...
	v2 := new(big.Int).SetInt64(9) // 9, 99, 999, ...

	for i := 1; i <= (dec128MinExp*-1 + dec128Digits); i++ {
		digits := digits10(v1)
		if digits != i {
			t.Fatalf("value %d int %s digits %d - expected digits %d", i, v1, digits, i)
		}
		digits = digits10(v2)
		if digits != i {
			t.Fatalf("value %d int %s digits %d - expected digits %d", i, v2, digits, i)
		}
		v1.Mul(v1, natTen)
		v2.Mul(v2, natTen)
		v2.Add(v2, natNine)
	}
}

func testConvertRatToDecimal(t *testing.T) {

	parseRat := func(s string) *big.Rat {
		if r, ok := new(big.Rat).SetString(s); ok {
			return r
		}
		t.Fatal("invalid big.Rat number")
		return nil
	}
	parseInt := func(s string) *big.Int { return parseRat(s).Num() }

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

		{parseRat("-9.9E6144"), dec128Digits, dec128MinExp, dec128MaxExp, parseInt("-9.9E33"), dec128MaxExp, 0},
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
