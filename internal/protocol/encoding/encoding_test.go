// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package encoding

import (
	"encoding/binary"
	"math"
	"testing"
)

func testTwosComplement(t *testing.T) {
	b := make([]byte, 8) // space for Int64

	// test 0 <= i <= MaxInt32
	for i := 0; i <= math.MaxInt32; i++ {
		binary.LittleEndian.PutUint32(b, uint32(int32(i)))
		twosComplement(b[:4])
		c := int(int32(binary.LittleEndian.Uint32(b)))
		if i != c*-1 {
			t.Fatalf("converted value for %d: %d - expected %d", i, c, i*-1)
		}
	}

	// test MinInt32 <= i <= -1 // use int64 as MinInt32 * -1 would overflow
	for i := math.MinInt32; i <= -1; i++ {
		binary.LittleEndian.PutUint64(b, uint64(int64(i)))
		twosComplement(b[:8])
		c := int(int64(binary.LittleEndian.Uint64(b)))
		if i != c*-1 {
			t.Fatalf("converted value for %d: %d - expected %d", i, c, i*-1)
		}
	}
}

func TestEncoding(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"twosComplement", testTwosComplement},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
