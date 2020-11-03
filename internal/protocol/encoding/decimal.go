// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package encoding

import (
	"math/big"
	"math/bits"
)

const _S = bits.UintSize / 8 // word size in bytes
// http://en.wikipedia.org/wiki/Decimal128_floating-point_format
const dec128Bias = 6176
const decSize = 16

var natOne = big.NewInt(1)

/*
two's complement 'in-place' of
- 'integer' with len(b) bytes
- little-endian lsbyte: b[0]; msbyte: b[len(b)-1]

Algorithm:
- loop from lsbit to msbit
- keep all bits until bit pos i is found where bit[i] = 1
  - then invert all bits with bit[j], j > i
*/
func twosComplement(bs []byte) {

	i := 0
	l := len(bs)

	for i < l && bs[i] == 0 {
		i++
	}
	if i == l { // zero value -> done
		return
	}

	// find first '1' position in bs[i]
	b := bs[i]
	p := 0
	m := byte(1)
	for p < 8 && b&m != m {
		p++
		m <<= 1
	}
	p++

	bs[i] = ^bs[i]                // invert byte
	bs[i] = (bs[i] >> p) << p     // delete non invert relevant part
	b = (b << (8 - p)) >> (8 - p) // delete revert relevant part
	bs[i] |= b                    // combine

	i++
	// rest of bytes get inverted
	for i < l {
		bs[i] = ^bs[i]
		i++
	}
}
