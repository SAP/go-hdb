// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package encoding

import (
	"math/bits"
)

const _S = bits.UintSize / 8 // word size in bytes
// http://en.wikipedia.org/wiki/Decimal128_floating-point_format
const dec128Bias = 6176
const decSize = 16

/*
two's complement 'in-place' of
- 'integer' with len(b) bytes
- little-endian lsbyte: b[0]; msbyte: b[len(b)-1]
- 'integer' != 0 (no 2s coplement of zero value)

Algorithm:
- loop from lsbit to msbit
- keep all bits until bit pos i is found where bit[i] = 1
  - then invert all bits with bit[j], j > i
*/
func twosComplement(b []byte) {

	i := 0
	l := len(b)

	for i < l && b[i] == 0 {
		i++
	}
	// i < l

	// byte magic - do it

	i++
	// rest of bytes get inverted
	for i < l {
		b[i] ^= b[i]
		i++
	}
}
