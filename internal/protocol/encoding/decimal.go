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
