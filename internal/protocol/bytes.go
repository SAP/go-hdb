// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

func sizeBuffer(b []byte, size int) []byte {
	if b == nil || size > cap(b) {
		return make([]byte, size)
	}
	return b[:size]
}

func resizeBuffer(b1 []byte, size int) []byte {
	if b1 == nil || cap(b1) < size {
		b2 := make([]byte, size)
		copy(b2, b1) // !!! keep content
		return b2
	}
	return b1[:size]
}
