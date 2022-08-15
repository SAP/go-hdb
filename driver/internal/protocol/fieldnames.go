// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"sort"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

const noFieldName uint32 = 0xFFFFFFFF

type offsetName struct {
	offset uint32
	name   string
}

type fieldNames []offsetName

func (fn fieldNames) search(offset uint32) int {
	// binary search
	return sort.Search(len(fn), func(i int) bool { return fn[i].offset >= offset })
}

func (fn *fieldNames) insert(offset uint32) {
	if offset == noFieldName {
		return
	}
	i := fn.search(offset)
	switch {
	case i >= len(*fn): // not found -> append
		*fn = append(*fn, offsetName{offset: offset})
	case (*fn)[i].offset == offset: // duplicate
	default: // insert
		*fn = append(*fn, offsetName{})
		copy((*fn)[i+1:], (*fn)[i:])
		(*fn)[i] = offsetName{offset: offset}
	}
}

func (fn fieldNames) name(offset uint32) string {
	i := fn.search(offset)
	if i < len(fn) {
		return fn[i].name
	}
	return ""
}

func (fn fieldNames) decode(dec *encoding.Decoder) (err error) {
	// TODO sniffer - python client texts are returned differently?
	// - double check offset calc (CESU8 issue?)
	pos := uint32(0)
	for i, on := range fn {
		diff := int(on.offset - pos)
		if diff > 0 {
			dec.Skip(diff)
		}
		var n int
		var s string
		n, s, err = dec.CESU8LIString()
		fn[i].name = s
		pos += uint32(n + diff) // len byte + size + diff
	}
	return err
}
