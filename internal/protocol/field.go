/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package protocol

import (
	"database/sql/driver"
	"sort"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

const noFieldName uint32 = 0xFFFFFFFF

type offsetName struct {
	offset uint32
	name   string
}

type fieldNames []offsetName

func (n fieldNames) search(offset uint32) int {
	// binary search
	return sort.Search(len(n), func(i int) bool { return n[i].offset >= offset })
}

func (n *fieldNames) insert(offset uint32) {
	if offset == noFieldName {
		return
	}
	i := n.search(offset)
	switch {
	case i >= len(*n): // not found -> append
		*n = append(*n, offsetName{offset: offset})
	case (*n)[i].offset == offset: // duplicate
	default: // insert
		*n = append(*n, offsetName{})
		copy((*n)[i+1:], (*n)[i:])
		(*n)[i] = offsetName{offset: offset}
	}
}

func (n fieldNames) name(offset uint32) string {
	i := n.search(offset)
	if i < len(n) {
		return n[i].name
	}
	return ""
}

func (n fieldNames) decode(dec *encoding.Decoder) {
	// TODO sniffer - python client texts are returned differently?
	// - double check offset calc (CESU8 issue?)
	pos := uint32(0)
	for i, on := range n {
		diff := int(on.offset - pos)
		if diff > 0 {
			dec.Skip(diff)
		}
		size := int(dec.Byte())
		b := dec.CESU8Bytes(size)
		n[i].name = string(b)
		pos += uint32(1 + size + diff) // len byte + size + diff
	}
}

// A Field represents whether a db result or a parameter Field.
type Field interface {
	Name() string
	TypeName() string
	TypeLength() (int64, bool)
	TypePrecisionScale() (int64, int64, bool)
	ScanType() DataType
	Nullable() bool
	In() bool
	Out() bool
	Converter() Converter
}

var (
	_ Field = (*resultField)(nil)
	_ Field = (*parameterField)(nil)
)

// TODO cache
func newFieldValues(size int) []driver.Value {
	return make([]driver.Value, size)
}
