//go:build go1.18
// +build go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"golang.org/x/exp/slices"
)

// Options represents a generic option part.
type Options[K ~int8] map[K]any

func (ops Options[K]) String() string {
	s := []string{}
	for i, typ := range ops {
		s = append(s, fmt.Sprintf("%v: %v", K(i), typ))
	}
	slices.Sort(s)
	return fmt.Sprintf("%v", s)
}

func (ops Options[K]) size() int {
	size := 2 * len(ops) //option + type
	for _, v := range ops {
		ot := getOptType(v)
		size += ot.size(v)
	}
	return size
}

func (ops Options[K]) numArg() int { return len(ops) }

func (ops *Options[K]) decode(dec *encoding.Decoder, ph *PartHeader) error {
	*ops = Options[K]{} // no reuse of maps - create new one
	for i := 0; i < ph.numArg(); i++ {
		k := K(dec.Int8())
		tc := typeCode(dec.Byte())
		ot := tc.optType()
		(*ops)[k] = ot.decode(dec)
	}
	return dec.Error()
}

func (ops Options[K]) encode(enc *encoding.Encoder) error {
	for k, v := range ops {
		enc.Int8(int8(k))
		ot := getOptType(v)
		enc.Int8(int8(ot.typeCode()))
		ot.encode(enc, v)
	}
	return nil
}
