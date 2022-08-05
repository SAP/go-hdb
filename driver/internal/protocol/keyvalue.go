// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

type keyValues map[string]string

func (kv keyValues) size() int {
	size := 0
	for k, v := range kv {
		size += cesu8Type.prmSize(k)
		size += cesu8Type.prmSize(v)
	}
	return size
}

func (kv keyValues) decode(dec *encoding.Decoder, cnt int) error {
	for i := 0; i < cnt; i++ {
		k, err := cesu8Type.decodeRes(dec)
		if err != nil {
			return err
		}
		v, err := cesu8Type.decodeRes(dec)
		if err != nil {
			return err
		}
		kv[string(k.([]byte))] = string(v.([]byte)) // set key value
	}
	return nil
}

func (kv keyValues) encode(enc *encoding.Encoder) error {
	for k, v := range kv {
		if err := cesu8Type.encodePrm(enc, k); err != nil {
			return err
		}
		if err := cesu8Type.encodePrm(enc, v); err != nil {
			return err
		}
	}
	return nil
}