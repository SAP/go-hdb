// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
)

// cesu8 command
type command []byte

func (c command) String() string { return string(c) }
func (c *command) resize(size int) {
	if c == nil || size > cap(*c) {
		*c = make([]byte, size)
	} else {
		*c = (*c)[:size]
	}
}
func (c command) size() int { return cesu8.Size(c) }
func (c *command) decode(dec *encoding.Decoder, ph *partHeader) error {
	c.resize(int(ph.bufferLength))
	var err error
	if *c, err = dec.CESU8Bytes(len(*c)); err != nil {
		return err
	}
	return dec.Error()
}
func (c command) encode(enc *encoding.Encoder) error { enc.CESU8Bytes(c); return nil }
