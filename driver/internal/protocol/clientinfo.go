// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

type clientInfo keyValues

func (c clientInfo) String() string { return fmt.Sprintf("client info %s", keyValues(c)) }

func (c clientInfo) size() int   { return keyValues(c).size() }
func (c clientInfo) numArg() int { return len(c) }

func (c *clientInfo) decode(dec *encoding.Decoder, ph *partHeader) error {
	*c = clientInfo{} // no reuse of maps - create new one
	if err := keyValues(*c).decode(dec, ph.numArg()); err != nil {
		return err
	}
	return dec.Error()
}

func (c clientInfo) encode(enc *encoding.Encoder) error {
	return keyValues(c).encode(enc)
}
