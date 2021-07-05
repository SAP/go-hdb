// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

type dbConnectInfo plainOptions

func (ci dbConnectInfo) String() string {
	m := make(map[dbConnectInfoType]interface{})
	for k, v := range ci {
		m[dbConnectInfoType(k)] = v
	}
	return fmt.Sprintf("connect info %s", m)
}

func (ci dbConnectInfo) size() int   { return plainOptions(ci).size() }
func (ci dbConnectInfo) numArg() int { return len(ci) }

func (ci *dbConnectInfo) decode(dec *encoding.Decoder, ph *partHeader) error {
	if ci == nil {
		*ci = dbConnectInfo{}
	}
	plainOptions(*ci).decode(dec, ph.numArg())
	return dec.Error()
}

func (ci dbConnectInfo) encode(enc *encoding.Encoder) error {
	plainOptions(ci).encode(enc)
	return nil
}
