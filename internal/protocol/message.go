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
	"fmt"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

const (
	messageHeaderSize = 32 //nolint:varcheck
)

//message header
type messageHeader struct {
	sessionID     int64
	packetCount   int32
	varPartLength uint32
	varPartSize   uint32
	noOfSegm      int16
}

func (h *messageHeader) String() string {
	return fmt.Sprintf("session id %d packetCount %d varPartLength %d, varPartSize %d noOfSegm %d",
		h.sessionID,
		h.packetCount,
		h.varPartLength,
		h.varPartSize,
		h.noOfSegm)
}

func (h *messageHeader) encode(enc *encoding.Encoder) error {
	enc.Int64(h.sessionID)
	enc.Int32(h.packetCount)
	enc.Uint32(h.varPartLength)
	enc.Uint32(h.varPartSize)
	enc.Int16(h.noOfSegm)
	enc.Zeroes(10) //messageHeaderSize
	return nil
}

func (h *messageHeader) decode(dec *encoding.Decoder) error {
	h.sessionID = dec.Int64()
	h.packetCount = dec.Int32()
	h.varPartLength = dec.Uint32()
	h.varPartSize = dec.Uint32()
	h.noOfSegm = dec.Int16()
	dec.Skip(10) //messageHeaderSize
	return dec.Error()
}
