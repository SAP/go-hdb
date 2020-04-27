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
	"github.com/SAP/go-hdb/internal/protocol/encoding"
	"github.com/SAP/go-hdb/internal/unicode/cesu8"
)

// cesu8 command
type command []byte

func (c command) String() string { return string(c) }
func (c command) size() int      { return cesu8.Size(c) }
func (c *command) decode(dec *encoding.Decoder, ph *partHeader) error {
	*c = sizeBuffer(*c, int(ph.bufferLength))
	*c = dec.CESU8Bytes(len(*c))
	return dec.Error()
}
func (c command) encode(enc *encoding.Encoder) error { enc.CESU8Bytes(c); return nil }
