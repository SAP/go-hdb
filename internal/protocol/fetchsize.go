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

//fetch size
type fetchsize int32

func (s fetchsize) String() string { return fmt.Sprintf("fetchsize %d", s) }
func (s *fetchsize) decode(dec *encoding.Decoder, ph *partHeader) error {
	*s = fetchsize(dec.Int32())
	return dec.Error()
}
func (s fetchsize) encode(enc *encoding.Encoder) error { enc.Int32(int32(s)); return nil }
