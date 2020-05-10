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

// client distribution mode
//nolint
const (
	cdmOff                 optIntType = 0
	cdmConnection          optIntType = 1
	cdmStatement           optIntType = 2
	cdmConnectionStatement optIntType = 3
)

// distribution protocol version
//nolint
const (
	dpvBaseline                       = 0
	dpvClientHandlesStatementSequence = 1
)

type connectOptions plainOptions

func (o connectOptions) String() string {
	m := make(map[connectOption]interface{})
	for k, v := range o {
		m[connectOption(k)] = v
	}
	return fmt.Sprintf("options %s", m)
}

func (o connectOptions) size() int   { return plainOptions(o).size() }
func (o connectOptions) numArg() int { return len(o) }

func (o connectOptions) set(k connectOption, v interface{}) {
	o[k] = v
}

//linter:unused
func (o connectOptions) get(k connectOption) (interface{}, bool) {
	v, ok := o[k]
	return v, ok
}

func (o *connectOptions) decode(dec *encoding.Decoder, ph *partHeader) error {
	*o = connectOptions{} // no reuse of maps - create new one
	plainOptions(*o).decode(dec, ph.numArg())
	return dec.Error()
}

func (o connectOptions) encode(enc *encoding.Encoder) error {
	plainOptions(o).encode(enc)
	return nil
}
