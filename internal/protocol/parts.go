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
	"fmt"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

// part kind methods
func (*inPrmEnc) kind() partKind { return pkParameters }

// check if part types implement partWriter interface
var (
	_ partWriter = (*inPrmEnc)(nil)
)

func (s *Session) newInPrmEnc(pr *PrepareResult, args []driver.NamedValue) *inPrmEnc {

	e := &inPrmEnc{}

	numField := len(pr.prmFields)
	for _, f := range pr.prmFields {
		if f.In() {
			e._fields = append(e._fields, f)
		}
	}
	e._size = 0
	for i, arg := range args {
		f := pr.prmFields[i%numField]
		if f.In() {
			e._args = append(e._args, arg)
			e._size += prmSize(f.tc, arg)
		}
	}
	e._size += len(e._args)
	if len(e._fields) != 0 {
		e._numArg = len(e._args) / len(e._fields)
	}
	return e
}

// encoder input parameters
type inPrmEnc struct {
	_fields        []*parameterField
	_args          []driver.NamedValue
	_size, _numArg int
}

func (e *inPrmEnc) String() string {
	return fmt.Sprintf("fields %s len(args) %d args %v", e._fields, len(e._args), e._args)
}

func (e *inPrmEnc) size() int   { return e._size }
func (e *inPrmEnc) numArg() int { return e._numArg }

func (e *inPrmEnc) encode(enc *encoding.Encoder) error {
	numField := len(e._fields)

	for i, arg := range e._args {
		f := e._fields[i%numField]
		if err := encodePrm(enc, f.tc, arg); err != nil {
			return err
		}
	}
	return nil
}
