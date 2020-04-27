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

//rows affected
const (
	raSuccessNoInfo   = -2
	raExecutionFailed = -3
)

//rows affected
type rowsAffected []int32

func (r rowsAffected) String() string {
	return fmt.Sprintf("%v", []int32(r))
}

func (r *rowsAffected) reset(numArg int) {
	if r == nil || numArg > cap(*r) {
		*r = make(rowsAffected, numArg)
	} else {
		*r = (*r)[:numArg]
	}
}

func (r *rowsAffected) decode(dec *encoding.Decoder, ph *partHeader) error {
	r.reset(ph.numArg())

	for i := 0; i < ph.numArg(); i++ {
		(*r)[i] = dec.Int32()
	}
	return dec.Error()
}

func (r rowsAffected) total() int64 {
	if r == nil {
		return 0
	}

	total := int64(0)
	for _, rows := range r {
		if rows > 0 {
			total += int64(rows)
		}
	}
	return total
}
