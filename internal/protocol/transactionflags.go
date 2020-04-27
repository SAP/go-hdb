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

type transactionFlags plainOptions

func (f transactionFlags) String() string {
	typedSc := make(map[transactionFlagType]interface{})
	for k, v := range f {
		typedSc[transactionFlagType(k)] = v
	}
	return fmt.Sprintf("flags %s", typedSc)
}

func (f *transactionFlags) decode(dec *encoding.Decoder, ph *partHeader) error {
	*f = transactionFlags{} // no reuse of maps - create new one
	plainOptions(*f).decode(dec, ph.numArg())
	return dec.Error()
}
