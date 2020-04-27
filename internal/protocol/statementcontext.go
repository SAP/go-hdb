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

type statementContext plainOptions

func (c statementContext) String() string {
	typedSc := make(map[statementContextType]interface{})
	for k, v := range c {
		typedSc[statementContextType(k)] = v
	}
	return fmt.Sprintf("options %s", typedSc)
}

func (c *statementContext) decode(dec *encoding.Decoder, ph *partHeader) error {
	*c = statementContext{} // no reuse of maps - create new one
	plainOptions(*c).decode(dec, ph.numArg())
	return dec.Error()
}
