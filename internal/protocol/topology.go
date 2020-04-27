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

type topologyInformation multiLineOptions

func (o topologyInformation) String() string {
	mlo := make([]map[topologyOption]interface{}, len(o))
	for i, po := range o {
		typedPo := make(map[topologyOption]interface{})
		for k, v := range po {
			typedPo[topologyOption(k)] = v
		}
		mlo[i] = typedPo
	}
	return fmt.Sprintf("options %s", mlo)
}

func (o *topologyInformation) decode(dec *encoding.Decoder, ph *partHeader) error {
	(*multiLineOptions)(o).decode(dec, ph.numArg())
	return dec.Error()
}
