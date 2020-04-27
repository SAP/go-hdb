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

package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"

	p "github.com/SAP/go-hdb/internal/protocol"
)

func convertNamedValue(pr *p.PrepareResult, nv *driver.NamedValue) error {
	idx := nv.Ordinal - 1

	f := pr.PrmField(idx)

	converter := f.Converter()

	v, out := normNamedValue(nv)

	if out != f.Out() {
		return fmt.Errorf("parameter descr / value mismatch - descr out %t value out %t", f.Out(), out)
	}

	if out && reflect.ValueOf(v).Kind() != reflect.Ptr {
		return fmt.Errorf("out parameter %v needs to be pointer variable", v)
	}

	var err error

	// let fields with own Value converter convert themselves first (e.g. NullInt64, ...)
	if valuer, ok := v.(driver.Valuer); ok {
		if v, err = valuer.Value(); err != nil {
			return err
		}
	}

	// special cases
	switch v := v.(type) {
	case io.Reader:
		if f.Out() {
			return fmt.Errorf("out parameter not writeable: %v", v)
		}
	case io.Writer:
		if f.In() {
			return fmt.Errorf("in parameter not readable: %v", v)
		}
	}

	if out {
		_, err = converter.Convert(v) // check field only
	} else {
		v, err = converter.Convert(v) // convert field
	}
	if err != nil {
		return err
	}

	nv.Value = v
	return nil
}

func normNamedValue(nv *driver.NamedValue) (interface{}, bool) {
	if out, isOut := nv.Value.(sql.Out); isOut { // out parameter
		return out.Dest, true // 'flatten' driver.NamedValue (remove sql.Out)
	}
	return nv.Value, false
}
