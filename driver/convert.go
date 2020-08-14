// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

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
