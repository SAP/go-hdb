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

	v, out := normNamedValue(nv)

	if out != f.Out() {
		return fmt.Errorf("parameter descr / value mismatch - descr out %t value out %t", f.Out(), out)
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
		if reflect.ValueOf(v).Kind() != reflect.Ptr {
			return fmt.Errorf("out parameter %v needs to be pointer variable", v)
		}
		if _, err := f.Convert(v); err != nil { // check field only
			return err
		}
		return nil
	}

	if nv.Value, err = f.Convert(v); err != nil { // convert field
		return err
	}
	return nil
}

func normNamedValue(nv *driver.NamedValue) (interface{}, bool) {
	if out, isOut := nv.Value.(sql.Out); isOut { // out parameter
		return out.Dest, true // 'flatten' driver.NamedValue (remove sql.Out)
	}
	return nv.Value, false
}
