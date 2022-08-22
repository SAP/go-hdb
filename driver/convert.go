// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
)

func convertNamedValue(conn *conn, pr *prepareResult, nv *driver.NamedValue) error {

	idx := nv.Ordinal - 1

	f := pr.parameterField(idx)

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

	if out {
		if reflect.ValueOf(v).Kind() != reflect.Ptr {
			return fmt.Errorf("out parameter %v needs to be pointer variable", v)
		}
		if _, err := f.Convert(conn._cesu8Encoder(), v); err != nil { // check field only
			return err
		}
		return nil
	}

	if v, err = f.Convert(conn._cesu8Encoder(), v); err != nil { // convert field
		return err
	}

	nv.Value = v
	return nil
}

func convertValue(conn *conn, pr *prepareResult, idx int, v driver.Value) (driver.Value, error) {
	var err error
	f := pr.parameterField(idx)
	// let fields with own Value converter convert themselves first (e.g. NullInt64, ...)
	if valuer, ok := v.(driver.Valuer); ok {
		if v, err = valuer.Value(); err != nil {
			return nil, err
		}
	}
	// convert field
	return f.Convert(conn._cesu8Encoder(), v)
}

func normNamedValue(nv *driver.NamedValue) (interface{}, bool) {
	if out, isOut := nv.Value.(sql.Out); isOut { // out parameter
		return out.Dest, true // 'flatten' driver.NamedValue (remove sql.Out)
	}
	return nv.Value, false
}

func convertMany(v interface{}) (interface{}, bool) {
	// allow slice ,array, and pointers to it
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Array, reflect.Slice:
		// but do not allow slice, array of bytes
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return nil, false
		}
		return rv.Interface(), true
	case reflect.Ptr:
		return convertMany(rv.Elem().Interface())
	default:
		return nil, false
	}
}
