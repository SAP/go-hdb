//go:build go1.26

package driver

import (
	"reflect"
)

var boolReflectType = reflect.TypeFor[bool]()

// see https://github.com/golang/go/issues/54393
func isGenericNull(t reflect.Type) (ok bool, vField reflect.Type, validField reflect.Type) {
	if t.Kind() != reflect.Struct {
		return false, nil, nil
	}

	// quirky check - we don't do and let similar definitions pass as well.
	/*
		if !strings.HasPrefix(t.String(), "sql.Null[") {
			return false
		}
	*/

	for field := range t.Fields() {
		switch field.Name {
		case "V":
			vField = field.Type
			if validField != nil {
				return true, vField, validField
			}
		case "Valid":
			if field.Type != boolReflectType { // valid needs to be a boolean
				return false, nil, nil
			}
			validField = field.Type
			if vField != nil {
				return true, vField, validField
			}
		}
	}
	return false, nil, nil
}
