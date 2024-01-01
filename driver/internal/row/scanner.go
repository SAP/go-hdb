// Package row provides database row functions.
package row

import (
	"database/sql"
	"fmt"
	"reflect"
)

// StructScanner is a database rows wrapper providing struct scanning of type T.
type StructScanner[S any] struct {
	rows      *sql.Rows
	indexList [][]int
}

// NewStructScanner returns a new row scanner.
func NewStructScanner[S any](rows *sql.Rows) (*StructScanner[S], error) {
	var s S
	rt := reflect.TypeOf(s)
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid type %s", rt.Kind())
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var fields []reflect.StructField
	indexList := make([][]int, len(columns))

	fieldByTag := func(i int, name string) bool {
		for _, field := range fields {
			// TODO: split tag by ','
			if tag, ok := field.Tag.Lookup("sql"); ok && name == tag {
				indexList[i] = field.Index
				return true
			}
		}
		return false
	}

	for i, name := range columns {
		// search by field name
		if field, ok := rt.FieldByName(name); ok {
			indexList[i] = field.Index
			continue
		}
		// search by tag
		if fields == nil {
			fields = reflect.VisibleFields(rt)
		}
		if !fieldByTag(i, name) {
			return nil, fmt.Errorf("field %s not found", name)
		}
	}

	// check exported // chech column name

	return &StructScanner[S]{
		rows:      rows,
		indexList: indexList,
	}, nil

}

// Scan scans row values into struct v of type *S.
func (sc StructScanner[S]) Scan(s *S) error {
	values := make([]any, len(sc.indexList))
	rv := reflect.ValueOf(s).Elem()
	for i, index := range sc.indexList {
		values[i] = rv.FieldByIndex(index).Addr().Interface()
	}
	return sc.rows.Scan(values...)
}
