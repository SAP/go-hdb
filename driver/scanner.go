package driver

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// Tagger is an interface used to tag structure fields dynamically.
type Tagger interface {
	Tag(fieldName string) (value string, ok bool)
}

// StructScanner is a database scanner to scan rows into a struct of type S.
// This enables using structs as scan targets.
// For usage please refer to the example.
type StructScanner[S any] struct {
	nameFieldMap map[string]reflect.StructField // map names to field
}

// NewStructScanner returns a new struct scanner.
func NewStructScanner[S any]() (*StructScanner[S], error) {
	var s *S

	rt := reflect.TypeOf(s).Elem()
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid type %s", rt.Kind())
	}

	tagger, hasTagger := any(s).(Tagger)

	nameFieldMap := map[string]reflect.StructField{} // map names to field
	for _, field := range reflect.VisibleFields(rt) {
		if hasTagger {
			if tag, ok := tagger.Tag(field.Name); ok {
				if sql, ok := reflect.StructTag(tag).Lookup("sql"); ok {
					nameFieldMap[strings.Split(sql, ",")[0]] = field
					continue
				}
			}
		}
		if sql, ok := field.Tag.Lookup("sql"); ok {
			nameFieldMap[strings.Split(sql, ",")[0]] = field
			continue
		}
		nameFieldMap[field.Name] = field
	}

	return &StructScanner[S]{nameFieldMap: nameFieldMap}, nil
}

// ScanRow scans the field values of the first row in rows into struct s of type *S and closes rows.
func (sc StructScanner[S]) ScanRow(rows *sql.Rows, s *S) error {
	if rows.Err() != nil {
		return rows.Err()
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := sc.Scan(rows, s)
	if err != nil {
		return err
	}
	return rows.Close()
}

// Scan scans row field values into struct s of type *S.
func (sc StructScanner[S]) Scan(rows *sql.Rows, s *S) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	rv := reflect.ValueOf(s).Elem()
	values := make([]any, len(columns))
	for i, name := range columns {
		field, ok := sc.nameFieldMap[name]
		if !ok {
			return fmt.Errorf("field for column name %s not found", name)
		}
		if !field.IsExported() {
			return fmt.Errorf("field %s for column name %s is not exported", field.Name, name)
		}
		values[i] = rv.FieldByIndex(field.Index).Addr().Interface()
	}
	return rows.Scan(values...)
}
