// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

type columnOptions int8

const (
	coMandatory columnOptions = 0x01
	coOptional  columnOptions = 0x02
)

var columnOptionsText = map[columnOptions]string{
	coMandatory: "mandatory",
	coOptional:  "optional",
}

func (k columnOptions) String() string {
	t := make([]string, 0, len(columnOptionsText))

	for option, text := range columnOptionsText {
		if (k & option) != 0 {
			t = append(t, text)
		}
	}
	return fmt.Sprintf("%v", t)
}

// ResultsetID represents a resultset id.
type ResultsetID uint64

func (id ResultsetID) String() string { return fmt.Sprintf("%d", id) }
func (id *ResultsetID) decode(dec *encoding.Decoder, ph *PartHeader) error {
	*id = ResultsetID(dec.Uint64())
	return dec.Error()
}
func (id ResultsetID) encode(enc *encoding.Encoder) error { enc.Uint64(uint64(id)); return nil }

// TODO cache
func newResultFields(size int) []*ResultField {
	return make([]*ResultField, size)
}

// ResultField represents a database result field.
type ResultField struct {
	// field alignment
	tableName               string
	schemaName              string
	columnName              string
	columnDisplayName       string
	ft                      fieldType // avoid tc.fieldType() calls
	tableNameOffset         uint32
	schemaNameOffset        uint32
	columnNameOffset        uint32
	columnDisplayNameOffset uint32
	length                  int16
	fraction                int16
	columnOptions           columnOptions
	tc                      TypeCode
}

// String implements the Stringer interface.
func (f *ResultField) String() string {
	return fmt.Sprintf("columnsOptions %s typeCode %s fraction %d length %d tablename %s schemaname %s columnname %s columnDisplayname %s",
		f.columnOptions,
		f.tc,
		f.fraction,
		f.length,
		f.tableName,
		f.schemaName,
		f.columnName,
		f.columnDisplayName,
	)
}

// TypeName returns the type name of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeDatabaseTypeName
func (f *ResultField) TypeName() string { return f.tc.typeName() }

// ScanType returns the scan type of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeScanType
func (f *ResultField) ScanType() reflect.Type { return f.tc.dataType().ScanType() }

// TypeLength returns the type length of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeLength
func (f *ResultField) TypeLength() (int64, bool) {
	if f.tc.isVariableLength() {
		return int64(f.length), true
	}
	return 0, false
}

// TypePrecisionScale returns the type precision and scale (decimal types) of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypePrecisionScale
func (f *ResultField) TypePrecisionScale() (int64, int64, bool) {
	if f.tc.isDecimalType() {
		return int64(f.length), int64(f.fraction), true
	}
	return 0, 0, false
}

// Nullable returns true if the field may be null, false otherwise.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeNullable
func (f *ResultField) Nullable() bool { return f.columnOptions == coOptional }

// Name returns the result field name.
func (f *ResultField) Name() string { return f.columnDisplayName }

func (f *ResultField) decode(dec *encoding.Decoder) {
	f.columnOptions = columnOptions(dec.Int8())
	f.tc = TypeCode(dec.Int8())
	f.fraction = dec.Int16()
	f.length = dec.Int16()
	dec.Skip(2) //filler
	f.tableNameOffset = dec.Uint32()
	f.schemaNameOffset = dec.Uint32()
	f.columnNameOffset = dec.Uint32()
	f.columnDisplayNameOffset = dec.Uint32()
	f.ft = f.tc.fieldType(int(f.length), int(f.fraction))
}

func (f *ResultField) decodeRes(dec *encoding.Decoder) (interface{}, error) {
	return f.ft.decodeRes(dec)
}

// ResultMetadata represents the metadata of a set of database result fields.
type ResultMetadata struct {
	ResultFields []*ResultField
}

func (r *ResultMetadata) String() string {
	return fmt.Sprintf("result fields %v", r.ResultFields)
}

func (r *ResultMetadata) decode(dec *encoding.Decoder, ph *PartHeader) error {
	r.ResultFields = newResultFields(ph.numArg())

	names := fieldNames{}

	for i := 0; i < len(r.ResultFields); i++ {
		f := new(ResultField)
		f.decode(dec)
		r.ResultFields[i] = f
		names.insert(f.tableNameOffset)
		names.insert(f.schemaNameOffset)
		names.insert(f.columnNameOffset)
		names.insert(f.columnDisplayNameOffset)
	}
	names.decode(dec)
	for _, f := range r.ResultFields {
		f.tableName = names.name(f.tableNameOffset)
		f.schemaName = names.name(f.schemaNameOffset)
		f.columnName = names.name(f.columnNameOffset)
		f.columnDisplayName = names.name(f.columnDisplayNameOffset)
	}
	return dec.Error()
}

// Resultset represents a database result set.
type Resultset struct {
	ResultFields []*ResultField
	FieldValues  []driver.Value
	DecodeErrors DecodeErrors
}

func (r *Resultset) String() string {
	return fmt.Sprintf("result fields %v field values %v", r.ResultFields, r.FieldValues)
}

func (r *Resultset) decode(dec *encoding.Decoder, ph *PartHeader) error {
	numArg := ph.numArg()
	cols := len(r.ResultFields)
	r.FieldValues = resizeFieldValues(r.FieldValues, numArg*cols)

	for i := 0; i < numArg; i++ {
		for j, f := range r.ResultFields {
			var err error
			if r.FieldValues[i*cols+j], err = f.decodeRes(dec); err != nil {
				r.DecodeErrors = append(r.DecodeErrors, &DecodeError{row: i, fieldName: f.Name(), s: err.Error()}) // collect decode / conversion errors
			}
		}
	}
	return dec.Error()
}
