// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"database/sql/driver"
	"fmt"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
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

//resultset id
type resultsetID uint64

func (id resultsetID) String() string { return fmt.Sprintf("%d", id) }
func (id *resultsetID) decode(dec *encoding.Decoder, ph *partHeader) error {
	*id = resultsetID(dec.Uint64())
	return dec.Error()
}
func (id resultsetID) encode(enc *encoding.Encoder) error { enc.Uint64(uint64(id)); return nil }

// TODO cache
func newResultFields(size int) []*resultField {
	return make([]*resultField, size)
}

// resultField contains database field attributes for result fields.
type resultField struct {
	tableName               string
	schemaName              string
	columnName              string
	columnDisplayName       string
	columnOptions           columnOptions
	tc                      typeCode
	fraction                int16
	length                  int16
	tableNameOffset         uint32
	schemaNameOffset        uint32
	columnNameOffset        uint32
	columnDisplayNameOffset uint32
}

// String implements the Stringer interface.
func (f *resultField) String() string {
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

func (f *resultField) Converter() Converter { return f.tc.fieldType() }

// TypeName returns the type name of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeDatabaseTypeName
func (f *resultField) TypeName() string { return f.tc.typeName() }

// ScanType returns the scan type of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeScanType
func (f *resultField) ScanType() DataType { return f.tc.dataType() }

// TypeLength returns the type length of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeLength
func (f *resultField) TypeLength() (int64, bool) {
	if f.tc.isVariableLength() {
		return int64(f.length), true
	}
	return 0, false
}

// TypePrecisionScale returns the type precision and scale (decimal types) of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypePrecisionScale
func (f *resultField) TypePrecisionScale() (int64, int64, bool) {
	if f.tc.isDecimalType() {
		return int64(f.length), int64(f.fraction), true
	}
	return 0, 0, false
}

// Nullable returns true if the field may be null, false otherwise.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeNullable
func (f *resultField) Nullable() bool { return f.columnOptions == coOptional }

// Name returns the result field name.
func (f *resultField) Name() string { return f.columnDisplayName }
func (f *resultField) In() bool     { return false }
func (f *resultField) Out() bool    { return true }

func (f *resultField) decode(dec *encoding.Decoder) {
	f.columnOptions = columnOptions(dec.Int8())
	f.tc = typeCode(dec.Int8())
	f.fraction = dec.Int16()
	f.length = dec.Int16()
	dec.Skip(2) //filler
	f.tableNameOffset = dec.Uint32()
	f.schemaNameOffset = dec.Uint32()
	f.columnNameOffset = dec.Uint32()
	f.columnDisplayNameOffset = dec.Uint32()
}

//resultset metadata
type resultMetadata struct {
	resultFields []*resultField
}

func (r *resultMetadata) String() string {
	return fmt.Sprintf("result fields %v", r.resultFields)
}

func (r *resultMetadata) decode(dec *encoding.Decoder, ph *partHeader) error {
	r.resultFields = newResultFields(ph.numArg())

	names := fieldNames{}

	for i := 0; i < len(r.resultFields); i++ {
		f := new(resultField)
		f.decode(dec)
		r.resultFields[i] = f
		names.insert(f.tableNameOffset)
		names.insert(f.schemaNameOffset)
		names.insert(f.columnNameOffset)
		names.insert(f.columnDisplayNameOffset)
	}

	names.decode(dec)

	for _, f := range r.resultFields {
		f.tableName = names.name(f.tableNameOffset)
		f.schemaName = names.name(f.schemaNameOffset)
		f.columnName = names.name(f.columnNameOffset)
		f.columnDisplayName = names.name(f.columnDisplayNameOffset)
	}

	//r.resultFieldSet.decode(dec)
	return dec.Error()
}

//resultset
type resultset struct {
	resultFields []*resultField
	fieldValues  []driver.Value
}

func (r *resultset) String() string {
	return fmt.Sprintf("result fields %v field values %v", r.resultFields, r.fieldValues)
}

func (r *resultset) decode(dec *encoding.Decoder, ph *partHeader) error {
	numArg := ph.numArg()
	cols := len(r.resultFields)
	r.fieldValues = newFieldValues(numArg * cols)

	for i := 0; i < numArg; i++ {
		for j, field := range r.resultFields {
			var err error
			if r.fieldValues[i*cols+j], err = decodeRes(dec, field.tc); err != nil {
				return err
			}
		}
	}
	return dec.Error()
}
