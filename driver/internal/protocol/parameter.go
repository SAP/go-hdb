// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"database/sql/driver"
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

type parameterOptions int8

const (
	poMandatory parameterOptions = 0x01
	poOptional  parameterOptions = 0x02
	poDefault   parameterOptions = 0x04
)

var parameterOptionsText = map[parameterOptions]string{
	poMandatory: "mandatory",
	poOptional:  "optional",
	poDefault:   "default",
}

func (k parameterOptions) String() string {
	t := make([]string, 0, len(parameterOptionsText))

	for option, text := range parameterOptionsText {
		if (k & option) != 0 {
			t = append(t, text)
		}
	}
	return fmt.Sprintf("%v", t)
}

type parameterMode int8

const (
	pmIn    parameterMode = 0x01
	pmInout parameterMode = 0x02
	pmOut   parameterMode = 0x04
)

var parameterModeText = map[parameterMode]string{
	pmIn:    "in",
	pmInout: "inout",
	pmOut:   "out",
}

func (k parameterMode) String() string {
	t := make([]string, 0, len(parameterModeText))

	for mode, text := range parameterModeText {
		if (k & mode) != 0 {
			t = append(t, text)
		}
	}
	return fmt.Sprintf("%v", t)
}

func newParameterFields(size int) []*ParameterField {
	return make([]*ParameterField, size)
}

// ParameterField contains database field attributes for parameters.
type ParameterField struct {
	// field alignment
	fieldName        string
	ft               fieldType // avoid tc.fieldType() calls in Converter (e.g. bulk insert)
	offset           uint32
	length           int16
	fraction         int16
	parameterOptions parameterOptions
	tc               typeCode
	mode             parameterMode
}

func (f *ParameterField) String() string {
	return fmt.Sprintf("parameterOptions %s typeCode %s mode %s fraction %d length %d name %s",
		f.parameterOptions,
		f.tc,
		f.mode,
		f.fraction,
		f.length,
		f.fieldName,
	)
}

// Convert returns the result of the fieldType conversion.
func (f *ParameterField) Convert(s *Session, v interface{}) (interface{}, error) {
	return f.ft.convert(s, v)
}

// TypeName returns the type name of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeDatabaseTypeName
func (f *ParameterField) typeName() string { return f.tc.typeName() }

// ScanType returns the scan type of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeScanType
func (f *ParameterField) scanType() DataType { return f.tc.dataType() }

// typeLength returns the type length of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeLength
func (f *ParameterField) typeLength() (int64, bool) {
	if f.tc.isVariableLength() {
		return int64(f.length), true
	}
	return 0, false
}

// typePrecisionScale returns the type precision and scale (decimal types) of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypePrecisionScale
func (f *ParameterField) typePrecisionScale() (int64, int64, bool) {
	if f.tc.isDecimalType() {
		return int64(f.length), int64(f.fraction), true
	}
	return 0, 0, false
}

// nullable returns true if the field may be null, false otherwise.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeNullable
func (f *ParameterField) nullable() bool {
	return f.parameterOptions == poOptional
}

// In returns true if the parameter field is an input field.
func (f *ParameterField) In() bool {
	return f.mode == pmInout || f.mode == pmIn
}

// Out returns true if the parameter field is an output field.
func (f *ParameterField) Out() bool {
	return f.mode == pmInout || f.mode == pmOut
}

// name returns the parameter field name.
func (f *ParameterField) name() string {
	return f.fieldName
}

func (f *ParameterField) decode(dec *encoding.Decoder) {
	f.parameterOptions = parameterOptions(dec.Int8())
	f.tc = typeCode(dec.Int8())
	f.mode = parameterMode(dec.Int8())
	dec.Skip(1) //filler
	f.offset = dec.Uint32()
	f.length = dec.Int16()
	f.fraction = dec.Int16()
	dec.Skip(4) //filler
	f.ft = f.tc.fieldType(int(f.length), int(f.fraction))
}

func (f *ParameterField) prmSize(v interface{}) int {
	if v == nil && f.tc.supportNullValue() {
		return 0
	}
	return f.ft.prmSize(v)
}

func (f *ParameterField) encodePrm(enc *encoding.Encoder, v interface{}) error {
	encTc := f.tc.encTc()
	if v == nil && f.tc.supportNullValue() {
		enc.Byte(byte(f.tc.nullValue())) // null value type code
		return nil
	}
	enc.Byte(byte(encTc)) // type code
	return f.ft.encodePrm(enc, v)
}

func (f *ParameterField) decodeRes(dec *encoding.Decoder) (interface{}, error) {
	return f.ft.decodeRes(dec)
}

/*
decode parameter
- currently not used
- type code is first byte (see encodePrm)
*/
func (f *ParameterField) decodePrm(dec *encoding.Decoder) (interface{}, error) {
	tc := typeCode(dec.Byte())
	if tc&0x80 != 0 { // high bit set -> null value
		return nil, nil
	}
	return f.ft.decodePrm(dec)
}

// parameter metadata
type parameterMetadata struct {
	parameterFields []*ParameterField
}

func (m *parameterMetadata) String() string {
	return fmt.Sprintf("parameter %v", m.parameterFields)
}

func (m *parameterMetadata) decode(dec *encoding.Decoder, ph *partHeader) error {
	m.parameterFields = newParameterFields(ph.numArg())

	names := fieldNames{}

	for i := 0; i < len(m.parameterFields); i++ {
		f := new(ParameterField)
		f.decode(dec)
		m.parameterFields[i] = f
		names.insert(f.offset)
	}

	if err := names.decode(dec); err != nil {
		return err
	}

	for _, f := range m.parameterFields {
		f.fieldName = names.name(f.offset)
	}
	return dec.Error()
}

// input parameters
type inputParameters struct {
	inputFields []*ParameterField
	args        []interface{}
	hasLob      bool
}

func newInputParameters(inputFields []*ParameterField, args []interface{}, hasLob bool) (*inputParameters, error) {
	return &inputParameters{inputFields: inputFields, args: args, hasLob: hasLob}, nil
}

func (p *inputParameters) String() string {
	return fmt.Sprintf("fields %s len(args) %d args %v", p.inputFields, len(p.args), p.args)
}

func (p *inputParameters) size() int {
	size := 0
	numColumns := len(p.inputFields)
	if numColumns == 0 { // avoid divide-by-zero (e.g. prepare without parameters)
		return 0
	}

	for i := 0; i < len(p.args)/numColumns; i++ { // row-by-row

		size += numColumns

		for j := 0; j < numColumns; j++ {
			f := p.inputFields[j]
			size += f.prmSize(p.args[i*numColumns+j])
		}

		// lob input parameter: set offset position of lob data
		if p.hasLob {
			for j := 0; j < numColumns; j++ {
				if lobInDescr, ok := p.args[i*numColumns+j].(*lobInDescr); ok {
					lobInDescr.setPos(size)
					size += lobInDescr.size()
				}
			}
		}
	}
	return size
}

func (p *inputParameters) numArg() int {
	numColumns := len(p.inputFields)
	if numColumns == 0 { // avoid divide-by-zero (e.g. prepare without parameters)
		return 0
	}
	return len(p.args) / numColumns
}

func (p *inputParameters) decode(dec *encoding.Decoder, ph *partHeader) error {
	// TODO Sniffer
	//return fmt.Errorf("not implemented")
	return nil
}

func (p *inputParameters) encode(enc *encoding.Encoder) error {
	numColumns := len(p.inputFields)
	if numColumns == 0 { // avoid divide-by-zero (e.g. prepare without parameters)
		return nil
	}

	for i := 0; i < len(p.args)/numColumns; i++ { // row-by-row
		for j := 0; j < numColumns; j++ {
			//mass insert
			f := p.inputFields[j]
			if err := f.encodePrm(enc, p.args[i*numColumns+j]); err != nil {
				return err
			}
		}
		// lob input parameter: write first data chunk
		if p.hasLob {
			for j := 0; j < numColumns; j++ {
				if lobInDescr, ok := p.args[i*numColumns+j].(*lobInDescr); ok {
					lobInDescr.writeFirst(enc)
				}
			}
		}
	}
	return nil
}

// output parameter
type outputParameters struct {
	outputFields []*ParameterField
	fieldValues  []driver.Value
	decodeErrors decodeErrors
}

func (p *outputParameters) String() string {
	return fmt.Sprintf("fields %v values %v", p.outputFields, p.fieldValues)
}

func (p *outputParameters) decode(dec *encoding.Decoder, ph *partHeader) error {
	numArg := ph.numArg()
	cols := len(p.outputFields)
	p.fieldValues = resizeFieldValues(numArg*cols, p.fieldValues)

	for i := 0; i < numArg; i++ {
		for j, f := range p.outputFields {
			var err error
			if p.fieldValues[i*cols+j], err = f.decodeRes(dec); err != nil {
				p.decodeErrors = append(p.decodeErrors, &decodeError{row: i, fieldName: f.name(), s: err.Error()}) // collect decode / conversion errors
			}
		}
	}
	return dec.Error()
}
