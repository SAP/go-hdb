// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"database/sql/driver"
	"fmt"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
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

func newParameterFields(size int) []*parameterField {
	return make([]*parameterField, size)
}

// parameterField contains database field attributes for parameters.
type parameterField struct {
	name             string
	parameterOptions parameterOptions
	tc               typeCode
	mode             parameterMode
	fraction         int16
	length           int16
	offset           uint32
}

func (f *parameterField) String() string {
	return fmt.Sprintf("parameterOptions %s typeCode %s mode %s fraction %d length %d name %s",
		f.parameterOptions,
		f.tc,
		f.mode,
		f.fraction,
		f.length,
		f.name,
	)
}

func (f *parameterField) Converter() Converter { return f.tc.fieldType() }

// TypeName returns the type name of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeDatabaseTypeName
func (f *parameterField) TypeName() string { return f.tc.typeName() }

// ScanType returns the scan type of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeScanType
func (f *parameterField) ScanType() DataType { return f.tc.dataType() }

// typeLength returns the type length of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeLength
func (f *parameterField) TypeLength() (int64, bool) {
	if f.tc.isVariableLength() {
		return int64(f.length), true
	}
	return 0, false
}

// typePrecisionScale returns the type precision and scale (decimal types) of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypePrecisionScale
func (f *parameterField) TypePrecisionScale() (int64, int64, bool) {
	if f.tc.isDecimalType() {
		return int64(f.length), int64(f.fraction), true
	}
	return 0, 0, false
}

// nullable returns true if the field may be null, false otherwise.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeNullable
func (f *parameterField) Nullable() bool {
	return f.parameterOptions == poOptional
}

// in returns true if the parameter field is an input field.
func (f *parameterField) In() bool {
	return f.mode == pmInout || f.mode == pmIn
}

// out returns true if the parameter field is an output field.
func (f *parameterField) Out() bool {
	return f.mode == pmInout || f.mode == pmOut
}

// name returns the parameter field name.
func (f *parameterField) Name() string {
	return f.name
}

func (f *parameterField) decode(dec *encoding.Decoder) {
	f.parameterOptions = parameterOptions(dec.Int8())
	f.tc = typeCode(dec.Int8())
	f.mode = parameterMode(dec.Int8())
	dec.Skip(1) //filler
	f.offset = dec.Uint32()
	f.length = dec.Int16()
	f.fraction = dec.Int16()
	dec.Skip(4) //filler
}

// parameter metadata
type parameterMetadata struct {
	parameterFields []*parameterField
}

func (m *parameterMetadata) String() string {
	return fmt.Sprintf("parameter forlds %v", m.parameterFields)
}

func (m *parameterMetadata) decode(dec *encoding.Decoder, ph *partHeader) error {
	m.parameterFields = newParameterFields(ph.numArg())

	names := fieldNames{}

	for i := 0; i < len(m.parameterFields); i++ {
		f := new(parameterField)
		f.decode(dec)
		m.parameterFields[i] = f
		names.insert(f.offset)
	}

	names.decode(dec)

	for _, f := range m.parameterFields {
		f.name = names.name(f.offset)
	}
	return dec.Error()
}

// input parameters
type inputParameters struct {
	inputFields []*parameterField
	args        []driver.NamedValue
}

func newInputParameters(inputFields []*parameterField, args []driver.NamedValue) *inputParameters {
	return &inputParameters{inputFields: inputFields, args: args}
}

func (p *inputParameters) String() string {
	return fmt.Sprintf("fields %s len(args) %d args %v", p.inputFields, len(p.args), p.args)
}

func (p *inputParameters) size() int {
	size := len(p.args)
	cnt := len(p.inputFields)

	for i, arg := range p.args {
		// mass insert
		f := p.inputFields[i%cnt]
		size += prmSize(f.tc, arg)
	}
	return size
}

func (p *inputParameters) numArg() int {
	cnt := len(p.inputFields)
	if cnt == 0 { // avoid divide-by-zero (e.g. prepare without parameters)
		return 0
	}
	return len(p.args) / cnt
}

func (p *inputParameters) decode(dec *encoding.Decoder, ph *partHeader) error {
	// TODO Sniffer
	//return fmt.Errorf("not implemented")
	return nil
}

func (p *inputParameters) encode(enc *encoding.Encoder) error {
	cnt := len(p.inputFields)

	for i, arg := range p.args {
		//mass insert
		f := p.inputFields[i%cnt]
		if err := encodePrm(enc, f.tc, arg); err != nil {
			return err
		}
	}
	return nil
}

// output parameter
type outputParameters struct {
	outputFields []*parameterField
	fieldValues  []driver.Value
}

func (p *outputParameters) String() string {
	return fmt.Sprintf("fields %v values %v", p.outputFields, p.fieldValues)
}

func (p *outputParameters) decode(dec *encoding.Decoder, ph *partHeader) error {
	numArg := ph.numArg()
	cols := len(p.outputFields)
	p.fieldValues = newFieldValues(numArg * cols)

	for i := 0; i < numArg; i++ {
		for j, field := range p.outputFields {
			var err error
			if p.fieldValues[i*cols+j], err = decodeRes(dec, field.tc); err != nil {
				return err
			}
		}
	}
	return dec.Error()
}
