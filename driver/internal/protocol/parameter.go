package protocol

import (
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"golang.org/x/text/transform"
)

type parameterOptions int8

const (
	poMandatory parameterOptions = 0x01
	poOptional  parameterOptions = 0x02
	poDefault   parameterOptions = 0x04
)

const (
	poMandatoryText = "mandatory"
	poOptionalText  = "optional"
	poDefaultText   = "default"
)

func (k parameterOptions) String() string {
	var s []string
	if k&poMandatory != 0 {
		s = append(s, poMandatoryText)
	}
	if k&poOptional != 0 {
		s = append(s, poOptionalText)
	}
	if k&poDefault != 0 {
		s = append(s, poDefaultText)
	}
	return fmt.Sprintf("%v", s)
}

// ParameterMode represents the parameter mode set.
type ParameterMode int8

// ParameterMode constants.
const (
	pmIn    ParameterMode = 0x01
	pmInout ParameterMode = 0x02
	pmOut   ParameterMode = 0x04
)

const (
	pmInText    = "in"
	pmInoutText = "inout"
	pmOutText   = "out"
)

func (k ParameterMode) String() string {
	var s []string
	if k&pmIn != 0 {
		s = append(s, pmInText)
	}
	if k&pmInout != 0 {
		s = append(s, pmInoutText)
	}
	if k&pmOut != 0 {
		s = append(s, pmOutText)
	}
	return fmt.Sprintf("%v", s)
}

// ParameterField contains database field attributes for parameters.
type ParameterField struct {
	names            *fieldNames
	ft               fieldType // avoid tc.fieldType() calls in Converter (e.g. bulk insert)
	ofs              int       // field name offset & used for index in case of tableRef or tableRows type
	length           int16
	fraction         int16
	parameterOptions parameterOptions
	tc               typeCode
	mode             ParameterMode
}

// NewTableRowsParameterField returns a ParameterField representing table rows.
func NewTableRowsParameterField(idx int) *ParameterField {
	return &ParameterField{ofs: idx, tc: TcTableRows, mode: pmOut}
}

func (f *ParameterField) fieldName() string {
	switch f.tc {
	case TcTableRows:
		return fmt.Sprintf("table %d", f.ofs)
	default:
		return f.names.name(uint32(f.ofs))
	}
}

func (f *ParameterField) String() string {
	return fmt.Sprintf("parameterOptions %s typeCode %s mode %s fraction %d length %d name %s",
		f.parameterOptions,
		f.tc,
		f.mode,
		f.fraction,
		f.length,
		f.fieldName(),
	)
}

// IsLob returns true if the ParameterField is of type lob, false otherwise.
func (f *ParameterField) IsLob() bool { return f.tc.isLob() }

// Convert returns the result of the fieldType conversion.
func (f *ParameterField) Convert(t transform.Transformer, v any) (any, error) {
	switch ft := f.ft.(type) {
	case fieldConverter:
		return ft.convert(v)
	case cesu8FieldConverter:
		return ft.convertCESU8(t, v)
	default:
		panic(fmt.Sprintf("field type %v does not implement converter", ft)) // should never happen
	}
}

// TypeName returns the type name of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeDatabaseTypeName
func (f *ParameterField) TypeName() string { return f.tc.typeName() }

// ScanType returns the scan type of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeScanType
func (f *ParameterField) ScanType() reflect.Type { return f.tc.dataType().ScanType(f.Nullable()) }

// TypeLength returns the type length of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeLength
func (f *ParameterField) TypeLength() (int64, bool) {
	if f.tc.isVariableLength() {
		return int64(f.length), true
	}
	return 0, false
}

// TypePrecisionScale returns the type precision and scale (decimal types) of the field.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypePrecisionScale
func (f *ParameterField) TypePrecisionScale() (int64, int64, bool) {
	if f.tc.isDecimalType() {
		return int64(f.length), int64(f.fraction), true
	}
	return 0, 0, false
}

// Nullable returns true if the field may be null, false otherwise.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeNullable
func (f *ParameterField) Nullable() bool { return f.parameterOptions == poOptional }

// In returns true if the parameter field is an input field.
func (f *ParameterField) In() bool { return f.mode == pmInout || f.mode == pmIn }

// Out returns true if the parameter field is an output field.
func (f *ParameterField) Out() bool { return f.mode == pmInout || f.mode == pmOut }

// InOut returns true if the parameter field is an in,- output field.
func (f *ParameterField) InOut() bool { return f.mode == pmInout }

// Name returns the parameter field name.
func (f *ParameterField) Name() string { return f.fieldName() }

func (f *ParameterField) decode(dec *encoding.Decoder, ftc *FieldTypeCtx) {
	f.parameterOptions = parameterOptions(dec.Int8())
	f.tc = typeCode(dec.Int8())
	f.mode = ParameterMode(dec.Int8())
	dec.Skip(1) // filler
	f.ofs = int(dec.Uint32())
	f.length = dec.Int16()
	f.fraction = dec.Int16()
	dec.Skip(4) // filler

	f.names.insert(uint32(f.ofs))

	f.ft = ftc.fieldType(f.tc, int(f.length), int(f.fraction))
}

func (f *ParameterField) prmSize(v any) int {
	if v == nil && f.tc.supportNullValue() {
		return 0
	}
	return f.ft.prmSize(v)
}

func (f *ParameterField) encodePrm(enc *encoding.Encoder, v any) error {
	encTc := f.tc.encTc()
	if v == nil && f.tc.supportNullValue() {
		enc.Byte(byte(f.tc.nullValue())) // null value type code
		return nil
	}
	enc.Byte(byte(encTc)) // type code
	return f.ft.encodePrm(enc, v)
}

func (f *ParameterField) decodeRes(dec *encoding.Decoder) (any, error) {
	return f.ft.decodeRes(dec)
}

/*
decode parameter
- currently not used
- type code is first byte (see encodePrm).
*/
func (f *ParameterField) decodePrm(dec *encoding.Decoder) (any, error) {
	tc := typeCode(dec.Byte())
	if tc&0x80 != 0 { // high bit set -> null value
		return nil, nil
	}
	return f.ft.decodePrm(dec)
}

// ParameterMetadata represents the metadata of a parameter.
type ParameterMetadata struct {
	FieldTypeCtx    *FieldTypeCtx
	ParameterFields []*ParameterField
}

func (m *ParameterMetadata) String() string {
	return fmt.Sprintf("parameter %v", m.ParameterFields)
}

func (m *ParameterMetadata) decodeNumArg(dec *encoding.Decoder, numArg int) error {
	m.ParameterFields = make([]*ParameterField, numArg)
	names := &fieldNames{}
	for i := 0; i < len(m.ParameterFields); i++ {
		f := &ParameterField{names: names}
		f.decode(dec, m.FieldTypeCtx)
		m.ParameterFields[i] = f
	}
	if err := names.decode(dec); err != nil {
		return err
	}
	return dec.Error()
}

// InputParameters represents the set of input parameters.
type InputParameters struct {
	InputFields []*ParameterField
	nvargs      []driver.NamedValue
}

// NewInputParameters returns a InputParameters instance.
func NewInputParameters(inputFields []*ParameterField, nvargs []driver.NamedValue) (*InputParameters, error) {
	return &InputParameters{InputFields: inputFields, nvargs: nvargs}, nil
}

func (p *InputParameters) String() string {
	return fmt.Sprintf("fields %s len(args) %d args %v", p.InputFields, len(p.nvargs), p.nvargs)
}

func (p *InputParameters) size() int {
	size := 0
	numColumns := len(p.InputFields)
	if numColumns == 0 { // avoid divide-by-zero (e.g. prepare without parameters)
		return 0
	}

	for i := 0; i < len(p.nvargs)/numColumns; i++ { // row-by-row
		size += numColumns

		hasInLob := false

		for j := 0; j < numColumns; j++ {
			f := p.InputFields[j]
			size += f.prmSize(p.nvargs[i*numColumns+j].Value)
			if f.IsLob() && f.In() {
				hasInLob = true
			}
		}

		// lob input parameter: set offset position of lob data
		if hasInLob {
			for j := 0; j < numColumns; j++ {
				if lobInDescr, ok := p.nvargs[i*numColumns+j].Value.(*LobInDescr); ok {
					lobInDescr.setPos(size)
					size += lobInDescr.size()
				}
			}
		}
	}
	return size
}

func (p *InputParameters) numArg() int {
	numColumns := len(p.InputFields)
	if numColumns == 0 { // avoid divide-by-zero (e.g. prepare without parameters)
		return 0
	}
	return len(p.nvargs) / numColumns
}

func (p *InputParameters) decodeNumArg(dec *encoding.Decoder, numArg int) error {
	// TODO Sniffer
	// return fmt.Errorf("not implemented")
	return nil
}

func (p *InputParameters) encode(enc *encoding.Encoder) error {
	numColumns := len(p.InputFields)
	if numColumns == 0 { // avoid divide-by-zero (e.g. prepare without parameters)
		return nil
	}

	for i := 0; i < len(p.nvargs)/numColumns; i++ { // row-by-row
		hasInLob := false

		for j := 0; j < numColumns; j++ {
			// mass insert
			f := p.InputFields[j]
			if err := f.encodePrm(enc, p.nvargs[i*numColumns+j].Value); err != nil {
				return err
			}
			if f.IsLob() && f.In() {
				hasInLob = true
			}
		}
		// lob input parameter: write first data chunk
		if hasInLob {
			for j := 0; j < numColumns; j++ {
				if lobInDescr, ok := p.nvargs[i*numColumns+j].Value.(*LobInDescr); ok {
					lobInDescr.writeFirst(enc)
				}
			}
		}
	}
	return nil
}

// OutputParameters represents the set of output parameters.
type OutputParameters struct {
	OutputFields []*ParameterField
	FieldValues  []driver.Value
	DecodeErrors DecodeErrors
}

func (p *OutputParameters) String() string {
	return fmt.Sprintf("fields %v values %v", p.OutputFields, p.FieldValues)
}

func (p *OutputParameters) decodeNumArg(dec *encoding.Decoder, numArg int) error {
	cols := len(p.OutputFields)
	p.FieldValues = resizeSlice(p.FieldValues, numArg*cols)

	for i := 0; i < numArg; i++ {
		for j, f := range p.OutputFields {
			var err error
			if p.FieldValues[i*cols+j], err = f.decodeRes(dec); err != nil {
				p.DecodeErrors = append(p.DecodeErrors, &DecodeError{row: i, fieldName: f.Name(), s: err.Error()}) // collect decode / conversion errors
			}
		}
	}
	return dec.Error()
}
