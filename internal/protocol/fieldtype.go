/*
Copyright 2020 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package protocol

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"
	"unicode/utf8"

	"golang.org/x/text/transform"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
	"github.com/SAP/go-hdb/internal/unicode"
	"github.com/SAP/go-hdb/internal/unicode/cesu8"
)

const (
	minTinyint  = 0
	maxTinyint  = math.MaxUint8
	minSmallint = math.MinInt16
	maxSmallint = math.MaxInt16
	minInteger  = math.MinInt32
	maxInteger  = math.MaxInt32
	minBigint   = math.MinInt64
	maxBigint   = math.MaxInt64
	maxReal     = math.MaxFloat32
	maxDouble   = math.MaxFloat64
)

type locatorID uint64 // byte[locatorIdSize]

// ErrIntegerOutOfRange means that an integer exceeds the size of the hdb integer field.
var ErrIntegerOutOfRange = errors.New("integer out of range error")

// ErrFloatOutOfRange means that a float exceeds the size of the hdb float field.
var ErrFloatOutOfRange = errors.New("float out of range error")

var typeOfTime = reflect.TypeOf((*time.Time)(nil)).Elem()
var typeOfBytes = reflect.TypeOf((*[]byte)(nil)).Elem()

var zeroTime = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)

const (
	tinyintFieldSize    = 1
	smallintFieldSize   = 2
	integerFieldSize    = 4
	bigintFieldSize     = 8
	realFieldSize       = 4
	doubleFieldSize     = 8
	dateFieldSize       = 4
	timeFieldSize       = 4
	timestampFieldSize  = dateFieldSize + timeFieldSize
	longdateFieldSize   = 8
	seconddateFieldSize = 8
	daydateFieldSize    = 4
	secondtimeFieldSize = 4
	decimalFieldSize    = 16

	lobInputParametersSize = 9
)

// Converter is the interface that wraps the Convert method.
// Convert is used to convert query parameters from go datatypes to hdb datatypes.
type Converter interface {
	Convert(interface{}) (interface{}, error)
}

type fieldType interface {
	Converter
	prmSize(interface{}) int
	encodePrm(*encoding.Encoder, interface{}) error
}

// can use decoder for parameter and result fields
type commonDecoder interface {
	decode(*encoding.Decoder) (interface{}, error)
}

// specific parameter decoder
type prmDecoder interface {
	decodePrm(*encoding.Decoder) (interface{}, error)
}

// specific result decoder
type resDecoder interface {
	decodeRes(*encoding.Decoder) (interface{}, error)
}

/*
(*1)
HDB bug: secondtime null value cannot be set by setting high bit
- trying so, gives:
  SQL HdbError 1033 - error while parsing protocol: no such data type: type_code=192, index=2

Traffic analysis of python client (https://pypi.org/project/hdbcli) resulted in:
- set null value constant directly instead of using high bit

Please see handling of this special case in:
- fieldSize()
- writeParameterField()

*/

// parameter size
func prmSize(tc typeCode, arg driver.NamedValue) int {
	v := arg.Value
	if v == nil && tc != tcSecondtime { // secondTime exception (see (*1))
		return 0
	}
	return tc.fieldType().prmSize(v)
}

// encode parameter
func encodePrm(e *encoding.Encoder, tc typeCode, arg driver.NamedValue) error {
	v := arg.Value
	if v == nil && tc != tcSecondtime { // secondTime exception (see (*1))
		e.Byte(byte(tc) | 0x80) // type code null value: set high bit
		return nil
	}
	e.Byte(byte(tc)) // type code
	return tc.fieldType().encodePrm(e, v)
}

/*
decode parameter
- used for Sniffer
- type code is first byte (see encodePrm)
*/
func decodePrm(d *encoding.Decoder) (typeCode, interface{}, error) {
	tc := typeCode(d.Byte())
	if tc&0x80 != 0 { // high bit set -> null value
		return tc, nil, nil
	}

	ft := tc.fieldType()

	switch ft := ft.(type) {
	default:
		panic("field type missing decoder")
	case prmDecoder:
		v, err := ft.decodePrm(d)
		return tc, v, err
	case commonDecoder:
		v, err := ft.decode(d)
		return tc, v, err
	}
}

/*
decode result
*/
func decodeRes(d *encoding.Decoder, tc typeCode) (interface{}, error) {
	ft := tc.fieldType()

	switch ft := ft.(type) {
	default:
		panic("field type missing decoder")
	case resDecoder:
		return ft.decodeRes(d)
	case commonDecoder:
		return ft.decode(d)
	}
}

var (
	tinyintType    = _tinyintType{}
	smallintType   = _smallintType{}
	integerType    = _integerType{}
	bigintType     = _bigintType{}
	realType       = _realType{}
	doubleType     = _doubleType{}
	dateType       = _dateType{}
	timeType       = _timeType{}
	timestampType  = _timestampType{}
	longdateType   = _longdateType{}
	seconddateType = _seconddateType{}
	daydateType    = _daydateType{}
	secondtimeType = _secondtimeType{}
	decimalType    = _decimalType{}
	varType        = _varType{}
	cesu8Type      = _cesu8Type{}
	lobVarType     = _lobVarType{}
	lobCESU8Type   = _lobCESU8Type{}
)

type _tinyintType struct{}
type _smallintType struct{}
type _integerType struct{}
type _bigintType struct{}
type _realType struct{}
type _doubleType struct{}
type _dateType struct{}
type _timeType struct{}
type _timestampType struct{}
type _longdateType struct{}
type _seconddateType struct{}
type _daydateType struct{}
type _secondtimeType struct{}
type _decimalType struct{}
type _varType struct{}
type _cesu8Type struct{}
type _lobVarType struct{}
type _lobCESU8Type struct{}

var (
	_ fieldType = (*_tinyintType)(nil)
	_ fieldType = (*_smallintType)(nil)
	_ fieldType = (*_integerType)(nil)
	_ fieldType = (*_bigintType)(nil)
	_ fieldType = (*_realType)(nil)
	_ fieldType = (*_doubleType)(nil)
	_ fieldType = (*_dateType)(nil)
	_ fieldType = (*_timeType)(nil)
	_ fieldType = (*_timestampType)(nil)
	_ fieldType = (*_longdateType)(nil)
	_ fieldType = (*_seconddateType)(nil)
	_ fieldType = (*_daydateType)(nil)
	_ fieldType = (*_secondtimeType)(nil)
	_ fieldType = (*_decimalType)(nil)
	_ fieldType = (*_varType)(nil)
	_ fieldType = (*_cesu8Type)(nil)
	_ fieldType = (*_lobVarType)(nil)
	_ fieldType = (*_lobCESU8Type)(nil)
)

// A ConvertError is returned by conversion methods if a go datatype to hdb datatype conversion fails.
type ConvertError struct {
	ft fieldType
	v  interface{}
}

func (e *ConvertError) Error() string {
	return fmt.Sprintf("unsupported %[1]s conversion: %[2]T %[2]v", e.ft, e.v)
}

func newConvertError(ft fieldType, v interface{}) *ConvertError {
	return &ConvertError{ft: ft, v: v}
}

func (_tinyintType) String() string    { return "tinyintType" }
func (_smallintType) String() string   { return "smallintType" }
func (_integerType) String() string    { return "integerType" }
func (_bigintType) String() string     { return "bigintType" }
func (_realType) String() string       { return "realType" }
func (_doubleType) String() string     { return "doubleType" }
func (_dateType) String() string       { return "dateType" }
func (_timeType) String() string       { return "timeType" }
func (_timestampType) String() string  { return "timestampType" }
func (_longdateType) String() string   { return "longdateType" }
func (_seconddateType) String() string { return "seconddateType" }
func (_daydateType) String() string    { return "daydateType" }
func (_secondtimeType) String() string { return "secondtimeType" }
func (_decimalType) String() string    { return "decimalType" }
func (_varType) String() string        { return "varType" }
func (_cesu8Type) String() string      { return "cesu8Type" }
func (_lobVarType) String() string     { return "lobVarType" }
func (_lobCESU8Type) String() string   { return "lobCESU8Type" }

func (ft _tinyintType) Convert(v interface{}) (interface{}, error) {
	return convertInteger(ft, v, minTinyint, maxTinyint)
}
func (ft _smallintType) Convert(v interface{}) (interface{}, error) {
	return convertInteger(ft, v, minSmallint, maxSmallint)
}
func (ft _integerType) Convert(v interface{}) (interface{}, error) {
	return convertInteger(ft, v, minInteger, maxInteger)
}
func (ft _bigintType) Convert(v interface{}) (interface{}, error) {
	return convertInteger(ft, v, minBigint, maxBigint)
}

// integer types
func convertInteger(ft fieldType, v interface{}, min, max int64) (driver.Value, error) {
	if v == nil {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {

	// bool is represented in HDB as tinyint
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i64 := rv.Int()
		if i64 > max || i64 < min {
			return nil, ErrIntegerOutOfRange
		}
		return i64, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64 := rv.Uint()
		if u64 > uint64(max) {
			return nil, ErrIntegerOutOfRange
		}
		return int64(u64), nil
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertInteger(ft, rv.Elem().Interface(), min, max)
	}
	return nil, newConvertError(ft, v)
}

func (ft _realType) Convert(v interface{}) (interface{}, error) { return convertFloat(ft, v, maxReal) }
func (ft _doubleType) Convert(v interface{}) (interface{}, error) {
	return convertFloat(ft, v, maxDouble)
}

// float types
func convertFloat(ft fieldType, v interface{}, max float64) (driver.Value, error) {
	if v == nil {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {

	case reflect.Float32, reflect.Float64:
		f64 := rv.Float()
		if math.Abs(f64) > max {
			return nil, ErrFloatOutOfRange
		}
		return f64, nil
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertFloat(ft, rv.Elem().Interface(), max)
	}
	return nil, newConvertError(ft, v)
}

func (ft _dateType) Convert(v interface{}) (interface{}, error)       { return convertTime(ft, v) }
func (ft _timeType) Convert(v interface{}) (interface{}, error)       { return convertTime(ft, v) }
func (ft _timestampType) Convert(v interface{}) (interface{}, error)  { return convertTime(ft, v) }
func (ft _longdateType) Convert(v interface{}) (interface{}, error)   { return convertTime(ft, v) }
func (ft _seconddateType) Convert(v interface{}) (interface{}, error) { return convertTime(ft, v) }
func (ft _daydateType) Convert(v interface{}) (interface{}, error)    { return convertTime(ft, v) }
func (ft _secondtimeType) Convert(v interface{}) (interface{}, error) { return convertTime(ft, v) }

// time
func convertTime(ft fieldType, v interface{}) (driver.Value, error) {
	if v == nil {
		return nil, nil
	}

	switch v := v.(type) {

	case time.Time:
		return v, nil
	}

	rv := reflect.ValueOf(v)

	switch rv.Kind() {

	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertTime(ft, rv.Elem().Interface())
	}

	if rv.Type().ConvertibleTo(typeOfTime) {
		tv := rv.Convert(typeOfTime)
		return tv.Interface().(time.Time), nil
	}
	return nil, newConvertError(ft, v)
}

func (ft _decimalType) Convert(v interface{}) (interface{}, error) { return convertDecimal(ft, v) }

// decimal
func convertDecimal(ft fieldType, v interface{}) (driver.Value, error) {
	if v == nil {
		return nil, nil
	}
	if v, ok := v.([]byte); ok {
		return v, nil
	}
	return nil, newConvertError(ft, v)
}

func (ft _varType) Convert(v interface{}) (interface{}, error)   { return convertBytes(ft, v) }
func (ft _cesu8Type) Convert(v interface{}) (interface{}, error) { return convertBytes(ft, v) }

// bytes
func convertBytes(ft fieldType, v interface{}) (driver.Value, error) {
	if v == nil {
		return v, nil
	}

	switch v := v.(type) {

	case string, []byte:
		return v, nil
	}

	rv := reflect.ValueOf(v)

	switch rv.Kind() {

	case reflect.String:
		return rv.String(), nil

	case reflect.Slice:
		if rv.Type() == typeOfBytes {
			return rv.Bytes(), nil
		}

	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertBytes(ft, rv.Elem().Interface())
	}

	if rv.Type().ConvertibleTo(typeOfBytes) {
		bv := rv.Convert(typeOfBytes)
		return bv.Interface().([]byte), nil
	}
	return nil, newConvertError(ft, v)
}

func (ft _lobVarType) Convert(v interface{}) (interface{}, error)   { return convertLob(false, ft, v) }
func (ft _lobCESU8Type) Convert(v interface{}) (interface{}, error) { return convertLob(true, ft, v) }

// ReadProvider is the interface wrapping the Reader which provides an io.Reader.
type ReadProvider interface {
	Reader() io.Reader
}

// Lob
func convertLob(isCharBased bool, ft fieldType, v interface{}) (driver.Value, error) {
	if v == nil {
		return v, nil
	}

	switch v := v.(type) {
	case io.Reader:
		return v, nil
	case ReadProvider:
		return v.Reader(), nil
	default:
		return nil, newConvertError(ft, v)
	}
}

func (_tinyintType) prmSize(interface{}) int    { return tinyintFieldSize }
func (_smallintType) prmSize(interface{}) int   { return smallintFieldSize }
func (_integerType) prmSize(interface{}) int    { return integerFieldSize }
func (_bigintType) prmSize(interface{}) int     { return bigintFieldSize }
func (_realType) prmSize(interface{}) int       { return realFieldSize }
func (_doubleType) prmSize(interface{}) int     { return doubleFieldSize }
func (_dateType) prmSize(interface{}) int       { return dateFieldSize }
func (_timeType) prmSize(interface{}) int       { return timeFieldSize }
func (_timestampType) prmSize(interface{}) int  { return timestampFieldSize }
func (_longdateType) prmSize(interface{}) int   { return longdateFieldSize }
func (_seconddateType) prmSize(interface{}) int { return seconddateFieldSize }
func (_daydateType) prmSize(interface{}) int    { return daydateFieldSize }
func (_secondtimeType) prmSize(interface{}) int { return secondtimeFieldSize }
func (_decimalType) prmSize(interface{}) int    { return decimalFieldSize }
func (_lobVarType) prmSize(v interface{}) int   { return lobInputParametersSize }
func (_lobCESU8Type) prmSize(v interface{}) int { return lobInputParametersSize }

func (ft _varType) prmSize(v interface{}) int {
	switch v := v.(type) {
	case []byte:
		return varBytesSize(ft, len(v))
	case string:
		return varBytesSize(ft, len(v))
	default:
		return -1
	}
}
func (ft _cesu8Type) prmSize(v interface{}) int {
	switch v := v.(type) {
	case []byte:
		return varBytesSize(ft, cesu8.Size(v))
	case string:
		return varBytesSize(ft, cesu8.StringSize(v))
	default:
		return -1
	}
}

func varBytesSize(ft fieldType, size int) int {
	switch {
	default:
		return -1
	case size <= int(bytesLenIndSmall):
		return size + 1
	case size <= math.MaxInt16:
		return size + 3
	case size <= math.MaxInt32:
		return size + 5
	}
}

func (ft _tinyintType) encodePrm(e *encoding.Encoder, v interface{}) error {
	i, err := asInt64(ft, v)
	if err != nil {
		return err
	}
	e.Byte(byte(i))
	return nil
}
func (ft _smallintType) encodePrm(e *encoding.Encoder, v interface{}) error {
	i, err := asInt64(ft, v)
	if err != nil {
		return err
	}
	e.Int16(int16(i))
	return nil
}
func (ft _integerType) encodePrm(e *encoding.Encoder, v interface{}) error {
	i, err := asInt64(ft, v)
	if err != nil {
		return err
	}
	e.Int32(int32(i))
	return nil
}
func (ft _bigintType) encodePrm(e *encoding.Encoder, v interface{}) error {
	i, err := asInt64(ft, v)
	if err != nil {
		return err
	}
	e.Int64(i)
	return nil
}

func asInt64(ft fieldType, v interface{}) (int64, error) {
	switch v := v.(type) {
	default:
		return 0, newConvertError(ft, v)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case int64:
		return v, nil
	}
}

func (ft _realType) encodePrm(e *encoding.Encoder, v interface{}) error {
	switch v := v.(type) {
	case float64:
		e.Float32(float32(v))
		return nil
	default:
		return newConvertError(ft, v)
	}
}
func (ft _doubleType) encodePrm(e *encoding.Encoder, v interface{}) error {
	switch v := v.(type) {
	case float64:
		e.Float64(v)
		return nil
	default:
		return newConvertError(ft, v)
	}
}

func (ft _dateType) encodePrm(e *encoding.Encoder, v interface{}) error {
	t, err := asTime(ft, v)
	if err != nil {
		return err
	}
	encodeDate(e, t)
	return nil
}
func (ft _timeType) encodePrm(e *encoding.Encoder, v interface{}) error {
	t, err := asTime(ft, v)
	if err != nil {
		return err
	}
	encodeTime(e, t)
	return nil
}
func (ft _timestampType) encodePrm(e *encoding.Encoder, v interface{}) error {
	t, err := asTime(ft, v)
	if err != nil {
		return err
	}
	encodeDate(e, t)
	encodeTime(e, t)
	return nil
}

func encodeDate(e *encoding.Encoder, t time.Time) {
	// year: set most sig bit
	// month 0 based
	year, month, day := t.Date()
	e.Uint16(uint16(year) | 0x8000)
	e.Int8(int8(month) - 1)
	e.Int8(int8(day))
}

func encodeTime(e *encoding.Encoder, t time.Time) {
	e.Byte(byte(t.Hour()) | 0x80)
	e.Int8(int8(t.Minute()))
	millisecs := t.Second()*1000 + t.Nanosecond()/1000000
	e.Uint16(uint16(millisecs))
}

func (ft _longdateType) encodePrm(e *encoding.Encoder, v interface{}) error {
	t, err := asTime(ft, v)
	if err != nil {
		return err
	}
	e.Int64(convertTimeToLongdate(t))
	return nil
}
func (ft _seconddateType) encodePrm(e *encoding.Encoder, v interface{}) error {
	t, err := asTime(ft, v)
	if err != nil {
		return err
	}
	e.Int64(convertTimeToSeconddate(t))
	return nil
}
func (ft _daydateType) encodePrm(e *encoding.Encoder, v interface{}) error {
	t, err := asTime(ft, v)
	if err != nil {
		return err
	}
	e.Int32(int32(convertTimeToDayDate(t)))
	return nil
}
func (ft _secondtimeType) encodePrm(e *encoding.Encoder, v interface{}) error {
	if v == nil {
		e.Int32(secondtimeNullValue)
		return nil
	}
	t, err := asTime(ft, v)
	if err != nil {
		return err
	}
	e.Int32(int32(convertTimeToSecondtime(t)))
	return nil
}

func asTime(ft fieldType, v interface{}) (time.Time, error) {
	t, ok := v.(time.Time)
	if !ok {
		return zeroTime, newConvertError(ft, v)
	}
	//store in utc
	return t.UTC(), nil
}

func (ft _decimalType) encodePrm(e *encoding.Encoder, v interface{}) error {
	p, ok := v.([]byte)
	if !ok {
		return newConvertError(ft, v)
	}
	if len(p) != decimalFieldSize {
		return fmt.Errorf("invalid argument length %d - expected %d", len(p), decimalFieldSize)
	}
	e.Bytes(p)
	return nil
}

func (ft _varType) encodePrm(e *encoding.Encoder, v interface{}) error {
	switch v := v.(type) {
	case []byte:
		return encodeVarBytes(e, v)
	case string:
		return encodeVarString(e, v)
	default:
		return newConvertError(ft, v)
	}
}

func encodeVarBytesSize(e *encoding.Encoder, size int) error {
	switch {
	default:
		return fmt.Errorf("max argument length %d of string exceeded", size)
	case size <= int(bytesLenIndSmall):
		e.Byte(byte(size))
	case size <= math.MaxInt16:
		e.Byte(bytesLenIndMedium)
		e.Int16(int16(size))
	case size <= math.MaxInt32:
		e.Byte(bytesLenIndBig)
		e.Int32(int32(size))
	}
	return nil
}

func encodeVarBytes(e *encoding.Encoder, p []byte) error {
	if err := encodeVarBytesSize(e, len(p)); err != nil {
		return err
	}
	e.Bytes(p)
	return nil
}

func encodeVarString(e *encoding.Encoder, s string) error {
	if err := encodeVarBytesSize(e, len(s)); err != nil {
		return err
	}
	e.String(s)
	return nil
}

func (ft _cesu8Type) encodePrm(e *encoding.Encoder, v interface{}) error {
	switch v := v.(type) {
	case []byte:
		return encodeCESU8Bytes(e, v)
	case string:
		return encodeCESU8String(e, v)
	default:
		return newConvertError(ft, v)
	}
}

func encodeCESU8Bytes(e *encoding.Encoder, p []byte) error {
	size := cesu8.Size(p)
	if err := encodeVarBytesSize(e, size); err != nil {
		return err
	}
	e.CESU8Bytes(p)
	return nil
}

func encodeCESU8String(e *encoding.Encoder, s string) error {
	size := cesu8.StringSize(s)
	if err := encodeVarBytesSize(e, size); err != nil {
		return err
	}
	e.CESU8String(s)
	return nil
}

func (_lobVarType) encodePrm(e *encoding.Encoder, v interface{}) error {
	// TODO: first write: add content? - currently no data transferred
	e.Byte(0)
	e.Int32(0)
	e.Int32(0)
	return nil
}

func (_lobCESU8Type) encodePrm(e *encoding.Encoder, v interface{}) error {
	// TODO: first write: add content? - currently no data transferred
	e.Byte(0)
	e.Int32(0)
	e.Int32(0)
	return nil
}

func (_tinyintType) decodePrm(d *encoding.Decoder) (interface{}, error)  { return int64(d.Byte()), nil }
func (_smallintType) decodePrm(d *encoding.Decoder) (interface{}, error) { return int64(d.Int16()), nil }
func (_integerType) decodePrm(d *encoding.Decoder) (interface{}, error)  { return int64(d.Int32()), nil }
func (_bigintType) decodePrm(d *encoding.Decoder) (interface{}, error)   { return d.Int64(), nil }

func (ft _tinyintType) decodeRes(d *encoding.Decoder) (interface{}, error) {
	if !d.Bool() { //null value
		return nil, nil
	}
	return ft.decodePrm(d)
}
func (ft _smallintType) decodeRes(d *encoding.Decoder) (interface{}, error) {
	if !d.Bool() { //null value
		return nil, nil
	}
	return ft.decodePrm(d)
}
func (ft _integerType) decodeRes(d *encoding.Decoder) (interface{}, error) {
	if !d.Bool() { //null value
		return nil, nil
	}
	return ft.decodePrm(d)
}
func (ft _bigintType) decodeRes(d *encoding.Decoder) (interface{}, error) {
	if !d.Bool() { //null value
		return nil, nil
	}
	return ft.decodePrm(d)
}

func (_realType) decode(d *encoding.Decoder) (interface{}, error) {
	v := d.Uint32()
	if v == realNullValue {
		return nil, nil
	}
	return float64(math.Float32frombits(v)), nil
}
func (_doubleType) decode(d *encoding.Decoder) (interface{}, error) {
	v := d.Uint64()
	if v == doubleNullValue {
		return nil, nil
	}
	return math.Float64frombits(v), nil
}

func (_dateType) decode(d *encoding.Decoder) (interface{}, error) {
	year, month, day, null := decodeDate(d)
	if null {
		return nil, nil
	}
	return time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC), nil
}
func (_timeType) decode(d *encoding.Decoder) (interface{}, error) {
	// time read gives only seconds (cut), no milliseconds
	hour, minute, nanosecs, null := decodeTime(d)
	if null {
		return nil, nil
	}
	return time.Date(1, 1, 1, hour, minute, 0, nanosecs, time.UTC), nil
}
func (_timestampType) decode(d *encoding.Decoder) (interface{}, error) {
	year, month, day, dateNull := decodeDate(d)
	hour, minute, nanosecs, timeNull := decodeTime(d)
	if dateNull || timeNull {
		return nil, nil
	}
	return time.Date(year, month, day, hour, minute, 0, nanosecs, time.UTC), nil
}

// null values: most sig bit unset
// year: unset second most sig bit (subtract 2^15)
// --> read year as unsigned
// month is 0-based
// day is 1 byte
func decodeDate(d *encoding.Decoder) (int, time.Month, int, bool) {
	year := d.Uint16()
	null := ((year & 0x8000) == 0) //null value
	year &= 0x3fff
	month := d.Int8()
	month++
	day := d.Int8()
	return int(year), time.Month(month), int(day), null
}

func decodeTime(d *encoding.Decoder) (int, int, int, bool) {
	hour := d.Byte()
	null := (hour & 0x80) == 0 //null value
	hour &= 0x7f
	minute := d.Int8()
	millisecs := d.Uint16()
	nanosecs := int(millisecs) * 1000000
	return int(hour), int(minute), nanosecs, null
}

func (_longdateType) decode(d *encoding.Decoder) (interface{}, error) {
	longdate := d.Int64()
	if longdate == longdateNullValue {
		return nil, nil
	}
	return convertLongdateToTime(longdate), nil
}
func (_seconddateType) decode(d *encoding.Decoder) (interface{}, error) {
	seconddate := d.Int64()
	if seconddate == seconddateNullValue {
		return nil, nil
	}
	return convertSeconddateToTime(seconddate), nil
}
func (_daydateType) decode(d *encoding.Decoder) (interface{}, error) {
	daydate := d.Int32()
	if daydate == daydateNullValue {
		return nil, nil
	}
	return convertDaydateToTime(int64(daydate)), nil
}
func (_secondtimeType) decode(d *encoding.Decoder) (interface{}, error) {
	secondtime := d.Int32()
	if secondtime == secondtimeNullValue {
		return nil, nil
	}
	return convertSecondtimeToTime(int(secondtime)), nil
}

func (_decimalType) decode(d *encoding.Decoder) (interface{}, error) {
	b := make([]byte, decimalFieldSize)
	d.Bytes(b)
	if (b[15] & 0x70) == 0x70 { //null value (bit 4,5,6 set)
		return nil, nil
	}
	return b, nil
}

func (_varType) decode(d *encoding.Decoder) (interface{}, error) {
	size, null := decodeVarBytesSize(d)
	if null {
		return nil, nil
	}
	b := make([]byte, size)
	d.Bytes(b)
	return b, nil
}
func (_cesu8Type) decode(d *encoding.Decoder) (interface{}, error) {
	size, null := decodeVarBytesSize(d)
	if null {
		return nil, nil
	}
	return d.CESU8Bytes(size), nil
}

func decodeVarBytesSize(d *encoding.Decoder) (int, bool) {
	ind := d.Byte() //length indicator
	switch {
	default:
		return 0, false
	case ind == bytesLenIndNullValue:
		return 0, true
	case ind <= bytesLenIndSmall:
		return int(ind), false
	case ind == bytesLenIndMedium:
		return int(d.Int16()), false
	case ind == bytesLenIndBig:
		return int(d.Int32()), false
	}
}

func (_lobVarType) decodePrm(d *encoding.Decoder) (interface{}, error) {
	// TODO used for Sniffer - check encodePrm
	d.Byte()
	d.Int32()
	d.Int32()
	return nil, nil
}

func (_lobCESU8Type) decodePrm(d *encoding.Decoder) (interface{}, error) {
	// TODO used for Sniffer - check encodePrm
	d.Byte()
	d.Int32()
	d.Int32()
	return nil, nil
}

func decodeLobRes(d *encoding.Decoder, isCharBased bool) (interface{}, error) {
	d.Int8() // type code (is int here)
	opt := d.Int8()
	null := (lobOptions(opt) & loNullindicator) != 0
	if null {
		return nil, nil
	}
	eof := (lobOptions(opt) & loLastdata) != 0
	d.Skip(2)

	charLen := d.Int64()
	byteLen := d.Int64()
	id := d.Uint64()
	chunkLen := d.Int32()

	// TODO set session and chunksize ????
	lcw := newChunkWriter(isCharBased, nil, locatorID(id), 4096, charLen, byteLen)
	if err := lcw.write(d, int(chunkLen), eof); err != nil {
		return lcw, err
	}
	return lcw, nil
}

func (_lobVarType) decodeRes(d *encoding.Decoder) (interface{}, error) {
	return decodeLobRes(d, false)
}

func (_lobCESU8Type) decodeRes(d *encoding.Decoder) (interface{}, error) {
	return decodeLobRes(d, true)
}

// chunkReader reads in chunks for writing to db.
type chunkReader interface {
	locatorID() locatorID
	next() int
	bytes() ([]byte, error)
	eof() bool
}

func newChunkReader(isCharBased bool, id locatorID, chunkSize int, rd io.Reader) chunkReader {
	if isCharBased {
		rd = transform.NewReader(rd, unicode.Utf8ToCesu8Transformer) // CESU8 transformer
	}
	return &_chunkReader{id: id, chunkSize: chunkSize, rd: rd}
}

type _chunkReader struct {
	id        locatorID
	chunkSize int
	rd        io.Reader
	b         []byte
	err       error
}

func (r *_chunkReader) locatorID() locatorID { return r.id }
func (r *_chunkReader) eof() bool            { return r.err == io.EOF }
func (r *_chunkReader) bytes() ([]byte, error) {
	if r.err == io.EOF {
		return r.b, nil
	}
	return r.b, r.err
}

func (r *_chunkReader) next() int {
	if r.err != nil {
		return 0
	}
	r.b = sizeBuffer(r.b, r.chunkSize)
	size, err := r.rd.Read(r.b)
	r.b = r.b[:size]
	r.err = err
	return size
}

// WriterSetter is the interface wrapping the SetWriter method (Lob handling).
type WriterSetter interface {
	SetWriter(w io.Writer) error
}

// TODO eliminate
type sessionSetter interface {
	setSession(s *Session)
}

// lobChunkWriter reads db lob chunks and writes them into lob field io.Writer.
type chunkWriter interface {
	sessionSetter
	WriterSetter

	id() locatorID
	write(dec *encoding.Decoder, size int, eof bool) error
	readOfsLen() (int64, int32)
	eof() bool
}

func newChunkWriter(isCharBased bool, s *Session, id locatorID, chunkSize int32, charLen, byteLen int64) chunkWriter {
	if isCharBased {
		return &charChunkWriter{s: s, _id: id, chunkSize: chunkSize, charLen: charLen, byteLen: byteLen}
	}
	return &binaryChunkWriter{s: s, _id: id, chunkSize: chunkSize, charLen: charLen, byteLen: byteLen}
}

// binaryChunkWriter (byte based lobs).
type binaryChunkWriter struct {
	s *Session

	_id       locatorID
	chunkSize int32
	charLen   int64
	byteLen   int64

	readOfs int64
	_eof    bool

	ofs int

	wr io.Writer

	b []byte
}

func (l *binaryChunkWriter) setSession(s *Session) { l.s = s }
func (l *binaryChunkWriter) SetWriter(wr io.Writer) error {
	l.wr = wr
	if err := l.flush(); err != nil {
		return err
	}
	return l.s.decodeLobs(l)
}

func (l *binaryChunkWriter) id() locatorID { return l._id }
func (l *binaryChunkWriter) eof() bool     { return l._eof }

func (l *binaryChunkWriter) write(dec *encoding.Decoder, size int, eof bool) error {
	l._eof = eof // store eof

	if size == 0 {
		return nil
	}

	l.b = resizeBuffer(l.b, size+l.ofs)
	dec.Bytes(l.b[l.ofs:])
	if l.wr != nil {
		return l.flush()
	}
	return nil
}

func (l *binaryChunkWriter) readOfsLen() (int64, int32) {
	readLen := l.charLen - l.readOfs
	if readLen > int64(math.MaxInt32) || readLen > int64(l.chunkSize) {
		return l.readOfs, l.chunkSize
	}
	return l.readOfs, int32(readLen)
}

func (l *binaryChunkWriter) flush() error {
	if _, err := l.wr.Write(l.b); err != nil {
		return err
	}
	l.readOfs += int64(len(l.b))
	return nil
}

type charChunkWriter struct {
	s *Session

	_id       locatorID
	chunkSize int32
	charLen   int64
	byteLen   int64

	readOfs int64
	_eof    bool

	ofs int

	wr io.Writer

	b []byte
}

func (l *charChunkWriter) setSession(s *Session) { l.s = s }
func (l *charChunkWriter) SetWriter(wr io.Writer) error {
	l.wr = wr
	if err := l.flush(); err != nil {
		return err
	}
	return l.s.decodeLobs(l)
}

func (l *charChunkWriter) id() locatorID { return l._id }
func (l *charChunkWriter) eof() bool     { return l._eof }

func (l *charChunkWriter) write(dec *encoding.Decoder, size int, eof bool) error {
	l._eof = eof // store eof

	if size == 0 {
		return nil
	}

	l.b = resizeBuffer(l.b, size+l.ofs)
	dec.Bytes(l.b[l.ofs:])
	if l.wr != nil {
		return l.flush()
	}
	return nil
}

func (l *charChunkWriter) readOfsLen() (int64, int32) {
	readLen := l.charLen - l.readOfs
	if readLen > int64(math.MaxInt32) || readLen > int64(l.chunkSize) {
		return l.readOfs, l.chunkSize
	}
	return l.readOfs, int32(readLen)
}

func (l *charChunkWriter) flush() error {
	nDst, nSrc, err := unicode.Cesu8ToUtf8Transformer.Transform(l.b, l.b, true) // inline cesu8 to utf8 transformation
	if err != nil && err != transform.ErrShortSrc {
		return err
	}
	if _, err := l.wr.Write(l.b[:nDst]); err != nil {
		return err
	}
	l.ofs = len(l.b) - nSrc
	if l.ofs != 0 && l.ofs != cesu8.CESUMax/2 { // assert remaining bytes
		return unicode.ErrInvalidCesu8
	}
	l.readOfs += int64(l.runeCount(l.b[:nDst]))
	if l.ofs != 0 {
		l.readOfs++                   // add half encoding
		copy(l.b, l.b[nSrc:len(l.b)]) // move half encoding to buffer begin
	}
	return nil
}

// Caution: hdb counts 4 byte utf-8 encodings (cesu-8 6 bytes) as 2 (3 byte) chars
func (l *charChunkWriter) runeCount(b []byte) int {
	numChars := 0
	for len(b) > 0 {
		_, size := utf8.DecodeRune(b)
		b = b[size:]
		numChars++
		if size == utf8.UTFMax {
			numChars++
		}
	}
	return numChars
}
