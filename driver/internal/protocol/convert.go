package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	hdbreflect "github.com/SAP/go-hdb/driver/internal/reflect"
	"golang.org/x/text/transform"
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

var (
	timeReflectType   = hdbreflect.TypeFor[time.Time]()
	bytesReflectType  = hdbreflect.TypeFor[[]byte]()
	stringReflectType = hdbreflect.TypeFor[string]()
	ratReflectType    = hdbreflect.TypeFor[big.Rat]()
)

// ErrUint64OutOfRange means that a uint64 exceeds the size of a int64.
var ErrUint64OutOfRange = errors.New("uint64 values with high bit set are not supported")

// ErrIntegerOutOfRange means that an integer exceeds the size of the hdb integer field.
var ErrIntegerOutOfRange = errors.New("integer out of range error")

// ErrFloatOutOfRange means that a float exceeds the size of the hdb float field.
var ErrFloatOutOfRange = errors.New("float out of range error")

// A ConvertError is returned by conversion methods if a go datatype to hdb datatype conversion fails.
type ConvertError struct {
	err error
	tc  typeCode
	v   any
}

func (e *ConvertError) Error() string {
	if e.err == nil {
		return fmt.Sprintf("unsupported %[1]s conversion: %[2]T %[2]v", e.tc, e.v)
	}
	return fmt.Sprintf("unsupported %[1]s conversion: %[2]T %[2]v - %[3]s", e.tc, e.v, e.err)
}

// Unwrap returns the nested error.
func (e *ConvertError) Unwrap() error { return e.err }
func newConvertError(tc typeCode, v any, err error) *ConvertError {
	return &ConvertError{tc: tc, v: v, err: err}
}

/*
Conversion routines hdb parameters
  - return value is any to avoid allocations in case
    parameter is already of target type
*/
func convertBool(tc typeCode, v any) (any, error) {
	if v == nil {
		return v, nil
	}

	if v, ok := v.(bool); ok {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int() != 0, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint() != 0, nil
	case reflect.Float32, reflect.Float64:
		return rv.Float() != 0, nil
	case reflect.String:
		b, err := strconv.ParseBool(rv.String())
		if err != nil {
			return nil, newConvertError(tc, v, err)
		}
		return b, nil
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertBool(tc, rv.Elem().Interface())
	}

	if rv.Type().ConvertibleTo(stringReflectType) {
		return convertBool(tc, rv.Convert(stringReflectType).Interface())
	}
	return nil, newConvertError(tc, v, nil)
}

func convertInteger(tc typeCode, v any, min, max int64) (any, error) {
	if v == nil {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	// conversions without allocations (return v)
	case reflect.Bool:
		return v, nil // return (no furhter check needed)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i64 := rv.Int()
		if i64 > max || i64 < min {
			return nil, newConvertError(tc, v, ErrIntegerOutOfRange)
		}
		return i64, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64 := rv.Uint()
		if u64 >= 1<<63 {
			return nil, newConvertError(tc, v, ErrUint64OutOfRange)
		}
		if int64(u64) > max || int64(u64) < min {
			return nil, newConvertError(tc, v, ErrIntegerOutOfRange)
		}
		return u64, nil
	// conversions with allocations (return i64)
	case reflect.Float32, reflect.Float64:
		f64 := rv.Float()
		i64 := int64(f64)
		if f64 != float64(i64) { // should work for overflow, NaN, +-INF as well
			return nil, newConvertError(tc, v, nil)
		}
		if i64 > max || i64 < min {
			return nil, newConvertError(tc, v, ErrIntegerOutOfRange)
		}
		return i64, nil
	case reflect.String:
		i64, err := strconv.ParseInt(rv.String(), 10, 64)
		if err != nil {
			return nil, newConvertError(tc, v, err)
		}
		if i64 > max || i64 < min {
			return nil, newConvertError(tc, v, ErrIntegerOutOfRange)
		}
		return i64, nil
	// pointer
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertInteger(tc, rv.Elem().Interface(), min, max)
	}
	// last resort (try via string)
	if rv.Type().ConvertibleTo(stringReflectType) {
		return convertInteger(tc, rv.Convert(stringReflectType).Interface(), min, max)
	}
	return nil, newConvertError(tc, v, nil)
}

func convertFloat(tc typeCode, v any, max float64) (any, error) {
	if v == nil {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	// conversions without allocations (return v)
	case reflect.Float32, reflect.Float64:
		f64 := rv.Float()
		if math.Abs(f64) > max {
			return nil, newConvertError(tc, v, ErrFloatOutOfRange)
		}
		return f64, nil
	// conversions with allocations (return f64)
	case reflect.String:
		f64, err := strconv.ParseFloat(rv.String(), 64)
		if err != nil {
			return nil, newConvertError(tc, v, err)
		}
		if math.Abs(f64) > max {
			return nil, newConvertError(tc, v, ErrFloatOutOfRange)
		}
		return f64, nil
	// pointer
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertFloat(tc, rv.Elem().Interface(), max)
	}
	// last resort (try via string)
	if rv.Type().ConvertibleTo(stringReflectType) {
		return convertFloat(tc, rv.Convert(stringReflectType).Interface(), max)
	}
	return nil, newConvertError(tc, v, nil)
}

func convertTime(tc typeCode, v any) (any, error) {
	if v == nil {
		return nil, nil
	}

	if v, ok := v.(time.Time); ok {
		return v, nil
	}

	rv := reflect.ValueOf(v)

	if rv.Kind() == reflect.Ptr {
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertTime(tc, rv.Elem().Interface())
	}

	if rv.Type().ConvertibleTo(timeReflectType) {
		tv := rv.Convert(timeReflectType)
		return tv.Interface().(time.Time), nil
	}
	return nil, newConvertError(tc, v, nil)
}

/*
Currently the min, max check is done during encoding, as the check is expensive and
we want to avoid doing the conversion twice (convert + encode).
These checks could be done in convert only, but then we would need a
struct{m *big.Int, exp int} for decimals as intermediate format.

We would be able to accept other datatypes as well, like
int??, *big.Int, string, ...
but as the user needs to use Decimal anyway (scan), we go with
*big.Rat only for the time being.
*/
func convertDecimal(tc typeCode, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if v, ok := v.(*big.Rat); ok {
		return v, nil
	}

	rv := reflect.ValueOf(v)

	if rv.Kind() == reflect.Ptr {
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertDecimal(tc, rv.Elem().Interface())
	}

	if rv.Type().ConvertibleTo(ratReflectType) {
		tv := rv.Convert(ratReflectType)
		return tv.Interface().(big.Rat), nil
	}

	return nil, newConvertError(tc, v, nil)
}

func convertBytes(tc typeCode, v any) (any, error) {
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
		if rv.Type() == bytesReflectType {
			return rv.Bytes(), nil
		}

	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertBytes(tc, rv.Elem().Interface())
	}

	if rv.Type().ConvertibleTo(bytesReflectType) {
		bv := rv.Convert(bytesReflectType)
		return bv.Interface().([]byte), nil
	}
	return nil, newConvertError(tc, v, nil)
}

// readProvider is the interface wrapping the Reader which provides an io.Reader.
type readProvider interface {
	Reader() io.Reader
}

func convertToLobInDescr(t transform.Transformer, rd io.Reader) *LobInDescr {
	if t != nil { // cesu8Encoder
		rd = transform.NewReader(rd, t)
	}
	return newLobInDescr(rd)
}

func convertLob(t transform.Transformer, tc typeCode, v any) (any, error) {
	if v == nil {
		return v, nil
	}

	switch v := v.(type) {
	case io.Reader:
		return convertToLobInDescr(t, v), nil
	case readProvider:
		return convertToLobInDescr(t, v.Reader()), nil
	default:
		// check if string or []byte
		if v, err := convertBytes(tc, v); err == nil {
			switch v := v.(type) {
			case string:
				return convertToLobInDescr(t, strings.NewReader(v)), nil
			case []byte:
				return convertToLobInDescr(t, bytes.NewReader(v)), nil
			}
		}
	}

	rv := reflect.ValueOf(v)

	if rv.Kind() == reflect.Ptr {
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertLob(t, tc, rv.Elem().Interface())
	}

	return nil, fmt.Errorf("invalid lob type %[1]T value %[1]v", v)
}

func convertField(tc typeCode, v any, t transform.Transformer) (any, error) {
	switch tc {
	case tcBoolean:
		return convertBool(tc, v)
	case tcTinyint:
		return convertInteger(tc, v, minTinyint, maxTinyint)
	case tcSmallint:
		return convertInteger(tc, v, minSmallint, maxSmallint)
	case tcInteger:
		return convertInteger(tc, v, minInteger, maxInteger)
	case tcBigint:
		return convertInteger(tc, v, minBigint, maxBigint)
	case tcReal:
		return convertFloat(tc, v, maxReal)
	case tcDouble:
		return convertFloat(tc, v, maxDouble)
	case tcDate, tcTime, tcTimestamp, tcLongdate, tcSeconddate, tcDaydate, tcSecondtime:
		return convertTime(tc, v)
	case tcDecimal, tcFixed8, tcFixed12, tcFixed16:
		return convertDecimal(tc, v)
	case tcChar, tcVarchar, tcString, tcAlphanum, tcNchar, tcNvarchar, tcNstring, tcShorttext, tcBinary, tcVarbinary, tcStPoint, tcStGeometry:
		return convertBytes(tc, v)
	case tcBlob, tcClob, tcLocator:
		return convertLob(nil, tc, v)
	case tcNclob, tcText, tcNlocator:
		return convertLob(t, tc, v)
	case tcBintext: // ?? lobCESU8Type
		return convertLob(nil, tc, v)
	default:
		panic(fmt.Sprintf("invalid type code %s", tc))
	}
}
