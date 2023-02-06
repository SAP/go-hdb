package protocol

import (
	"database/sql"
	"reflect"
	"time"
)

// DataType is the type definition for data types supported by this package.
type DataType byte

// Data type constants.
const (
	DtUnknown DataType = iota // unknown data type
	DtBoolean
	DtTinyint
	DtSmallint
	DtInteger
	DtBigint
	DtReal
	DtDouble
	DtDecimal
	DtTime
	DtString
	DtBytes
	DtLob
	DtRows
)

// RegisterScanType registers driver owned datatype scantypes (e.g. Decimal, Lob).
func RegisterScanType(dt DataType, scanType, scanNullType reflect.Type) bool {
	scanTypes[dt].scanType = scanType
	scanTypes[dt].scanNullType = scanNullType
	return true
}

var scanTypes = []struct {
	scanType     reflect.Type
	scanNullType reflect.Type
}{
	DtUnknown:  {reflect.TypeOf((*any)(nil)).Elem(), reflect.TypeOf((*any)(nil)).Elem()},
	DtBoolean:  {reflect.TypeOf((*bool)(nil)).Elem(), reflect.TypeOf((*sql.NullBool)(nil)).Elem()},
	DtTinyint:  {reflect.TypeOf((*uint8)(nil)).Elem(), reflect.TypeOf((*sql.NullByte)(nil)).Elem()},
	DtSmallint: {reflect.TypeOf((*int16)(nil)).Elem(), reflect.TypeOf((*sql.NullInt16)(nil)).Elem()},
	DtInteger:  {reflect.TypeOf((*int32)(nil)).Elem(), reflect.TypeOf((*sql.NullInt32)(nil)).Elem()},
	DtBigint:   {reflect.TypeOf((*int64)(nil)).Elem(), reflect.TypeOf((*sql.NullInt64)(nil)).Elem()},
	DtReal:     {reflect.TypeOf((*float32)(nil)).Elem(), reflect.TypeOf((*sql.NullFloat64)(nil)).Elem()},
	DtDouble:   {reflect.TypeOf((*float64)(nil)).Elem(), reflect.TypeOf((*sql.NullFloat64)(nil)).Elem()},
	DtTime:     {reflect.TypeOf((*time.Time)(nil)).Elem(), reflect.TypeOf((*sql.NullTime)(nil)).Elem()},
	DtString:   {reflect.TypeOf((*string)(nil)).Elem(), reflect.TypeOf((*sql.NullString)(nil)).Elem()},
	DtBytes:    {reflect.TypeOf((*[]byte)(nil)).Elem(), reflect.TypeOf((*[]byte)(nil)).Elem()},
	DtDecimal:  {nil, nil}, // to be registered by driver
	DtLob:      {nil, nil}, // to be registered by driver
	DtRows:     {reflect.TypeOf((*sql.Rows)(nil)).Elem(), reflect.TypeOf((*sql.Rows)(nil)).Elem()},
}

// ScanType return the scan type (reflect.Type) of the corresponding data type.
func (dt DataType) ScanType(nullable bool) reflect.Type {
	if nullable {
		return scanTypes[dt].scanNullType
	}
	return scanTypes[dt].scanType
}
