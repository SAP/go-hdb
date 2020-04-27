/*
Copyright 2014 SAP SE

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
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

//go:generate stringer -type=DataType

// DataType is the type definition for data types supported by this package.
type DataType byte

// Data type constants.
const (
	DtUnknown DataType = iota // unknown data type
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
func RegisterScanType(dt DataType, scanType reflect.Type) {
	scanTypeMap[dt] = scanType
}

var scanTypeMap = map[DataType]reflect.Type{
	DtUnknown:  reflect.TypeOf((*interface{})(nil)).Elem(),
	DtTinyint:  reflect.TypeOf((*uint8)(nil)).Elem(),
	DtSmallint: reflect.TypeOf((*int16)(nil)).Elem(),
	DtInteger:  reflect.TypeOf((*int32)(nil)).Elem(),
	DtBigint:   reflect.TypeOf((*int64)(nil)).Elem(),
	DtReal:     reflect.TypeOf((*float32)(nil)).Elem(),
	DtDouble:   reflect.TypeOf((*float64)(nil)).Elem(),
	DtTime:     reflect.TypeOf((*time.Time)(nil)).Elem(),
	DtString:   reflect.TypeOf((*string)(nil)).Elem(),
	DtBytes:    reflect.TypeOf((*[]byte)(nil)).Elem(),
	DtDecimal:  nil, // to be registered by driver
	DtLob:      nil, // to be registered by driver
	DtRows:     reflect.TypeOf((*sql.Rows)(nil)).Elem(),
}

// ScanType return the scan type (reflect.Type) of the corresponding data type.
func (dt DataType) ScanType() reflect.Type {
	st, ok := scanTypeMap[dt]
	if !ok {
		panic(fmt.Sprintf("Missing ScanType for DataType %s", dt))
	}
	return st
}
