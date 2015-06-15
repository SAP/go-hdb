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

package driver

import (
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"time"

	p "github.com/SAP/go-hdb/internal/protocol"
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

/*
const (
	realNullValue   uint32 = ^uint32(0)
	doubleNullValue uint64 = ^uint64(0)
)
*/

type intOutOfRangeError struct {
	v, min, max int64
}

func newIntOutOfRangeError(v, min, max int64) error {
	return &intOutOfRangeError{
		v:   v,
		min: min,
		max: max,
	}
}

func (e *intOutOfRangeError) Error() string {
	return fmt.Sprintf("value %d out of range (%d - %d)", e.v, e.min, e.max)
}

type uintOutOfRangeError struct {
	v   uint64
	max int64
}

func newUintOutOfRangeError(v uint64, max int64) error {
	return &uintOutOfRangeError{
		v:   v,
		max: max,
	}
}

func (e *uintOutOfRangeError) Error() string {
	return fmt.Sprintf("value %d out of range (0 - %d)", e.v, e.max)
}

type invalidValueTypeError struct {
	v interface{}
}

func newInvalidValueTypeError(v interface{}) error {
	return &invalidValueTypeError{v: v}
}

func (e *invalidValueTypeError) Error() string {
	return fmt.Sprintf("invalid value %v (type %T)", e.v, e.v)
}

func columnConverter(dt p.DataType) driver.ValueConverter {

	switch dt {

	default:
		return dbUnknownType{}
	case p.DtTinyint:
		return dbTinyint
	case p.DtSmallint:
		return dbSmallint
	case p.DtInt:
		return dbInt
	case p.DtBigint:
		return dbBigint
	case p.DtReal:
		return dbReal
	case p.DtDouble:
		return dbDouble
	case p.DtTime:
		return dbTime
	case p.DtDecimal:
		return dbDecimal
	case p.DtVarchar:
		return dbVarchar
	case p.DtNvarchar:
		return dbNvarchar
	case p.DtLob:
		return dbLob
	}
}

// unknown type
type dbUnknownType struct {
	//tc p.TypeCode
}

var _ driver.ValueConverter = dbUnknownType{} //check that type implements interface

func (t dbUnknownType) ConvertValue(v interface{}) (driver.Value, error) {
	return nil, fmt.Errorf("column converter for data type %s is not implemented")
}

// int types
var dbTinyint = dbIntType{min: minTinyint, max: maxTinyint}
var dbSmallint = dbIntType{min: minSmallint, max: maxSmallint}
var dbInt = dbIntType{min: minInteger, max: maxInteger}
var dbBigint = dbIntType{min: minBigint, max: maxBigint}

type dbIntType struct {
	min int64
	max int64
}

var _ driver.ValueConverter = dbIntType{} //check that type implements interface

func (i dbIntType) ConvertValue(v interface{}) (driver.Value, error) {

	if v == nil {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i64 := rv.Int()
		if i64 > i.max || i64 < i.min {
			return nil, newIntOutOfRangeError(i64, i.min, i.max)
		}
		return i64, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64 := rv.Uint()
		if u64 > uint64(i.max) {
			return nil, newUintOutOfRangeError(u64, i.max)
		}
		return int64(u64), nil
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return i.ConvertValue(rv.Elem().Interface())
	}
	return nil, newInvalidValueTypeError(v)
}

//float types
var dbReal = dbFloatType{max: maxReal}
var dbDouble = dbFloatType{max: maxDouble}

type dbFloatType struct {
	max float64
}

var _ driver.ValueConverter = dbFloatType{} //check that type implements interface

func (f dbFloatType) ConvertValue(v interface{}) (driver.Value, error) {

	if v == nil {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {

	case reflect.Float32, reflect.Float64:
		f64 := rv.Float()
		if math.Abs(f64) > f.max {
			return nil, fmt.Errorf("float converter: value %g out of range (%g - %g)", v, f.max*-1, f.max)
		}
		return f64, nil
	}
	return nil, newInvalidValueTypeError(v)
}

//time
var dbTime = dbTimeType{}

type dbTimeType struct{}

var _ driver.ValueConverter = dbTimeType{} //check that type implements interface

func (d dbTimeType) ConvertValue(v interface{}) (driver.Value, error) {

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
		return d.ConvertValue(rv.Elem().Interface())
	}
	return nil, newInvalidValueTypeError(v)
}

//decimal
var dbDecimal = dbDecimalType{}

type dbDecimalType struct{}

var _ driver.ValueConverter = dbDecimalType{} //check that type implements interface

func (d dbDecimalType) ConvertValue(v interface{}) (driver.Value, error) {

	if v == nil {
		return nil, nil
	}

	switch v := v.(type) {

	case []byte:
		return v, nil
	}
	return nil, newInvalidValueTypeError(v)
}

//varchar
var dbVarchar = dbVarcharType{}

type dbVarcharType struct{}

var _ driver.ValueConverter = dbVarcharType{} //check that type implements interface

func (d dbVarcharType) ConvertValue(v interface{}) (driver.Value, error) {

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
		return v, nil

	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return d.ConvertValue(rv.Elem().Interface())
	}
	return nil, newInvalidValueTypeError(v)
}

//nvarchar
var dbNvarchar = dbNvarcharType{}

type dbNvarcharType struct{}

var _ driver.ValueConverter = dbNvarcharType{} //check that type implements interface

func (d dbNvarcharType) ConvertValue(v interface{}) (driver.Value, error) {

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
		return v, nil

	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return d.ConvertValue(rv.Elem().Interface())
	}
	return nil, newInvalidValueTypeError(v)
}

//lob
var dbLob = dbLobType{}

type dbLobType struct{}

var _ driver.ValueConverter = dbLobType{} //check that type implements interface

func (d dbLobType) ConvertValue(v interface{}) (driver.Value, error) {

	switch v := v.(type) {

	case int64:
		return v, nil
	}
	return nil, newInvalidValueTypeError(v)
}
