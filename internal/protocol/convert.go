// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

// ErrUint64OutOfRange means that a uint64 exceeds the size of a int64.
var ErrUint64OutOfRange = errors.New("uint64 values with high bit set are not supported")

// ErrIntegerOutOfRange means that an integer exceeds the size of the hdb integer field.
var ErrIntegerOutOfRange = errors.New("integer out of range error")

// ErrFloatOutOfRange means that a float exceeds the size of the hdb float field.
var ErrFloatOutOfRange = errors.New("float out of range error")

/*
Conversion routines hdb parameters
- return value is interface{} to avoid allocations in case
  parameter is already of target type
*/
func convertBool(v interface{}) (interface{}, error) {
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
			return nil, err
		}
		return b, nil
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertBool(rv.Elem().Interface())
	}

	if rv.Type().ConvertibleTo(stringReflectType) {
		return convertBool(rv.Convert(stringReflectType).Interface())
	}
	return nil, fmt.Errorf("unsupported bool conversion: %[1]T %[1]v", v)
}

func convertInteger(v interface{}, min, max int64) (interface{}, error) {
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
			return nil, ErrIntegerOutOfRange
		}
		return v, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64 := rv.Uint()
		if u64 >= 1<<63 {
			return nil, ErrUint64OutOfRange
		}
		if int64(u64) > max || int64(u64) < min {
			return nil, ErrIntegerOutOfRange
		}
		return v, nil
	// conversions with allocations (return i64)
	case reflect.Float32, reflect.Float64:
		f64 := rv.Float()
		i64 := int64(f64)
		if f64 != float64(i64) { // should work for overflow, NaN, +-INF as well
			return nil, fmt.Errorf("unsupported integer conversion: %[1]T %[1]v", v)
		}
		if i64 > max || i64 < min {
			return nil, ErrIntegerOutOfRange
		}
		return i64, nil
	case reflect.String:
		i64, err := strconv.ParseInt(rv.String(), 10, 64)
		if err != nil {
			return nil, err
		}
		if i64 > max || i64 < min {
			return nil, ErrIntegerOutOfRange
		}
		return i64, nil
	// pointer
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertInteger(rv.Elem().Interface(), min, max)
	}
	// last resort (try via string)
	if rv.Type().ConvertibleTo(stringReflectType) {
		return convertInteger(rv.Convert(stringReflectType).Interface(), min, max)
	}
	return nil, fmt.Errorf("unsupported integer conversion: %[1]T %[1]v", v)
}

func convertFloat(v interface{}, max float64) (interface{}, error) {
	if v == nil {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	// conversions without allocations (return v)
	case reflect.Float32, reflect.Float64:
		if math.Abs(rv.Float()) > max {
			return nil, ErrFloatOutOfRange
		}
		return v, nil
	// conversions with allocations (return f64)
	case reflect.String:
		f64, err := strconv.ParseFloat(rv.String(), 64)
		if err != nil {
			return nil, err
		}
		if math.Abs(f64) > max {
			return nil, ErrFloatOutOfRange
		}
		return f64, nil
	// pointer
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return convertFloat(rv.Elem().Interface(), max)
	}
	// last resort (try via string)
	if rv.Type().ConvertibleTo(stringReflectType) {
		return convertFloat(rv.Convert(stringReflectType).Interface(), max)
	}
	return nil, fmt.Errorf("unsupported float conversion: %[1]T %[1]v", v)
}

func convertTime(v interface{}) (interface{}, error) {
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
		return convertTime(rv.Elem().Interface())
	}

	if rv.Type().ConvertibleTo(timeReflectType) {
		tv := rv.Convert(timeReflectType)
		return tv.Interface().(time.Time), nil
	}
	return nil, fmt.Errorf("unsupported time conversion: %[1]T %[1]v", v)
}

func convertBytes(v interface{}) (interface{}, error) {
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
		return convertBytes(rv.Elem().Interface())
	}

	if rv.Type().ConvertibleTo(bytesReflectType) {
		bv := rv.Convert(bytesReflectType)
		return bv.Interface().([]byte), nil
	}
	return nil, fmt.Errorf("unsupported bytes conversion: %[1]T %[1]v", v)
}

// Longdate
func convertLongdateToTime(longdate int64) time.Time {
	const dayfactor = 10000000 * 24 * 60 * 60
	longdate--
	d := (longdate % dayfactor) * 100
	t := convertDaydateToTime((longdate / dayfactor) + 1)
	return t.Add(time.Duration(d))
}

// nanosecond: HDB - 7 digits precision (not 9 digits)
func convertTimeToLongdate(t time.Time) int64 {
	return (((((((convertTimeToDayDate(t)-1)*24)+int64(t.Hour()))*60)+int64(t.Minute()))*60)+int64(t.Second()))*1e7 + int64(t.Nanosecond()/1e2) + 1
}

// Seconddate
func convertSeconddateToTime(seconddate int64) time.Time {
	const dayfactor = 24 * 60 * 60
	seconddate--
	d := (seconddate % dayfactor) * 1e9
	t := convertDaydateToTime((seconddate / dayfactor) + 1)
	return t.Add(time.Duration(d))
}
func convertTimeToSeconddate(t time.Time) int64 {
	return (((((convertTimeToDayDate(t)-1)*24)+int64(t.Hour()))*60)+int64(t.Minute()))*60 + int64(t.Second()) + 1
}

const julianHdb = 1721423 // 1 January 0001 00:00:00 (1721424) - 1

// Daydate
func convertDaydateToTime(daydate int64) time.Time {
	return julianDayToTime(int(daydate) + julianHdb)
}
func convertTimeToDayDate(t time.Time) int64 {
	return int64(timeToJulianDay(t) - julianHdb)
}

// Secondtime
func convertSecondtimeToTime(secondtime int) time.Time {
	return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(int64(secondtime-1) * 1e9))
}
func convertTimeToSecondtime(t time.Time) int {
	return (t.Hour()*60+t.Minute())*60 + t.Second() + 1
}
