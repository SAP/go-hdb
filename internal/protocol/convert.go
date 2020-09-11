// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"time"
)

// string / binary length indicators
const (
	bytesLenIndNullValue byte = 255
	bytesLenIndSmall     byte = 245
	bytesLenIndMedium    byte = 246
	bytesLenIndBig       byte = 247
)

const (
	realNullValue   uint32 = ^uint32(0)
	doubleNullValue uint64 = ^uint64(0)
)

const (
	booleanFalseValue   byte  = 0
	booleanNullValue    byte  = 1
	booleanTrueValue    byte  = 2
	longdateNullValue   int64 = 3155380704000000001
	seconddateNullValue int64 = 315538070401
	daydateNullValue    int32 = 3652062
	secondtimeNullValue int32 = 86402
)

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
	return (((((((convertTimeToDayDate(t)-1)*24)+int64(t.Hour()))*60)+int64(t.Minute()))*60)+int64(t.Second()))*10000000 + int64(t.Nanosecond()/100) + 1
}

// Seconddate
func convertSeconddateToTime(seconddate int64) time.Time {
	const dayfactor = 24 * 60 * 60
	seconddate--
	d := (seconddate % dayfactor) * 1000000000
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
	return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(int64(secondtime-1) * 1000000000))
}
func convertTimeToSecondtime(t time.Time) int {
	return (t.Hour()*60+t.Minute())*60 + t.Second() + 1
}
