// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package drivertest

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/SAP/go-hdb/driver/hdb"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// DataType is the type definition for the supported database types.
type DataType byte

// DataType constants.
const (
	DtTinyint DataType = iota
	DtSmallint
	DtInteger
	DtBigint
	DtDecimal
	DtSmalldecimal
	DtReal
	DtDouble
	DtChar
	DtVarchar
	DtNChar
	DtNVarchar
	DtShorttext
	DtAlphanum
	DtBinary
	DtVarbinary
	DtDate
	DtTime
	DtTimestamp
	DtLongdate
	DtSeconddate
	DtDaydate
	DtSecondtime
	DtClob
	DtNClob
	DtBlob
	DtText
	DtBintext
	DtBoolean
	DtSTPoint
	DtSTGeometry
)

var typeNames = []string{
	"tinyint",
	"smallint",
	"integer",
	"bigint",
	"decimal",
	"smalldecimal",
	"real",
	"double",
	"char",
	"varchar",
	"nchar",
	"nvarchar",
	"shorttext",
	"alphanum",
	"binary",
	"varbinary",
	"date",
	"time",
	"timestamp",
	"longdate",
	"seconddate",
	"daydate",
	"secondtime",
	"clob",
	"nclob",
	"blob",
	"text",
	"bintext",
	"boolean",
	"st_point",
	"st_geometry",
}

const (
	dfvLevel3 int = 3 // additional types Longdate, Secondate, Daydate, Secondtime supported for NGAP
	dfvLevel4 int = 4 // generic support for new date/time types
	dfvLevel6 int = 6 // BINTEXT
	dfvLevel7 int = 7 // with boolean support
	dfvLevel8 int = 8 // with FIXED8/12/16 support
)

var databaseTypeNames []string

func init() {
	databaseTypeNames = make([]string, len(typeNames))
	for i, name := range typeNames {
		databaseTypeNames[i] = strings.ToUpper(name)
	}
}

var goDataTypes = []p.DataType{
	p.DtTinyint,
	p.DtSmallint,
	p.DtInteger,
	p.DtBigint,
	p.DtDecimal,
	p.DtDecimal,
	p.DtReal,
	p.DtDouble,
	p.DtString,
	p.DtString,
	p.DtString,
	p.DtString,
	p.DtString,
	p.DtString,
	p.DtBytes,
	p.DtBytes,
	p.DtTime,
	p.DtTime,
	p.DtTime,
	p.DtTime,
	p.DtTime,
	p.DtTime,
	p.DtTime,
	p.DtLob,
	p.DtLob,
	p.DtLob,
	p.DtLob,
	p.DtLob,
	p.DtBoolean,
	p.DtLob,
	p.DtLob,
}

const (
	dbtnFixed8  = "FIXED8"
	dbtnFixed12 = "FIXED12"
	dbtnFixed16 = "FIXED16"
)

const notNull = "not null"

func column(typeName string, nullable bool) string {
	if nullable {
		return typeName
	}
	return fmt.Sprintf("%s %s", typeName, notNull)
}

func varColumn(typeName string, length int64, nullable bool) string {
	if nullable {
		return fmt.Sprintf("%s(%d)", typeName, length)
	}
	return fmt.Sprintf("%s(%d) %s", typeName, length, notNull)
}

func decimalColumn(typeName string, precision, scale int64, nullable bool) string {
	if precision == 0 && scale == 0 {
		return column(typeName, nullable)
	}
	if nullable {
		return fmt.Sprintf("%s(%d, %d)", typeName, precision, scale)
	}
	return fmt.Sprintf("%s(%d, %d) %s", typeName, precision, scale, notNull)
}

func spatialColumn(typeName string, srid int32, nullable bool) string {
	if srid == 0 {
		return column(typeName, nullable)
	}
	if nullable {
		return fmt.Sprintf("%s(%d)", typeName, srid)
	}
	return fmt.Sprintf("%s(%d) %s", typeName, srid, notNull)
}

// ColumnType represents a database column.
type ColumnType interface {
	IsSupportedHDBVersion(version *hdb.Version) bool
	IsSupportedDfv(dfv int) bool
	TypeName() string
	DatabaseTypeName(version *hdb.Version, dfv int) string
	Column() string
	Length() (length int64, ok bool)
	DecimalSize() (precision, scale int64, ok bool)
	ScanType(dfv int) reflect.Type
	Nullable() (nullable, ok bool)
	SetNullable(nullable bool) ColumnType
	SRID() int32
}

// IsSupportedHDBVersion implements the ColumnType interface.
func (t DataType) IsSupportedHDBVersion(version *hdb.Version) bool {
	switch t {
	case DtShorttext:
		return version.Major() < 4 // no longer supported with hdb version 4
	case DtAlphanum:
		return version.Major() < 4 // no longer supported with hdb version 4
	case DtText:
		return version.Major() < 4 // no longer supported with hdb version 4
	case DtBintext:
		return version.Major() < 4 // no longer supported with hdb version 4
	default:
		return true
	}
}

// IsSupportedDfv implements the ColumnType interface.
func (t DataType) IsSupportedDfv(dfv int) bool {
	switch t {
	case DtText:
		return dfv >= dfvLevel4
	default:
		return true
	}
}

// TypeName implements the ColumnType interface.
func (t DataType) TypeName() string { return typeNames[t] }

// ScanType implements the ColumnType interface.
func (t DataType) ScanType(dfv int) reflect.Type {
	switch {
	case t == DtBoolean && dfv < dfvLevel7:
		return goDataTypes[DtTinyint].ScanType()
	default:
		return goDataTypes[t].ScanType()
	}
}

// DatabaseTypeName implements the ColumnType interface.
func (t DataType) DatabaseTypeName(version *hdb.Version, dfv int) string {
	switch {
	case t == DtSmalldecimal:
		return databaseTypeNames[DtDecimal]
	case t == DtShorttext && dfv < dfvLevel3:
		return databaseTypeNames[DtNVarchar]
	case t == DtAlphanum && dfv < dfvLevel3:
		return databaseTypeNames[DtNVarchar]
	case t == DtDate && dfv >= dfvLevel3:
		return databaseTypeNames[DtDaydate]
	case t == DtTime && dfv >= dfvLevel3:
		return databaseTypeNames[DtSecondtime]
	case t == DtTimestamp && dfv >= dfvLevel3:
		return databaseTypeNames[DtLongdate]
	case t == DtLongdate && dfv < dfvLevel3:
		return databaseTypeNames[DtTimestamp]
	case t == DtSeconddate && dfv < dfvLevel3:
		return databaseTypeNames[DtTimestamp]
	case t == DtDaydate && dfv < dfvLevel3:
		return databaseTypeNames[DtDate]
	case t == DtSecondtime && dfv < dfvLevel3:
		return databaseTypeNames[DtTime]
	case t == DtBoolean && dfv < dfvLevel7:
		return databaseTypeNames[DtTinyint]
	case t == DtBintext && dfv < dfvLevel6:
		return databaseTypeNames[DtNClob]

	case t == DtChar && version.Major() >= 4: // since hdb version 4: char equals nchar
		return databaseTypeNames[DtNChar]
	case t == DtVarchar && version.Major() >= 4: // since hdb version 4: varchar equals nvarchar
		return databaseTypeNames[DtNVarchar]
	case t == DtClob && version.Major() >= 4: // since hdb version 4: clob equals nclob
		return databaseTypeNames[DtNClob]

	default:
		return databaseTypeNames[t]
	}
}

type stdType struct {
	DataType
	notNullable bool
}

func (t *stdType) Column() string                                 { return column(t.TypeName(), !t.notNullable) }
func (t *stdType) Length() (length int64, ok bool)                { return 0, false }
func (t *stdType) DecimalSize() (precision, scale int64, ok bool) { return 0, 0, false }
func (t *stdType) Nullable() (nullable, ok bool)                  { return !t.notNullable, true }
func (t *stdType) SetNullable(nullable bool) ColumnType           { t.notNullable = !nullable; return t }
func (t *stdType) SRID() int32                                    { panic("not available") }

type varType struct {
	DataType
	length      int64
	notNullable bool
}

func (t *varType) Column() string                                 { return varColumn(t.TypeName(), t.length, !t.notNullable) }
func (t *varType) Length() (length int64, ok bool)                { return t.length, true }
func (t *varType) DecimalSize() (precision, scale int64, ok bool) { return 0, 0, false }
func (t *varType) Nullable() (nullable, ok bool)                  { return !t.notNullable, true }
func (t *varType) SetNullable(nullable bool) ColumnType           { t.notNullable = !nullable; return t }
func (t *varType) SRID() int32                                    { panic("not available") }

type decimalType struct {
	DataType
	precision, scale int64
	notNullable      bool
}

func (t *decimalType) DatabaseTypeName(version *hdb.Version, dfv int) string {
	if dfv < dfvLevel8 {
		return t.DataType.DatabaseTypeName(version, dfv)
	}
	// dfv >= 8
	switch {
	case t.precision == 0:
		return t.DataType.DatabaseTypeName(version, dfv)
	case t.precision <= 18:
		return dbtnFixed8
	case t.precision <= 28:
		return dbtnFixed12
	default: // precision <= 38
		return dbtnFixed16
	}
}
func (t *decimalType) Column() string {
	return decimalColumn(t.TypeName(), t.precision, t.scale, !t.notNullable)
}
func (t *decimalType) Length() (length int64, ok bool) { return 0, false }
func (t *decimalType) DecimalSize() (precision, scale int64, ok bool) {
	if t.precision == 0 {
		if t.DataType == DtSmalldecimal {
			return 16, 32767, true
		}
		return 34, 32767, true
	}
	return t.precision, t.scale, true
}
func (t *decimalType) Nullable() (nullable, ok bool)        { return !t.notNullable, true }
func (t *decimalType) SetNullable(nullable bool) ColumnType { t.notNullable = !nullable; return t }
func (t *decimalType) SRID() int32                          { panic("not available") }

type spatialType struct {
	DataType
	srid        int32
	notNullable bool
}

func (t *spatialType) Column() string                                 { return spatialColumn(t.TypeName(), t.srid, !t.notNullable) }
func (t *spatialType) Length() (length int64, ok bool)                { return 0, false }
func (t *spatialType) DecimalSize() (precision, scale int64, ok bool) { return 0, 0, false }
func (t *spatialType) Nullable() (nullable, ok bool)                  { return !t.notNullable, true }
func (t *spatialType) SetNullable(nullable bool) ColumnType           { t.notNullable = !nullable; return t }
func (t *spatialType) SRID() int32                                    { return t.srid }

// NewStdColumn returns a new standard database column.
func NewStdColumn(dataType DataType) ColumnType { return &stdType{DataType: dataType} }

// NewDecimalColumn returns a new decimal database column.
func NewDecimalColumn(dataType DataType, precision, scale int64) ColumnType {
	return &decimalType{DataType: dataType, precision: precision, scale: scale}
}

// NewVarColumn returns a new variable database column.
func NewVarColumn(dataType DataType, length int64) ColumnType {
	return &varType{DataType: dataType, length: length}
}

// NewSpatialColumn returns a new spatial database column.
func NewSpatialColumn(dataType DataType, srid int32) ColumnType {
	return &spatialType{DataType: dataType, srid: srid}
}
