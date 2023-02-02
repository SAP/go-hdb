package driver

import (
	"fmt"
	"reflect"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// columnType represents a database column.
type columnType interface {
	dfv() int
	isSupported() bool
	typeName() string
	databaseTypeName() string
	column() string
	length() (length int64, ok bool)
	precisionScale() (precision, scale int64, ok bool)
	scanType() reflect.Type
	nullable() (nullable, ok bool)
}

type spatialColumnType interface {
	srid() int32
}

type _baseType struct {
	minDfv       *int
	maxMV        *uint64
	typeName     string
	fnDBTypeName func(v *Version, dfv int) (bool, string)
	_scanType    reflect.Type
	fnScanType   func(v *Version, dfv int) (bool, reflect.Type)
}

func (t *_baseType) isSupported(version *Version, dfv int) bool {
	switch {
	case t.minDfv != nil && t.maxMV != nil:
		return version.Major() <= *t.maxMV && dfv >= *t.minDfv
	case t.maxMV != nil:
		return version.Major() <= *t.maxMV
	case t.minDfv != nil:
		return dfv >= *t.minDfv
	default:
		return true
	}
}

func (t *_baseType) databaseTypeName(v *Version, dfv int) string {
	if t.fnDBTypeName != nil {
		if ok, name := t.fnDBTypeName(v, dfv); ok {
			return name
		}
	}
	return t.typeName
}

func (t *_baseType) scanType(v *Version, dfv int) reflect.Type {
	if t.fnScanType != nil {
		if ok, typ := t.fnScanType(v, dfv); ok {
			return typ
		}
	}
	return t._scanType
}

type _basicType struct{ _baseType }
type _varType struct{ _baseType }
type _decimalType struct{ _baseType }
type _spatialType struct{ _baseType }

// basicKind describes the kind of basic type.
type basicKind byte

const (
	dtTinyint basicKind = iota
	dtSmallint
	dtInteger
	dtBigint
	dtReal
	dtDouble
	dtDate
	dtTime
	dtTimestamp
	dtLongdate
	dtSeconddate
	dtDaydate
	dtSecondtime
	dtClob
	dtNClob
	dtBlob
	dtText
	dtBintext
	dtBoolean
)

// varKind describes the kind of variable type.
type varKind byte

const (
	dtChar varKind = iota
	dtVarchar
	dtNChar
	dtNVarchar
	dtShorttext
	dtAlphanum
	dtBinary
	dtVarbinary
)

// decimalKind describes the kind of decimal type.
type decimalKind byte

const (
	dtDecimal decimalKind = iota
	dtSmalldecimal
)

// spatialKind describes the kind of spatial type.
type spatialKind byte

const (
	dtSTPoint spatialKind = iota
	dtSTGeometry
)

func _dateDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv >= p.DfvLevel3 {
		return true, "DAYDATE"
	}
	return false, ""
}

func _timeDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv >= p.DfvLevel3 {
		return true, "SECONDTIME"
	}
	return false, ""
}

func _timestampDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv >= p.DfvLevel3 {
		return true, "LONGDATE"
	}
	return false, ""
}

func _longdateDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, "TIMESTAMP"
	}
	return false, ""
}

func _seconddateDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, "TIMESTAMP"
	}
	return false, ""
}

func _daydateDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, "DATE"
	}
	return false, ""
}

func _secondtimeDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, "TIME"
	}
	return false, ""
}

func _clobDBTypeName(version *Version, dfv int) (bool, string) {
	if version.Major() >= 4 {
		return true, "NCLOB"
	}
	return false, ""
}

func _bintextDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv < p.DfvLevel6 {
		return true, "NCLOB"
	}
	return false, ""
}

func _booleanDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv < p.DfvLevel7 {
		return true, "TINYINT"
	}
	return false, ""
}

func _charDBTypeName(version *Version, dfv int) (bool, string) {
	if version.Major() >= 4 { // since hdb version 4: char equals nchar
		return true, "NCHAR"
	}
	return false, ""
}

func _varcharDBTypeName(version *Version, dfv int) (bool, string) {
	if version.Major() >= 4 { // since hdb version 4: char equals nchar
		return true, "NVARCHAR"
	}
	return false, ""
}

func _shorttextDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, "NVARCHAR"
	}
	return false, ""
}

func _alphanumDBTypeName(version *Version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, "NVARCHAR"
	}
	return false, ""
}

func _smalldecimalDBTypeName(version *Version, dfv int) (bool, string) { return true, "DECIMAL" }

func _booleanScanType(version *Version, dfv int) (bool, reflect.Type) {
	if dfv < p.DfvLevel7 {
		return true, p.DtTinyint.ScanType()
	}
	return false, nil
}

var (
	dfvLevel4 = p.DfvLevel4
	dfvLevel6 = p.DfvLevel6
	mv3       = uint64(3)
)

var basicType = []*_basicType{
	dtTinyint:    {_baseType{nil, nil, "TINYINT", nil, p.DtTinyint.ScanType(), nil}},
	dtSmallint:   {_baseType{nil, nil, "SMALLINT", nil, p.DtSmallint.ScanType(), nil}},
	dtInteger:    {_baseType{nil, nil, "INTEGER", nil, p.DtInteger.ScanType(), nil}},
	dtBigint:     {_baseType{nil, nil, "BIGINT", nil, p.DtBigint.ScanType(), nil}},
	dtReal:       {_baseType{nil, nil, "REAL", nil, p.DtReal.ScanType(), nil}},
	dtDouble:     {_baseType{nil, nil, "DOUBLE", nil, p.DtDouble.ScanType(), nil}},
	dtDate:       {_baseType{nil, nil, "DATE", _dateDBTypeName, p.DtTime.ScanType(), nil}},
	dtTime:       {_baseType{nil, nil, "TIME", _timeDBTypeName, p.DtTime.ScanType(), nil}},
	dtTimestamp:  {_baseType{nil, nil, "TIMESTAMP", _timestampDBTypeName, p.DtTime.ScanType(), nil}},
	dtLongdate:   {_baseType{nil, nil, "LONGDATE", _longdateDBTypeName, p.DtTime.ScanType(), nil}},
	dtSeconddate: {_baseType{nil, nil, "SECONDDATE", _seconddateDBTypeName, p.DtTime.ScanType(), nil}},
	dtDaydate:    {_baseType{nil, nil, "DAYDATE", _daydateDBTypeName, p.DtTime.ScanType(), nil}},
	dtSecondtime: {_baseType{nil, nil, "SECONDTIME", _secondtimeDBTypeName, p.DtTime.ScanType(), nil}},
	dtClob:       {_baseType{nil, nil, "CLOB", _clobDBTypeName, p.DtLob.ScanType(), nil}},
	dtNClob:      {_baseType{nil, nil, "NCLOB", nil, p.DtLob.ScanType(), nil}},
	dtBlob:       {_baseType{nil, nil, "BLOB", nil, p.DtLob.ScanType(), nil}},
	dtText:       {_baseType{&dfvLevel4, &mv3, "TEXT", nil, p.DtLob.ScanType(), nil}},
	dtBintext:    {_baseType{&dfvLevel6, &mv3, "BINTEXT", _bintextDBTypeName, p.DtLob.ScanType(), nil}},
	dtBoolean:    {_baseType{nil, nil, "BOOLEAN", _booleanDBTypeName, p.DtBoolean.ScanType(), _booleanScanType}},
}

var varType = []*_varType{
	dtChar:      {_baseType{nil, nil, "CHAR", _charDBTypeName, p.DtString.ScanType(), nil}},
	dtVarchar:   {_baseType{nil, nil, "VARCHAR", _varcharDBTypeName, p.DtString.ScanType(), nil}},
	dtNChar:     {_baseType{nil, nil, "NCHAR", nil, p.DtString.ScanType(), nil}},
	dtNVarchar:  {_baseType{nil, nil, "NVARCHAR", nil, p.DtString.ScanType(), nil}},
	dtShorttext: {_baseType{nil, &mv3, "SHORTTEXT", _shorttextDBTypeName, p.DtString.ScanType(), nil}},
	dtAlphanum:  {_baseType{nil, &mv3, "ALPHANUM", _alphanumDBTypeName, p.DtString.ScanType(), nil}},
	dtBinary:    {_baseType{nil, nil, "BINARY", nil, p.DtBytes.ScanType(), nil}},
	dtVarbinary: {_baseType{nil, nil, "VARBINARY", nil, p.DtBytes.ScanType(), nil}},
}

var decimalType = []*_decimalType{
	dtDecimal:      {_baseType{nil, nil, "DECIMAL", nil, p.DtDecimal.ScanType(), nil}},
	dtSmalldecimal: {_baseType{nil, nil, "SMALLDECIMAL", _smalldecimalDBTypeName, p.DtDecimal.ScanType(), nil}},
}

var spatialType = []*_spatialType{
	dtSTPoint:    {_baseType{&dfvLevel6, nil, "ST_POINT", nil, p.DtLob.ScanType(), nil}},
	dtSTGeometry: {_baseType{&dfvLevel6, nil, "ST_GEOMETRY", nil, p.DtLob.ScanType(), nil}},
}

type basicColumn struct {
	version   *Version
	_dfv      int
	dt        *_basicType
	_nullable bool
}

func (t *basicColumn) dfv() int                                          { return t._dfv }
func (t *basicColumn) isSupported() bool                                 { return t.dt.isSupported(t.version, t._dfv) }
func (t *basicColumn) typeName() string                                  { return t.dt.typeName }
func (t *basicColumn) databaseTypeName() string                          { return t.dt.databaseTypeName(t.version, t._dfv) }
func (t *basicColumn) column() string                                    { return formatColumn(t.typeName(), t._nullable) }
func (t *basicColumn) length() (length int64, ok bool)                   { return 0, false }
func (t *basicColumn) precisionScale() (precision, scale int64, ok bool) { return 0, 0, false }
func (t *basicColumn) scanType() reflect.Type                            { return t.dt.scanType(t.version, t._dfv) }
func (t *basicColumn) nullable() (nullable, ok bool)                     { return t._nullable, true }

type varColumn struct {
	version   *Version
	_dfv      int
	dt        *_varType
	_nullable bool
	_length   int64
}

func (t *varColumn) dfv() int                                          { return t._dfv }
func (t *varColumn) isSupported() bool                                 { return t.dt.isSupported(t.version, t._dfv) }
func (t *varColumn) typeName() string                                  { return t.dt.typeName }
func (t *varColumn) databaseTypeName() string                          { return t.dt.databaseTypeName(t.version, t._dfv) }
func (t *varColumn) column() string                                    { return formatVarColumn(t.typeName(), t._length, t._nullable) }
func (t *varColumn) length() (length int64, ok bool)                   { return t._length, true }
func (t *varColumn) precisionScale() (precision, scale int64, ok bool) { return 0, 0, false }
func (t *varColumn) scanType() reflect.Type                            { return t.dt.scanType(t.version, t._dfv) }
func (t *varColumn) nullable() (nullable, ok bool)                     { return t._nullable, true }

type decimalColumn struct {
	version          *Version
	_dfv             int
	dt               *_decimalType
	_nullable        bool
	precision, scale int64
}

func (t *decimalColumn) dfv() int { return t._dfv }
func (t *decimalColumn) isSupported() bool {
	if t.precision == 38 && t._dfv < p.DfvLevel8 { // does not work with dfv < 8
		return false
	}
	return true
}
func (t *decimalColumn) typeName() string { return t.dt.typeName }

const (
	dbtnFixed8  = "FIXED8"
	dbtnFixed12 = "FIXED12"
	dbtnFixed16 = "FIXED16"
)

func (t *decimalColumn) databaseTypeName() string {
	if t._dfv < p.DfvLevel8 {
		return t.dt.databaseTypeName(t.version, t._dfv)
	}
	// dfv >= 8
	switch {
	case t.precision == 0:
		return t.dt.databaseTypeName(t.version, t._dfv)
	case t.precision <= 18:
		return dbtnFixed8
	case t.precision <= 28:
		return dbtnFixed12
	default: // precision <= 38
		return dbtnFixed16
	}
}
func (t *decimalColumn) column() string {
	return formatDecimalColumn(t.typeName(), t.precision, t.scale, t._nullable)
}
func (t *decimalColumn) length() (length int64, ok bool) { return 0, false }
func (t *decimalColumn) precisionScale() (precision, scale int64, ok bool) {
	if t.precision == 0 {
		if t.dt == decimalType[dtSmalldecimal] {
			return 16, 32767, true
		}
		return 34, 32767, true
	}
	return t.precision, t.scale, true
}
func (t *decimalColumn) scanType() reflect.Type        { return t.dt.scanType(t.version, t._dfv) }
func (t *decimalColumn) nullable() (nullable, ok bool) { return t._nullable, true }

type spatialColumn struct {
	version   *Version
	_dfv      int
	dt        *_spatialType
	_nullable bool
	_srid     int32
}

func (t *spatialColumn) dfv() int                 { return t._dfv }
func (t *spatialColumn) isSupported() bool        { return t.dt.isSupported(t.version, t._dfv) }
func (t *spatialColumn) typeName() string         { return t.dt.typeName }
func (t *spatialColumn) databaseTypeName() string { return t.dt.databaseTypeName(t.version, t._dfv) }
func (t *spatialColumn) column() string {
	return formatSpatialColumn(t.typeName(), t._srid, t._nullable)
}
func (t *spatialColumn) length() (length int64, ok bool)                   { return 0, false }
func (t *spatialColumn) precisionScale() (precision, scale int64, ok bool) { return 0, 0, false }
func (t *spatialColumn) scanType() reflect.Type                            { return t.dt.scanType(t.version, t._dfv) }
func (t *spatialColumn) nullable() (nullable, ok bool)                     { return t._nullable, true }
func (t *spatialColumn) srid() int32                                       { return t._srid }

const notNull = "not null"

func formatColumn(typeName string, nullable bool) string {
	if nullable {
		return typeName
	}
	return fmt.Sprintf("%s %s", typeName, notNull)
}

func formatVarColumn(typeName string, length int64, nullable bool) string {
	if nullable {
		return fmt.Sprintf("%s(%d)", typeName, length)
	}
	return fmt.Sprintf("%s(%d) %s", typeName, length, notNull)
}

func formatDecimalColumn(typeName string, precision, scale int64, nullable bool) string {
	if precision == 0 && scale == 0 {
		return formatColumn(typeName, nullable)
	}
	if nullable {
		return fmt.Sprintf("%s(%d, %d)", typeName, precision, scale)
	}
	return fmt.Sprintf("%s(%d, %d) %s", typeName, precision, scale, notNull)
}

func formatSpatialColumn(typeName string, srid int32, nullable bool) string {
	if srid == 0 {
		return formatColumn(typeName, nullable)
	}
	if nullable {
		return fmt.Sprintf("%s(%d)", typeName, srid)
	}
	return fmt.Sprintf("%s(%d) %s", typeName, srid, notNull)
}
