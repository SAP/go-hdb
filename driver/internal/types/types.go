// Package types provides database types.
package types

import (
	"fmt"
	"reflect"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// Column represents a database column.
type Column interface {
	IsSupported(version, dfv int) bool
	TypeName() string
	DatabaseTypeName(version, dfv int) string
	DataType() string
	Length() (length int64, ok bool)
	PrecisionScale() (precision, scale int64, ok bool)
	ScanType(version, dfv int) reflect.Type
	Nullable() (nullable, ok bool)
}

// Spatial is implemented by spatial database columns.
type Spatial interface {
	SRID() int32
}

// check if column types implement the right column interfaces.
var (
	_ Column  = (*basicColumn)(nil)
	_ Column  = (*varColumn)(nil)
	_ Column  = (*decimalColumn)(nil)
	_ Column  = (*spatialColumn)(nil)
	_ Spatial = (*spatialColumn)(nil)
)

type _type struct {
	minDfv       *int
	maxVersion   *int
	typeName     string
	fnDBTypeName func(version, dfv int) (bool, string)
	dataType     p.DataType
	fnScanType   func(version, dfv int, nullable bool) (bool, reflect.Type)
}

func (t *_type) isSupported(version, dfv int) bool {
	switch {
	case t.minDfv != nil && t.maxVersion != nil:
		return version <= *t.maxVersion && dfv >= *t.minDfv
	case t.maxVersion != nil:
		return version <= *t.maxVersion
	case t.minDfv != nil:
		return dfv >= *t.minDfv
	default:
		return true
	}
}

func (t *_type) databaseTypeName(version, dfv int) string {
	if t.fnDBTypeName != nil {
		if ok, name := t.fnDBTypeName(version, dfv); ok {
			return name
		}
	}
	return t.typeName
}

func (t *_type) scanType(version, dfv int, nullable bool) reflect.Type {
	if t.fnScanType != nil {
		if ok, typ := t.fnScanType(version, dfv, nullable); ok {
			return typ
		}
	}
	return t.dataType.ScanType(nullable)
}

const (
	dbtnDate       = "DATE"
	dbtnTime       = "TIME"
	dbtnTimestamp  = "TIMESTAMP"
	dbtnLongdate   = "LONGDATE"
	dbtnSeconddate = "SECONDDATE"
	dbtnDaydate    = "DAYDATE"
	dbtnSecondtime = "SECONDTIME"
	dbtnClob       = "CLOB"
	dbtnNClob      = "NCLOB"
	dbtnBlob       = "BLOB"

	dbtnText    = "TEXT"
	dbtnBintext = "BINTEXT"

	dbtnBoolean = "BOOLEAN"

	dbtnTinyint  = "TINYINT"
	dbtnSmallint = "SMALLINT"
	dbtnInteger  = "INTEGER"
	dbtnBigint   = "BIGINT"
	dbtnReal     = "REAL"
	dbtnDouble   = "DOUBLE"

	dbtnChar      = "CHAR"
	dbtnVarchar   = "VARCHAR"
	dbtnNChar     = "NCHAR"
	dbtnNVarchar  = "NVARCHAR"
	dbtnShorttext = "SHORTTEXT"
	dbtnAlphanum  = "ALPHANUM"
	dbtnBinary    = "BINARY"
	dbtnVarbinary = "VARBINARY"

	dbtnDecimal      = "DECIMAL"
	dbtnSmalldecimal = "SMALLDECIMAL"

	dbtnStPoint    = "ST_POINT"
	dbtnStGeometry = "ST_GEOMETRY"

	dbtnFixed8  = "FIXED8"
	dbtnFixed12 = "FIXED12"
	dbtnFixed16 = "FIXED16"
)

func _dateDBTypeName(version, dfv int) (bool, string) {
	if dfv >= p.DfvLevel3 {
		return true, dbtnDaydate
	}
	return false, ""
}

func _timeDBTypeName(version, dfv int) (bool, string) {
	if dfv >= p.DfvLevel3 {
		return true, dbtnSecondtime
	}
	return false, ""
}

func _timestampDBTypeName(version, dfv int) (bool, string) {
	if dfv >= p.DfvLevel3 {
		return true, dbtnLongdate
	}
	return false, ""
}

func _longdateDBTypeName(version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, dbtnTimestamp
	}
	return false, ""
}

func _seconddateDBTypeName(version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, dbtnTimestamp
	}
	return false, ""
}

func _daydateDBTypeName(version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, dbtnDate
	}
	return false, ""
}

func _secondtimeDBTypeName(version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, dbtnTime
	}
	return false, ""
}

func _clobDBTypeName(version, dfv int) (bool, string) {
	if version >= 4 {
		return true, dbtnNClob
	}
	return false, ""
}

func _bintextDBTypeName(version, dfv int) (bool, string) {
	if dfv < p.DfvLevel6 {
		return true, dbtnNClob
	}
	return false, ""
}

func _booleanDBTypeName(version, dfv int) (bool, string) {
	if dfv < p.DfvLevel7 {
		return true, dbtnTinyint
	}
	return false, ""
}

func _charDBTypeName(version, dfv int) (bool, string) {
	if version >= 4 { // since hdb version 4: char equals nchar
		return true, dbtnNChar
	}
	return false, ""
}

func _varcharDBTypeName(version, dfv int) (bool, string) {
	if version >= 4 { // since hdb version 4: char equals nchar
		return true, dbtnNVarchar
	}
	return false, ""
}

func _shorttextDBTypeName(version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, dbtnNVarchar
	}
	return false, ""
}

func _alphanumDBTypeName(version, dfv int) (bool, string) {
	if dfv < p.DfvLevel3 {
		return true, dbtnNVarchar
	}
	return false, ""
}

func _smalldecimalDBTypeName(version, dfv int) (bool, string) { return true, dbtnDecimal }

func _booleanScanType(version, dfv int, nullable bool) (bool, reflect.Type) {
	if dfv < p.DfvLevel7 {
		return true, p.DtTinyint.ScanType(nullable)
	}
	return false, nil
}

var (
	dfvLevel4 = p.DfvLevel4
	dfvLevel6 = p.DfvLevel6
	mv3       = int(3)
)

type basicColumn struct {
	dt       *_type
	nullable bool
}

func (t *basicColumn) IsSupported(version, dfv int) bool                 { return t.dt.isSupported(version, dfv) }
func (t *basicColumn) TypeName() string                                  { return t.dt.typeName }
func (t *basicColumn) DataType() string                                  { return formatColumn(t.TypeName(), t.nullable) }
func (t *basicColumn) Length() (length int64, ok bool)                   { return 0, false }
func (t *basicColumn) PrecisionScale() (precision, scale int64, ok bool) { return 0, 0, false }
func (t *basicColumn) Nullable() (nullable, ok bool)                     { return t.nullable, true }
func (t *basicColumn) DatabaseTypeName(version, dfv int) string {
	return t.dt.databaseTypeName(version, dfv)
}
func (t *basicColumn) ScanType(version, dfv int) reflect.Type {
	return t.dt.scanType(version, dfv, t.nullable)
}

type varColumn struct {
	dt       *_type
	nullable bool
	length   int64
}

func (t *varColumn) IsSupported(version, dfv int) bool                 { return t.dt.isSupported(version, dfv) }
func (t *varColumn) TypeName() string                                  { return t.dt.typeName }
func (t *varColumn) DataType() string                                  { return formatVarColumn(t.TypeName(), t.length, t.nullable) }
func (t *varColumn) Length() (length int64, ok bool)                   { return t.length, true }
func (t *varColumn) PrecisionScale() (precision, scale int64, ok bool) { return 0, 0, false }
func (t *varColumn) Nullable() (nullable, ok bool)                     { return t.nullable, true }
func (t *varColumn) DatabaseTypeName(version, dfv int) string {
	return t.dt.databaseTypeName(version, dfv)
}
func (t *varColumn) ScanType(version, dfv int) reflect.Type {
	return t.dt.scanType(version, dfv, t.nullable)
}

type decimalColumn struct {
	dt               *_type
	nullable         bool
	precision, scale int64
}

func (t *decimalColumn) IsSupported(version, dfv int) bool {
	if t.precision == 38 && dfv < p.DfvLevel8 { // does not work with dfv < 8
		return false
	}
	return true
}
func (t *decimalColumn) TypeName() string { return t.dt.typeName }

func (t *decimalColumn) DatabaseTypeName(version, dfv int) string {
	if dfv < p.DfvLevel8 {
		return t.dt.databaseTypeName(version, dfv)
	}
	// dfv >= 8
	switch {
	case t.precision == 0:
		return t.dt.databaseTypeName(version, dfv)
	case t.precision <= 18:
		return dbtnFixed8
	case t.precision <= 28:
		return dbtnFixed12
	default: // precision <= 38
		return dbtnFixed16
	}
}
func (t *decimalColumn) DataType() string {
	return formatDecimalColumn(t.TypeName(), t.precision, t.scale, t.nullable)
}
func (t *decimalColumn) Length() (length int64, ok bool) { return 0, false }
func (t *decimalColumn) PrecisionScale() (precision, scale int64, ok bool) {
	if t.precision == 0 {
		if t.dt == _smalldecimal {
			return 16, 32767, true
		}
		return 34, 32767, true
	}
	return t.precision, t.scale, true
}
func (t *decimalColumn) ScanType(version, dfv int) reflect.Type {
	return t.dt.scanType(version, dfv, t.nullable)
}
func (t *decimalColumn) Nullable() (nullable, ok bool) { return t.nullable, true }

type spatialColumn struct {
	dt       *_type
	nullable bool
	srid     int32
}

func (t *spatialColumn) IsSupported(version, dfv int) bool                 { return t.dt.isSupported(version, dfv) }
func (t *spatialColumn) TypeName() string                                  { return t.dt.typeName }
func (t *spatialColumn) Length() (length int64, ok bool)                   { return 0, false }
func (t *spatialColumn) PrecisionScale() (precision, scale int64, ok bool) { return 0, 0, false }
func (t *spatialColumn) Nullable() (nullable, ok bool)                     { return t.nullable, true }
func (t *spatialColumn) SRID() int32                                       { return t.srid }
func (t *spatialColumn) DatabaseTypeName(version, dfv int) string {
	return t.dt.databaseTypeName(version, dfv)
}
func (t *spatialColumn) DataType() string {
	return formatSpatialColumn(t.TypeName(), t.srid, t.nullable)
}
func (t *spatialColumn) ScanType(version, dfv int) reflect.Type {
	return t.dt.scanType(version, dfv, t.nullable)
}

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

var (
	_tinyint      = &_type{nil, nil, dbtnTinyint, nil, p.DtTinyint, nil}
	_smallint     = &_type{nil, nil, dbtnSmallint, nil, p.DtSmallint, nil}
	_integer      = &_type{nil, nil, dbtnInteger, nil, p.DtInteger, nil}
	_bigint       = &_type{nil, nil, dbtnBigint, nil, p.DtBigint, nil}
	_real         = &_type{nil, nil, dbtnReal, nil, p.DtReal, nil}
	_double       = &_type{nil, nil, dbtnDouble, nil, p.DtDouble, nil}
	_date         = &_type{nil, nil, dbtnDate, _dateDBTypeName, p.DtTime, nil}
	_time         = &_type{nil, nil, dbtnTime, _timeDBTypeName, p.DtTime, nil}
	_timestamp    = &_type{nil, nil, dbtnTimestamp, _timestampDBTypeName, p.DtTime, nil}
	_longdate     = &_type{nil, nil, dbtnLongdate, _longdateDBTypeName, p.DtTime, nil}
	_seconddate   = &_type{nil, nil, dbtnSeconddate, _seconddateDBTypeName, p.DtTime, nil}
	_daydate      = &_type{nil, nil, dbtnDaydate, _daydateDBTypeName, p.DtTime, nil}
	_secondtime   = &_type{nil, nil, dbtnSecondtime, _secondtimeDBTypeName, p.DtTime, nil}
	_clob         = &_type{nil, nil, dbtnClob, _clobDBTypeName, p.DtLob, nil}
	_nclob        = &_type{nil, nil, dbtnNClob, nil, p.DtLob, nil}
	_blob         = &_type{nil, nil, dbtnBlob, nil, p.DtLob, nil}
	_text         = &_type{&dfvLevel4, &mv3, dbtnText, nil, p.DtLob, nil}
	_bintext      = &_type{&dfvLevel6, &mv3, dbtnBintext, _bintextDBTypeName, p.DtLob, nil}
	_boolean      = &_type{nil, nil, dbtnBoolean, _booleanDBTypeName, p.DtBoolean, _booleanScanType}
	_char         = &_type{nil, nil, dbtnChar, _charDBTypeName, p.DtString, nil}
	_varchar      = &_type{nil, nil, dbtnVarchar, _varcharDBTypeName, p.DtString, nil}
	_nchar        = &_type{nil, nil, dbtnNChar, nil, p.DtString, nil}
	_nvarchar     = &_type{nil, nil, dbtnNVarchar, nil, p.DtString, nil}
	_shorttext    = &_type{nil, &mv3, dbtnShorttext, _shorttextDBTypeName, p.DtString, nil}
	_alphanum     = &_type{nil, &mv3, dbtnAlphanum, _alphanumDBTypeName, p.DtString, nil}
	_binary       = &_type{nil, nil, dbtnBinary, nil, p.DtBytes, nil}
	_varbinary    = &_type{nil, nil, dbtnVarbinary, nil, p.DtBytes, nil}
	_decimal      = &_type{nil, nil, dbtnDecimal, nil, p.DtDecimal, nil}
	_smalldecimal = &_type{nil, nil, dbtnSmalldecimal, _smalldecimalDBTypeName, p.DtDecimal, nil}
	_stpoint      = &_type{&dfvLevel6, nil, dbtnStPoint, nil, p.DtLob, nil}
	_stgeometry   = &_type{&dfvLevel6, nil, dbtnStGeometry, nil, p.DtLob, nil}
)

// Basic column types.
var (
	Tinyint    = &basicColumn{dt: _tinyint, nullable: false}
	Smallint   = &basicColumn{dt: _smallint, nullable: false}
	Integer    = &basicColumn{dt: _integer, nullable: false}
	Bigint     = &basicColumn{dt: _bigint, nullable: false}
	Real       = &basicColumn{dt: _real, nullable: false}
	Double     = &basicColumn{dt: _double, nullable: false}
	Date       = &basicColumn{dt: _date, nullable: false}
	Time       = &basicColumn{dt: _time, nullable: false}
	Timestamp  = &basicColumn{dt: _timestamp, nullable: false}
	Longdate   = &basicColumn{dt: _longdate, nullable: false}
	Seconddate = &basicColumn{dt: _seconddate, nullable: false}
	Daydate    = &basicColumn{dt: _daydate, nullable: false}
	Secondtime = &basicColumn{dt: _secondtime, nullable: false}
	Clob       = &basicColumn{dt: _clob, nullable: false}
	NClob      = &basicColumn{dt: _nclob, nullable: false}
	Blob       = &basicColumn{dt: _blob, nullable: false}
	Text       = &basicColumn{dt: _text, nullable: false}
	Bintext    = &basicColumn{dt: _bintext, nullable: false}
	Boolean    = &basicColumn{dt: _boolean, nullable: false}
)

// Basic nullable column types.
var (
	NullTinyint    = &basicColumn{dt: _tinyint, nullable: true}
	NullSmallint   = &basicColumn{dt: _smallint, nullable: true}
	NullInteger    = &basicColumn{dt: _integer, nullable: true}
	NullBigint     = &basicColumn{dt: _bigint, nullable: true}
	NullReal       = &basicColumn{dt: _real, nullable: true}
	NullDouble     = &basicColumn{dt: _double, nullable: true}
	NullDate       = &basicColumn{dt: _date, nullable: true}
	NullTime       = &basicColumn{dt: _time, nullable: true}
	NullTimestamp  = &basicColumn{dt: _timestamp, nullable: true}
	NullLongdate   = &basicColumn{dt: _longdate, nullable: true}
	NullSeconddate = &basicColumn{dt: _seconddate, nullable: true}
	NullDaydate    = &basicColumn{dt: _daydate, nullable: true}
	NullSecondtime = &basicColumn{dt: _secondtime, nullable: true}
	NullClob       = &basicColumn{dt: _clob, nullable: true}
	NullNClob      = &basicColumn{dt: _nclob, nullable: true}
	NullBlob       = &basicColumn{dt: _blob, nullable: true}
	NullText       = &basicColumn{dt: _text, nullable: true}
	NullBintext    = &basicColumn{dt: _bintext, nullable: true}
	NullBoolean    = &basicColumn{dt: _boolean, nullable: true}
)

// NewChar return a new char column.
func NewChar(length int64) Column {
	return &varColumn{dt: _char, nullable: false, length: length}
}

// NewVarchar return a new varchar column.
func NewVarchar(length int64) Column {
	return &varColumn{dt: _varchar, nullable: false, length: length}
}

// NewNChar return a new nchar column.
func NewNChar(length int64) Column {
	return &varColumn{dt: _nchar, nullable: false, length: length}
}

// NewNVarchar return a new nvarchar column.
func NewNVarchar(length int64) Column {
	return &varColumn{dt: _nvarchar, nullable: false, length: length}
}

// NewShorttext return a new shortext column.
func NewShorttext(length int64) Column {
	return &varColumn{dt: _shorttext, nullable: false, length: length}
}

// NewAlphanum return a new alphanum column.
func NewAlphanum(length int64) Column {
	return &varColumn{dt: _alphanum, nullable: false, length: length}
}

// NewBinary return a new binary column.
func NewBinary(length int64) Column {
	return &varColumn{dt: _binary, nullable: false, length: length}
}

// NewVarbinary return a new varbinary column.
func NewVarbinary(length int64) Column {
	return &varColumn{dt: _varbinary, nullable: false, length: length}
}

// NewDecimal return a new decimal column.
func NewDecimal(precision, scale int64) Column {
	return &decimalColumn{dt: _decimal, nullable: false, precision: precision, scale: scale}
}

// NewSmalldecimal return a new smalldecimal column.
func NewSmalldecimal(precision, scale int64) Column {
	return &decimalColumn{dt: _smalldecimal, nullable: false, precision: precision, scale: scale}
}

// NewSTPoint return a new stpoint column.
func NewSTPoint(srid int32) Column {
	return &spatialColumn{dt: _stpoint, nullable: false, srid: srid}
}

// NewSTGeometry return a new stgeometry column.
func NewSTGeometry(srid int32) Column {
	return &spatialColumn{dt: _stgeometry, nullable: false, srid: srid}
}

// NewNullChar return a new nullable char column.
func NewNullChar(length int64) Column {
	return &varColumn{dt: _char, nullable: true, length: length}
}

// NewNullVarchar return a new nullable varchar column.
func NewNullVarchar(length int64) Column {
	return &varColumn{dt: _varchar, nullable: true, length: length}
}

// NewNullNChar return a new nullable nchar column.
func NewNullNChar(length int64) Column {
	return &varColumn{dt: _nchar, nullable: true, length: length}
}

// NewNullNVarchar return a new nullable nvarchar column.
func NewNullNVarchar(length int64) Column {
	return &varColumn{dt: _nvarchar, nullable: true, length: length}
}

// NewNullShorttext return a new nullable shorttext column.
func NewNullShorttext(length int64) Column {
	return &varColumn{dt: _shorttext, nullable: true, length: length}
}

// NewNullAlphanum return a new nullable alphanum column.
func NewNullAlphanum(length int64) Column {
	return &varColumn{dt: _alphanum, nullable: true, length: length}
}

// NewNullBinary return a new nullable binary column.
func NewNullBinary(length int64) Column {
	return &varColumn{dt: _binary, nullable: true, length: length}
}

// NewNullVarbinary return a new nullable varbinary column.
func NewNullVarbinary(length int64) Column {
	return &varColumn{dt: _varbinary, nullable: true, length: length}
}

// NewNullDecimal return a new nullable decimal column.
func NewNullDecimal(precision, scale int64) Column {
	return &decimalColumn{dt: _decimal, nullable: true, precision: precision, scale: scale}
}

// NewNullSmalldecimal return a new nullable smalldecimal column.
func NewNullSmalldecimal(precision, scale int64) Column {
	return &decimalColumn{dt: _smalldecimal, nullable: true, precision: precision, scale: scale}
}

// NewNullSTPoint return a new nullable stpoint column.
func NewNullSTPoint(srid int32) Column {
	return &spatialColumn{dt: _stpoint, nullable: true, srid: srid}
}

// NewNullSTGeometry return a new nullable stgeometry column.
func NewNullSTGeometry(srid int32) Column {
	return &spatialColumn{dt: _stgeometry, nullable: true, srid: srid}
}
