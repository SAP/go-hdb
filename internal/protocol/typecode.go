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
	"fmt"
	"strings"
)

//go:generate stringer -type=typeCode

// typeCode identify the type of a field transferred to or from the database.
type typeCode byte

// null value indicator is high bit

//nolint
const (
	tcNull      typeCode = 0
	tcTinyint   typeCode = 1
	tcSmallint  typeCode = 2
	tcInteger   typeCode = 3
	tcBigint    typeCode = 4
	tcDecimal   typeCode = 5
	tcReal      typeCode = 6
	tcDouble    typeCode = 7
	tcChar      typeCode = 8
	tcVarchar   typeCode = 9
	tcNchar     typeCode = 10
	tcNvarchar  typeCode = 11
	tcBinary    typeCode = 12
	tcVarbinary typeCode = 13
	// deprecated with 3 (doku) - but table 'date' field uses it
	tcDate typeCode = 14
	// deprecated with 3 (doku) - but table 'time' field uses it
	tcTime typeCode = 15
	// deprecated with 3 (doku) - but table 'timestamp' field uses it
	tcTimestamp typeCode = 16
	//tcTimetz            typeCode = 17 // reserved: do not use
	//tcTimeltz           typeCode = 18 // reserved: do not use
	//tcTimestamptz       typeCode = 19 // reserved: do not use
	//tcTimestampltz      typeCode = 20 // reserved: do not use
	//tcInvervalym        typeCode = 21 // reserved: do not use
	//tcInvervalds        typeCode = 22 // reserved: do not use
	//tcRowid             typeCode = 23 // reserved: do not use
	//tcUrowid            typeCode = 24 // reserved: do not use
	tcClob     typeCode = 25
	tcNclob    typeCode = 26
	tcBlob     typeCode = 27
	tcBoolean  typeCode = 28
	tcString   typeCode = 29
	tcNstring  typeCode = 30
	tcBlocator typeCode = 31
	tcNlocator typeCode = 32
	tcBstring  typeCode = 33
	//tcDecimaldigitarray typeCode = 34 // reserved: do not use
	tcVarchar2   typeCode = 35
	tcVarchar3   typeCode = 36
	tcNvarchar3  typeCode = 37
	tcVarbinary3 typeCode = 38
	//tcVargroup          typeCode = 39 // reserved: do not use
	//tcTinyintnotnull    typeCode = 40 // reserved: do not use
	//tcSmallintnotnull   typeCode = 41 // reserved: do not use
	//tcIntnotnull        typeCode = 42 // reserved: do not use
	//tcBigintnotnull     typeCode = 43 // reserved: do not use
	//tcArgument          typeCode = 44 // reserved: do not use
	//tcTable             typeCode = 45 // reserved: do not use
	//tcCursor            typeCode = 46 // reserved: do not use
	tcSmalldecimal typeCode = 47
	//tcAbapitab          typeCode = 48 // not supported by GO hdb driver
	//tcAbapstruct        typeCode = 49 // not supported by GO hdb driver
	tcArray     typeCode = 50
	tcText      typeCode = 51
	tcShorttext typeCode = 52
	//tcFixedString       typeCode = 53 // reserved: do not use
	//tcFixedpointdecimal typeCode = 54 // reserved: do not use
	tcAlphanum typeCode = 55
	//tcTlocator    typeCode = 56 // reserved: do not use
	tcLongdate   typeCode = 61
	tcSeconddate typeCode = 62
	tcDaydate    typeCode = 63
	tcSecondtime typeCode = 64
	//tcCte         typeCode = 65 // reserved: do not use
	//tcCstimesda   typeCode = 66 // reserved: do not use
	//tcBlobdisk    typeCode = 71 // reserved: do not use
	//tcClobdisk    typeCode = 72 // reserved: do not use
	//tcNclobdisk   typeCode = 73 // reserved: do not use
	//tcGeometry    typeCode = 74 // reserved: do not use
	//tcPoint       typeCode = 75 // reserved: do not use
	//tcFixed16     typeCode = 76 // reserved: do not use
	//tcBlobhybrid  typeCode = 77 // reserved: do not use
	//tcClobhybrid  typeCode = 78 // reserved: do not use
	//tcNclobhybrid typeCode = 79 // reserved: do not use
	//tcPointz      typeCode = 80 // reserved: do not use

	// additional internal typecodes
	tcTableRef  typeCode = 126
	tcTableRows typeCode = 127
)

func (tc typeCode) isLob() bool {
	return tc == tcClob || tc == tcNclob || tc == tcBlob
}

func (tc typeCode) isCharBased() bool {
	return tc == tcNvarchar || tc == tcNstring || tc == tcNclob
}

func (tc typeCode) isVariableLength() bool {
	return tc == tcChar || tc == tcNchar || tc == tcVarchar || tc == tcNvarchar || tc == tcBinary || tc == tcVarbinary || tc == tcShorttext || tc == tcAlphanum
}

func (tc typeCode) isIntegerType() bool {
	return tc == tcTinyint || tc == tcSmallint || tc == tcInteger || tc == tcBigint
}

func (tc typeCode) isDecimalType() bool {
	return tc == tcSmalldecimal || tc == tcDecimal
}

var dataTypeMap = map[typeCode]DataType{
	tcTinyint:    DtTinyint,
	tcSmallint:   DtSmallint,
	tcInteger:    DtInteger,
	tcBigint:     DtBigint,
	tcReal:       DtReal,
	tcDouble:     DtDouble,
	tcDate:       DtTime,
	tcTime:       DtTime,
	tcTimestamp:  DtTime,
	tcLongdate:   DtTime,
	tcSeconddate: DtTime,
	tcDaydate:    DtTime,
	tcSecondtime: DtTime,
	tcDecimal:    DtDecimal,
	tcChar:       DtString,
	tcVarchar:    DtString,
	tcString:     DtString,
	tcAlphanum:   DtString,
	tcNchar:      DtString,
	tcNvarchar:   DtString,
	tcNstring:    DtString,
	tcShorttext:  DtString,
	tcBinary:     DtBytes,
	tcVarbinary:  DtBytes,
	tcBlob:       DtLob,
	tcClob:       DtLob,
	tcNclob:      DtLob,
	tcText:       DtLob, // TODO - check with python client
	tcTableRef:   DtString,
	tcTableRows:  DtRows,
}

// DataType converts a type code into one of the supported data types by the driver.
func (tc typeCode) dataType() DataType {
	dt, ok := dataTypeMap[tc]
	if !ok {
		panic(fmt.Sprintf("Missing DataType for typeCode %s", tc))
	}
	return dt
}

// typeName returns the database type name.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeDatabaseTypeName
func (tc typeCode) typeName() string {
	return strings.ToUpper(tc.String()[2:])
}

var tcFieldTypeMap = map[typeCode]fieldType{
	tcTinyint:    tinyintType,
	tcSmallint:   smallintType,
	tcInteger:    integerType,
	tcBigint:     bigintType,
	tcReal:       realType,
	tcDouble:     doubleType,
	tcDate:       dateType,
	tcTime:       timeType,
	tcTimestamp:  timestampType,
	tcLongdate:   longdateType,
	tcSeconddate: seconddateType,
	tcDaydate:    daydateType,
	tcSecondtime: secondtimeType,
	tcDecimal:    decimalType,
	tcChar:       varType,
	tcVarchar:    varType,
	tcString:     varType,
	tcAlphanum:   varType,
	tcNchar:      cesu8Type,
	tcNvarchar:   cesu8Type,
	tcNstring:    cesu8Type,
	tcShorttext:  cesu8Type,
	tcBinary:     varType,
	tcVarbinary:  varType,
	tcBlob:       lobVarType,
	tcClob:       lobVarType,
	tcNclob:      lobCESU8Type,
	tcText:       lobCESU8Type, // TODO - check with python client
}

func (tc typeCode) fieldType() fieldType {
	f, ok := tcFieldTypeMap[tc]
	if !ok {
		panic(fmt.Sprintf("Missing FieldType for typeCode %s", tc))
	}
	return f
}
