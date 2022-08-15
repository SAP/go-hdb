// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"
	"strings"
)

// TypeCode identify the type of a field transferred to or from the database.
type TypeCode byte

// null value indicator is high bit

const (
	tcNullL             TypeCode = 0x00
	tcTinyint           TypeCode = 0x01
	tcSmallint          TypeCode = 0x02
	tcInteger           TypeCode = 0x03
	tcBigint            TypeCode = 0x04
	tcDecimal           TypeCode = 0x05
	tcReal              TypeCode = 0x06
	tcDouble            TypeCode = 0x07
	tcChar              TypeCode = 0x08
	tcVarchar           TypeCode = 0x09 // changed from tcVarchar1 to tcVarchar (ref hdbclient)
	tcNchar             TypeCode = 0x0A
	tcNvarchar          TypeCode = 0x0B
	tcBinary            TypeCode = 0x0C
	tcVarbinary         TypeCode = 0x0D
	tcDate              TypeCode = 0x0E
	tcTime              TypeCode = 0x0F
	tcTimestamp         TypeCode = 0x10
	tcTimetz            TypeCode = 0x11
	tcTimeltz           TypeCode = 0x12
	tcTimestampTz       TypeCode = 0x13
	tcTimestampLtz      TypeCode = 0x14
	tcIntervalYm        TypeCode = 0x15
	tcIntervalDs        TypeCode = 0x16
	tcRowid             TypeCode = 0x17
	tcUrowid            TypeCode = 0x18
	tcClob              TypeCode = 0x19
	tcNclob             TypeCode = 0x1A
	tcBlob              TypeCode = 0x1B
	tcBoolean           TypeCode = 0x1C
	tcString            TypeCode = 0x1D
	tcNstring           TypeCode = 0x1E
	tcLocator           TypeCode = 0x1F
	tcNlocator          TypeCode = 0x20
	tcBstring           TypeCode = 0x21
	tcDecimalDigitArray TypeCode = 0x22
	tcVarchar2          TypeCode = 0x23
	tcTable             TypeCode = 0x2D
	tcSmalldecimal      TypeCode = 0x2f // inserted (not existent in hdbclient)
	tcAbapstream        TypeCode = 0x30
	tcAbapstruct        TypeCode = 0x31
	tcAarray            TypeCode = 0x32
	tcText              TypeCode = 0x33
	tcShorttext         TypeCode = 0x34
	tcBintext           TypeCode = 0x35
	tcAlphanum          TypeCode = 0x37
	tcLongdate          TypeCode = 0x3D
	tcSeconddate        TypeCode = 0x3E
	tcDaydate           TypeCode = 0x3F
	tcSecondtime        TypeCode = 0x40
	tcClocator          TypeCode = 0x46
	tcBlobDiskReserved  TypeCode = 0x47
	tcClobDiskReserved  TypeCode = 0x48
	tcNclobDiskReserved TypeCode = 0x49
	tcStGeometry        TypeCode = 0x4A
	tcStPoint           TypeCode = 0x4B
	tcFixed16           TypeCode = 0x4C
	tcAbapItab          TypeCode = 0x4D
	tcRecordRowStore    TypeCode = 0x4E
	tcRecordColumnStore TypeCode = 0x4F
	tcFixed8            TypeCode = 0x51
	tcFixed12           TypeCode = 0x52
	tcCiphertext        TypeCode = 0x5A

	// special null values
	tcSecondtimeNull TypeCode = 0xB0

	// TcTableRef is the TypeCode for table references.
	TcTableRef TypeCode = 0x7e // 126
	// TcTableRows is the TypeCode for table rows.
	TcTableRows TypeCode = 0x7f // 127
)

// IsLob returns true if the TypeCode represents a Lob, false otherwise.
func (tc TypeCode) IsLob() bool {
	return tc == tcClob || tc == tcNclob || tc == tcBlob || tc == tcText || tc == tcBintext || tc == tcLocator || tc == tcNlocator
}

func (tc TypeCode) isVariableLength() bool {
	return tc == tcChar || tc == tcNchar || tc == tcVarchar || tc == tcNvarchar || tc == tcBinary || tc == tcVarbinary || tc == tcShorttext || tc == tcAlphanum
}

func (tc TypeCode) isDecimalType() bool {
	return tc == tcSmalldecimal || tc == tcDecimal || tc == tcFixed8 || tc == tcFixed12 || tc == tcFixed16
}

func (tc TypeCode) supportNullValue() bool {
	// boolean values: false =:= 0; null =:= 1; true =:= 2
	return !(tc == tcBoolean)
}

func (tc TypeCode) nullValue() TypeCode {
	if tc == tcSecondtime {
		/*
			HDB bug: secondtime null value cannot be set by setting high bit
			- trying so, gives:
			  SQL HdbError 1033 - error while parsing protocol: no such data type: type_code=192, index=2

			HDB version 2: Traffic analysis of python client (https://pypi.org/project/hdbcli) resulted in:
			- set null value constant directly instead of using high bit

			HDB version 4: Setting null value constant does not work anymore
			- secondtime null value typecode is 0xb0 (decimal: 176) instead of 0xc0 (decimal: 192)
			- null typecode 0xb0 does work for HDB version 2 as well
		*/
		return tcSecondtimeNull
	}
	return tc | 0x80 // type code null value: set high bit (like documented in hdb protocol spec)
}

// see hdbclient
func (tc TypeCode) encTc() TypeCode {
	switch tc {
	default:
		return tc
	case tcText, tcBintext, tcLocator:
		return tcNclob
	}
}

/*
tcBintext:
- protocol returns tcLocator for tcBintext
- see dataTypeMap and encTc
*/

func (tc TypeCode) dataType() DataType {
	// performance: use switch instead of map
	switch tc {
	case tcBoolean:
		return DtBoolean
	case tcTinyint:
		return DtTinyint
	case tcSmallint:
		return DtSmallint
	case tcInteger:
		return DtInteger
	case tcBigint:
		return DtBigint
	case tcReal:
		return DtReal
	case tcDouble:
		return DtDouble
	case tcDate:
		return DtTime
	case tcTime, tcTimestamp, tcLongdate, tcSeconddate, tcDaydate, tcSecondtime:
		return DtTime
	case tcDecimal, tcFixed8, tcFixed12, tcFixed16:
		return DtDecimal
	case tcChar, tcVarchar, tcString, tcAlphanum, tcNchar, tcNvarchar, tcNstring, tcShorttext, tcStPoint, tcStGeometry, TcTableRef:
		return DtString
	case tcBinary, tcVarbinary:
		return DtBytes
	case tcBlob, tcClob, tcNclob, tcText, tcBintext:
		return DtLob
	case TcTableRows:
		return DtRows
	default:
		panic(fmt.Sprintf("missing DataType for typeCode %s", tc))
	}
}

// typeName returns the database type name.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeDatabaseTypeName
func (tc TypeCode) typeName() string {
	return strings.ToUpper(tc.String()[2:])
}

func (tc TypeCode) fieldType(length, fraction int) fieldType {
	// performance: use switch instead of map
	switch tc {
	case tcBoolean:
		return booleanType
	case tcTinyint:
		return tinyintType
	case tcSmallint:
		return smallintType
	case tcInteger:
		return integerType
	case tcBigint:
		return bigintType
	case tcReal:
		return realType
	case tcDouble:
		return doubleType
	case tcDate:
		return dateType
	case tcTime:
		return timeType
	case tcTimestamp:
		return timestampType
	case tcLongdate:
		return longdateType
	case tcSeconddate:
		return seconddateType
	case tcDaydate:
		return daydateType
	case tcSecondtime:
		return secondtimeType
	case tcDecimal:
		return decimalType
	case tcChar, tcVarchar, tcString:
		return varType
	case tcAlphanum:
		return alphaType
	case tcNchar, tcNvarchar, tcNstring, tcShorttext:
		return cesu8Type
	case tcBinary, tcVarbinary:
		return varType
	case tcStPoint, tcStGeometry:
		return hexType
	case tcBlob, tcClob, tcLocator:
		return lobVarType
	case tcNclob, tcText, tcNlocator:
		return lobCESU8Type
	case tcBintext: // ?? lobCESU8Type
		return lobVarType
	case tcFixed8:
		return _fixed8Type{prec: length, scale: fraction} // used for decimals(x,y) 2^63 - 1 (int64)
	case tcFixed12:
		return _fixed12Type{prec: length, scale: fraction} // used for decimals(x,y) 2^96 - 1 (int96)
	case tcFixed16:
		return _fixed16Type{prec: length, scale: fraction} // used for decimals(x,y) 2^63 - 1 (int128)
	default:
		panic(fmt.Sprintf("missing fieldType for typeCode %s", tc))
	}
}

func (tc TypeCode) optType() optType {
	switch tc {
	case tcBoolean:
		return optBooleanType
	case tcTinyint:
		return optTinyintType
	case tcInteger:
		return optIntegerType
	case tcBigint:
		return optBigintType
	case tcDouble:
		return optDoubleType
	case tcString:
		return optStringType
	case tcBstring:
		return optBstringType
	default:
		panic(fmt.Sprintf("missing optType for typeCode %s", tc))
	}
}
