// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package drivertest

import (
	"fmt"
)

// HDB datatype constants.
const (
	DtBoolean = "boolean"

	DtTinyint  = "tinyint"
	DtSmallint = "smallint"
	DtInteger  = "integer"
	DtBigint   = "bigint"
	DtReal     = "real"
	DtDouble   = "double"

	DtDecimal = "decimal"

	DtChar     = "char"
	DtVarchar  = "varchar"
	DtNchar    = "nchar"
	DtNvarchar = "nvarchar"
	DtAlphanum = "alphanum"

	DtText    = "text"
	DtBintext = "bintext"

	DtBinary    = "binary"
	DtVarbinary = "varbinary"

	DtDate       = "date"
	DtTime       = "time"
	DtSeconddate = "seconddate"
	DtDaydate    = "daydate"
	DtSecondtime = "secondtime"
	DtTimestamp  = "timestamp"
	DtLongdate   = "longdate"

	DtClob  = "clob"
	DtNclob = "nclob"
	DtBlob  = "blob"
)

// HDBColumn is the subset of methods needed to be implemented by all column types.
type HDBColumn interface {
	Name() string   // Name of the column.
	Column() string // Definition of the column.
}

// HDB default column datatypes.
var (
	HDBBoolean = column{typ: DtBoolean}

	HDBTinyint  = column{typ: DtTinyint}
	HDBSmallint = column{typ: DtSmallint}
	HDBInteger  = column{typ: DtInteger}
	HDBBigint   = column{typ: DtBigint}
	HDBReal     = column{typ: DtReal}
	HDBDouble   = column{typ: DtDouble}

	HDBText    = column{typ: DtText}
	HDBBintext = column{typ: DtBintext}

	HDBDate       = column{typ: DtDate}
	HDBTime       = column{typ: DtTime}
	HDBSeconddate = column{typ: DtSeconddate}
	HDBDaydate    = column{typ: DtDaydate}
	HDBSecondtime = column{typ: DtSecondtime}
	HDBTimestamp  = column{typ: DtTimestamp}
	HDBLongdate   = column{typ: DtLongdate}

	HDBClob  = column{typ: DtClob}
	HDBNclob = column{typ: DtNclob}
	HDBBlob  = column{typ: DtBlob}
)

type column struct {
	typ string
}

func (c column) Type() string   { return c.typ }
func (c column) Name() string   { return c.typ }
func (c column) Column() string { return c.typ }

// SizeColumn represents a database column with a size parameter (e.g. varchar).
type SizeColumn struct {
	column
	size int
}

// NewSizeColumn returns a new SizeColumn object.
func NewSizeColumn(typ string, size int) *SizeColumn {
	return &SizeColumn{column: column{typ: typ}, size: size}
}

// Size return the size of the column.
func (c SizeColumn) Size() int { return c.size }

// Name implements the HDBColumn interface.
func (c SizeColumn) Name() string { return fmt.Sprintf("%s_%d", c.Type(), c.size) }

// Column implements the HDBColumn interface.
func (c SizeColumn) Column() string { return fmt.Sprintf("%s(%d)", c.Type(), c.size) }

// PrecScaleColumn represents a database column with precision and scale parameters (e.g. decimal).
type PrecScaleColumn struct {
	column
	prec, scale int
}

// NewPrecScalColumn returns a new PrecScaleColumn object.
func NewPrecScalColumn(typ string, prec, scale int) *PrecScaleColumn {
	return &PrecScaleColumn{column: column{typ: typ}, prec: prec, scale: scale}
}

// Name implements the HDBColumn interface.
func (c PrecScaleColumn) Name() string {
	if c.prec == 0 && c.scale == 0 {
		return c.column.Name()
	}
	return fmt.Sprintf("%s_%d_%d", c.Type(), c.prec, c.scale)
}

// Column implements the HDBColumn interface.
func (c PrecScaleColumn) Column() string {
	if c.prec == 0 && c.scale == 0 {
		return c.column.Column()
	}
	return fmt.Sprintf("%s(%d, %d)", c.Type(), c.prec, c.scale)
}
