// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package spatial

import (
	"bytes"
	"math"
	"reflect"
	"strconv"
	"strings"
)

func wktTypeName(g Geometry) string {
	name := reflect.TypeOf(g).Name()
	size := len(name)
	switch {
	case name[size-2:] == "ZM":
		return strings.ToUpper(name[:size-2]) + " ZM"
	case name[size-1:] == "M":
		return strings.ToUpper(name[:size-1]) + " M"
	case name[size-1:] == "Z":
		return strings.ToUpper(name[:size-1]) + " Z"
	default:
		return strings.ToUpper(name)
	}
}

func wktShortTypeName(g Geometry) string {
	return strings.ToUpper(geoTypeName(g))
}

func formatFloat(f float64) string {
	if math.IsNaN(f) {
		return "NULL"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

type wktBuffer struct {
	bytes.Buffer
}

func (b *wktBuffer) writeCoord(fs ...float64) {
	b.WriteString(formatFloat(fs[0]))
	for _, f := range fs[1:] {
		b.WriteString(" ")
		b.WriteString(formatFloat(f))
	}
}

func (b *wktBuffer) withBrackets(fn func()) {
	b.WriteByte('(')
	fn()
	b.WriteByte(')')
}

func (b *wktBuffer) writeList(size int, fn func(i int)) {
	if size == 0 {
		b.WriteString("EMPTY")
		return
	}
	b.WriteByte('(')
	fn(0)
	for i := 1; i < size; i++ {
		b.WriteByte(',')
		fn(i)
	}
	b.WriteByte(')')
}

func (b *wktBuffer) writeStrings(strs ...string) {
	for _, s := range strs {
		b.WriteString(s)
	}
}

func (c Coord) encodeWKT(b *wktBuffer)   { b.writeCoord(c.X, c.Y) }
func (c CoordZ) encodeWKT(b *wktBuffer)  { b.writeCoord(c.X, c.Y, c.Z) }
func (c CoordM) encodeWKT(b *wktBuffer)  { b.writeCoord(c.X, c.Y, c.M) }
func (c CoordZM) encodeWKT(b *wktBuffer) { b.writeCoord(c.X, c.Y, c.Z, c.M) }

func encodeWKTCoord(b *wktBuffer, c any) {
	cv := reflect.ValueOf(c)
	switch {
	case cv.Type().ConvertibleTo(coordType):
		cv.Convert(coordType).Interface().(Coord).encodeWKT(b)
	case cv.Type().ConvertibleTo(coordZType):
		cv.Convert(coordZType).Interface().(CoordZ).encodeWKT(b)
	case cv.Type().ConvertibleTo(coordMType):
		cv.Convert(coordMType).Interface().(CoordM).encodeWKT(b)
	case cv.Type().ConvertibleTo(coordZMType):
		cv.Convert(coordZMType).Interface().(CoordZM).encodeWKT(b)
	default:
		panic("invalid coordinate type")
	}
}

const (
	typeFull byte = iota
	typeShort
	typeNone
)

func encodeWKT(b *wktBuffer, typeFlag byte, g Geometry) {
	switch typeFlag {
	case typeFull:
		b.writeStrings(wktTypeName(g), " ")
	case typeShort:
		b.writeStrings(wktShortTypeName(g), " ")
	}

	switch geoType(g) {
	case geoPoint:
		b.withBrackets(func() {
			encodeWKTCoord(b, g)
		})
	case geoLineString, geoCircularString:
		gv := reflect.ValueOf(g)
		b.writeList(gv.Len(), func(i int) {
			encodeWKTCoord(b, gv.Index(i).Interface())
		})
	case geoPolygon:
		gv := reflect.ValueOf(g)
		b.writeList(gv.Len(), func(i int) {
			ringv := gv.Index(i)
			b.writeList(ringv.Len(), func(i int) {
				encodeWKTCoord(b, ringv.Index(i).Interface())
			})
		})
	case geoMultiPoint, geoMultiLineString, geoMultiPolygon:
		gv := reflect.ValueOf(g)
		b.writeList(gv.Len(), func(i int) {
			encodeWKT(b, typeNone, gv.Index(i).Interface().(Geometry))
		})
	case geoGeometryCollection:
		gv := reflect.ValueOf(g)
		b.writeList(gv.Len(), func(i int) {
			encodeWKT(b, typeShort, gv.Index(i).Interface().(Geometry))
		})
	}
}

// EncodeWKT encodes a geometry to the "well known text" format.
func EncodeWKT(g Geometry) ([]byte, error) {
	b := new(wktBuffer)
	encodeWKT(b, typeFull, g)
	return b.Bytes(), nil
}

// EncodeEWKT encodes a geometry to the "well known text" format.
func EncodeEWKT(g Geometry, srid int32) ([]byte, error) {
	b := new(wktBuffer)
	b.writeStrings("SRID=", strconv.Itoa(int(srid)), ";")
	encodeWKT(b, typeFull, g)
	return b.Bytes(), nil
}
