package spatial

import (
	"bytes"
	"math"
	"reflect"
	"strconv"
	"strings"
)

func wktTypeName(g Geometry) string {
	name := strings.ToUpper(geoTypeName(g))
	switch g.(type) {
	case GeometryZM:
		return name + " ZM"
	case GeometryM:
		return name + " M"
	case GeometryZ:
		return name + " Z"
	default:
		return name
	}
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
	switch cv := c.(type) {
	case Coord:
		cv.encodeWKT(b)
	case CoordZ:
		cv.encodeWKT(b)
	case CoordM:
		cv.encodeWKT(b)
	case CoordZM:
		cv.encodeWKT(b)
	case Point:
		Coord(cv).encodeWKT(b)
	case PointZ:
		CoordZ(cv).encodeWKT(b)
	case PointM:
		CoordM(cv).encodeWKT(b)
	case PointZM:
		CoordZM(cv).encodeWKT(b)
	default:
		panic("invalid coordinate type")
	}
}

func encodeWKT(b *wktBuffer, printType bool, g Geometry) {
	if printType {
		b.writeStrings(wktTypeName(g), " ")
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
			encodeWKT(b, false, gv.Index(i).Interface().(Geometry))
		})
	case geoGeometryCollection:
		gv := reflect.ValueOf(g)
		b.writeList(gv.Len(), func(i int) {
			encodeWKT(b, true, gv.Index(i).Interface().(Geometry))
		})
	}
}

// EncodeWKT encodes a geometry to the "well known text" format.
func EncodeWKT(g Geometry) ([]byte, error) {
	b := new(wktBuffer)
	encodeWKT(b, true, g)
	return b.Bytes(), nil
}

// EncodeEWKT encodes a geometry to the "well known text" format.
func EncodeEWKT(g Geometry, srid int32) ([]byte, error) {
	b := new(wktBuffer)
	b.writeStrings("SRID=", strconv.Itoa(int(srid)), ";")
	encodeWKT(b, true, g)
	return b.Bytes(), nil
}
