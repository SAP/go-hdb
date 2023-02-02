package spatial

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"reflect"
)

// Byte orders
const (
	XDR byte = 0x00 // Big endian
	NDR byte = 0x01 // Little endian
)

// flags
const sridFlag uint32 = 0x20000000

// dim offsets
const (
	dimZ  uint32 = 1000
	dimM  uint32 = 2000
	dimZM uint32 = 3000
)

func wkbType(g Geometry) uint32 {
	gt := geoType(g)

	name := reflect.TypeOf(g).Name()
	size := len(name)
	switch {
	case name[size-2:] == "ZM":
		return gt + dimZM
	case name[size-1:] == "Z":
		return gt + dimZ
	case name[size-1:] == "M":
		return gt + dimM
	default:
		return gt
	}
}

type wkbBuffer struct {
	io.Writer
	b         *bytes.Buffer
	order     binary.ByteOrder
	orderByte byte
	extended  bool
	srid      int32
}

func newWKBBuffer(isXDR, extended bool, srid int32) *wkbBuffer {
	b := new(bytes.Buffer)
	w := hex.NewEncoder(b)

	var order binary.ByteOrder
	var orderByte byte
	if isXDR {
		order = binary.BigEndian
		orderByte = XDR
	} else {
		order = binary.LittleEndian
		orderByte = NDR
	}

	return &wkbBuffer{Writer: w, b: b, order: order, orderByte: orderByte, extended: extended, srid: srid}
}

func (b *wkbBuffer) writeCoord(fs ...float64) { binary.Write(b, b.order, fs) }

func (b *wkbBuffer) writeSize(size int) { binary.Write(b, b.order, uint32(size)) }

func (b *wkbBuffer) writeType(g Geometry) {
	b.Write([]byte{b.orderByte})
	if b.extended {
		binary.Write(b, b.order, wkbType(g)|sridFlag)
		binary.Write(b, b.order, b.srid)
		b.extended = false
	} else {
		binary.Write(b, b.order, wkbType(g))
	}
}

func (b *wkbBuffer) bytes() []byte { return b.b.Bytes() }

func (c Coord) encodeWKB(b *wkbBuffer)   { b.writeCoord(c.X, c.Y) }
func (c CoordZ) encodeWKB(b *wkbBuffer)  { b.writeCoord(c.X, c.Y, c.Z) }
func (c CoordM) encodeWKB(b *wkbBuffer)  { b.writeCoord(c.X, c.Y, c.M) }
func (c CoordZM) encodeWKB(b *wkbBuffer) { b.writeCoord(c.X, c.Y, c.Z, c.M) }

func encodeWKBCoord(b *wkbBuffer, c any) {
	cv := reflect.ValueOf(c)
	switch {
	case cv.Type().ConvertibleTo(coordType):
		cv.Convert(coordType).Interface().(Coord).encodeWKB(b)
	case cv.Type().ConvertibleTo(coordZType):
		cv.Convert(coordZType).Interface().(CoordZ).encodeWKB(b)
	case cv.Type().ConvertibleTo(coordMType):
		cv.Convert(coordMType).Interface().(CoordM).encodeWKB(b)
	case cv.Type().ConvertibleTo(coordZMType):
		cv.Convert(coordZMType).Interface().(CoordZM).encodeWKB(b)
	default:
		panic("invalid coordinate type")
	}
}

func encodeWKB(b *wkbBuffer, g Geometry) {

	b.writeType(g)

	switch geoType(g) {
	case geoPoint:
		encodeWKBCoord(b, g)
	case geoLineString, geoCircularString:
		gv := reflect.ValueOf(g)
		size := gv.Len()
		b.writeSize(size)
		for i := 0; i < size; i++ {
			encodeWKBCoord(b, gv.Index(i).Interface())
		}
	case geoPolygon:
		gv := reflect.ValueOf(g)
		size := gv.Len()
		b.writeSize(size)
		for i := 0; i < size; i++ {
			ringv := gv.Index(i)
			size := ringv.Len()
			b.writeSize(size)
			for j := 0; j < size; j++ {
				encodeWKBCoord(b, ringv.Index(j).Interface())
			}
		}
	case geoMultiPoint, geoMultiLineString, geoMultiPolygon, geoGeometryCollection:
		gv := reflect.ValueOf(g)
		size := gv.Len()
		b.writeSize(size)
		for i := 0; i < size; i++ {
			encodeWKB(b, gv.Index(i).Interface().(Geometry))
		}
	}
}

// EncodeWKB encodes a geometry to the "well known binary" format.
func EncodeWKB(g Geometry, isXDR bool) ([]byte, error) {
	b := newWKBBuffer(isXDR, false, -1)
	encodeWKB(b, g)
	return b.bytes(), nil
}

// EncodeEWKB encodes a geometry to the "extended well known binary" format.
func EncodeEWKB(g Geometry, isXDR bool, srid int32) ([]byte, error) {
	b := newWKBBuffer(isXDR, true, srid)
	encodeWKB(b, g)
	return b.bytes(), nil
}
