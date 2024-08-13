package spatial

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"reflect"
)

// Byte orders.
const (
	XDR byte = 0x00 // Big endian
	NDR byte = 0x01 // Little endian
)

// flags.
const sridFlag uint32 = 0x20000000

// dim offsets.
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

func (b *wkbBuffer) writeCoord(fs ...float64) error { return binary.Write(b, b.order, fs) }

func (b *wkbBuffer) writeSize(size int) error { return binary.Write(b, b.order, uint32(size)) }

func (b *wkbBuffer) writeType(g Geometry) error {
	if _, err := b.Write([]byte{b.orderByte}); err != nil {
		return err
	}
	if b.extended {
		if err := binary.Write(b, b.order, wkbType(g)|sridFlag); err != nil {
			return err
		}
		if err := binary.Write(b, b.order, b.srid); err != nil {
			return err
		}
		b.extended = false
	} else {
		if err := binary.Write(b, b.order, wkbType(g)); err != nil {
			return err
		}
	}
	return nil
}

func (b *wkbBuffer) bytes() []byte { return b.b.Bytes() }

func (c Coord) encodeWKB(b *wkbBuffer) error   { return b.writeCoord(c.X, c.Y) }
func (c CoordZ) encodeWKB(b *wkbBuffer) error  { return b.writeCoord(c.X, c.Y, c.Z) }
func (c CoordM) encodeWKB(b *wkbBuffer) error  { return b.writeCoord(c.X, c.Y, c.M) }
func (c CoordZM) encodeWKB(b *wkbBuffer) error { return b.writeCoord(c.X, c.Y, c.Z, c.M) }

func encodeWKBCoord(b *wkbBuffer, c any) error {
	var err error
	cv := reflect.ValueOf(c)
	switch {
	case cv.Type().ConvertibleTo(coordType):
		err = cv.Convert(coordType).Interface().(Coord).encodeWKB(b)
	case cv.Type().ConvertibleTo(coordZType):
		err = cv.Convert(coordZType).Interface().(CoordZ).encodeWKB(b)
	case cv.Type().ConvertibleTo(coordMType):
		err = cv.Convert(coordMType).Interface().(CoordM).encodeWKB(b)
	case cv.Type().ConvertibleTo(coordZMType):
		err = cv.Convert(coordZMType).Interface().(CoordZM).encodeWKB(b)
	default:
		panic("invalid coordinate type")
	}
	return err
}

func encodeWKB(b *wkbBuffer, g Geometry) error {
	if err := b.writeType(g); err != nil {
		return err
	}

	switch geoType(g) {
	case geoPoint:
		if err := encodeWKBCoord(b, g); err != nil {
			return err
		}
	case geoLineString, geoCircularString:
		gv := reflect.ValueOf(g)
		size := gv.Len()
		if err := b.writeSize(size); err != nil {
			return err
		}
		for i := range size {
			if err := encodeWKBCoord(b, gv.Index(i).Interface()); err != nil {
				return err
			}
		}
	case geoPolygon:
		gv := reflect.ValueOf(g)
		size := gv.Len()
		if err := b.writeSize(size); err != nil {
			return err
		}
		for i := range size {
			ringv := gv.Index(i)
			size := ringv.Len()
			if err := b.writeSize(size); err != nil {
				return err
			}
			for j := range size {
				if err := encodeWKBCoord(b, ringv.Index(j).Interface()); err != nil {
					return err
				}
			}
		}
	case geoMultiPoint, geoMultiLineString, geoMultiPolygon, geoGeometryCollection:
		gv := reflect.ValueOf(g)
		size := gv.Len()
		if err := b.writeSize(size); err != nil {
			return err
		}
		for i := range size {
			if err := encodeWKB(b, gv.Index(i).Interface().(Geometry)); err != nil {
				return err
			}
		}
	}
	return nil
}

// EncodeWKB encodes a geometry to the "well known binary" format.
func EncodeWKB(g Geometry, isXDR bool) ([]byte, error) {
	b := newWKBBuffer(isXDR, false, -1)
	if err := encodeWKB(b, g); err != nil {
		return nil, err
	}
	return b.bytes(), nil
}

// EncodeEWKB encodes a geometry to the "extended well known binary" format.
func EncodeEWKB(g Geometry, isXDR bool, srid int32) ([]byte, error) {
	b := newWKBBuffer(isXDR, true, srid)
	if err := encodeWKB(b, g); err != nil {
		return nil, err
	}
	return b.bytes(), nil
}
