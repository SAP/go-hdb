// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package spatial

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// Byte orders
const (
	XDR byte = 0x00 // Big endian
	NDR      = 0x01 // Little endian
)

// wbkType flags
const (
	noFlag   uint32 = 0x00000000 // 2d flag
	sridFlag        = 0x20000000 // SRID flag
	mFlag           = 0x40000000 // M flag
	zFlag           = 0x80000000 // Z flag
)

// wbkType
const (
	wkbPoint              uint32 = 0x000000001
	wkbLineString                = 0x000000002
	wkbPolygon                   = 0x000000003
	wkbMultiPoint                = 0x000000004
	wkbMultiLineString           = 0x000000005
	wkbMultiPolygon              = 0x000000006
	wkbGeometryCollection        = 0x000000007
	//wkbPolyhedralSurface         = 0x00000000F
	//wkbTIN                       = 0x000000010
	//wkbTriangle                  = 0x000000011
)

// Dim is the type for dimensions (2d, Z, M, MZ)
type Dim byte

// Dimensions
const (
	Dim2d Dim = iota // two dimensional
	DimZ             // three dimensional
	DimM             // two dimensional with additinal dimensional information in M as time, road-mile, distance or the like
	DimZM            // three dimensional with additinal dimensional information in M
)

var dimFlags = map[Dim]uint32{
	Dim2d: noFlag,
	DimZ:  zFlag,
	DimM:  mFlag,
	DimZM: zFlag | mFlag,
}

var dimTypes = map[Dim]string{
	Dim2d: "",
	DimZ:  "Z",
	DimM:  "M",
	DimZM: "ZM",
}

// handle unknown dim as 2d.
func dimFlag(dim Dim) uint32 {
	f, ok := dimFlags[dim]
	if !ok {
		return noFlag
	}
	return f
}

// handle unknown dim as 2d.
func dimType(dim Dim) string {
	t, ok := dimTypes[dim]
	if !ok {
		return ""
	}
	return t
}

// NaN returns a 'not-a-number' value.
func NaN() float64 { return math.NaN() }

// token
const (
	space        string = " "
	openBracket         = "("
	closeBracket        = ")"
	empty               = "EMPTY"
)

func writeFloat(w io.StringWriter, f float64) { w.WriteString(strconv.FormatFloat(f, 'f', -1, 64)) }
func writeToken(w io.StringWriter, token ...string) {
	for _, t := range token {
		w.WriteString(t)
	}
}

// Point represents whether a 2d, Z, M or ZM dimensional point.
type Point struct{ X, Y, Z, M float64 }

func (p Point) String() string {
	return fmt.Sprintf("x: %f, y: %f, z: %f, m: %f", p.X, p.Y, p.Z, p.M)
}

func (p Point) encodeWKB(w io.Writer, byteOrder binary.ByteOrder, dim Dim) error {
	if err := binary.Write(w, byteOrder, p.X); err != nil {
		return err
	}
	if err := binary.Write(w, byteOrder, p.Y); err != nil {
		return err
	}
	if dim == DimZ || dim == DimZM {
		if err := binary.Write(w, byteOrder, p.Z); err != nil {
			return err
		}
	}
	if dim == DimM || dim == DimZM {
		if err := binary.Write(w, byteOrder, p.M); err != nil {
			return err
		}
	}
	return nil
}

func (p Point) encodeWKT(w io.StringWriter, dim Dim) error {
	writeToken(w, openBracket)
	writeFloat(w, p.X)
	writeToken(w, space)
	writeFloat(w, p.Y)
	if dim == DimZ || dim == DimZM {
		writeToken(w, space)
		writeFloat(w, p.Z)
	}
	if dim == DimM || dim == DimZM {
		writeToken(w, space)
		if math.IsNaN(p.M) {
			w.WriteString("NULL")
		} else {
			writeFloat(w, p.M)
		}
	}
	writeToken(w, closeBracket)
	return nil
}

// Points is a collection of 0..n points.
type Points []Point

func (pts Points) String() string {
	return fmt.Sprintf("%s", []Point(pts))
}

func (pts Points) encodeWKB(w io.Writer, byteOrder binary.ByteOrder, dim Dim) error {
	if err := binary.Write(w, byteOrder, uint32(len(pts))); err != nil {
		return err
	}
	for _, p := range pts {
		if err := p.encodeWKB(w, byteOrder, dim); err != nil {
			return err
		}
	}
	return nil
}

func (pts Points) encodeWKT(w io.StringWriter, dim Dim) error {
	writeToken(w, openBracket)
	for _, p := range pts {
		p.encodeWKT(w, dim)
	}
	writeToken(w, closeBracket)
	return nil
}

// STGeometry is the interface representing a spatial type.
type STGeometry interface {
	encodeWKB(w io.Writer, byteOrder binary.ByteOrder, writeHeader bool) error
	encodeWKT(w io.StringWriter) error
	wkbType() uint32
	wktType() string
}

// wkt methods
func (g STPoint) wktType() string              { return "POINT" }
func (g STLineString) wktType() string         { return "LINESTRING" }
func (g STPolygon) wktType() string            { return "POLYGON" }
func (g STMultiPoint) wktType() string         { return "MULTIPOINT" }
func (g STMultiLineString) wktType() string    { return "MULTILINESTRING" }
func (g STMultiPolygon) wktType() string       { return "MULTIPOLYGON" }
func (g STGeometryCollection) wktType() string { return "GEOMETRYCOLLECTION" }

// wkb methods
func (g STPoint) wkbType() uint32 {
	if g.Point == nil {
		return wkbMultiPoint // if point is nil -> multi point
	}
	return wkbPoint | dimFlag(g.Dim)
}
func (g STLineString) wkbType() uint32         { return wkbLineString | dimFlag(g.Dim) }
func (g STPolygon) wkbType() uint32            { return wkbPolygon | dimFlag(g.Dim) }
func (g STMultiPoint) wkbType() uint32         { return wkbMultiPoint | dimFlag(g.Dim) }
func (g STMultiLineString) wkbType() uint32    { return wkbMultiLineString | dimFlag(g.Dim) }
func (g STMultiPolygon) wkbType() uint32       { return wkbMultiPolygon | dimFlag(g.Dim) }
func (g STGeometryCollection) wkbType() uint32 { return wkbGeometryCollection | dimFlag(g.Dim) }

// STPoint is the spatial type for a point.
type STPoint struct {
	Dim   Dim
	Point *Point
}

var nullPoint = new(STMultiPoint) // null point is an initial multi point

func (g STPoint) encodeWKB(w io.Writer, byteOrder binary.ByteOrder, writeHeader bool) error {
	if g.Point == nil {
		return nullPoint.encodeWKB(w, byteOrder, writeHeader)
	}
	if writeHeader {
		if err := binary.Write(w, byteOrder, g.wkbType()); err != nil {
			return err
		}
	}
	return g.Point.encodeWKB(w, byteOrder, g.Dim)
}

func (g STPoint) encodeWKT(w io.StringWriter) error {
	w.WriteString(g.wktType())
	if g.Dim != Dim2d {
		writeToken(w, space)
		w.WriteString(dimType(g.Dim))
	}
	if g.Point == nil {
		writeToken(w, space, empty)
	} else {
		g.Point.encodeWKT(w, g.Dim)
	}
	return nil
}

// STLineString is the spatial type for a linestring.
type STLineString struct {
	Dim    Dim
	Points Points
}

func (g STLineString) encodeWKB(w io.Writer, byteOrder binary.ByteOrder, writeHeader bool) error {
	if writeHeader {
		if err := binary.Write(w, byteOrder, g.wkbType()); err != nil {
			return err
		}
	}
	return g.Points.encodeWKB(w, byteOrder, g.Dim)
}

func (g STLineString) encodeWKT(w io.StringWriter) error {
	w.WriteString(g.wktType())
	if g.Dim != Dim2d {
		writeToken(w, space)
		w.WriteString(dimType(g.Dim))
	}
	if g.Points == nil || len(g.Points) == 0 {
		writeToken(w, space, empty)
	} else {
		g.Points.encodeWKT(w, g.Dim)
	}
	return nil
}

// STPolygon is the spatial type for a polygon.
type STPolygon struct {
	Dim   Dim
	Rings []Points
}

// STMultiPoint is the spatial type for a multipoint.
type STMultiPoint struct {
	Dim    Dim
	Points Points
}

func (g STMultiPoint) encodeWKB(w io.Writer, byteOrder binary.ByteOrder, writeHeader bool) error {
	if writeHeader {
		if err := binary.Write(w, byteOrder, g.wkbType()); err != nil {
			return err
		}
	}
	return g.Points.encodeWKB(w, byteOrder, g.Dim)
}

// STMultiLineString is the spatial type for a multi line string.
type STMultiLineString struct {
	Dim         Dim
	LineStrings []STLineString
}

// STMultiPolygon is the spatial type for a multi polygon.
type STMultiPolygon struct {
	Dim      Dim
	Polygons []STPolygon
}

// STGeometryCollection is the spatial type for a geometry collection.
type STGeometryCollection struct {
	Dim        Dim
	Geometries []STGeometry
}

// EncodeEWKB encodes a geometry to the "extended well known binary" format.
func EncodeEWKB(g STGeometry, xdr bool, srid int32) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := hex.NewEncoder(buf)

	// byte order
	var byteOrder binary.ByteOrder
	if xdr {
		byteOrder = binary.BigEndian
	} else {
		byteOrder = binary.LittleEndian
	}

	// write header
	if xdr {
		w.Write([]byte{XDR})
	} else {
		w.Write([]byte{NDR})
	}
	if err := binary.Write(w, byteOrder, g.wkbType()|sridFlag); err != nil {
		return nil, err
	}
	if err := binary.Write(w, byteOrder, srid); err != nil {
		return nil, err
	}

	// encode
	if err := g.encodeWKB(w, byteOrder, false); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// EncodeWKB encodes a geometry to the "well known binary" format.
func EncodeWKB(g STGeometry, xdr bool) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := hex.NewEncoder(buf)

	// byte order
	var byteOrder binary.ByteOrder
	if xdr {
		byteOrder = binary.BigEndian
	} else {
		byteOrder = binary.LittleEndian
	}

	// write header
	if xdr {
		w.Write([]byte{XDR})
	} else {
		w.Write([]byte{NDR})
	}
	if err := binary.Write(w, byteOrder, g.wkbType()); err != nil {
		return nil, err
	}

	// write geometry
	if err := g.encodeWKB(w, byteOrder, false); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// EncodeWKT encodes a geometry to the "well known text" format.
func EncodeWKT(g STGeometry) (string, error) {
	b := new(strings.Builder)
	if err := g.encodeWKT(b); err != nil {
		return "", err
	}
	return b.String(), nil
}
