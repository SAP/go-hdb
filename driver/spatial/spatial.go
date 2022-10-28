// Package spatial implements geo spatial types and functions.
package spatial

import (
	"math"
	"reflect"
	"strings"
)

// NaN returns a 'not-a-number' value.
func NaN() float64 { return math.NaN() }

// Geometry is the interface representing a spatial type.
type Geometry interface {
	geotype()
}

// Geometry2d is the interface representing a two dimensional spatial type.
type Geometry2d interface {
	type2d()
}

// GeometryZ is the interface representing a three dimensional spatial type.
type GeometryZ interface {
	Geometry
	typeZ()
}

// GeometryM is the interface representing an annotated two dimensional spatial type.
type GeometryM interface {
	Geometry
	typeM()
}

// GeometryZM is the interface representing an annotated three dimensional annotated spatial type.
type GeometryZM interface {
	Geometry
	typeZM()
}

// Coord represents a two dimensional coordinate.
type Coord struct{ X, Y float64 }

// CoordZ represents a three dimensional coordinate.
type CoordZ struct{ X, Y, Z float64 }

// CoordM represents an annotated two dimensional coordinate.
type CoordM struct{ X, Y, M float64 }

// CoordZM represents an annotated three dimensional coordinate.
type CoordZM struct{ X, Y, M, Z float64 }

var (
	coordType   = reflect.TypeOf((*Coord)(nil)).Elem()
	coordZType  = reflect.TypeOf((*CoordZ)(nil)).Elem()
	coordMType  = reflect.TypeOf((*CoordM)(nil)).Elem()
	coordZMType = reflect.TypeOf((*CoordZM)(nil)).Elem()
)

// Point represents a two dimensional point.
type Point Coord

// PointZ represents a three dimensional point.
type PointZ CoordZ

// PointM represents an annotated two dimensional point.
type PointM CoordM

// PointZM represents an annotated three dimensional point.
type PointZM CoordZM

// LineString represents a two dimensional line string.
type LineString []Coord

// LineStringZ represents a three dimensional line string.
type LineStringZ []CoordZ

// LineStringM represents an annotated two dimensional line string.
type LineStringM []CoordM

// LineStringZM represents an annotated three dimensional line string.
type LineStringZM []CoordZM

// CircularString represents a two dimensional circular string.
type CircularString []Coord

// CircularStringZ represents a three dimensional circular string.
type CircularStringZ []CoordZ

// CircularStringM represents an annotated two dimensional circular string.
type CircularStringM []CoordM

// CircularStringZM represents an annotated three dimensional circular string.
type CircularStringZM []CoordZM

// Polygon represents a two dimensional polygon.
type Polygon [][]Coord

// PolygonZ represents a three dimensional polygon.
type PolygonZ [][]CoordZ

// PolygonM represents an annotated two dimensional polygon.
type PolygonM [][]CoordM

// PolygonZM represents an annotated three dimensional polygon.
type PolygonZM [][]CoordZM

// MultiPoint represents a two dimensional multi point.
type MultiPoint []Point

// MultiPointZ represents a three dimensional multi point.
type MultiPointZ []PointZ

// MultiPointM represents an annotated two dimensional multi point.
type MultiPointM []PointM

// MultiPointZM represents an annotated three dimensional multi point.
type MultiPointZM []PointZM

// MultiLineString represents a two dimensional multi line string.
type MultiLineString []LineString

// MultiLineStringZ represents a three dimensional multi line string.
type MultiLineStringZ []LineStringZ

// MultiLineStringM represents an annotated two dimensional multi line string.
type MultiLineStringM []LineStringM

// MultiLineStringZM represents an annotated three dimensional multi line string.
type MultiLineStringZM []LineStringZM

// MultiPolygon represents a two dimensional multi polygon.
type MultiPolygon []Polygon

// MultiPolygonZ represents a three dimensional multi polygon.
type MultiPolygonZ []PolygonZ

// MultiPolygonM represents an annotated two dimensional multi polygon.
type MultiPolygonM []PolygonM

// MultiPolygonZM represents an annotated three dimensional multi polygon.
type MultiPolygonZM []PolygonZM

// GeometryCollection represents a two dimensional geometry collection.
type GeometryCollection []Geometry2d

// GeometryCollectionZ represents a three dimensional geometry collection.
type GeometryCollectionZ []GeometryZ

// GeometryCollectionM represents an annotated two dimensional geometry collection.
type GeometryCollectionM []GeometryM

// GeometryCollectionZM represents an annotated three dimensional geometry collection.
type GeometryCollectionZM []GeometryZM

// marker interface
func (g Point) geotype()   {}
func (g PointZ) geotype()  {}
func (g PointM) geotype()  {}
func (g PointZM) geotype() {}

func (g LineString) geotype()   {}
func (g LineStringZ) geotype()  {}
func (g LineStringM) geotype()  {}
func (g LineStringZM) geotype() {}

func (g CircularString) geotype()   {}
func (g CircularStringZ) geotype()  {}
func (g CircularStringM) geotype()  {}
func (g CircularStringZM) geotype() {}

func (g Polygon) geotype()   {}
func (g PolygonZ) geotype()  {}
func (g PolygonM) geotype()  {}
func (g PolygonZM) geotype() {}

func (g MultiPoint) geotype()   {}
func (g MultiPointZ) geotype()  {}
func (g MultiPointM) geotype()  {}
func (g MultiPointZM) geotype() {}

func (g MultiLineString) geotype()   {}
func (g MultiLineStringM) geotype()  {}
func (g MultiLineStringZ) geotype()  {}
func (g MultiLineStringZM) geotype() {}

func (g MultiPolygon) geotype()   {}
func (g MultiPolygonZ) geotype()  {}
func (g MultiPolygonM) geotype()  {}
func (g MultiPolygonZM) geotype() {}

func (g GeometryCollection) geotype()   {}
func (g GeometryCollectionZ) geotype()  {}
func (g GeometryCollectionM) geotype()  {}
func (g GeometryCollectionZM) geotype() {}

func (g Point) type2d()              {}
func (g LineString) type2d()         {}
func (g CircularString) type2d()     {}
func (g Polygon) type2d()            {}
func (g MultiPoint) type2d()         {}
func (g MultiLineString) type2d()    {}
func (g MultiPolygon) type2d()       {}
func (g GeometryCollection) type2d() {}

func (g PointZ) typeZ()              {}
func (g LineStringZ) typeZ()         {}
func (g CircularStringZ) typeZ()     {}
func (g PolygonZ) typeZ()            {}
func (g MultiPointZ) typeZ()         {}
func (g MultiLineStringZ) typeZ()    {}
func (g MultiPolygonZ) typeZ()       {}
func (g GeometryCollectionZ) typeZ() {}

func (g PointM) typeM()              {}
func (g LineStringM) typeM()         {}
func (g CircularStringM) typeM()     {}
func (g PolygonM) typeM()            {}
func (g MultiPointM) typeM()         {}
func (g MultiLineStringM) typeM()    {}
func (g MultiPolygonM) typeM()       {}
func (g GeometryCollectionM) typeM() {}

func (g PointZM) typeZM()              {}
func (g LineStringZM) typeZM()         {}
func (g CircularStringZM) typeZM()     {}
func (g PolygonZM) typeZM()            {}
func (g MultiPointZM) typeZM()         {}
func (g MultiLineStringZM) typeZM()    {}
func (g MultiPolygonZM) typeZM()       {}
func (g GeometryCollectionZM) typeZM() {}

const (
	geoPoint              uint32 = 1
	geoLineString         uint32 = 2
	geoPolygon            uint32 = 3
	geoMultiPoint         uint32 = 4
	geoMultiLineString    uint32 = 5
	geoMultiPolygon       uint32 = 6
	geoGeometryCollection uint32 = 7
	geoCircularString     uint32 = 8
)

func geoTypeName(g Geometry) string {
	name := reflect.TypeOf(g).Name()
	size := len(name)
	switch {
	case name[size-2:] == "ZM":
		return name[:size-2]
	case strings.ContainsAny(name[size-1:], "MZ"):
		return name[:size-1]
	default:
		return name
	}
}

func geoType(g Geometry) uint32 {
	switch geoTypeName(g) {
	case "Point":
		return geoPoint
	case "LineString":
		return geoLineString
	case "Polygon":
		return geoPolygon
	case "MultiPoint":
		return geoMultiPoint
	case "MultiLineString":
		return geoMultiLineString
	case "MultiPolygon":
		return geoMultiPolygon
	case "GeometryCollection":
		return geoGeometryCollection
	case "CircularString":
		return geoCircularString
	default:
		panic("invalid geoTypeName")
	}
}
