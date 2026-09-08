package spatial

import (
	"encoding/json"
	"math"
	"reflect"
)

func coordToSlice(fs ...float64) []*float64 {
	cs := make([]*float64, len(fs))
	for i, f := range fs {
		if !math.IsNaN(f) {
			cs[i] = &fs[i]
		}
	}
	return cs
}

func (c Coord) coordToSlice() []*float64   { return coordToSlice(c.X, c.Y) }
func (c CoordZ) coordToSlice() []*float64  { return coordToSlice(c.X, c.Y, c.Z) }
func (c CoordM) coordToSlice() []*float64  { return coordToSlice(c.X, c.Y, 0, c.M) }
func (c CoordZM) coordToSlice() []*float64 { return coordToSlice(c.X, c.Y, c.Z, c.M) }

func jsonCoord(c any) []*float64 {
	switch cv := c.(type) {
	case Coord:
		return cv.coordToSlice()
	case CoordZ:
		return cv.coordToSlice()
	case CoordM:
		return cv.coordToSlice()
	case CoordZM:
		return cv.coordToSlice()
	case Point:
		return Coord(cv).coordToSlice()
	case PointZ:
		return CoordZ(cv).coordToSlice()
	case PointM:
		return CoordM(cv).coordToSlice()
	case PointZM:
		return CoordZM(cv).coordToSlice()
	default:
		panic("invalid coordinate type")
	}
}

func jsonConvert(rv reflect.Value) any {
	switch rv.Kind() {
	case reflect.Slice:
		size := rv.Len()
		s := make([]any, size)
		for i := range size {
			s[i] = jsonConvert(rv.Index(i))
		}
		return s
	case reflect.Interface:
		return jsonConvert(rv.Elem())
	default:
		return jsonCoord(rv.Interface())
	}
}

func jsonConvertGeometries(rv reflect.Value) any {
	size := rv.Len()
	s := make([]any, size)
	for i := range size {
		iv := rv.Index(i)
		g := iv.Interface().(Geometry)
		if geoType(g) == geoGeometryCollection {
			s[i] = jsonTypeGeometries{Type: geoTypeName(g), Geometries: jsonConvertGeometries(reflect.ValueOf(g))}
		} else {
			s[i] = jsonType{Type: geoTypeName(g), Coordinates: jsonConvert(iv)}
		}
	}
	return s
}

type jsonType struct {
	Type        string `json:"type"`
	Coordinates any    `json:"coordinates"`
}

type jsonTypeGeometries struct {
	Type       string `json:"type"`
	Geometries any    `json:"geometries"`
}

// EncodeGeoJSON encodes a geometry to the geoJSON format.
func EncodeGeoJSON(g Geometry) ([]byte, error) {
	switch geoType(g) {
	case geoGeometryCollection:
		return json.Marshal(jsonTypeGeometries{Type: geoTypeName(g), Geometries: jsonConvertGeometries(reflect.ValueOf(g))})
	default:
		return json.Marshal(jsonType{Type: geoTypeName(g), Coordinates: jsonConvert(reflect.ValueOf(g))})
	}
}
