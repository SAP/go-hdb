// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

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

func jsonCoord(v reflect.Value) []*float64 {
	switch {
	case v.Type().ConvertibleTo(coordType):
		return v.Convert(coordType).Interface().(Coord).coordToSlice()
	case v.Type().ConvertibleTo(coordZType):
		return v.Convert(coordZType).Interface().(CoordZ).coordToSlice()
	case v.Type().ConvertibleTo(coordMType):
		return v.Convert(coordMType).Interface().(CoordM).coordToSlice()
	case v.Type().ConvertibleTo(coordZMType):
		return v.Convert(coordZMType).Interface().(CoordZM).coordToSlice()
	default:
		panic("invalid coordinate type")
	}
}

func jsonConvert(rv reflect.Value) any {
	switch rv.Kind() {
	case reflect.Slice:
		size := rv.Len()
		s := make([]any, size)
		for i := 0; i < size; i++ {
			s[i] = jsonConvert(rv.Index(i))
		}
		return s
	case reflect.Interface:
		return jsonConvert(rv.Elem())
	default:
		return jsonCoord(rv)
	}
}

func jsonConvertGeometries(rv reflect.Value) any {
	size := rv.Len()
	s := make([]any, size)
	for i := 0; i < size; i++ {
		iv := rv.Index(i)
		s[i] = jsonType{Type: geoTypeName(iv.Interface().(Geometry)), Coordinates: jsonConvert(iv)}
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
