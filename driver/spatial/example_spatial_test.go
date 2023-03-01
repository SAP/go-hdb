package spatial_test

import (
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver/spatial"
)

// ExampleGeometry demonstrates the conversion of a geospatial object to
// - 'well known binary' format
// - 'extended well known binary' format
// - 'well known text' format
// - 'extended known text' format
// - 'geoJSON' format
func ExampleGeometry() {
	// create geospatial object
	g := spatial.GeometryCollection{spatial.Point{X: 1, Y: 1}, spatial.LineString{{X: 1, Y: 1}, {X: 2, Y: 2}}}

	// 'well known binary' format
	wkb, err := spatial.EncodeWKB(g, false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", wkb)

	// 'extended well known binary' format
	ewkb, err := spatial.EncodeEWKB(g, false, 4711)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", ewkb)

	// - 'well known text' format
	wkt, err := spatial.EncodeWKT(g)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", wkt)

	// - 'extended known text' format
	ewkt, err := spatial.EncodeEWKT(g, 4711)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", ewkt)

	// - 'geoJSON' format
	geoJSON, err := spatial.EncodeGeoJSON(g)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", geoJSON)

	// output: 0107000000020000000101000000000000000000f03f000000000000f03f010200000002000000000000000000f03f000000000000f03f00000000000000400000000000000040
	// 010700002067120000020000000101000000000000000000f03f000000000000f03f010200000002000000000000000000f03f000000000000f03f00000000000000400000000000000040
	// GEOMETRYCOLLECTION (POINT (1 1),LINESTRING (1 1,2 2))
	// SRID=4711;GEOMETRYCOLLECTION (POINT (1 1),LINESTRING (1 1,2 2))
	// {"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,1]},{"type":"LineString","coordinates":[[1,1],[2,2]]}]}
}
