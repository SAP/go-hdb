//go:build !unit

package driver

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/coltest"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/spatial"
)

func equalJSON(b1, b2 []byte) (bool, error) {
	var j1, j2 any

	if err := json.Unmarshal(b1, &j1); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b2, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j1, j2), nil
}

// testSpatial inserts each geometry via st_geomfromewkb and verifies all read-back encodings
// (wkb, ewkb, wkt, ewkt, geojson) against the driver's own encoders.
func testSpatial(t *testing.T, db *sql.DB, column coltest.Type, testData []spatial.Geometry) {
	tableName := RandomIdentifier(column.DataType() + "_")
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("create table %s (x %s, i integer)", tableName, column.DataType())); err != nil {
		t.Fatal(err)
	}
	srid := column.(coltest.Spatial).SRID()

	// insert within a transaction (SQL Error 596 - LOB streaming not permitted in auto-commit mode).
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(t.Context(), fmt.Sprintf("insert into %s values(st_geomfromewkb(?), ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i, g := range testData {
		ewkb, err := spatial.EncodeEWKB(g, false, srid)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stmt.ExecContext(t.Context(), new(Lob).SetReader(bytes.NewReader(ewkb)), i); err != nil {
			t.Fatalf("%d - %s", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// read back and compare all encodings.
	var (
		x               string
		recIdx          int
		asWKBBuffer     = new(bytes.Buffer)
		asEWKBBuffer    = new(bytes.Buffer)
		asWKTBuffer     = new(bytes.Buffer)
		asEWKTBuffer    = new(bytes.Buffer)
		asGeoJSONBuffer = new(bytes.Buffer)
	)
	rows, err := db.QueryContext(t.Context(), fmt.Sprintf("select x, i, x.st_aswkb(), x.st_asewkb(), x.st_aswkt(), x.st_asewkt(), x.st_asgeojson() from %s order by i", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		if err := rows.Scan(&x, &recIdx,
			&Lob{wr: asWKBBuffer}, &Lob{wr: asEWKBBuffer}, &Lob{wr: asWKTBuffer}, &Lob{wr: asEWKTBuffer}, &Lob{wr: asGeoJSONBuffer}); err != nil {
			t.Fatal(err)
		}

		wkb, err := spatial.EncodeWKB(testData[i], false)
		if err != nil {
			t.Fatal(err)
		}
		if string(wkb) != x {
			t.Fatalf("test %d: x value %v - expected %v", i, x, string(wkb))
		}

		ewkb, err := spatial.EncodeEWKB(testData[i], false, srid)
		if err != nil {
			t.Fatal(err)
		}
		wkt, err := spatial.EncodeWKT(testData[i])
		if err != nil {
			t.Fatal(err)
		}
		ewkt, err := spatial.EncodeEWKT(testData[i], srid)
		if err != nil {
			t.Fatal(err)
		}
		geoJSON, err := spatial.EncodeGeoJSON(testData[i])
		if err != nil {
			t.Fatal(err)
		}

		if asWKB := hex.EncodeToString(asWKBBuffer.Bytes()); string(wkb) != asWKB {
			t.Fatalf("test %d: wkb value %v - expected %v", i, asWKB, string(wkb))
		}
		if asEWKB := hex.EncodeToString(asEWKBBuffer.Bytes()); string(ewkb) != asEWKB {
			t.Fatalf("test %d: ewkb value %v - expected %v", i, asEWKB, string(ewkb))
		}
		if asWKT := asWKTBuffer.Bytes(); !bytes.Equal(wkt, asWKT) {
			t.Fatalf("test %d: wkt value %s - expected %s", i, asWKT, wkt)
		}
		if asEWKT := asEWKTBuffer.Bytes(); !bytes.Equal(ewkt, asEWKT) {
			t.Fatalf("test %d: ewkt value %s - expected %s", i, asEWKT, ewkt)
		}
		if ok, err := equalJSON(geoJSON, asGeoJSONBuffer.Bytes()); err != nil {
			t.Fatal(err)
		} else if !ok {
			t.Fatalf("test %d: geoJSON value %s - expected %s", i, asGeoJSONBuffer.Bytes(), geoJSON)
		}

		asWKBBuffer.Reset()
		asEWKBBuffer.Reset()
		asWKTBuffer.Reset()
		asEWKTBuffer.Reset()
		asGeoJSONBuffer.Reset()
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != len(testData) {
		t.Fatalf("rows %d - expected %d", i, len(testData))
	}
}

func TestDataTypeSpatial(t *testing.T) {
	t.Parallel()

	stPointTestData := []spatial.Geometry{
		spatial.Point{},
		spatial.Point{X: 2.5, Y: 3.0},
		spatial.Point{X: -3.0, Y: -4.5},
	}

	stGeometryTestData := []spatial.Geometry{
		spatial.Point{X: 2.5, Y: 3.0},
		spatial.Point{X: -3.0, Y: -4.5},
		spatial.PointZ{X: -3.0, Y: -4.5, Z: 5.0},
		spatial.PointM{X: -3.0, Y: -4.5, M: 6.0},
		spatial.PointM{X: -3.0, Y: -4.5, M: spatial.NaN()},
		spatial.PointZM{X: -3.0, Y: -4.5, Z: 5.0, M: 6.0},
		spatial.PointZM{X: -3.0, Y: -4.5, Z: 5.0, M: spatial.NaN()},

		spatial.LineString{},
		spatial.LineString{{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}, {X: 6.0, Y: 3.0}},
		spatial.LineString{{X: 4.0, Y: 4.0}, {X: 6.0, Y: 5.0}, {X: 7.0, Y: 4.0}},
		spatial.LineString{{X: 7.0, Y: 5.0}, {X: 9.0, Y: 7.0}},
		spatial.LineString{{X: 7.0, Y: 3.0}, {X: 8.0, Y: 5.0}},

		spatial.CircularString{},
		spatial.CircularString{{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}, {X: 6.0, Y: 3.0}},

		spatial.Polygon{},
		spatial.Polygon{{{X: 6.0, Y: 7.0}, {X: 10.0, Y: 3.0}, {X: 10.0, Y: 10.0}, {X: 6.0, Y: 7.0}}},
		// hdb permutes ring points?
		// same call with
		// spatial.Polygon{{{6.0, 7.0}, {10.0, 3.0}, {10.0, 10.0}, {6.0, 7.0}}, {{6.0, 7.0}, {10.0, 3.0}, {10.0, 10.0}, {6.0, 7.0}}}
		// would give errors as hdb changes 'middle' coordinates for included ring
		spatial.Polygon{{{X: 6.0, Y: 7.0}, {X: 10.0, Y: 3.0}, {X: 10.0, Y: 10.0}, {X: 6.0, Y: 7.0}}, {{X: 6.0, Y: 7.0}, {X: 10.0, Y: 10.0}, {X: 10.0, Y: 3.0}, {X: 6.0, Y: 7.0}}},

		spatial.MultiPoint{},
		spatial.MultiPoint{{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}},

		spatial.MultiLineString{},
		spatial.MultiLineString{{{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}, {X: 6.0, Y: 3.0}}, {{X: 3.0, Y: 3.0}, {X: 5.0, Y: 4.0}, {X: 6.0, Y: 3.0}}},

		spatial.MultiPolygon{},
		spatial.MultiPolygon{
			{{{X: 6.0, Y: 7.0}, {X: 10.0, Y: 3.0}, {X: 10.0, Y: 10.0}, {X: 6.0, Y: 7.0}}, {{X: 6.0, Y: 7.0}, {X: 10.0, Y: 10.0}, {X: 10.0, Y: 3.0}, {X: 6.0, Y: 7.0}}},
			{{{X: 6.0, Y: 7.0}, {X: 10.0, Y: 3.0}, {X: 10.0, Y: 10.0}, {X: 6.0, Y: 7.0}}, {{X: 6.0, Y: 7.0}, {X: 10.0, Y: 10.0}, {X: 10.0, Y: 3.0}, {X: 6.0, Y: 7.0}}},
		},

		spatial.GeometryCollection{},
		spatial.GeometryCollection{spatial.Point{X: 1, Y: 1}, spatial.LineString{{X: 1, Y: 1}, {X: 2, Y: 2}}},
		spatial.GeometryCollection{spatial.GeometryCollection{spatial.Point{X: 1, Y: 1}, spatial.Point{X: 2, Y: 2}}, spatial.Point{X: 3, Y: 3}},
		spatial.GeometryCollectionZ{spatial.PointZ{X: 1, Y: 1, Z: 3}, spatial.LineStringZ{{X: 1, Y: 1, Z: 3}, {X: 2, Y: 2, Z: 3}}},
	}

	type test struct {
		column   coltest.Type
		testData []spatial.Geometry
	}
	tests := []test{
		{coltest.NewNullSTPoint(0), stPointTestData},
		{coltest.NewNullSTPoint(3857), stPointTestData},
		{coltest.NewNullSTGeometry(0), stGeometryTestData},
		{coltest.NewNullSTGeometry(3857), stGeometryTestData},
	}

	version := MT.Version().Major()

	for _, dfv := range p.SupportedDfvs(testing.Short()) {
		t.Run(fmt.Sprintf("dfv %d", dfv), func(t *testing.T) {
			t.Parallel()

			connector := MT.NewConnector()
			connector.SetDfv(dfv)
			db := sql.OpenDB(connector)
			db.SetMaxIdleConns(10)
			t.Cleanup(func() { db.Close() })

			for i, test := range tests {
				if test.column.IsSupported(version, dfv) {
					t.Run(fmt.Sprintf("%s %d", test.column.DataType(), i), func(t *testing.T) {
						t.Parallel()
						testSpatial(t, db, test.column, test.testData)
					})
				}
			}
		})
	}
}
