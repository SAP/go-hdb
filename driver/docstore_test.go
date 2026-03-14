//go:build !unit

package driver_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

// https://help.sap.com/docs/hana-cloud-database/sap-hana-cloud-sap-hana-database-json-document-store-guide/json-document-store-statements

type testDocstoreFlat struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
	Flag bool   `json:"flag"`
}

type testDocstoreAddr struct {
	Name   string `json:"name"`
	Street string `json:"street"`
	City   string `json:"city"`
	State  string `json:"state"`
}

type testDocstoreNested struct {
	ID   int               `json:"id"`
	Addr *testDocstoreAddr `json:"addr"`
	Flag bool              `json:"flag"`
}

func testDocstoreCompare[T any](t *testing.T, db *sql.DB, collectionName string, id int, resultDoc *T) {
	lob := driver.Lob{}
	b := new(bytes.Buffer)
	lob.SetWriter(b)

	if err := db.QueryRow(fmt.Sprintf("select * from %s where \"id\" = %d", collectionName, id)).Scan(&lob); err != nil {
		t.Fatal(err)
	}

	var doc T
	if err := json.Unmarshal(b.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(doc, *resultDoc) {
		t.Fatalf("got %v - expected %v", doc, resultDoc)
	}
}

func testDocstoreCreateCollection(t *testing.T, db *sql.DB) string {
	name := driver.RandomIdentifier("docstore_").String()

	if _, err := db.Exec("create collection " + name); err != nil {
		t.Fatal(err)
	}
	return name
}

func testDocstoreDestroyCollection(t *testing.T, db *sql.DB, name string) {
	if _, err := db.Exec(fmt.Sprintf("drop collection %s cascade", name)); err != nil {
		t.Fatal(err)
	}
}

func testDocstoreMarshal(t *testing.T, docs []any) []any {
	marshalDocs := make([]any, 0, len(docs))
	for _, doc := range docs {
		marshalDoc, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		marshalDocs = append(marshalDocs, marshalDoc)
	}
	return marshalDocs
}

func testDocstoreDefault(t *testing.T, db *sql.DB) {

	collectionName := testDocstoreCreateCollection(t, db)
	defer testDocstoreDestroyCollection(t, db, collectionName)

	testDocs := []any{
		&testDocstoreFlat{ID: 1, Addr: "address 1", Flag: true},
	}

	resultDoc1 := &testDocstoreFlat{ID: 1, Addr: "address 1 - update", Flag: true}

	marshalDocs := testDocstoreMarshal(t, testDocs)

	if _, err := db.Exec(fmt.Sprintf("insert into %s values(?)", collectionName), marshalDocs...); err != nil {
		t.Fatal(err)
	}

	// update attribute 'inline'
	if _, err := db.Exec(fmt.Sprintf("update %s set \"addr\" = '%s' where \"id\" = %d", collectionName, "address 1 - update", 1)); err != nil {
		t.Fatal(err)
	}

	// compare
	testDocstoreCompare(t, db, collectionName, 1, resultDoc1)
}

func testDocstoreHDBCloud(t *testing.T, db *sql.DB) {
	// parse_json is only abailable in hdb cloud versions

	collectionName := testDocstoreCreateCollection(t, db)
	defer testDocstoreDestroyCollection(t, db, collectionName)

	testDocs := []any{
		&testDocstoreFlat{ID: 2, Addr: "address 2", Flag: false},
		&testDocstoreFlat{ID: 3, Addr: "address 3", Flag: true},
	}

	resultDoc2 := &testDocstoreFlat{ID: 2, Addr: "address 2 - update", Flag: true}
	resultDoc3 := &testDocstoreNested{
		ID: 3,
		Addr: &testDocstoreAddr{
			Name:   "Donald Duck",
			Street: "1313 Webfoot Walk",
			City:   "Duckburg",
			State:  "Calisota",
		},
		Flag: true}

	marshalDocs := testDocstoreMarshal(t, testDocs)

	if _, err := db.Exec(fmt.Sprintf("insert into %s values(?)", collectionName), marshalDocs...); err != nil {
		t.Fatal(err)
	}

	// update entire document
	marshalDoc, err := json.Marshal(resultDoc2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("update %[1]s set %[1]s = parse_json(?) where \"id\" = %d", collectionName, 2), marshalDoc); err != nil {
		t.Fatal(err)
	}

	// update with nested json
	marshalDoc, err = json.Marshal(resultDoc3.Addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("update %s set \"addr\" = parse_json(?) where \"id\" = %d", collectionName, 3), marshalDoc); err != nil {
		t.Fatal(err)
	}

	// compare
	testDocstoreCompare(t, db, collectionName, 2, resultDoc2)
	testDocstoreCompare(t, db, collectionName, 3, resultDoc3)
}

func TestDocstore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fct          func(t *testing.T, db *sql.DB)
		onlyHDBCloud bool
	}{
		{"default", testDocstoreDefault, false},
		{"hdbCloud", testDocstoreHDBCloud, true},
	}

	db := driver.MT.DB()

	isHDBCloud := driver.MT.Version().Major() > 3

	for _, test := range tests {
		if !isHDBCloud && test.onlyHDBCloud {
			continue
		}

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fct(t, db)
		})
	}
}
