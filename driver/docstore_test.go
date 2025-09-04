//go:build !unit

package driver_test

import (
	"bytes"
	"cmp"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func TestDocstore(t *testing.T) {
	t.Parallel()

	type testData struct {
		ID    int    `json:"id"`
		Attr1 string `json:"attr1"`
		Attr2 bool   `json:"attr2"`
	}

	testRecords := []testData{
		{ID: 1, Attr1: "test text1", Attr2: true},
		{ID: 2, Attr1: "test text2", Attr2: false},
		{ID: 3, Attr1: "test text3", Attr2: true},
	}

	collectionName := driver.RandomIdentifier("docstore_").String()

	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	if _, err := db.Exec("create collection " + collectionName); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := db.Exec(fmt.Sprintf("drop collection %s cascade", collectionName)); err != nil {
			t.Fatal(err)
		}
	}()

	// marshall test data
	fields := make([]any, len(testRecords))
	for i, record := range testRecords {
		var err error
		fields[i], err = json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
	}

	if _, err := db.Exec(fmt.Sprintf("insert into %s values(?)", collectionName), fields...); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s", collectionName))
	// TODO: follow up on error
	// rows, err := db.Query(fmt.Sprintf("select * from %s where \"id\" = %d", collectionName, 1))
	// rows, err := db.Query(fmt.Sprintf("select * from %s where \"id\" = ?", collectionName), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	cmpRecords := []testData{}

	lob := driver.Lob{}
	for rows.Next() {
		b := new(bytes.Buffer)
		lob.SetWriter(b)

		if err := rows.Scan(&lob); err != nil {
			t.Fatal(err)
		}

		var field testData
		if err := json.Unmarshal(b.Bytes(), &field); err != nil {
			t.Fatal(err)
		}
		cmpRecords = append(cmpRecords, field)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	// compare
	cmpFn := func(a, b testData) int {
		return cmp.Compare(a.ID, b.ID)
	}
	slices.SortFunc(testRecords, cmpFn)
	slices.SortFunc(cmpRecords, cmpFn)
	if !reflect.DeepEqual(testRecords, cmpRecords) {
		t.Fatalf("got %v - expected %v", cmpRecords, testRecords)
	}
}
