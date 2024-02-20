package driver_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func TestDocstore(t *testing.T) {
	t.Parallel()

	testData := struct {
		Attr1 string `json:"attr1"`
		Attr2 bool   `json:"attr2"`
	}{
		Attr1: "test text",
		Attr2: true,
	}

	v, err := json.Marshal(testData)
	if err != nil {
		t.Fatal(err)
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

	if _, err := db.Exec(fmt.Sprintf("insert into %s values(?)", collectionName), v); err != nil {
		t.Fatal(err)
	}
}
