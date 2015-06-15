/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"testing"
)

func TestCheckBulkInsert(t *testing.T) {

	var data = []struct {
		bulkSql    string
		sql        string
		bulkInsert bool
	}{
		{"bulk insert", "insert", true},
		{"   bulk   insert  ", "insert  ", true},
		{"BuLk iNsErT", "iNsErT", true},
		{"   bUlK   InSeRt  ", "InSeRt  ", true},
		{"  bulkinsert  ", "  bulkinsert  ", false},
		{"bulk", "bulk", false},
		{"insert", "insert", false},
	}

	for i, d := range data {
		sql, bulkInsert := checkBulkInsert(d.bulkSql)
		if sql != d.sql {
			t.Fatalf("test %d failed: bulk insert flag %t - %t expected", i, bulkInsert, d.bulkInsert)
		}
		if sql != d.sql {
			t.Fatalf("test %d failed: sql %s - %s expected", i, sql, d.sql)
		}
	}
}
