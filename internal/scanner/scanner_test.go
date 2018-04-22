// +build go1.10

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

package scanner

import (
	"fmt"
	"reflect"
	"testing"
)

var testSQL = []string{
	``,      //empty
	`     `, // only whitespaces
	`delete from Invoice where TimeCreated < :end and TimeCreated >= :start;`,
	`delete from _Invoice where _TimeCreated < :end and _TimeCreated >= :start;`,
	`delete from "Invoice" where "TimeCreated" < :end and "TimeCreated" >= :start;`,
	`delete from schema."Invoice";`,
	`delete from "schema"."Invoice";`,
	`delete from "Inv"""oice" where "Time""Created" < :end and "Time""Created" >= :start;`,
}

type tokenValue struct {
	tok rune
	val string
}

func (tv *tokenValue) String() string {
	return fmt.Sprintf("%s: %s\n", TokenString(tv.tok), tv.val)
}

var testResults = [][]*tokenValue{
	{},
	{},
	{
		{Identifier, "delete"},
		{Identifier, "from"},
		{Identifier, "Invoice"},
		{Identifier, "where"},
		{Identifier, "TimeCreated"},
		{Operator, "<"},
		{NamedVariable, "end"},
		{Identifier, "and"},
		{Identifier, "TimeCreated"},
		{Operator, ">="},
		{NamedVariable, "start"},
		{Delimiter, ";"},
	},
	{
		{Identifier, "delete"},
		{Identifier, "from"},
		{Identifier, "_Invoice"},
		{Identifier, "where"},
		{Identifier, "_TimeCreated"},
		{Operator, "<"},
		{NamedVariable, "end"},
		{Identifier, "and"},
		{Identifier, "_TimeCreated"},
		{Operator, ">="},
		{NamedVariable, "start"},
		{Delimiter, ";"},
	},
	{
		{Identifier, "delete"},
		{Identifier, "from"},
		{QuotedIdentifier, "Invoice"},
		{Identifier, "where"},
		{QuotedIdentifier, "TimeCreated"},
		{Operator, "<"},
		{NamedVariable, "end"},
		{Identifier, "and"},
		{QuotedIdentifier, "TimeCreated"},
		{Operator, ">="},
		{NamedVariable, "start"},
		{Delimiter, ";"},
	},
	{
		{Identifier, "delete"},
		{Identifier, "from"},
		{Identifier, "schema"},
		{IdentifierDelimiter, "."},
		{QuotedIdentifier, "Invoice"},
		{Delimiter, ";"},
	},
	{
		{Identifier, "delete"},
		{Identifier, "from"},
		{QuotedIdentifier, "schema"},
		{IdentifierDelimiter, "."},
		{QuotedIdentifier, "Invoice"},
		{Delimiter, ";"},
	},
	{
		{Identifier, "delete"},
		{Identifier, "from"},
		{QuotedIdentifier, `Inv"`},
		{Identifier, "oice"},
		{QuotedIdentifier, ` where `},
		{Identifier, "Time"},
		{QuotedIdentifier, ``},
		{Identifier, "Created"},
		{QuotedIdentifier, ` < :end and `},
		{Identifier, "Time"},
		{QuotedIdentifier, ``},
		{Identifier, "Created"},
		{Error, ` >= :start;`},
	},
}

func scan(t *testing.T, sql string, tv *([]*tokenValue)) {
	s := NewScanner(sql)
	defer s.FreeScanner()

	for {
		tok := s.Next()
		if tok == EOF {
			break
		}
		*tv = append(*tv, &tokenValue{tok: tok, val: s.Value()})
	}
}

func TestSQL(t *testing.T) {
	tv := make([]*tokenValue, 0)

	for i, sql := range testSQL {
		tv = tv[:0]
		scan(t, sql, &tv)

		if !reflect.DeepEqual(testResults[i], tv) {
			t.Logf("%v", tv)
			t.Logf("%v", testResults[i])
			t.Errorf("test %d failed", i)
		}
	}
}
