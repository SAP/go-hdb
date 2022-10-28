package scanner

import (
	"reflect"
	"testing"
)

type tokenValue struct {
	token Token
	value string
}

var testData = []struct {
	s string
	r []tokenValue
}{
	{``, []tokenValue{}},      // empty
	{`     `, []tokenValue{}}, // only whitespaces
	{
		`delete from Invoice where TimeCreated < :end and TimeCreated >= :start;`,
		[]tokenValue{
			{Identifier, "delete"},
			{Identifier, "from"},
			{Identifier, "Invoice"},
			{Identifier, "where"},
			{Identifier, "TimeCreated"},
			{Operator, "<"},
			{NamedVariable, ":end"},
			{Identifier, "and"},
			{Identifier, "TimeCreated"},
			{Operator, ">="},
			{NamedVariable, ":start"},
			{Delimiter, ";"},
		},
	},
	{
		`delete from _Invoice where _TimeCreated < :end and _TimeCreated >= :start;`,
		[]tokenValue{
			{Identifier, "delete"},
			{Identifier, "from"},
			{Identifier, "_Invoice"},
			{Identifier, "where"},
			{Identifier, "_TimeCreated"},
			{Operator, "<"},
			{NamedVariable, ":end"},
			{Identifier, "and"},
			{Identifier, "_TimeCreated"},
			{Operator, ">="},
			{NamedVariable, ":start"},
			{Delimiter, ";"},
		},
	},
	{
		`delete from "Invoice" where "TimeCreated" < :end and "TimeCreated" >= :start;`,
		[]tokenValue{
			{Identifier, "delete"},
			{Identifier, "from"},
			{QuotedIdentifier, `"Invoice"`},
			{Identifier, "where"},
			{QuotedIdentifier, `"TimeCreated"`},
			{Operator, "<"},
			{NamedVariable, ":end"},
			{Identifier, "and"},
			{QuotedIdentifier, `"TimeCreated"`},
			{Operator, ">="},
			{NamedVariable, ":start"},
			{Delimiter, ";"},
		},
	},
	{
		`delete from schema."Invoice";`,
		[]tokenValue{
			{Identifier, "delete"},
			{Identifier, "from"},
			{Identifier, "schema"},
			{IdentifierDelimiter, "."},
			{QuotedIdentifier, `"Invoice"`},
			{Delimiter, ";"},
		},
	},
	{
		`delete from "schema"."Invoice";`,
		[]tokenValue{
			{Identifier, "delete"},
			{Identifier, "from"},
			{QuotedIdentifier, `"schema"`},
			{IdentifierDelimiter, "."},
			{QuotedIdentifier, `"Invoice"`},
			{Delimiter, ";"},
		},
	},
	{
		`delete from "Inv"""oice" where "Time""Created" < :end and "Time""Created" >= :start;`,
		[]tokenValue{
			{Identifier, "delete"},
			{Identifier, "from"},
			{QuotedIdentifier, `"Inv"""`},
			{Identifier, "oice"},
			{QuotedIdentifier, `" where "`},
			{Identifier, "Time"},
			{QuotedIdentifier, `""`},
			{Identifier, "Created"},
			{QuotedIdentifier, `" < :end and "`},
			{Identifier, "Time"},
			{QuotedIdentifier, `""`},
			{Identifier, "Created"},
			{Error, `" >= :start;`},
		},
	},
	{
		// call table result query
		`rsid 1234567890`,
		[]tokenValue{
			{Identifier, "rsid"},
			{Number, "1234567890"},
		},
	},
}

func testScannerX(t *testing.T) {
	tvs := make([]tokenValue, 0)
	scanner := Scanner{}

	for i, d := range testData {
		tvs = tvs[:0]
		scanner.Reset(d.s)

		for {
			token, start, end := scanner.Next()
			if token == EOS {
				break
			}
			tvs = append(tvs, tokenValue{token: token, value: d.s[start:end]})
		}

		if !reflect.DeepEqual(tvs, d.r) {
			if len(tvs) != len(d.r) {
				t.Fatalf("different length test %d: %d %d %v %v", i, len(tvs), len(d.r), tvs, d.r)
			}
			for j, tv := range tvs {
				if tv.token != d.r[j].token {
					t.Fatalf("different token %d %d %s %s", i, j, tv.token, d.r[j].token)
				}
				if tv.value != d.r[j].value {
					t.Fatalf("different value %d %d %s %s", i, j, tv.value, d.r[j].value)
				}
			}
		}
	}
}

func TestScanner(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"scannerX", testScannerX},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
