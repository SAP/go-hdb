package driver

import (
	"testing"
)

type testIdentifier struct {
	id Identifier
	s  string
}

var testIdentifierData = []*testIdentifier{
	{"_", "_"},
	{"_A", "_A"},
	{"A#$_", "A#$_"},
	{"1", `"1"`},
	{"a", `"a"`},
	{"$", `"$"`},
	{"日本語", `"日本語"`},
	{"testTransaction", `"testTransaction"`},
	{"a.b.c", `"a.b.c"`},
	{"AAA.BBB.CCC", `"AAA.BBB.CCC"`},
}

func TestIdentifierStringer(t *testing.T) {
	for i, d := range testIdentifierData {
		if d.id.String() != d.s {
			t.Fatalf("%d id %s - expected %s", i, d.id, d.s)
		}
	}
}
