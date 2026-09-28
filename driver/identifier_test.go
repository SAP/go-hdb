package driver

import (
	"testing"
)

func TestIdentifierStringer(t *testing.T) {
	t.Parallel()

	type testIdentifier struct {
		id Identifier
		s  string
	}

	testIdentifierData := []*testIdentifier{
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
		{`a"b`, `"a""b"`},
		{`a"b"c`, `"a""b""c"`},
		{`a\b`, `"a\b"`},
		{`a"b\c`, `"a""b\c"`},
	}

	for i, d := range testIdentifierData {
		if d.id.String() != d.s {
			t.Fatalf("%d id %s - expected %s", i, d.id, d.s)
		}
	}
}
