package main

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixture = `//go:build (!go1.28 && !go_hdb_compat_go1_28) || (go1.28 && !go1.29 && go_hdb_compat_go1_28)

package sample

// Keep does nothing.
//
// Deprecated: use Replacement instead.
func Keep(a int, b string) error { return nil }

// unexported is skipped.
//
// Deprecated: internal.
func unexported() {}

// Plain has no deprecation note and is skipped.
func Plain() {}

// Vars is a deprecated type.
//
// Deprecated: use a map instead.
type Vars map[string]string

// Recv is a deprecated method.
//
// Deprecated: use Other.
func (t *T) Recv() string { return "" }

type T struct{}

// OldConst is a deprecated constant.
//
// Deprecated: use NewConst.
const OldConst = 1

// OldVar is a deprecated variable.
//
// Deprecated: use NewVar.
var OldVar = 2
`

func parseFixture(t *testing.T, src string) ([]symbol, int) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "sample.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse fixture: %s", err)
	}
	minor, ok := fileFlipRelease(file)
	if !ok {
		t.Fatalf("fileFlipRelease: no compat tag found")
	}
	return fileSymbols(fset, file), minor
}

// writeGo writes src to name under dir, failing the test on error.
func writeGo(t *testing.T, dir, name, src string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o600); err != nil {
		t.Fatalf("write %s: %s", name, err)
	}
}

func testFlipRelease(t *testing.T) {
	if _, minor := parseFixture(t, fixture); minor != 28 {
		t.Errorf("flip release = %d, want 28", minor)
	}
}

func testFlipReleaseNoCompatTag(t *testing.T) {
	src := "//go:build go1.27\n\npackage sample\n"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "sample.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse: %s", err)
	}
	if _, ok := fileFlipRelease(file); ok {
		t.Error("fileFlipRelease matched a non-compat build tag")
	}
}

func testFileSymbols(t *testing.T) {
	syms, _ := parseFixture(t, fixture)

	got := map[string]symbol{}
	for _, s := range syms {
		got[s.name] = s
	}

	// Exported + deprecated are kept; unexported and note-less are dropped.
	for _, name := range []string{"unexported", "Plain"} {
		if _, ok := got[name]; ok {
			t.Errorf("symbol %q should have been skipped", name)
		}
	}
	for _, name := range []string{"Keep", "Vars", "T.Recv", "OldConst", "OldVar"} {
		if _, ok := got[name]; !ok {
			t.Errorf("symbol %q missing", name)
		}
	}

	for name, want := range map[string]string{
		"Keep":     "func Keep(a int, b string) error",
		"T.Recv":   "func (t *T) Recv() string",
		"Vars":     "type Vars",
		"OldConst": "const OldConst",
		"OldVar":   "var OldVar",
	} {
		if got[name].sig != want {
			t.Errorf("%s sig = %q, want %q", name, got[name].sig, want)
		}
	}

	if want := "Keep does nothing.\n\nDeprecated: use Replacement instead."; got["Keep"].note != want {
		t.Errorf("Keep note = %q, want %q", got["Keep"].note, want)
	}
}

func testRender(t *testing.T) {
	syms, minor := parseFixture(t, fixture)
	doc := render([]bundle{{minor: minor, symbols: syms}})

	for _, want := range []string{
		"DO NOT EDIT",
		"## Bundle `go_hdb_compat_go1_28`",
		"Compiled by default through Go 1.27",
		"Opt-in on Go 1.28",
		"Removed in Go 1.29",
		"### Keep",
		"use Replacement instead.",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("rendered doc missing %q", want)
		}
	}
}

func testRenderEmpty(t *testing.T) {
	if doc := render(nil); !strings.Contains(doc, "No deprecated surface") {
		t.Errorf("empty render missing placeholder; got:\n%s", doc)
	}
}

func testScanGuarded(t *testing.T) {
	dir := t.TempDir()
	writeGo(t, dir, "guarded.go", fixture)

	bundles, err := scan(dir)
	if err != nil {
		t.Fatalf("scan: %s", err)
	}
	if len(bundles) != 1 {
		t.Fatalf("bundles = %d, want 1", len(bundles))
	}
	if bundles[0].minor != 28 {
		t.Errorf("bundle minor = %d, want 28", bundles[0].minor)
	}
	// Keep, Vars, T.Recv, OldConst, OldVar — unexported and note-less dropped.
	if len(bundles[0].symbols) != 5 {
		t.Errorf("symbols = %d, want 5", len(bundles[0].symbols))
	}
}

func testScanUnguardedErrors(t *testing.T) {
	dir := t.TempDir()
	// Deprecated symbol in a file WITHOUT a compat tag.
	writeGo(t, dir, "loose.go", "package sample\n\n// Loose is unguarded.\n//\n// Deprecated: no tag.\nfunc Loose() {}\n")

	_, err := scan(dir)
	if err == nil {
		t.Fatal("scan: expected error for unguarded deprecation, got nil")
	}
	for _, want := range []string{"unguarded", "Loose", "loose.go"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err.Error(), want)
		}
	}
}

func TestDeprecated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"flipRelease", testFlipRelease},
		{"flipReleaseNoCompatTag", testFlipReleaseNoCompatTag},
		{"fileSymbols", testFileSymbols},
		{"render", testRender},
		{"renderEmpty", testRenderEmpty},
		{"scanGuarded", testScanGuarded},
		{"scanUnguardedErrors", testScanUnguardedErrors},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fn(t)
		})
	}
}
