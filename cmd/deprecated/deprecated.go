// Command deprecated generates docs/DEPRECATED.md, a holistic overview of the
// driver's deprecated surface across all compat bundles.
//
// It scans the driver source for files guarded by a go_hdb_compat_go1_NN build
// tag (see docs/DEPRECATIONS.md for the lifecycle), collects the exported
// declarations in those files together with their "Deprecated:" doc notes, and
// writes one Markdown document grouping every bundle by its flip release.
//
// Run via go generate (see the directive in generate.go) or directly:
//
//	go run ./cmd/deprecated -root driver -out docs/DEPRECATED.md
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/printer"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func main() {
	log.SetFlags(0)

	root := flag.String("root", "driver", "root directory to scan for compat-tagged files")
	out := flag.String("out", "docs/DEPRECATED.md", "output Markdown file")
	flag.Parse()

	bundles, err := scan(*root)
	if err != nil {
		log.Fatalf("deprecated: %s", err)
	}

	doc := render(bundles)

	if err := os.WriteFile(*out, []byte(doc), 0o600); err != nil {
		log.Fatalf("deprecated: write %s: %s", *out, err)
	}
}

// compatTag matches a go_hdb_compat_go1_NN build tag and captures the minor
// release NN (the flip release).
var compatTag = regexp.MustCompile(`^go_hdb_compat_go1_(\d+)$`)

// symbol is one exported, deprecated declaration.
type symbol struct {
	name string // exported identifier (methods as Type.Method)
	sig  string // one-line signature
	note string // full doc comment (description + Deprecated: note)
	pos  string // file:line of the declaration
}

// bundle collects the deprecated symbols sharing one flip release.
type bundle struct {
	minor   int // NN in go1.NN
	symbols []symbol
}

// scan walks root and returns the compat bundles found, keyed by flip release.
// Every exported, Deprecated-marked declaration must live in a compat-tagged
// file; any that does not is an unguarded deprecation and scan returns an error
// listing every offender.
func scan(root string) ([]bundle, error) {
	byMinor := map[int][]symbol{}
	var unguarded []symbol

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}

		syms := fileSymbols(fset, file)
		if len(syms) == 0 {
			return nil
		}
		if minor, ok := fileFlipRelease(file); ok {
			byMinor[minor] = append(byMinor[minor], syms...)
		} else {
			unguarded = append(unguarded, syms...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(unguarded) > 0 {
		sort.Slice(unguarded, func(i, j int) bool { return unguarded[i].pos < unguarded[j].pos })
		var b strings.Builder
		b.WriteString("unguarded deprecations (missing a go_hdb_compat_go1_NN build tag):")
		for _, s := range unguarded {
			fmt.Fprintf(&b, "\n\t%s: %s", s.pos, s.name)
		}
		return nil, fmt.Errorf("%s", b.String())
	}

	bundles := make([]bundle, 0, len(byMinor))
	for minor, syms := range byMinor {
		sort.Slice(syms, func(i, j int) bool { return syms[i].name < syms[j].name })
		bundles = append(bundles, bundle{minor: minor, symbols: syms})
	}
	sort.Slice(bundles, func(i, j int) bool { return bundles[i].minor < bundles[j].minor })
	return bundles, nil
}

// fileFlipRelease reports the flip release NN if the file carries a
// go_hdb_compat_go1_NN build constraint.
func fileFlipRelease(file *ast.File) (int, bool) {
	for _, group := range file.Comments {
		for _, c := range group.List {
			if !constraint.IsGoBuild(c.Text) {
				continue
			}
			expr, err := constraint.Parse(c.Text)
			if err != nil {
				return 0, false
			}
			var minor int
			var found bool
			expr.Eval(func(tag string) bool {
				if m := compatTag.FindStringSubmatch(tag); m != nil {
					if n, err := strconv.Atoi(m[1]); err == nil {
						minor = n
						found = true
					}
				}
				return false
			})
			if found {
				return minor, true
			}
		}
	}
	return 0, false
}

// fileSymbols returns the exported, deprecated declarations of file: functions,
// methods, types, constants and variables whose doc comment carries a
// "Deprecated:" marker.
func fileSymbols(fset *token.FileSet, file *ast.File) []symbol {
	pos := func(n ast.Node) string { return fset.Position(n.Pos()).String() }

	var syms []symbol
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if !d.Name.IsExported() {
				continue
			}
			note, ok := deprecatedNote(d.Doc)
			if !ok {
				continue
			}
			syms = append(syms, symbol{name: funcName(d), sig: funcSig(fset, d), note: note, pos: pos(d)})
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if !s.Name.IsExported() {
						continue
					}
					// doc may sit on the spec or on a single-spec GenDecl.
					note, ok := deprecatedNote(specDoc(s.Doc, d.Doc))
					if !ok {
						continue
					}
					syms = append(syms, symbol{name: s.Name.Name, sig: "type " + s.Name.Name, note: note, pos: pos(s)})
				case *ast.ValueSpec:
					note, ok := deprecatedNote(specDoc(s.Doc, d.Doc))
					if !ok {
						continue
					}
					kind := d.Tok.String() // "const" or "var"
					for _, name := range s.Names {
						if !name.IsExported() {
							continue
						}
						syms = append(syms, symbol{name: name.Name, sig: kind + " " + name.Name, note: note, pos: pos(name)})
					}
				}
			}
		}
	}
	return syms
}

// specDoc returns the spec's own doc comment, falling back to the enclosing
// single-spec GenDecl's doc.
func specDoc(specDoc, declDoc *ast.CommentGroup) *ast.CommentGroup {
	if specDoc != nil {
		return specDoc
	}
	return declDoc
}

// deprecatedNote returns the full doc comment of a declaration carrying a
// "Deprecated:" marker. The whole comment (description and deprecation note)
// is kept verbatim, as it reads via go doc / pkg.go.dev.
func deprecatedNote(doc *ast.CommentGroup) (string, bool) {
	if doc == nil {
		return "", false
	}
	text := doc.Text()
	if !strings.Contains(text, "Deprecated:") {
		return "", false
	}
	return strings.TrimRight(text, "\n"), true
}

// funcName returns the identifier, as Type.Method for methods.
func funcName(d *ast.FuncDecl) string {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return d.Name.Name
	}
	return recvType(d.Recv.List[0].Type) + "." + d.Name.Name
}

// funcSig renders a one-line signature without the body by printing a copy of
// the declaration with the body removed.
func funcSig(fset *token.FileSet, d *ast.FuncDecl) string {
	sig := *d
	sig.Body = nil
	sig.Doc = nil
	var b strings.Builder
	if err := printer.Fprint(&b, fset, &sig); err != nil {
		return d.Name.Name
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

// recvType renders a receiver type name, stripping a leading pointer star.
func recvType(e ast.Expr) string {
	if star, ok := e.(*ast.StarExpr); ok {
		e = star.X
	}
	if id, ok := e.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// render produces the holistic Markdown document.
func render(bundles []bundle) string {
	var b strings.Builder
	b.WriteString("<!-- Code generated by cmd/deprecated; DO NOT EDIT. -->\n\n")
	b.WriteString("# Deprecated surface\n\n")
	b.WriteString("This document lists the driver's deprecated API, grouped by the compat\n")
	b.WriteString("build-tag bundle it belongs to. It is generated from the source; see\n")
	b.WriteString("[the deprecation and removal policy](DEPRECATIONS.md) for how the lifecycle\n")
	b.WriteString("and the `go_hdb_compat_go1_NN` build tags work.\n\n")

	if len(bundles) == 0 {
		b.WriteString("_No deprecated surface is currently on the compat lifecycle._\n")
		return b.String()
	}

	for _, bundle := range bundles {
		f := bundle.minor
		tag := fmt.Sprintf("go_hdb_compat_go1_%d", f)
		fmt.Fprintf(&b, "## Bundle `%s`\n\n", tag)
		fmt.Fprintf(&b, "- Compiled by default through Go 1.%d; set `-tags %s` to exclude it (opt-out self-check).\n", f-1, tag)
		fmt.Fprintf(&b, "- Opt-in on Go 1.%d: excluded by default, set `-tags %s` to include it.\n", f, tag)
		fmt.Fprintf(&b, "- Removed in Go 1.%d.\n\n", f+1)
		for _, s := range bundle.symbols {
			fmt.Fprintf(&b, "### %s\n\n", s.name)
			fmt.Fprintf(&b, "```go\n%s\n```\n\n", s.sig)
			fmt.Fprintf(&b, "%s\n\n", s.note)
		}
	}
	return b.String()
}
