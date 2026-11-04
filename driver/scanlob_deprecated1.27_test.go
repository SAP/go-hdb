//go:build !unit && go1.27 && !go1.28

package driver_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

type deprecatedStringLob string

func (s *deprecatedStringLob) Scan(src any) error {
	return driver.ScanLobString(src, (*string)(s))
}

type deprecatedBytesLob []byte

func (b *deprecatedBytesLob) Scan(src any) error {
	return driver.ScanLobBytes(src, (*[]byte)(b))
}

// Keep the writer in a field so the destination only implements sql.Scanner.
// This exercises ScanLobWriter rather than the driver's direct io.Writer path.
type deprecatedWriterLob struct{ buf bytes.Buffer }

func (w *deprecatedWriterLob) Scan(src any) error {
	return driver.ScanLobWriter(src, &w.buf)
}

func TestDeprecatedLobScanners(t *testing.T) {
	db := driver.MT.DB()
	for _, query := range []struct {
		name string
		sql  string
	}{
		{"blob", "select to_blob('replacement') from dummy"},
		{"nclob", "select to_nclob('replacement') from dummy"},
		{"nvarchar", "select cast('replacement' as nvarchar(20)) from dummy"},
		{"varbinary", "select hextobin('7265706c6163656d656e74') from dummy"},
	} {
		t.Run(query.name, func(t *testing.T) {
			s := deprecatedStringLob("stale")
			b := deprecatedBytesLob("stale")
			ns := sql.Null[deprecatedStringLob]{V: "stale"}
			nb := sql.Null[deprecatedBytesLob]{V: deprecatedBytesLob("stale")}
			var w deprecatedWriterLob
			for _, test := range []struct {
				name  string
				dest  any
				value func() (string, bool)
			}{
				{"string", &s, func() (string, bool) { return string(s), true }},
				{"bytes", &b, func() (string, bool) { return string(b), true }},
				{"nullable string", &ns, func() (string, bool) { return string(ns.V), ns.Valid }},
				{"nullable bytes", &nb, func() (string, bool) { return string(nb.V), nb.Valid }},
				{"writer", &w, func() (string, bool) { return w.buf.String(), true }},
			} {
				t.Run(test.name, func(t *testing.T) {
					if err := db.QueryRowContext(t.Context(), query.sql).Scan(test.dest); err != nil {
						t.Fatal(err)
					}
					got, valid := test.value()
					if got != "replacement" || !valid {
						t.Fatalf("got value %q, valid %t; want replacement, true", got, valid)
					}
				})
			}
		})
	}
}
