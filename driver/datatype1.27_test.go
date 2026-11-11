//go:build !unit && go1.27

package driver_test

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/internal/coltest"
)

// myString and myBytes exercise the named string- and []byte-kind scan paths.
type myString string
type myBytes []byte

// byteScanner is a custom sql.Scanner collecting the lob bytes, exercising the scanner-precedence
// path (and its generic Null[T] wrapper).
type byteScanner struct{ b []byte }

func (s *byteScanner) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		s.b = nil
	case []byte:
		s.b = append([]byte(nil), v...)
	case string:
		s.b = []byte(v)
	}
	return nil
}

func strBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return []byte(s)
}

// nullOutcome is how a scan target behaves when the source lob value is SQL NULL.
type nullOutcome int

const (
	nullErrors   nullOutcome = iota // scan returns an error (e.g. string, io.Writer)
	nullZero                        // scan succeeds with the zero value, no valid flag (exact []byte)
	nullNotValid                    // scan succeeds and reports !valid (nullable wrappers, scanner)
)

// lobTarget builds a scanTarget for a native lob destination (go1.27+): dest() creates a fresh
// destination, norm reduces a scanned destination to (bytes, valid), onNull states the behavior
// for a SQL NULL source row. verify recovers the expected bytes from the source lob value, applies
// the null rules, and compares content (skipped for TEXT/BINTEXT, which HDB may rewrite).
func lobTarget(onNull nullOutcome, dest func() any, norm func(any) ([]byte, bool)) *scanTarget {
	return &scanTarget{
		dest: dest,
		verify: func(dtt *dttDef, _ int, in, d any, scanErr error) error {
			valid, value, isNull := nullField(in)
			if isNull && !valid { // SQL NULL source row
				switch onNull {
				case nullErrors:
					if scanErr == nil {
						return errors.New("scanning SQL NULL: expected error, got none")
					}
				case nullZero:
					if scanErr != nil {
						return fmt.Errorf("scanning SQL NULL: %w", scanErr)
					}
				case nullNotValid:
					if scanErr != nil {
						return fmt.Errorf("scanning SQL NULL: %w", scanErr)
					}
					if _, gotValid := norm(d); gotValid {
						return errors.New("reported valid for SQL NULL")
					}
				}
				return nil
			}

			if scanErr != nil {
				return scanErr
			}
			gotBytes, gotValid := norm(d)
			if !gotValid {
				return errors.New("reported !valid for a non-null value")
			}
			lob := in // a plain lob carries its value in `in`; a valid wrapper in `value`.
			if isNull {
				lob = value
			}
			want := lob.(driver.Lob).Reader().(*lobReader).data

			compareContent := dtt.columnType().TypeName() != coltest.DbtnText && dtt.columnType().TypeName() != coltest.DbtnBintext
			if compareContent && !bytes.Equal(gotBytes, want) {
				return fmt.Errorf("content mismatch: got %d bytes, want %d", len(gotBytes), len(want))
			}
			return nil
		},
	}
}

// native lob scan targets (go1.27+): one constructor each.
func targetString() *scanTarget {
	return lobTarget(nullErrors, func() any { return new(string) },
		func(d any) ([]byte, bool) { return strBytes(*d.(*string)), true })
}
func targetBytes() *scanTarget {
	return lobTarget(nullZero, func() any { return new([]byte) },
		func(d any) ([]byte, bool) { return *d.(*[]byte), true })
}
func targetWriter() *scanTarget {
	return lobTarget(nullErrors, func() any { return new(bytes.Buffer) },
		func(d any) ([]byte, bool) { return d.(*bytes.Buffer).Bytes(), true })
}
func targetNamedString() *scanTarget {
	return lobTarget(nullErrors, func() any { return new(myString) },
		func(d any) ([]byte, bool) { return strBytes(string(*d.(*myString))), true })
}
func targetNamedBytes() *scanTarget {
	return lobTarget(nullErrors, func() any { return new(myBytes) },
		func(d any) ([]byte, bool) { return *d.(*myBytes), true })
}
func targetScanner() *scanTarget {
	return lobTarget(nullNotValid, func() any { return new(byteScanner) },
		func(d any) ([]byte, bool) { b := d.(*byteScanner).b; return b, b != nil })
}
func targetNullString() *scanTarget {
	return lobTarget(nullNotValid, func() any { return new(sql.NullString) },
		func(d any) ([]byte, bool) { v := d.(*sql.NullString); return strBytes(v.String), v.Valid })
}
func targetNullBytes() *scanTarget {
	return lobTarget(nullNotValid, func() any { return new(driver.NullBytes) },
		func(d any) ([]byte, bool) { v := d.(*driver.NullBytes); return v.Bytes, v.Valid })
}
func targetNullGenString() *scanTarget {
	return lobTarget(nullNotValid, func() any { return new(sql.Null[string]) },
		func(d any) ([]byte, bool) { v := d.(*sql.Null[string]); return strBytes(v.V), v.Valid })
}
func targetNullGenBytes() *scanTarget {
	return lobTarget(nullNotValid, func() any { return new(sql.Null[[]byte]) },
		func(d any) ([]byte, bool) { v := d.(*sql.Null[[]byte]); return v.V, v.Valid })
}
func targetNullWriter() *scanTarget {
	return lobTarget(nullNotValid, func() any { return &sql.Null[io.Writer]{V: new(bytes.Buffer)} },
		func(d any) ([]byte, bool) {
			v := d.(*sql.Null[io.Writer])
			if !v.Valid {
				return nil, false
			}
			return v.V.(*bytes.Buffer).Bytes(), true
		})
}

// extraScanTargetTests adds the native lob scan-target lines that work only on go1.27+. Each line
// scans one lob kind into one target, on small data (fast). Character paths are covered via NCLOB,
// binary via BLOB; the pair is enough to exercise both the CESU-8 decode and the raw byte paths.
func extraScanTargetTests() []tester {
	// small lob data: null variants, no 1 MB row (streaming is covered by the same-type lob lines).
	nclobData := func() []any {
		return []any{
			driver.NullLob{Valid: false, Lob: driver.NewLob(newLobReader([]byte("one")), nil)},
			*driver.NewLob(newLobReader([]byte("two")), nil),
		}
	}
	blobData := nclobData

	return []tester{
		&dttDef{_columnType: coltest.NullNClob, testDataFn: nclobData, tx: true, scanTarget: targetString()},
		&dttDef{_columnType: coltest.NullNClob, testDataFn: nclobData, tx: true, scanTarget: targetWriter()},
		&dttDef{_columnType: coltest.NullNClob, testDataFn: nclobData, tx: true, scanTarget: targetNamedString()},
		&dttDef{_columnType: coltest.NullNClob, testDataFn: nclobData, tx: true, scanTarget: targetScanner()},
		&dttDef{_columnType: coltest.NullNClob, testDataFn: nclobData, tx: true, scanTarget: targetNullString()},
		&dttDef{_columnType: coltest.NullNClob, testDataFn: nclobData, tx: true, scanTarget: targetNullGenString()},
		&dttDef{_columnType: coltest.NullNClob, testDataFn: nclobData, tx: true, scanTarget: targetNullWriter()},

		&dttDef{_columnType: coltest.NullBlob, testDataFn: blobData, tx: true, scanTarget: targetBytes()},
		&dttDef{_columnType: coltest.NullBlob, testDataFn: blobData, tx: true, scanTarget: targetNamedBytes()},
		&dttDef{_columnType: coltest.NullBlob, testDataFn: blobData, tx: true, scanTarget: targetNullBytes()},
		&dttDef{_columnType: coltest.NullBlob, testDataFn: blobData, tx: true, scanTarget: targetNullGenBytes()},
		&dttDef{_columnType: coltest.NullBlob, testDataFn: blobData, tx: true, scanTarget: targetNullWriter()},
	}
}
