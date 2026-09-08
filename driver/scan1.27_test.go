//go:build go1.27

package driver

import (
	"database/sql"
	"database/sql/driver"
	"testing"
)

type writerLob []byte

func (w *writerLob) Write(p []byte) (int, error) {
	*w = append(*w, p...)
	return len(p), nil
}

// TestConvertAssignLobGenericNull unit-tests the go1.27 generic sql.Null[T] LOB
// scan path (convertAssignLob). These cases are duplicates of the lobTestData /
// lobASCIITestData rows in datatype_test.go; they are kept as unit tests until
// go1.28 drops go1.26 support, then transfer to the integration tests.
func TestConvertAssignLobGenericNull(t *testing.T) {
	tests := []struct {
		name  string
		dst   any
		src   []byte
		check func(t *testing.T, dst any)
	}{
		{
			name: "string",
			dst:  &sql.Null[string]{},
			src:  []byte("hello"),
			check: func(t *testing.T, dst any) {
				v := dst.(*sql.Null[string])
				if !v.Valid || v.V != "hello" {
					t.Fatalf("got %+v, want V=hello Valid=true", v)
				}
			},
		},
		{
			name: "bytes",
			dst:  &sql.Null[[]byte]{},
			src:  []byte("world"),
			check: func(t *testing.T, dst any) {
				v := dst.(*sql.Null[[]byte])
				if !v.Valid || string(v.V) != "world" {
					t.Fatalf("got %+v, want V=[119 111 114 108 100] Valid=true", v)
				}
			},
		},
		{
			name: "writer",
			dst:  &sql.Null[writerLob]{},
			src:  []byte("lob"),
			check: func(t *testing.T, dst any) {
				v := dst.(*sql.Null[writerLob])
				if !v.Valid || string(v.V) != "lob" {
					t.Fatalf("got %+v, want V=[108 111 98] Valid=true", v)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := convertAssignLob(driver.ScanContext{}, nil, tt.dst, tt.src); err != nil {
				t.Fatalf("convertAssignLob returned error: %v", err)
			}
			tt.check(t, tt.dst)
		})
	}
}
