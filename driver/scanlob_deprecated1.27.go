//go:build (go1.27 && !go1.28 && !go_hdb_compat_go1_28) || (go1.28 && !go1.29 && go_hdb_compat_go1_28)

package driver

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
)

// ScanLobBytes scans a LOB into a byte slice.
//
// Deprecated: starting with go1.27 []byte scan targets are supported natively.
func ScanLobBytes(src any, b *[]byte) error {
	return sql.ConvertAssign(driver.ScanContext{}, b, src)
}

// ScanLobString scans a LOB into a string.
//
// Deprecated: starting with go1.27 string scan targets are supported natively.
func ScanLobString(src any, s *string) error {
	return sql.ConvertAssign(driver.ScanContext{}, s, src)
}

// ScanLobWriter scans a LOB into an io.Writer.
//
// Deprecated: starting with go1.27 io.Writer scan targets are supported natively.
func ScanLobWriter(src any, wr io.Writer) error {
	if wr == nil {
		return fmt.Errorf("lob scan error: parameter wr %T is nil", wr)
	}
	var rd io.Reader
	switch src := src.(type) {
	case []byte:
		rd = bytes.NewReader(src)
	case string:
		rd = strings.NewReader(src)
	case io.Reader:
		rd = src
	default:
		return fmt.Errorf("lob: invalid scan type %T", src)
	}
	_, err := io.Copy(wr, rd)
	return err
}
