//go:build (!go1.28 && !go_hdb_compat_go1_28) || (go1.28 && !go1.29 && go_hdb_compat_go1_28)

package driver

import (
	"database/sql/driver"
)

// NullBytes represents an []byte that may be null.
//
// Deprecated: []byte is already null-aware — database/sql represents a SQL NULL
// as a nil []byte, so a wrapper adds nothing. Scan into a plain []byte (nil on
// NULL), or use sql.Null[[]byte] if you want an explicit Valid flag.
type NullBytes struct {
	Bytes []byte
	Valid bool // Valid is true if Bytes is not NULL
}

// Scan implements the Scanner interface.
func (n *NullBytes) Scan(value any) error {
	n.Bytes, n.Valid = value.([]byte)
	return nil
}

// Value implements the driver Valuer interface.
func (n NullBytes) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Bytes, nil
}
