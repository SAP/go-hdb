package driver

import (
	"database/sql/driver"
	"io"
)

// byteSliceWriter implements io.Writer by appending to a byte slice.
type byteSliceWriter []byte

func (w *byteSliceWriter) Write(p []byte) (int, error) {
	*w = append(*w, p...)
	return len(p), nil
}

// A Lob is the driver representation of a database large object field.
// A Lob object uses an io.Reader object as source for writing content to a database lob field.
// A Lob object uses an io.Writer object as destination for reading content from a database lob field.
// A Lob can be created with the NewLob constructor with io.Reader and io.Writer as parameters or
// with new, setting io.Reader and io.Writer with the SetReader and SetWriter methods.
//
// On Go 1.27, *Lob implements io.Writer for scanning and has no Scan method.
// Prefer scanning directly into string or []byte for materialized values, or
// an io.Writer for streaming. A custom sql.Scanner takes precedence over
// io.Writer and receives materialized LOB bytes, or nil for SQL NULL.
// Callers must initialize embedded *Lob fields before scanning.
// On Go 1.26, *Lob continues to implement sql.Scanner.
type Lob struct {
	rd io.Reader
	wr io.Writer
}

// NewLob creates a new Lob instance with the io.Reader and io.Writer given as parameters.
func NewLob(rd io.Reader, wr io.Writer) *Lob {
	return &Lob{rd: rd, wr: wr}
}

// Reader returns the io.Reader of the Lob.
func (l Lob) Reader() io.Reader {
	return l.rd
}

// SetReader sets the io.Reader source for a lob field to be written to the database
// and returns *Lob, to enable simple call chaining.
func (l *Lob) SetReader(rd io.Reader) *Lob {
	l.rd = rd
	return l
}

// Writer returns the io.Writer of the Lob.
func (l Lob) Writer() io.Writer {
	return l.wr
}

// SetWriter sets the io.Writer destination for a lob field to be read from the database
// and returns *Lob, to enable simple call chaining.
func (l *Lob) SetWriter(wr io.Writer) *Lob {
	l.wr = wr
	return l
}

// NullLob represents a Lob that may be null.
// It can be used as a scan destination, similar to sql.NullString.
// On Go 1.27, *NullLob implements io.Writer and has no Scan method; the scanning
// rules documented for Lob also apply. Callers must initialize embedded
// *NullLob fields, but the contained Lob is allocated on the first write if nil.
// On Go 1.26, *NullLob continues to implement sql.Scanner.
type NullLob struct {
	Lob   *Lob
	Valid bool // Valid is true if Lob is not NULL
}

// Value implements the database/sql/Valuer interface.
func (n NullLob) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Lob, nil
}
