//go:build go1.27

package driver

import (
	"bytes"
	"io"
)

// Write writes p to the LOB destination, allocating a bytes.Buffer if none is set.
func (l *Lob) Write(p []byte) (int, error) {
	if l.wr == nil {
		l.wr = new(bytes.Buffer)
	}
	return l.wr.Write(p)
}

// Write marks the LOB non-NULL and writes p to its destination.
// An empty write also marks the LOB non-NULL.
func (n *NullLob) Write(p []byte) (int, error) {
	if n.Lob == nil {
		n.Lob = new(Lob)
	}
	n.Valid = true
	return n.Lob.Write(p)
}

// These private methods are promoted through embedded LOBs. They let the scan
// path preserve NULL state and close underlying pipes without bypassing Write.
func (l *Lob) lobWriter() io.Writer { return l.wr }

func (n *NullLob) lobWriter() io.Writer {
	if n.Lob == nil {
		return nil
	}
	return n.Lob.wr
}

func (n *NullLob) setNull() { n.Valid = false }
