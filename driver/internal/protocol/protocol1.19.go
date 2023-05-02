//go:build !go1.20

// Delete after go1.19 is out of maintenance.

package protocol

import (
	"bufio"
	"log"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

// writer represents a protocol writer.
type writer struct {
	tracer *log.Logger

	wr  *bufio.Writer
	enc *encoding.Encoder

	sv     map[string]string
	svSent bool

	// reuse header
	mh *messageHeader
	sh *segmentHeader
	ph *PartHeader

	lastWriteErr error
}

// Writer is the protocol writer interface.
type Writer interface {
	WriteProlog() error
	Write(sessionID int64, messageType MessageType, commit bool, writers ...partWriter) error
	LastWriteErr() error
}

func (w *writer) LastWriteErr() error { return w.lastWriteErr }

func (w *writer) lastErrorHandler(err error) error {
	if err != nil {
		w.lastWriteErr = err
	}
	return err
}
