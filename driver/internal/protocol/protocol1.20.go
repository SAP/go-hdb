//go:build go1.20

// Delete after go1.19 is out of maintenance.

package protocol

import (
	"bufio"
	"context"
	"database/sql/driver"
	"errors"

	"github.com/SAP/go-hdb/driver/internal/exp/slog"
	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

// writer represents a protocol writer.
type writer struct {
	protTrace bool
	logger    *slog.Logger

	wr  *bufio.Writer
	enc *encoding.Encoder

	sv     map[string]string
	svSent bool

	// reuse header
	mh *messageHeader
	sh *segmentHeader
	ph *PartHeader
}

// Writer is the protocol writer interface.
type Writer interface {
	WriteProlog(ctx context.Context) error
	Write(ctx context.Context, sessionID int64, messageType MessageType, commit bool, writers ...partWriter) error
}

func (w *writer) lastErrorHandler(err error) error { // remove after merging back into protocol
	if err != nil {
		return errors.Join(err, driver.ErrBadConn)
	}
	return nil
}
