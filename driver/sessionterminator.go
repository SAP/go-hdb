package driver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

type terminateEvent struct {
	host         string // HANA server host and port of the connection.
	serverConnID int    // HANA server connection id (connect option coConnectionID).
}

const (
	terminateQueueSize   = 64
	cancelSessionTimeout = 5 * time.Second
)

const disconnectSessionStmt = "alter system disconnect session '%d'"

// When a request context is canceled or times out, go-hdb returns the
// context error to the caller; the server keeps executing the request.
// sessionTerminator stops the server side execution asynchronously via the
// HANA session management statements.
// terminate requests server side cancellation of the executor session of the
// connection identified by serverConnID (see HANA system view
// M_CONNECTIONS.CONNECTION_ID).
//
// HANA identifies a client connection by the connect option coConnectionID,
// returned by the server during connection setup. The HANA session
// management statements take this connection id as identifier, even though
// their syntax reads SESSION:
//
//	alter system [cancel|disconnect] session '<connection id>'
//
// The session id HANA also returns during connection setup is not the connection id.
// It is the per-message context id and cannot be used in the session management statements.
//
// HANA provides two session management statements for statement
// cancellation:
//
//   - alter system cancel session: aborts the currently executing
//     statement, the session stays usable.
//   - alter system disconnect session: aborts the statement and discards
//     the session (open transactions are rolled back, the session cannot
//     be used afterwards).
//
// go-hdb uses DISCONNECT: after a canceled request the connection is
// discarded by database/sql anyway, so the DISCARD semantics fit. Using
// CANCEL with a reusable session would require keeping the connection
// viable, which is not implemented.

type sessionTerminator struct {
	connector *Connector

	mu     sync.Mutex
	n      int
	taskCh chan terminateEvent
	wg     sync.WaitGroup
}

func (t *sessionTerminator) incrConn() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.n++
	if t.n > 1 {
		return
	}
	t.taskCh = make(chan terminateEvent, terminateQueueSize)
	t.wg.Go(func() {
		for ev := range t.taskCh {
			t.executeDisconnect(ev)
		}
	})
}

func (t *sessionTerminator) decrConn() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.n--
	if t.n > 0 {
		return
	}
	if t.n < 0 {
		t.n = 0 // unpaired decrConn: silently ignore
		return
	}
	close(t.taskCh)
	t.wg.Wait()
	t.taskCh = nil
}

// terminate is best effort: the termination is dropped if the queue is
// full. The execution opens a new connection to the routing host of the
// connection to be terminated (parameter host) - routing can select a
// different node per connection, so the dialed host, not the connector
// host, is the DISCONNECT target.
func (t *sessionTerminator) terminate(host string, serverConnID int) {
	if serverConnID <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	select {
	case t.taskCh <- terminateEvent{host: host, serverConnID: serverConnID}:
	default:
	}
}

func (t *sessionTerminator) executeDisconnect(ev terminateEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), cancelSessionTimeout)
	defer cancel()

	logger := t.connector._logger

	// the worker's own connection is not accounted with the terminator (nil):
	// it is created on the worker and must never call open and close.
	conn, err := newConn(ctx, ev.host, t.connector.metrics, t.connector._routing, t.connector.connAttrs(), nil)
	if err != nil {
		logger.Warn("terminate server session failed", slog.String("host", ev.host), slog.Int("serverConnID", ev.serverConnID), slog.Any("error", err))
		return
	}
	defer conn.Close()

	if err = conn.authenticate(ctx, ev.host, t.connector.authHnd()); err != nil {
		logger.Warn("terminate server session failed", slog.String("host", ev.host), slog.Int("serverConnID", ev.serverConnID), slog.Any("error", err))
		return
	}

	if _, err = conn.session.execDirect(ctx, fmt.Sprintf(disconnectSessionStmt, ev.serverConnID)); err != nil {
		// HANA error 729 (unknown session id) means the session is already gone.
		if hdbErrors, ok := errors.AsType[*p.HdbErrors](err); ok && hdbErrors.Code() == 729 {
			return
		}
		logger.Warn("terminate server session failed", slog.String("host", ev.host), slog.Int("serverConnID", ev.serverConnID), slog.Any("error", err))
	}
}
