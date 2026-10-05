package driver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

const (
	// poolCap limits the number of pooled (reuse ready) connections per connector.
	poolCap = 2

	// maxPooledAge closes pooled connections that idle longer than the limit.
	// It bounds the server-side session resources of a connector whose pooled
	// connections are not reused.
	maxPooledAge = 5 * time.Minute

	// disconnectTimeout bounds the server-side disconnect of a single
	// terminated session (see cancelSession with force=true).
	disconnectTimeout = 10 * time.Second

	// disconnectSessionStmt is the HANA session management statement that drops
	// the server side session of a client connection (see cancelSession).
	disconnectSessionStmt = "alter system disconnect session '%d'"

	// cancelSessionStmt is the HANA session management statement that aborts
	// the running statement of a server side session, keeping the session
	// itself alive (see cancelSession).
	cancelSessionStmt = "alter system cancel session '%d'"
)

// disconnectEvent is a queued server-side session termination: the worker
// executes the disconnect statement of the executor session identified by
// serverConnID on the host (see cancelSession with force=true).
type disconnectEvent struct {
	host         string // HANA server host and port of the connection.
	serverConnID int    // HANA server connection id (connect option coConnectionID).
}

// poolEntry is a pooled connection. The pool behaves like the database/sql
// free conn pool: connections are opaque and interchangeable, the pool does
// not know anything about their internals. Each entry expires by its own timer
// (see addConn): the worker never runs on a schedule.
type poolEntry struct {
	conn  *conn
	timer *time.Timer // reaps the entry after maxPooledAge (see deleteConn).
}

// connLifecycle handles the connection duties of a connector on a single
// worker goroutine:
//
//   - disconnect events (server side disconnect of an executor session after
//     a request context was canceled) are executed asynchronously (see
//     disconnect),
//   - connections whose table output rows are no longer needed are queued
//     once the last table rowset is closed (see queue) and handed out again on
//     the next connect (see getConn),
//   - pooled connections are reaped after maxPooledAge.
//
// Like the database/sql connection cleaner, the lifecycle uses a signal
// channel and a mutex: work (disconnect events and pending trackers) is
// queued in slices under the mutex, the adders wake the worker on the signal
// channel or spawn it on demand (see wake), and the worker processes the queued
// work and retires once nothing is left to do. Unlike the cleaner there is no
// timer: pooled connections expire by their own per-entry timer (see addConn).
type connLifecycle struct {
	connector *Connector

	mu sync.Mutex

	wakeCh          chan struct{}      // signal channel: wake the worker (see wake).
	disconnectQueue []disconnectEvent  // queued disconnect events.
	pending         []*tableOutTracker // trackers waiting for their table rowsets to close.
	pool            []*poolEntry       // pooled connections, oldest first.

	noSessionAdmin atomic.Bool // last cancel outcome was 258 (rights missing).
}

// wake spawns the worker if it is not running and wakes it otherwise. It must
// be called with c.mu held. The wake signal is best effort (the channel holds
// one token); the queued work itself is never lost - it is picked up by the
// worker spawned here or by any subsequent wake.
func (c *connLifecycle) wakeLocked() {
	if c.wakeCh == nil {
		c.wakeCh = make(chan struct{}, 1)
		go c.worker()
	}
	select {
	case c.wakeCh <- struct{}{}:
	default:
	}
}

// wake signals the worker (spawning it if needed). It may be called without
// holding c.mu (see tableOutTracker.release).
func (c *connLifecycle) wake() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wakeLocked()
}

// disconnect requests the server side disconnect of the executor session of
// the connection identified by serverConnID. The event is queued under the
// lock and executed by the worker (see cancelSession with force=true).
func (c *connLifecycle) disconnect(host string, serverConnID int) {
	if serverConnID <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disconnectQueue = append(c.disconnectQueue, disconnectEvent{host: host, serverConnID: serverConnID})
	c.wakeLocked()
}

// queue hands a released table-out connection over to the worker. The task is
// queued under the lock and never dropped. The worker checks the table-out
// tracker: once the last table rowset is closed (n == 0) it finalizes the
// tracker and adds the connection to the pool (see worker).
func (c *connLifecycle) queue(tc *tableOutTracker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = append(c.pending, tc)
	c.wakeLocked()
}

// worker is the lifecycle worker goroutine: wait for work, take it out under
// the lock, execute it outside the lock, retire when nothing is queued anymore.
// There is no timer: pending trackers with open rowsets are re-driven by the
// release wake (see tableOutTracker.release), pooled connections expire by
// their own timer (see addConn). The disconnect statement and the rows/conn
// closes run outside the lock, so they never block the queue/disconnect adders
// or the reuse getConn.
func (c *connLifecycle) worker() {
	for {
		<-c.wakeCh // new work queued

		c.mu.Lock()
		disconnectEvents := c.disconnectQueue
		c.disconnectQueue = nil

		var ready []*tableOutTracker
		// In-place filter under c.mu: the pending backing array must only be
		// touched while holding c.mu. clear releases the filtered-out tail so
		// retired trackers don't linger.
		keep := c.pending[:0]
		for _, tc := range c.pending {
			if tc.n.Load() == 0 {
				ready = append(ready, tc)
			} else {
				keep = append(keep, tc)
			}
		}
		clear(c.pending[len(keep):])
		c.pending = keep
		c.mu.Unlock()

		if len(disconnectEvents) != 0 { // one-off, best effort: serially on the worker
			for _, ev := range disconnectEvents {
				c.cancelSession(ev.host, ev.serverConnID, true, disconnectTimeout)
			}
		}
		for _, tc := range ready { // finalized outside the lock
			tc.rows.Close() // fake parent rows
			tc.stmt.close() // drop statement
			conn := tc.stmt.conn
			// Safety net: unreachable via the public API (db/sql calls Close once and
			// holds the conn for the tx lifetime); guards against misuse and future queue sites.
			// Already-closed needs no second teardown and is never pooled.
			if !conn.closed.Load() && (conn.session.isBad() || conn.session.inTx.Load() || !c.addConn(conn)) {
				_ = conn.close() // bad, in-tx, or no pool space left: real teardown.
			}
			close(tc.done) // signaled exactly once: extracted from pending under the lock
		}

		// Retire only when nothing is queued anymore. The pool is not part of the
		// check: pooled connections expire by their own timer (see addConn), so the
		// worker never lingers on pooled entries. The check is atomic with
		// the nil: an add that wins the lock afterwards respawns the worker.
		c.mu.Lock()
		if len(c.disconnectQueue) == 0 && len(c.pending) == 0 {
			c.wakeCh = nil
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}

// addConn adds a connection to the pool. If the pool is at capacity, the
// connection is not pooled and is closed instead, mirroring database/sql's
// putConn: the pool never exceeds poolCap and nothing is evicted. Each pooled
// entry expires by its own timer after maxPooledAge (see deleteConn).
func (c *connLifecycle) addConn(cn *conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pool) >= poolCap {
		return false // at capacity don't pool
	}
	entry := &poolEntry{conn: cn}
	entry.timer = time.AfterFunc(maxPooledAge, func() { c.deleteConn(entry) })
	c.pool = append(c.pool, entry)
	return true
}

// deleteConn removes the expired entry from the pool, if still present, and closes
// its connection outside the lock. Entries handed out by getConn before the timer
// fires are disarmed there; a lost Stop race no-ops on the identity check.
func (c *connLifecycle) deleteConn(entry *poolEntry) {
	c.mu.Lock()
	for i, e := range c.pool {
		if e == entry {
			c.pool = slices.Delete(c.pool, i, i+1)
			c.mu.Unlock()
			_ = e.conn.close()
			return
		}
	}
	c.mu.Unlock()
}

// getConn returns the most recently pooled connection passing the shared reset
// check, if any, for reuse; the caller skips dial and authenticate then.
// Rejected candidates are closed and the next entry is tried. It is
// opportunistic: a connection pooled later than a concurrent getConn is not seen
// here and the caller dials as usual.
func (c *connLifecycle) getConn(ctx context.Context) (*conn, bool) {
	for {
		c.mu.Lock()
		if len(c.pool) == 0 {
			c.mu.Unlock()
			return nil, false
		}
		e := c.pool[len(c.pool)-1] // most recently pooled
		c.pool = c.pool[:len(c.pool)-1]
		c.mu.Unlock()

		e.timer.Stop() // disarm expiry: a lost race no-ops in reap.
		if err := e.conn.resetSession(ctx); err == nil {
			return e.conn, true
		}
		_ = e.conn.close()
	}
}

// cancelSession sends ALTER SYSTEM CANCEL (force=false) or DISCONNECT
// (force=true) for serverConnID on host over a fresh connection.
// Best effort: 729 fails quietly, 258 warns on the flip to refused. No result: the reuse
// gate keys on completion, not outcome.
func (c *connLifecycle) cancelSession(host string, serverConnID int, force bool, timeout time.Duration) {
	attrs := *c.connector.connAttrs()

	logErr := func(err error) {
		attrs.logger.Warn("cancel session failed", slog.String("host", host), slog.Int("serverConnID", serverConnID), slog.Any("error", err))
	}

	logNoSessionAdmin := func(err error) {
		if c.noSessionAdmin.CompareAndSwap(false, true) {
			attrs.logger.Warn("cancel session refused: SESSION ADMIN required", slog.String("host", host), slog.Int("serverConnID", serverConnID), slog.Any("error", err))
		}
	}

	stmt := cancelSessionStmt
	if force {
		stmt = disconnectSessionStmt
	}
	attrs.connectionRouting = false
	// Bound the cancel's sockets at the handed-in timeout: a wedged cancel
	// must die fast instead of leaning on the configured defaults.
	// Tighten only downward, including 0 (disabled) to the timeout.
	if timeout > 0 {
		if attrs.dialTimeout == 0 || attrs.dialTimeout > timeout {
			attrs.dialTimeout = timeout
		}
		if attrs.readTimeout == 0 || attrs.readTimeout > timeout {
			attrs.readTimeout = timeout
		}
		if attrs.writeTimeout == 0 || attrs.writeTimeout > timeout {
			attrs.writeTimeout = timeout
		}
	}
	// Isolated routing state: the cancel never picks, never advertises
	// distribution, and never feeds topology back into the connector.
	// Background: socket deadlines bound the cancel, no context deadline.
	ctx := context.Background()
	conn, err := newConn(ctx, host, c.connector.metrics, &routing{}, &attrs, c)
	if err != nil {
		logErr(err)
		return
	}
	defer conn.Close()
	if err := conn.authenticate(ctx, host, c.connector.authHnd()); err != nil {
		logErr(err)
		return
	}
	if _, err = conn.session.execDirect(ctx, fmt.Sprintf(stmt, serverConnID)); err == nil {
		c.noSessionAdmin.Store(false) // rights proven: next 258 re-warns.
		return
	}
	if hdbErrors, ok := errors.AsType[*p.HdbErrors](err); ok {
		switch hdbErrors.Code() {
		case 729: // victim already gone - not reusable, quiet.
			return
		case 258: // no SESSION ADMIN - victim untouched.
			logNoSessionAdmin(err)
			return
		}
	}
	logErr(err)
}
