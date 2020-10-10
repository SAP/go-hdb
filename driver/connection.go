// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/SAP/go-hdb/driver/common"
	"github.com/SAP/go-hdb/driver/sqltrace"
	p "github.com/SAP/go-hdb/internal/protocol"
	"github.com/SAP/go-hdb/internal/protocol/scanner"
)

// Transaction isolation levels supported by hdb.
const (
	LevelReadCommitted  = "READ COMMITTED"
	LevelRepeatableRead = "REPEATABLE READ"
	LevelSerializable   = "SERIALIZABLE"
)

// Access modes supported by hdb.
const (
	modeReadOnly  = "READ ONLY"
	modeReadWrite = "READ WRITE"
)

// map sql isolation level to hdb isolation level.
var isolationLevel = map[driver.IsolationLevel]string{
	driver.IsolationLevel(sql.LevelDefault):        LevelReadCommitted,
	driver.IsolationLevel(sql.LevelReadCommitted):  LevelReadCommitted,
	driver.IsolationLevel(sql.LevelRepeatableRead): LevelRepeatableRead,
	driver.IsolationLevel(sql.LevelSerializable):   LevelSerializable,
}

// map sql read only flag to hdb access mode.
var readOnly = map[bool]string{
	true:  modeReadOnly,
	false: modeReadWrite,
}

// ErrUnsupportedIsolationLevel is the error raised if a transaction is started with a not supported isolation level.
var ErrUnsupportedIsolationLevel = errors.New("unsupported isolation level")

// ErrNestedTransaction is the error raised if a tranasction is created within a transaction as this is not supported by hdb.
var ErrNestedTransaction = errors.New("nested transactions are not supported")

// ErrNestedQuery is the error raised if a sql statement is executed before an "active" statement is closed.
// Example: execute sql statement before rows of privious select statement are closed.
var ErrNestedQuery = errors.New("nested sql queries are not supported")

// queries
const (
	pingQuery          = "select 1 from dummy"
	isolationLevelStmt = "set transaction isolation level %s"
	accessModeStmt     = "set transaction %s"
	defaultSchema      = "set schema %s"
)

var minimalServerVersion = common.ParseHDBVersion("2.00.042")

// bulk statement
const (
	bulk = "b$"
)

var (
	flushTok   = new(struct{})
	noFlushTok = new(struct{})
)

var (
	// NoFlush is to be used as parameter in bulk statements to delay execution.
	NoFlush = sql.Named(bulk, &noFlushTok)
	// Flush can be used as optional parameter in bulk statements but is not required to trigger execution.
	Flush = sql.Named(bulk, &flushTok)
)

func init() {
	p.RegisterScanType(p.DtDecimal, reflect.TypeOf((*Decimal)(nil)).Elem())
	p.RegisterScanType(p.DtLob, reflect.TypeOf((*Lob)(nil)).Elem())
}

//  check if conn implements all required interfaces
var (
	_ driver.Conn               = (*Conn)(nil)
	_ driver.ConnPrepareContext = (*Conn)(nil)
	_ driver.Pinger             = (*Conn)(nil)
	_ driver.ConnBeginTx        = (*Conn)(nil)
	_ driver.ExecerContext      = (*Conn)(nil)
	_ driver.Execer             = (*Conn)(nil) //go 1.9 issue (ExecerContext is only called if Execer is implemented)
	_ driver.QueryerContext     = (*Conn)(nil)
	_ driver.Queryer            = (*Conn)(nil) //go 1.9 issue (QueryerContext is only called if Queryer is implemented)
	_ driver.NamedValueChecker  = (*Conn)(nil)
	_ driver.SessionResetter    = (*Conn)(nil)
)

// Conn is the implementation of the database/sql/driver Conn interface.
type Conn struct {
	ctr     *Connector
	session *p.Session
	scanner *scanner.Scanner
	closed  chan struct{}
}

func newConn(ctx context.Context, ctr *Connector) (driver.Conn, error) {
	session, err := p.NewSession(ctx, ctr.sessionConfig())
	if err != nil {
		return nil, err
	}

	sv := session.ServerVersion()
	/*
		hdb version < 2.00.042
		- no support of providing ClientInfo (server variables) in CONNECT message (see messageType.clientInfoSupported())
	*/
	switch {
	case sv.IsEmpty(): // hdb version 1 does not report fullVersionString
		return nil, fmt.Errorf("server version 1.00 is not supported - minimal server version: %s", minimalServerVersion)
	case sv.Compare(minimalServerVersion) == -1:
		return nil, fmt.Errorf("server version %s is not supported - minimal server version: %s", sv, minimalServerVersion)
	}

	c := &Conn{ctr: ctr, session: session, scanner: &scanner.Scanner{}, closed: make(chan struct{})}
	if err := c.init(ctx, ctr); err != nil {
		return nil, err
	}
	d := ctr.PingInterval()
	if d != 0 {
		go c.pinger(d, c.closed)
	}

	hdbDriver.addConn(1) // increment open connections.

	return c, nil
}

func (c *Conn) init(ctx context.Context, ctr *Connector) error {
	if ctr.defaultSchema != "" {
		if _, err := c.ExecContext(ctx, fmt.Sprintf(defaultSchema, ctr.defaultSchema), nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) pinger(d time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	ctx := context.Background()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			c.Ping(ctx)
		}
	}
}

// Ping implements the driver.Pinger interface.
func (c *Conn) Ping(ctx context.Context) (err error) {
	c.session.Lock()
	defer c.session.Unlock()

	if c.session.IsBad() {
		return driver.ErrBadConn
	}
	if c.session.InQuery() {
		return ErrNestedQuery
	}

	// caution!!!
	defer c.session.SetInQuery(false)

	done := make(chan struct{})
	go func() {
		_, err = c.session.QueryDirect(pingQuery)
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.session.Kill()
		return ctx.Err()
	case <-done:
		return err
	}
}

// ResetSession implements the driver.SessionResetter interface.
func (c *Conn) ResetSession(ctx context.Context) error {
	c.session.Lock()
	defer c.session.Unlock()

	c.session.Reset()
	if c.session.IsBad() {
		return driver.ErrBadConn
	}
	return nil
}

// PrepareContext implements the driver.ConnPrepareContext interface.
func (c *Conn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	c.session.Lock()
	defer c.session.Unlock()

	if c.session.IsBad() {
		return nil, driver.ErrBadConn
	}
	if c.session.InQuery() {
		return nil, ErrNestedQuery
	}

	done := make(chan struct{})
	go func() {
		var (
			qd *p.QueryDescr
			pr *p.PrepareResult
		)

		qd, err = p.NewQueryDescr(query, c.scanner)
		if err != nil {
			goto done
		}
		pr, err = c.session.Prepare(qd.Query())
		if err != nil {
			goto done
		}

		if err = pr.Check(qd); err != nil {
			goto done
		}

		select {
		default:
		case <-ctx.Done():
			return
		}
		stmt, err = newStmt(c.session, qd.Query(), qd.IsBulk(), c.ctr.BulkSize(), pr)
	done:
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.session.Kill()
		return nil, ctx.Err()
	case <-done:
		hdbDriver.addStmt(1) // increment number of statements.
		return stmt, err
	}
}

// Close implements the driver.Conn interface.
func (c *Conn) Close() error {
	c.session.Lock()
	defer c.session.Unlock()

	hdbDriver.addConn(-1) // decrement open connections.
	close(c.closed)       // signal connection close
	return c.session.Close()
}

// BeginTx implements the driver.ConnBeginTx interface.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	c.session.Lock()
	defer c.session.Unlock()

	if c.session.IsBad() {
		return nil, driver.ErrBadConn
	}
	if c.session.InTx() {
		return nil, ErrNestedTransaction
	}
	if c.session.InQuery() {
		return nil, ErrNestedQuery
	}

	level, ok := isolationLevel[opts.Isolation]
	if !ok {
		return nil, ErrUnsupportedIsolationLevel
	}

	done := make(chan struct{})
	go func() {
		// set isolation level
		if _, err = c.session.ExecDirect(fmt.Sprintf(isolationLevelStmt, level)); err != nil {
			goto done
		}
		// set access mode
		if _, err = c.session.ExecDirect(fmt.Sprintf(accessModeStmt, readOnly[opts.ReadOnly])); err != nil {
			goto done
		}
		c.session.SetInTx(true)
		tx = newTx(c.session)
	done:
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.session.Kill()
		return nil, ctx.Err()
	case <-done:
		hdbDriver.addTx(1) // increment number of transactions.
		return tx, err
	}
}

// QueryContext implements the driver.QueryerContext interface.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	c.session.Lock()
	defer c.session.Unlock()

	if c.session.IsBad() {
		return nil, driver.ErrBadConn
	}
	if c.session.InQuery() {
		return nil, ErrNestedQuery
	}

	if len(args) != 0 {
		return nil, driver.ErrSkip //fast path not possible (prepare needed)
	}

	qd, err := p.NewQueryDescr(query, c.scanner)
	if err != nil {
		return nil, err
	}
	switch qd.Kind() {
	case p.QkCall:
		// direct execution of call procedure
		// - returns no parameter metadata (sps 82) but only field values
		// --> let's take the 'prepare way' for stored procedures
		return nil, driver.ErrSkip
	case p.QkID:
		// query call table result
		qrs, ok := p.QrsCache.Get(qd.ID())
		if !ok {
			return nil, fmt.Errorf("invalid result set id %s", query)
		}
		return qrs, nil
	}

	if sqltrace.On() {
		sqltrace.Traceln(query)
	}

	done := make(chan struct{})
	go func() {
		rows, err = c.session.QueryDirect(query)
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.session.Kill()
		return nil, ctx.Err()
	case <-done:
		return rows, err
	}
}

// ExecContext implements the driver.ExecerContext interface.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, err error) {
	c.session.Lock()
	defer c.session.Unlock()

	if c.session.IsBad() {
		return nil, driver.ErrBadConn
	}
	if c.session.InQuery() {
		return nil, ErrNestedQuery
	}

	if len(args) != 0 {
		return nil, driver.ErrSkip //fast path not possible (prepare needed)
	}

	if sqltrace.On() {
		sqltrace.Traceln(query)
	}

	done := make(chan struct{})
	go func() {
		var qd *p.QueryDescr
		qd, err = p.NewQueryDescr(query, c.scanner)
		if err != nil {
			goto done
		}
		r, err = c.session.ExecDirect(qd.Query())
	done:
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.session.Kill()
		return nil, ctx.Err()
	case <-done:
		return r, err
	}
}

// CheckNamedValue implements the NamedValueChecker interface.
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	// - called by sql driver for ExecContext and QueryContext
	// - no check needs to be performed as ExecContext and QueryContext provided
	//   with parameters will force the 'prepare way' (driver.ErrSkip)
	// - Anyway, CheckNamedValue must be implemented to avoid default sql driver checks
	//   which would fail for custom arg types like Lob
	return nil
}

// Conn Raw access methods

// ServerInfo returns parameters reported by hdb server.
func (c *Conn) ServerInfo() *common.ServerInfo {
	return c.session.ServerInfo()
}

//transaction

//  check if tx implements all required interfaces
var (
	_ driver.Tx = (*tx)(nil)
)

type tx struct {
	closed  bool
	session *p.Session
}

func newTx(session *p.Session) *tx { return &tx{session: session} }

func (t *tx) close() {
	if t.closed {
		return
	}
	hdbDriver.addTx(-1) // decrement number of transactions.
	t.closed = true
}

func (t *tx) Commit() error {
	t.session.Lock()
	defer t.session.Unlock()

	t.close()
	if t.session.IsBad() {
		return driver.ErrBadConn
	}
	return t.session.Commit()
}

func (t *tx) Rollback() error {
	t.session.Lock()
	defer t.session.Unlock()

	t.close()
	if t.session.IsBad() {
		return driver.ErrBadConn
	}
	return t.session.Rollback()
}

//statement

//  check if stmt implements all required interfaces
var (
	_ driver.Stmt              = (*stmt)(nil)
	_ driver.StmtExecContext   = (*stmt)(nil)
	_ driver.StmtQueryContext  = (*stmt)(nil)
	_ driver.NamedValueChecker = (*stmt)(nil)
)

type stmt struct {
	pr                *p.PrepareResult
	session           *p.Session
	query             string
	bulk, flush       bool
	bulkSize, numBulk int
	trace             bool // store flag for performance reasons (especially bulk inserts)
	args              []driver.NamedValue
}

func newStmt(session *p.Session, query string, bulk bool, bulkSize int, pr *p.PrepareResult) (*stmt, error) {
	return &stmt{session: session, query: query, pr: pr, bulk: bulk, bulkSize: bulkSize, trace: sqltrace.On()}, nil
}

func (s *stmt) Close() error {
	s.session.Lock()
	defer s.session.Unlock()

	hdbDriver.addStmt(-1) // decrement number of statements.
	if len(s.args) != 0 { // log always //TODO: Fatal?
		sqltrace.Tracef("close: %s - not flushed records: %d)", s.query, len(s.args)/s.pr.NumField())
	}
	return s.session.DropStatementID(s.pr.StmtID())
}

func (s *stmt) NumInput() int {
	/*
		NumInput differs dependent on statement (check is done in QueryContext and ExecContext):
		- #args == #param (only in params):    query, exec, exec bulk (non control query)
		- #args == #param (in and out params): exec call
		- #args == 0:                          exec bulk (control query)
		- #args == #input param:               query call
	*/
	return -1
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	s.session.Lock()
	defer s.session.Unlock()

	if s.session.IsBad() {
		return nil, driver.ErrBadConn
	}
	if s.session.InQuery() {
		return nil, ErrNestedQuery
	}
	if s.trace {
		sqltrace.Tracef("%s %v", s.query, args)
	}

	numArg := len(args)
	var numExpected int
	if s.pr.IsProcedureCall() {
		numExpected = s.pr.NumInputField() // input fields only
	} else {
		numExpected = s.pr.NumField() // all fields needs to be input fields
	}
	if numArg != numExpected {
		return nil, fmt.Errorf("invalid number of arguments %d - %d expected", numArg, numExpected)
	}

	done := make(chan struct{})
	go func() {
		if s.pr.IsProcedureCall() {
			rows, err = s.session.QueryCall(s.pr, args)
		} else {
			rows, err = s.session.Query(s.pr, args)
		}
		close(done)
	}()

	select {
	case <-ctx.Done():
		s.session.Kill()
		return nil, ctx.Err()
	case <-done:
		return rows, err
	}
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (r driver.Result, err error) {
	if s.trace {
		sqltrace.Tracef("%s %v", s.query, args)
	}

	flush := s.flush // store s.flush
	s.flush = false  // reset s.flush

	numArg := len(args)
	numExpected := 0
	if s.bulk && numArg == 0 { // bulk flush
		flush = true
	} else {
		numExpected = s.pr.NumField()
	}
	if numArg != numExpected {
		return nil, fmt.Errorf("invalid number of arguments %d - %d expected", numArg, numExpected)
	}

	// handle bulk insert
	if s.bulk {
		if numArg != 0 { // add to argument buffer
			if s.args == nil {
				s.args = make([]driver.NamedValue, 0, DefaultBulkSize)
			}
			s.args = append(s.args, args...)
			s.numBulk++
			if s.numBulk == s.bulkSize {
				flush = true
			}
		}
		if !flush || s.numBulk == 0 { // done: no flush
			return driver.ResultNoRows, nil
		}
	}

	s.session.Lock()
	defer s.session.Unlock()

	if s.session.IsBad() {
		return nil, driver.ErrBadConn
	}
	if s.session.InQuery() {
		return nil, ErrNestedQuery
	}

	done := make(chan struct{})
	go func() {
		switch {
		case s.pr.IsProcedureCall():
			r, err = s.session.ExecCall(s.pr, args)
		case s.bulk: // flush case only
			r, err = s.session.Exec(s.pr, s.args)
			s.args = s.args[:0]
			s.numBulk = 0
		default:
			r, err = s.session.Exec(s.pr, args)
		}
		close(done)
	}()

	select {
	case <-ctx.Done():
		s.session.Kill()
		return nil, ctx.Err()
	case <-done:
		return r, err
	}
}

// CheckNamedValue implements NamedValueChecker interface.
func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if nv.Name == bulk {
		if ptr, ok := nv.Value.(**struct{}); ok {
			switch ptr {
			case &noFlushTok:
				s.bulk = true
				return driver.ErrRemoveArgument
			case &flushTok:
				s.flush = true
				return driver.ErrRemoveArgument
			}
		}
	}

	return convertNamedValue(s.pr, nv)
}
