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

// ErrNestedTransaction is the error raised if a transaction is created within a transaction as this is not supported by hdb.
var ErrNestedTransaction = errors.New("nested transactions are not supported")

// ErrNestedQuery is the error raised if a sql statement is executed before an "active" statement is closed.
// Example: execute sql statement before rows of previous select statement are closed.
var ErrNestedQuery = errors.New("nested sql queries are not supported")

// queries
const (
	pingQuery          = "select 1 from dummy"
	isolationLevelStmt = "set transaction isolation level %s"
	accessModeStmt     = "set transaction %s"
	defaultSchema      = "set schema %s"
)

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
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.Execer             = (*conn)(nil) //go 1.9 issue (ExecerContext is only called if Execer is implemented)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.Queryer            = (*conn)(nil) //go 1.9 issue (QueryerContext is only called if Queryer is implemented)
	_ driver.NamedValueChecker  = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
)

type conn struct {
	session *p.Session
	scanner *scanner.Scanner
	closed  chan struct{}
}

func newConn(ctx context.Context, ctr *Connector) (driver.Conn, error) {
	session, err := p.NewSession(ctx, ctr)
	if err != nil {
		return nil, err
	}
	c := &conn{session: session, scanner: &scanner.Scanner{}, closed: make(chan struct{})}
	if err := c.init(ctx, ctr); err != nil {
		return nil, err
	}
	d := ctr.PingInterval()
	if d != 0 {
		go c.pinger(d, c.closed)
	}
	return c, nil
}

func (c *conn) init(ctx context.Context, ctr *Connector) error {
	if ctr.defaultSchema != "" {
		if _, err := c.ExecContext(ctx, fmt.Sprintf(defaultSchema, ctr.defaultSchema), nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) pinger(d time.Duration, done <-chan struct{}) {
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

func (c *conn) Ping(ctx context.Context) (err error) {
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

func (c *conn) ResetSession(ctx context.Context) error {
	c.session.Lock()
	defer c.session.Unlock()

	c.session.Reset()
	if c.session.IsBad() {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
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
		stmt, err = newStmt(c.session, qd.Query(), qd.IsBulk(), pr)
	done:
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.session.Kill()
		return nil, ctx.Err()
	case <-done:
		return stmt, err
	}
}

func (c *conn) Close() error {
	c.session.Lock()
	defer c.session.Unlock()

	close(c.closed) // signal connection close
	return c.session.Close()
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
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
		return tx, err
	}
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
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

	sqltrace.Traceln(query)

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

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, err error) {
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

	sqltrace.Traceln(query)

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

// CheckNamedValue implements NamedValueChecker interface.
// - called by sql driver for ExecContext and QueryContext
// - no check needs to be performed as ExecContext and QueryContext provided
//   with parameters will force the 'prepare way' (driver.ErrSkip)
// - Anyway, CheckNamedValue must be implemented to avoid default sql driver checks
//   which would fail for custom arg types like Lob
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	return nil
}

//transaction

//  check if tx implements all required interfaces
var (
	_ driver.Tx = (*tx)(nil)
)

type tx struct {
	session *p.Session
}

func newTx(session *p.Session) *tx {
	return &tx{
		session: session,
	}
}

func (t *tx) Commit() error {
	t.session.Lock()
	defer t.session.Unlock()

	if t.session.IsBad() {
		return driver.ErrBadConn
	}

	return t.session.Commit()
}

func (t *tx) Rollback() error {
	t.session.Lock()
	defer t.session.Unlock()

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
	pr                  *p.PrepareResult
	session             *p.Session
	query               string
	bulk, flush         bool
	maxBulkNum, bulkNum int
	args                []driver.NamedValue
}

func newStmt(session *p.Session, query string, bulk bool, pr *p.PrepareResult) (*stmt, error) {
	return &stmt{session: session, query: query, pr: pr, bulk: bulk, maxBulkNum: session.MaxBulkNum()}, nil
}

func (s *stmt) Close() error {
	s.session.Lock()
	defer s.session.Unlock()

	if len(s.args) != 0 {
		sqltrace.Tracef("close: %s - not flushed records: %d)", s.query, len(s.args)/s.NumInput())
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

	sqltrace.Tracef("%s %v", s.query, args)

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
	s.session.Lock()
	defer s.session.Unlock()

	if s.session.IsBad() {
		return nil, driver.ErrBadConn
	}
	if s.session.InQuery() {
		return nil, ErrNestedQuery
	}

	sqltrace.Tracef("%s %v", s.query, args)

	numArg := len(args)
	var numExpected int
	if s.bulk && numArg == 0 { // ok - bulk control
		numExpected = 0
	} else {
		numExpected = s.pr.NumField()
	}
	if numArg != numExpected {
		return nil, fmt.Errorf("invalid number of arguments %d - %d expected", numArg, numExpected)
	}

	if numArg == 0 { // flush
		s.flush = true
	}
	defer func() { s.flush = false }()

	done := make(chan struct{})
	go func() {
		switch {
		case s.pr.IsProcedureCall():
			r, err = s.session.ExecCall(s.pr, args)
		case s.bulk:
			r, err = driver.ResultNoRows, nil

			if numArg != 0 { // add to argument buffer
				if s.args == nil {
					s.args = make([]driver.NamedValue, 0, DefaultBulkSize)
				}
				s.args = append(s.args, args...)
				s.bulkNum++
			}

			if s.bulkNum != 0 && (s.flush || s.bulkNum == s.maxBulkNum) { // flush
				r, err = s.session.Exec(s.pr, s.args)
				s.args = s.args[:0]
				s.bulkNum = 0
			}
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
