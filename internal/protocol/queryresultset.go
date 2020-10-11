// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"database/sql/driver"
	"io"
	"sync"
)

/*
Definition of queryResultSet in protocol layer:
- queryResultSet is sql.Rows
- sql.Rows can be used as datatype for scan
- used in go-hdb for call table output parameters
*/

// NoResult is the driver.Rows drop-in replacement if driver Query or QueryRow is used for statements that do not return rows.
var noResult = new(noResultType)

//  check if noResultType implements all required interfaces
var (
	_ driver.Rows = (*noResultType)(nil)
)

var noColumns = []string{}

type noResultType struct{}

func (r *noResultType) Columns() []string              { return noColumns }
func (r *noResultType) Close() error                   { return nil }
func (r *noResultType) Next(dest []driver.Value) error { return io.EOF }

// query result set

//  check if queryResult implements all required interfaces
var (
	_ driver.Rows                           = (*queryResultSet)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsColumnTypeLength           = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsColumnTypeNullable         = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsColumnTypePrecisionScale   = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsColumnTypeScanType         = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsNextResultSet              = (*queryResultSet)(nil) // go 1.8
)

type queryResultSet struct {
	session *Session
	rowsResult

	pos     int
	lastErr error
}

func newQueryResultSet(session *Session, rr rowsResult) *queryResultSet {
	return &queryResultSet{session: session, rowsResult: rr}
}

func (r *queryResultSet) Close() error {

	// TODO replace lock with hold lock by conn
	//r.session.Lock()
	//defer r.session.Unlock()

	//TODO replace in query with hold connection lock
	defer r.session.SetInQuery(false)

	// if lastError is set, attrs are nil
	if r.lastErr != nil {
		return r.lastErr
	}

	if !r.rowsResult.closed() {
		return r.session.CloseResultsetID(r.rowsResult.rsID())
	}
	return nil
}

func (r *queryResultSet) Next(dest []driver.Value) error {

	// TODO replace lock with hold lock by conn

	//r.session.Lock()
	//defer r.session.Unlock()

	// TODO replace isBad with new connection call
	//if r.session.IsBad() {
	//	return driver.ErrBadConn
	//}

	if r.pos >= r.rowsResult.numRow() {
		if r.rowsResult.lastPacket() {
			return io.EOF
		}
		if err := r.session.fetchNext(r.rowsResult); err != nil {
			r.lastErr = err //fieldValues and attrs are nil
			return err
		}
		if r.rowsResult.numRow() == 0 {
			return io.EOF
		}
		r.pos = 0
	}

	r.rowsResult.copyRow(r.pos, dest)
	r.pos++

	// TODO eliminate
	for _, v := range dest {
		if v, ok := v.(sessionSetter); ok {
			v.setSession(r.session)
		}
	}
	return nil
}

// QrsCache is a query result cache supporting reading
// procedure (call) table parameter via separate query (legacy mode).
var QrsCache = newQueryResultSetCache()

type queryResultSetCache struct {
	cache map[uint64]*queryResultSet
	mu    sync.RWMutex
}

func newQueryResultSetCache() *queryResultSetCache {
	return &queryResultSetCache{
		cache: map[uint64]*queryResultSet{},
	}
}

func (c *queryResultSetCache) set(id uint64, qrs *queryResultSet) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[id] = qrs
	return id
}

func (c *queryResultSetCache) Get(id uint64) (*queryResultSet, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	qrs, ok := c.cache[id]
	return qrs, ok
}

func (c *queryResultSetCache) Cleanup(session *Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, qrs := range c.cache {
		if qrs.session == session {
			delete(c.cache, id)
		}
	}
}
