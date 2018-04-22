/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sync"
	"time"

	"github.com/SAP/go-hdb/driver/sqltrace"

	p "github.com/SAP/go-hdb/internal/protocol"
)

// DriverVersion is the version number of the hdb driver.
const DriverVersion = "0.11.2"

// DriverName is the driver name to use with sql.Open for hdb databases.
const DriverName = "hdb"

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
var ErrUnsupportedIsolationLevel = errors.New("Unsupported isolation level")

// ErrNestedTransactions is the error raised if a tranasction is created within a transaction as this is not supported by hdb.
var ErrNestedTransaction = errors.New("Nested transactions are not supported")

// needed for testing
const driverDataFormatVersion = 1

// queries
const (
	pingQuery          = "select 1 from dummy"
	isolationLevelStmt = "set transaction isolation level %s"
	accessModeStmt     = "set transaction %s"
)

var drv = &hdbDrv{}

func init() {
	sql.Register(DriverName, drv)
}

var reBulk = regexp.MustCompile("(?i)^(\\s)*(bulk +)(.*)")

func checkBulkInsert(sql string) (string, bool) {
	if reBulk.MatchString(sql) {
		return reBulk.ReplaceAllString(sql, "${3}"), true
	}
	return sql, false
}

var reCall = regexp.MustCompile("(?i)^(\\s)*(call +)(.*)")

func checkCallProcedure(sql string) bool {
	return reCall.MatchString(sql)
}

var errProcTableQuery = errors.New("Invalid procedure table query")

// driver

//  check if driver implements all required interfaces
var (
	_ driver.Driver = (*hdbDrv)(nil)
)

type hdbDrv struct{}

func (d *hdbDrv) Open(dsn string) (driver.Conn, error) {
	connector, err := NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// database connection

//  check if conn implements all required interfaces
var (
	_ driver.Conn          = (*conn)(nil)
	_ driver.Pinger        = (*conn)(nil)
	_ driver.ConnBeginTx   = (*conn)(nil)
	_ driver.ExecerContext = (*conn)(nil)
	//go 1.9 issue (ExecerContext is only called if Execer is implemented)
	_ driver.Execer         = (*conn)(nil)
	_ driver.QueryerContext = (*conn)(nil)
	//go 1.9 issue (QueryerContext is only called if Queryer is implemented)
	// QueryContext is needed for stored procedures with table output parameters.
	_ driver.Queryer           = (*conn)(nil)
	_ driver.NamedValueChecker = (*conn)(nil)
)

type conn struct {
	session *p.Session
}

func newConn(ctx context.Context, c *Connector) (driver.Conn, error) {
	session, err := p.NewSession(ctx, c)
	if err != nil {
		return nil, err
	}
	return &conn{session: session}, nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if c.session.IsBad() {
		return nil, driver.ErrBadConn
	}

	prepareQuery, bulkInsert := checkBulkInsert(query)

	qt, id, parameterFieldSet, resultFieldSet, err := c.session.Prepare(prepareQuery)
	if err != nil {
		return nil, err
	}

	if bulkInsert {
		return newBulkInsertStmt(c.session, prepareQuery, id, parameterFieldSet)
	}
	return newStmt(qt, c.session, prepareQuery, id, parameterFieldSet, resultFieldSet)
}

func (c *conn) Close() error {
	c.session.Close()
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	panic("deprecated")
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {

	if c.session.IsBad() {
		return nil, driver.ErrBadConn
	}

	if c.session.InTx() {
		return nil, ErrNestedTransaction
	}

	level, ok := isolationLevel[opts.Isolation]
	if !ok {
		return nil, ErrUnsupportedIsolationLevel
	}
	// set isolation level
	if _, err := c.ExecContext(ctx, fmt.Sprintf(isolationLevelStmt, level), nil); err != nil {
		return nil, err
	}
	// set access mode
	if _, err := c.ExecContext(ctx, fmt.Sprintf(accessModeStmt, readOnly[opts.ReadOnly]), nil); err != nil {
		return nil, err
	}

	c.session.SetInTx(true)
	return newTx(c.session), nil

}

// Exec implements the database/sql/driver/Execer interface.
// delete after go 1.9 compatibility is given up.
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	panic("deprecated")
}

// ExecContext implements the database/sql/driver/ExecerContext interface.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.session.IsBad() {
		return nil, driver.ErrBadConn
	}

	if len(args) != 0 {
		return nil, driver.ErrSkip //fast path not possible (prepare needed)
	}

	sqltrace.Traceln(query)

	return c.session.ExecDirect(query)
}

// bug?: check args is performed indepently of queryer raising ErrSkip or not
// - leads to different behavior to prepare - stmt - execute default logic
// - seems to be the same for Execer interface

// Queryer implements the database/sql/driver/Queryer interface.
// delete after go 1.9 compatibility is given up.
func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	panic("deprecated")
}

// QueryContext implements the database/sql/driver/QueryerContext interface.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.session.IsBad() {
		return nil, driver.ErrBadConn
	}

	if len(args) != 0 {
		return nil, driver.ErrSkip //fast path not possible (prepare needed)
	}

	// direct execution of call procedure
	// - returns no parameter metadata (sps 82) but only field values
	// --> let's take the 'prepare way' for stored procedures
	if checkCallProcedure(query) {
		return nil, driver.ErrSkip
	}

	sqltrace.Traceln(query)

	id, idx, ok := decodeTableQuery(query)
	if ok {
		r := procedureCallResultStore.get(id)
		if r == nil {
			return nil, fmt.Errorf("invalid procedure table query %s", query)
		}
		return r.tableRows(int(idx))
	}

	id, meta, values, attributes, err := c.session.QueryDirect(query)
	if err != nil {
		return nil, err
	}
	if id == 0 { // non select query
		return noResult, nil
	}
	return newQueryResult(c.session, id, meta, values, attributes)
}

func (c *conn) Ping(ctx context.Context) error {
	if c.session.IsBad() {
		return driver.ErrBadConn
	}
	_, err := c.QueryContext(ctx, pingQuery, nil)
	return err
}

// CheckNamedValue implements NamedValueChecker interface.
// implemented for conn:
// if querier or execer is called, sql checks parameters before in case of
// parameters the method can be 'skipped' and force the prepare path
// --> guarantee that a valid driver value is returned
// --> if not implemented, Lob need to have a pseudo Value method to return a valid driver value
func (s *conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case Lob, *Lob:
		nv.Value = nil
	}
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
	if t.session.IsBad() {
		return driver.ErrBadConn
	}

	return t.session.Commit()
}

func (t *tx) Rollback() error {
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
	qt             p.QueryType
	session        *p.Session
	query          string
	id             uint64
	prmFieldSet    *p.FieldSet
	resultFieldSet *p.FieldSet
}

func newStmt(qt p.QueryType, session *p.Session, query string, id uint64, prmFieldSet *p.FieldSet, resultFieldSet *p.FieldSet) (*stmt, error) {
	return &stmt{qt: qt, session: session, query: query, id: id, prmFieldSet: prmFieldSet, resultFieldSet: resultFieldSet}, nil
}

func (s *stmt) Close() error {
	return s.session.DropStatementID(s.id)
}

func (s *stmt) NumInput() int {
	return s.prmFieldSet.NumInputField()
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	panic("deprecated")
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.session.IsBad() {
		return nil, driver.ErrBadConn
	}

	numField := s.prmFieldSet.NumInputField()
	if len(args) != numField {
		return nil, fmt.Errorf("invalid number of arguments %d - %d expected", len(args), numField)
	}

	sqltrace.Tracef("%s %v", s.query, args)

	dargs := make([]driver.Value, len(args))
	for i, arg := range args {
		dargs[i] = arg.Value
	}

	return s.session.Exec(s.id, s.prmFieldSet, dargs)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("deprecated")
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {

	if s.session.IsBad() {
		return nil, driver.ErrBadConn
	}

	switch s.qt {
	default:
		rows, err := s.defaultQuery(args)
		return rows, err
	case p.QtProcedureCall:
		rows, err := s.procedureCall(args)
		return rows, err
	}
}

func (s *stmt) defaultQuery(args []driver.NamedValue) (driver.Rows, error) {

	sqltrace.Tracef("%s %v", s.query, args)

	dargs := make([]driver.Value, len(args))
	for i, arg := range args {
		dargs[i] = arg.Value
	}

	rid, values, attributes, err := s.session.Query(s.id, s.prmFieldSet, s.resultFieldSet, dargs)
	if err != nil {
		return nil, err
	}
	if rid == 0 { // non select query
		return noResult, nil
	}
	return newQueryResult(s.session, rid, s.resultFieldSet, values, attributes)
}

func (s *stmt) procedureCall(args []driver.NamedValue) (driver.Rows, error) {

	sqltrace.Tracef("%s %v", s.query, args)

	dargs := make([]driver.Value, len(args))
	for i, arg := range args {
		dargs[i] = arg.Value
	}

	fieldValues, tableResults, err := s.session.Call(s.id, s.prmFieldSet, dargs)
	if err != nil {
		return nil, err
	}

	return newProcedureCallResult(s.session, s.prmFieldSet, fieldValues, tableResults)
}

// Deprecated: see NamedValueChecker.
//func (s *stmt) ColumnConverter(idx int) driver.ValueConverter {
//}

// CheckNamedValue implements NamedValueChecker interface.
func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error {
	return checkNamedValue(s.prmFieldSet, nv)
}

// bulk insert statement

//  check if bulkInsertStmt implements all required interfaces
var (
	_ driver.Stmt              = (*bulkInsertStmt)(nil)
	_ driver.StmtExecContext   = (*bulkInsertStmt)(nil)
	_ driver.StmtQueryContext  = (*bulkInsertStmt)(nil)
	_ driver.NamedValueChecker = (*bulkInsertStmt)(nil)
)

type bulkInsertStmt struct {
	session     *p.Session
	query       string
	id          uint64
	prmFieldSet *p.FieldSet
	numArg      int
	args        []driver.Value
}

func newBulkInsertStmt(session *p.Session, query string, id uint64, prmFieldSet *p.FieldSet) (*bulkInsertStmt, error) {
	return &bulkInsertStmt{session: session, query: query, id: id, prmFieldSet: prmFieldSet, args: make([]driver.Value, 0)}, nil
}

func (s *bulkInsertStmt) Close() error {
	return s.session.DropStatementID(s.id)
}

func (s *bulkInsertStmt) NumInput() int {
	return -1
}

func (s *bulkInsertStmt) Exec(args []driver.Value) (driver.Result, error) {
	panic("deprecated")
}

func (s *bulkInsertStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {

	if s.session.IsBad() {
		return nil, driver.ErrBadConn
	}

	sqltrace.Tracef("%s %v", s.query, args)

	if args == nil || len(args) == 0 {
		return s.execFlush()
	}

	dargs := make([]driver.Value, len(args))
	for i, arg := range args {
		dargs[i] = arg.Value
	}

	return s.execBuffer(dargs)
}

func (s *bulkInsertStmt) execFlush() (driver.Result, error) {

	if s.numArg == 0 {
		return driver.ResultNoRows, nil
	}

	sqltrace.Traceln("execFlush")

	result, err := s.session.Exec(s.id, s.prmFieldSet, s.args)
	s.args = s.args[:0]
	s.numArg = 0
	return result, err
}

func (s *bulkInsertStmt) execBuffer(args []driver.Value) (driver.Result, error) {

	numField := s.prmFieldSet.NumInputField()
	if len(args) != numField {
		return nil, fmt.Errorf("invalid number of arguments %d - %d expected", len(args), numField)
	}

	var result driver.Result = driver.ResultNoRows
	var err error

	if s.numArg == maxSmallint { // TODO: check why bigArgument count does not work
		result, err = s.execFlush()
	}

	s.args = append(s.args, args...)
	s.numArg++

	return result, err
}

func (s *bulkInsertStmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("deprecated")
}

func (s *bulkInsertStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return nil, fmt.Errorf("query not allowed in context of bulk insert statement %s", s.query)
}

// Deprecated: see NamedValueChecker.
//func (s *bulkInsertStmt) ColumnConverter(idx int) driver.ValueConverter {
//}

// CheckNamedValue implements NamedValueChecker interface.
func (s *bulkInsertStmt) CheckNamedValue(nv *driver.NamedValue) error {
	return checkNamedValue(s.prmFieldSet, nv)
}

// driver.Rows drop-in replacement if driver Query or QueryRow is used for statements that doesn't return rows
var noColumns = []string{}
var noResult = new(noResultType)

//  check if noResultType implements all required interfaces
var (
	_ driver.Rows = (*noResultType)(nil)
)

type noResultType struct{}

func (r *noResultType) Columns() []string              { return noColumns }
func (r *noResultType) Close() error                   { return nil }
func (r *noResultType) Next(dest []driver.Value) error { return io.EOF }

// rows
type rows struct {
}

// query result

//  check if queryResult implements all required interfaces
var (
	_ driver.Rows                           = (*queryResult)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*queryResult)(nil) // go 1.8
	_ driver.RowsColumnTypeLength           = (*queryResult)(nil) // go 1.8
	_ driver.RowsColumnTypeNullable         = (*queryResult)(nil) // go 1.8
	_ driver.RowsColumnTypePrecisionScale   = (*queryResult)(nil) // go 1.8
	_ driver.RowsColumnTypeScanType         = (*queryResult)(nil) // go 1.8
)

type queryResult struct {
	session     *p.Session
	id          uint64
	fieldSet    *p.FieldSet
	fieldValues *p.FieldValues
	pos         int
	attrs       p.PartAttributes
	columns     []string
	lastErr     error
}

func newQueryResult(session *p.Session, id uint64, fieldSet *p.FieldSet, fieldValues *p.FieldValues, attrs p.PartAttributes) (driver.Rows, error) {
	columns := make([]string, fieldSet.NumOutputField())
	if err := fieldSet.OutputNames(columns); err != nil {
		return nil, err
	}

	return &queryResult{
		session:     session,
		id:          id,
		fieldSet:    fieldSet,
		fieldValues: fieldValues,
		attrs:       attrs,
		columns:     columns,
	}, nil
}

func (r *queryResult) Columns() []string {
	return r.columns
}

func (r *queryResult) Close() error {
	// if lastError is set, attrs are nil
	if r.lastErr != nil {
		return r.lastErr
	}

	if !r.attrs.ResultsetClosed() {
		return r.session.CloseResultsetID(r.id)
	}
	return nil
}

func (r *queryResult) Next(dest []driver.Value) error {
	if r.session.IsBad() {
		return driver.ErrBadConn
	}

	if r.pos >= r.fieldValues.NumRow() {
		if r.attrs.LastPacket() {
			return io.EOF
		}

		var err error

		if r.fieldValues, r.attrs, err = r.session.FetchNext(r.id, r.fieldSet); err != nil {
			r.lastErr = err //fieldValues and attrs are nil
			return err
		}

		if r.attrs.NoRows() {
			return io.EOF
		}

		r.pos = 0

	}

	r.fieldValues.Row(r.pos, dest)
	r.pos++

	return nil
}

func (r *queryResult) ColumnTypeDatabaseTypeName(idx int) string {
	return r.fieldSet.Field(idx).TypeCode().TypeName()
}

func (r *queryResult) ColumnTypeLength(idx int) (int64, bool) {
	return r.fieldSet.Field(idx).TypeLength()
}

func (r *queryResult) ColumnTypePrecisionScale(idx int) (int64, int64, bool) {
	return r.fieldSet.Field(idx).TypePrecisionScale()
}

func (r *queryResult) ColumnTypeNullable(idx int) (bool, bool) {
	return r.fieldSet.Field(idx).Nullable(), true
}

var (
	scanTypeUnknown  = reflect.TypeOf(new(interface{})).Elem()
	scanTypeTinyint  = reflect.TypeOf(uint8(0))
	scanTypeSmallint = reflect.TypeOf(int16(0))
	scanTypeInteger  = reflect.TypeOf(int32(0))
	scanTypeBigint   = reflect.TypeOf(int64(0))
	scanTypeReal     = reflect.TypeOf(float32(0.0))
	scanTypeDouble   = reflect.TypeOf(float64(0.0))
	scanTypeTime     = reflect.TypeOf(time.Time{})
	scanTypeString   = reflect.TypeOf(string(""))
	scanTypeBytes    = reflect.TypeOf([]byte{})
	scanTypeDecimal  = reflect.TypeOf(Decimal{})
	scanTypeLob      = reflect.TypeOf(Lob{})
)

func (r *queryResult) ColumnTypeScanType(idx int) reflect.Type {
	switch r.fieldSet.Field(idx).TypeCode().DataType() {
	default:
		return scanTypeUnknown
	case p.DtTinyint:
		return scanTypeTinyint
	case p.DtSmallint:
		return scanTypeSmallint
	case p.DtInteger:
		return scanTypeInteger
	case p.DtBigint:
		return scanTypeBigint
	case p.DtReal:
		return scanTypeReal
	case p.DtDouble:
		return scanTypeDouble
	case p.DtTime:
		return scanTypeTime
	case p.DtDecimal:
		return scanTypeDecimal
	case p.DtString:
		return scanTypeString
	case p.DtBytes:
		return scanTypeBytes
	case p.DtLob:
		return scanTypeLob
	}
}

//call result store
type callResultStore struct {
	mu    sync.RWMutex
	store map[uint64]*procedureCallResult
	cnt   uint64
	free  []uint64
}

func (s *callResultStore) get(k uint64) *procedureCallResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if r, ok := s.store[k]; ok {
		return r
	}
	return nil
}

func (s *callResultStore) add(v *procedureCallResult) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var k uint64

	if s.free == nil || len(s.free) == 0 {
		s.cnt++
		k = s.cnt
	} else {
		size := len(s.free)
		k = s.free[size-1]
		s.free = s.free[:size-1]
	}

	if s.store == nil {
		s.store = make(map[uint64]*procedureCallResult)
	}

	s.store[k] = v

	return k
}

func (s *callResultStore) del(k uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.store, k)

	if s.free == nil {
		s.free = []uint64{k}
	} else {
		s.free = append(s.free, k)
	}
}

var procedureCallResultStore = new(callResultStore)

//procedure call result

//  check if procedureCallResult implements all required interfaces
var _ driver.Rows = (*procedureCallResult)(nil)

type procedureCallResult struct {
	id          uint64
	session     *p.Session
	fieldSet    *p.FieldSet
	fieldValues *p.FieldValues
	_tableRows  []driver.Rows
	columns     []string
	eof         error
}

func newProcedureCallResult(session *p.Session, fieldSet *p.FieldSet, fieldValues *p.FieldValues, tableResults []*p.TableResult) (driver.Rows, error) {

	fieldIdx := fieldSet.NumOutputField()
	columns := make([]string, fieldIdx+len(tableResults))
	if err := fieldSet.OutputNames(columns); err != nil {
		return nil, err
	}

	tableRows := make([]driver.Rows, len(tableResults))
	for i, tableResult := range tableResults {
		var err error

		if tableRows[i], err = newQueryResult(session, tableResult.ID(), tableResult.FieldSet(), tableResult.FieldValues(), tableResult.Attrs()); err != nil {
			return nil, err
		}

		columns[fieldIdx] = fmt.Sprintf("table %d", i)

		fieldIdx++

	}

	result := &procedureCallResult{
		session:     session,
		fieldSet:    fieldSet,
		fieldValues: fieldValues,
		_tableRows:  tableRows,
		columns:     columns,
	}
	id := procedureCallResultStore.add(result)
	result.id = id
	return result, nil
}

func (r *procedureCallResult) Columns() []string {
	return r.columns
}

func (r *procedureCallResult) Close() error {
	procedureCallResultStore.del(r.id)
	return nil
}

func (r *procedureCallResult) Next(dest []driver.Value) error {
	if r.session.IsBad() {
		return driver.ErrBadConn
	}

	if r.eof != nil {
		return r.eof
	}

	if r.fieldValues.NumRow() == 0 && len(r._tableRows) == 0 {
		r.eof = io.EOF
		return r.eof
	}

	if r.fieldValues.NumRow() != 0 {
		r.fieldValues.Row(0, dest)
	}

	i := r.fieldSet.NumOutputField()
	for j := range r._tableRows {
		dest[i] = encodeTableQuery(r.id, uint64(j))
		i++
	}

	r.eof = io.EOF
	return nil
}

func (r *procedureCallResult) tableRows(idx int) (driver.Rows, error) {
	if idx >= len(r._tableRows) {
		return nil, fmt.Errorf("table row index %d exceeds maximun %d", idx, len(r._tableRows)-1)
	}
	return r._tableRows[idx], nil
}

// helper
const tableQueryPrefix = "@tq"

func encodeTableQuery(id, idx uint64) string {
	start := len(tableQueryPrefix)
	b := make([]byte, start+8+8)
	copy(b, tableQueryPrefix)
	binary.LittleEndian.PutUint64(b[start:start+8], id)
	binary.LittleEndian.PutUint64(b[start+8:start+8+8], idx)
	return string(b)
}

func decodeTableQuery(query string) (uint64, uint64, bool) {
	size := len(query)
	start := len(tableQueryPrefix)
	if size != start+8+8 {
		return 0, 0, false
	}
	if query[:start] != tableQueryPrefix {
		return 0, 0, false
	}
	id := binary.LittleEndian.Uint64([]byte(query[start : start+8]))
	idx := binary.LittleEndian.Uint64([]byte(query[start+8 : start+8+8]))
	return id, idx, true
}
