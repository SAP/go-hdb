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

package protocol

import (
	"bufio"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"golang.org/x/text/transform"

	"github.com/SAP/go-hdb/internal/unicode"
	"github.com/SAP/go-hdb/internal/unicode/cesu8"
)

//padding
const padding = 8

func padBytes(size int) int {
	if r := size % padding; r != 0 {
		return padding - r
	}
	return 0
}

// sesion handling
const (
	sesRecording = "rec"
	sesReplay    = "rpl"
)

type sessionStatus interface {
	isBad() bool
}

type sessionConn interface {
	io.ReadWriteCloser
	sessionStatus
}

func newSessionConn(ctx context.Context, addr string, timeoutSec int, tcpKeepAlive time.Duration, tlsConfig *tls.Config) (sessionConn, error) {
	// session recording
	if wr, ok := ctx.Value(sesRecording).(io.Writer); ok {
		conn, err := newDbConn(ctx, addr, timeoutSec, tcpKeepAlive, tlsConfig)
		if err != nil {
			return nil, err
		}
		return proxyConn{
			Reader:        io.TeeReader(conn, wr), // teereader: write database replies to writer
			Writer:        conn,
			Closer:        conn,
			sessionStatus: conn,
		}, nil
	}
	// session replay
	if rd, ok := ctx.Value(sesReplay).(io.Reader); ok {
		nwc := nullWriterCloser{}
		return proxyConn{
			Reader:        rd,
			Writer:        nwc,
			Closer:        nwc,
			sessionStatus: nwc,
		}, nil
	}
	return newDbConn(ctx, addr, timeoutSec, tcpKeepAlive, tlsConfig)
}

type nullWriterCloser struct{}

func (n nullWriterCloser) Write(p []byte) (int, error) { return len(p), nil }
func (n nullWriterCloser) Close() error                { return nil }
func (n nullWriterCloser) isBad() bool                 { return false }

// proxy connection
type proxyConn struct {
	io.Reader
	io.Writer
	io.Closer
	sessionStatus
}

// dbConn wraps the database tcp connection. It sets timeouts and handles driver ErrBadConn behavior.
type dbConn struct {
	addr      string
	timeout   time.Duration
	conn      net.Conn
	lastError error // error bad connection
}

func newDbConn(ctx context.Context, addr string, timeoutSec int, tcpKeepAlive time.Duration, tlsConfig *tls.Config) (*dbConn, error) {
	timeout := time.Duration(timeoutSec) * time.Second
	dialer := net.Dialer{Timeout: timeout, KeepAlive: tcpKeepAlive}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	// is TLS connection requested?
	if tlsConfig != nil {
		conn = tls.Client(conn, tlsConfig)
	}

	return &dbConn{addr: addr, timeout: timeout, conn: conn}, nil
}

func (c *dbConn) isBad() bool { return c.lastError != nil }

func (c *dbConn) Close() error {
	return c.conn.Close()
}

// Read implements the io.Reader interface.
func (c *dbConn) Read(b []byte) (int, error) {
	//set timeout
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	n, err := c.conn.Read(b)
	if err != nil {
		plog.Printf("Connection read error local address %s remote address %s: %s", c.conn.LocalAddr(), c.conn.RemoteAddr(), err)
		c.lastError = err
		return n, driver.ErrBadConn
	}
	return n, nil
}

// Write implements the io.Writer interface.
func (c *dbConn) Write(b []byte) (int, error) {
	//set timeout
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	n, err := c.conn.Write(b)
	if err != nil {
		plog.Printf("Connection write error local address %s remote address %s: %s", c.conn.LocalAddr(), c.conn.RemoteAddr(), err)
		c.lastError = err
		return n, driver.ErrBadConn
	}
	return n, nil
}

// SessionConfig represents the session relevant driver connector options.
type SessionConfig interface {
	Host() string
	Username() string
	Password() string
	Locale() string
	BufferSize() int
	FetchSize() int
	BulkSize() int
	LobChunkSize() int32
	Timeout() int
	TCPKeepAlive() time.Duration
	Dfv() int
	TLSConfig() *tls.Config
	Legacy() bool
}

const dfvLevel1 = 1

const defaultSessionID = -1

// Session represents a HDB session.
type Session struct {
	cfg SessionConfig

	sessionID int64

	conn sessionConn
	rd   *bufio.Reader
	wr   *bufio.Writer

	pr *protocolReader
	pw *protocolWriter

	//serialize write request - read reply
	//supports calling session methods in go routines (driver methods with context cancellation)
	mu sync.Mutex

	inTx bool // in transaction

}

// NewSession creates a new database session.
func NewSession(ctx context.Context, cfg SessionConfig) (*Session, error) {
	var conn sessionConn

	conn, err := newSessionConn(ctx, cfg.Host(), cfg.Timeout(), cfg.TCPKeepAlive(), cfg.TLSConfig())
	if err != nil {
		return nil, err
	}

	var bufRd *bufio.Reader
	var bufWr *bufio.Writer

	bufferSize := cfg.BufferSize()
	if bufferSize > 0 {
		bufRd = bufio.NewReaderSize(conn, bufferSize)
		bufWr = bufio.NewWriterSize(conn, bufferSize)
	} else {
		bufRd = bufio.NewReader(conn)
		bufWr = bufio.NewWriter(conn)
	}

	pw := newProtocolWriter(bufWr) // write upstream
	if err := pw.writeProlog(); err != nil {
		return nil, err
	}

	pr := newProtocolReader(false, bufRd) // read downstream
	if err := pr.readProlog(); err != nil {
		return nil, err
	}

	s := &Session{
		cfg:       cfg,
		sessionID: defaultSessionID,
		conn:      conn,
		rd:        bufRd,
		wr:        bufWr,
		pr:        pr,
		pw:        pw,
	}
	return s, s.authenticate()
}

// Reset resets the session.
func (s *Session) Reset() {
	QrsCache.cleanup(s)
}

// Close closes the session.
func (s *Session) Close() error {
	QrsCache.cleanup(s)
	return s.conn.Close()
}

// InTx indicates, if the session is in transaction mode.
func (s *Session) InTx() bool {
	return s.inTx
}

// SetInTx sets session in transaction mode.
func (s *Session) SetInTx(v bool) {
	s.inTx = v
}

// IsBad indicates, that the session is in bad state.
func (s *Session) IsBad() bool {
	return s.conn.isBad()
}

// MaxBulkNum returns the maximal number of bulk calls before auto flush.
func (s *Session) MaxBulkNum() int {
	maxBulkNum := s.cfg.BulkSize()
	if maxBulkNum > maxPartNum {
		return maxPartNum // max number of parameters (see parameter header)
	}
	return maxBulkNum
}

func (s *Session) authenticate() error {
	authStepper := newAuth(s.cfg.Username(), s.cfg.Password())
	if err := s.authenticateMethod(authStepper); err != nil {
		return err
	}
	if s.sessionID <= 0 {
		return fmt.Errorf("invalid session id %d", s.sessionID)
	}
	return nil
}

func (s *Session) connectOptions() connectOptions {
	co := connectOptions{}
	co.set(coDistributionProtocolVersion, optBooleanType(false))
	co.set(coSelectForUpdateSupported, optBooleanType(false))
	co.set(coSplitBatchCommands, optBooleanType(true))
	co.set(coDataFormatVersion2, optIntType(s.cfg.Dfv()))
	co.set(coCompleteArrayExecution, optBooleanType(true))
	if s.cfg.Locale() != "" {
		co.set(coClientLocale, optStringType(s.cfg.Locale()))
	}
	co.set(coClientDistributionMode, cdmOff)
	// co.set(coImplicitLobStreaming, optBooleanType(true))
	return co
}

func (s *Session) authenticateMethod(stepper authStepper) error {
	var auth partReadWriter
	var err error

	if auth, err = stepper.next(); err != nil {
		return err
	}
	if err := s.pw.write(s.sessionID, mtAuthenticate, false, auth); err != nil {
		return err
	}

	if auth, err = stepper.next(); err != nil {
		return err
	}
	if err := s.pr.iterateParts(func(ph *partHeader) {
		if ph.partKind == pkAuthentication {
			s.pr.read(auth)
		}
	}); err != nil {
		return err
	}

	if auth, err = stepper.next(); err != nil {
		return err
	}
	id := newClientID()
	co := s.connectOptions()
	if err := s.pw.write(s.sessionID, mtConnect, false, auth, id, co); err != nil {
		return err
	}

	if auth, err = stepper.next(); err != nil {
		return err
	}
	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkAuthentication:
			s.pr.read(auth)
		case pkConnectOptions:
			s.pr.read(&co)
			// set data format version
			// TODO generalize for sniffer
			s.pr.setDfv(int(co[coDataFormatVersion2].(optIntType)))
		}
	}); err != nil {
		return err
	}
	s.sessionID = s.pr.sessionID()
	return nil
}

// QueryDirect executes a query without query parameters.
func (s *Session) QueryDirect(query string) (driver.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// allow e.g inserts as query -> handle commit like in ExecDirect
	if err := s.pw.write(s.sessionID, mtExecuteDirect, !s.inTx, command(query)); err != nil {
		return nil, err
	}

	qr := &queryResult{}
	meta := &resultMetadata{}
	resSet := &resultset{}

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkResultMetadata:
			s.pr.read(meta)
			qr.fields = meta.resultFields
		case pkResultsetID:
			s.pr.read((*resultsetID)(&qr._rsID))
		case pkResultset:
			resSet.resultFields = qr.fields
			s.pr.read(resSet)
			qr.fieldValues = resSet.fieldValues
			qr.attributes = ph.partAttributes
		}
	}); err != nil {
		return nil, err
	}
	if qr._rsID == 0 { // non select query
		return noResult, nil
	}
	return newQueryResultSet(s, qr), nil
}

// ExecDirect executes a sql statement without statement parameters.
func (s *Session) ExecDirect(query string) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.pw.write(s.sessionID, mtExecuteDirect, !s.inTx, command(query)); err != nil {
		return nil, err
	}

	rows := &rowsAffected{}
	var numRow int64
	if err := s.pr.iterateParts(func(ph *partHeader) {
		if ph.partKind == pkRowsAffected {
			s.pr.read(rows)
			numRow = rows.total()
		}
	}); err != nil {
		return nil, err
	}
	if s.pr.functionCode() == fcDDL {
		return driver.ResultNoRows, nil
	}
	return driver.RowsAffected(numRow), nil
}

// Prepare prepares a sql statement.
func (s *Session) Prepare(query string) (*PrepareResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.pw.write(s.sessionID, mtPrepare, false, command(query)); err != nil {
		return nil, err
	}

	pr := &PrepareResult{}
	resMeta := &resultMetadata{}
	prmMeta := &parameterMetadata{}

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkStatementID:
			s.pr.read((*statementID)(&pr.stmtID))
		case pkResultMetadata:
			s.pr.read(resMeta)
			pr.resultFields = resMeta.resultFields
		case pkParameterMetadata:
			s.pr.read(prmMeta)
			pr.prmFields = prmMeta.parameterFields
		}
	}); err != nil {
		return nil, err
	}
	pr.fc = s.pr.functionCode()
	return pr, nil
}

// Exec executes a sql statement.
func (s *Session) Exec(pr *PrepareResult, args []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.pw.write(s.sessionID, mtExecute, !s.inTx, statementID(pr.stmtID), newInputParameters(pr.prmFields, args)); err != nil {
		return nil, err
	}

	rows := &rowsAffected{}
	var ids []locatorID
	lobReply := &writeLobReply{}
	var numRow int64

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkRowsAffected:
			s.pr.read(rows)
			numRow = rows.total()
		case pkWriteLobReply:
			s.pr.read(lobReply)
			ids = lobReply.ids
		}
	}); err != nil {
		return nil, err
	}
	fc := s.pr.functionCode()

	if len(ids) != 0 {
		/*
			writeLobParameters:
			- chunkReaders
			- nil (no callResult, exec does not have output parameters)
		*/
		if err := s.encodeLobs(nil, ids, pr.prmFields, args); err != nil {
			return nil, err
		}
	}

	if fc == fcDDL {
		return driver.ResultNoRows, nil
	}
	return driver.RowsAffected(numRow), nil
}

// QueryCall executes a stored procecure (by Query).
func (s *Session) QueryCall(pr *PrepareResult, args []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	/*
		only in args
		invariant: #inPrmFields == #args
	*/
	var inPrmFields, outPrmFields []*parameterField
	for _, f := range pr.prmFields {
		if f.In() {
			inPrmFields = append(inPrmFields, f)
		}
		if f.Out() {
			outPrmFields = append(outPrmFields, f)
		}
	}

	if err := s.pw.write(s.sessionID, mtExecute, false, statementID(pr.stmtID), newInputParameters(inPrmFields, args)); err != nil {
		return nil, err
	}

	/*
		call without lob input parameters:
		--> callResult output parameter values are set after read call
		call with lob input parameters:
		--> callResult output parameter values are set after last lob input write
	*/

	cr, ids, err := s.readCall(outPrmFields)
	if err != nil {
		return nil, err
	}

	if len(ids) != 0 {
		/*
			writeLobParameters:
			- chunkReaders
			- cr (callResult output parameters are set after all lob input parameters are written)
		*/
		if err := s.encodeLobs(cr, ids, inPrmFields, args); err != nil {
			return nil, err
		}
	}

	// legacy mode?
	if s.cfg.Legacy() {
		cr.appendTableRefFields() // TODO review
		for _, qr := range cr.qrs {
			// add to cache
			QrsCache.set(qr._rsID, newQueryResultSet(s, qr))
		}
	} else {
		cr.appendTableRowsFields(s)
	}
	return newQueryResultSet(s, cr), nil
}

// ExecCall executes a stored procecure (by Exec).
func (s *Session) ExecCall(pr *PrepareResult, args []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	/*
		in,- and output args
		invariant: #prmFields == #args
	*/
	var inPrmFields, outPrmFields []*parameterField
	var inArgs, outArgs []driver.NamedValue
	for i, f := range pr.prmFields {
		if f.In() {
			inPrmFields = append(inPrmFields, f)
			inArgs = append(inArgs, args[i])
		}
		if f.Out() {
			outPrmFields = append(outPrmFields, f)
			outArgs = append(outArgs, args[i])
		}
	}

	if err := s.pw.write(s.sessionID, mtExecute, false, statementID(pr.stmtID), newInputParameters(inPrmFields, inArgs)); err != nil {
		return nil, err
	}

	/*
		call without lob input parameters:
		--> callResult output parameter values are set after read call
		call with lob input parameters:
		--> callResult output parameter values are set after last lob input write
	*/

	cr, ids, err := s.readCall(outPrmFields)
	if err != nil {
		return nil, err
	}

	if len(ids) != 0 {
		/*
			writeLobParameters:
			- chunkReaders
			- cr (callResult output parameters are set after all lob input parameters are written)
		*/
		if err := s.encodeLobs(cr, ids, inPrmFields, inArgs); err != nil {
			return nil, err
		}
	}

	// TODO release v1.0.0 - assign output parameters
	return nil, fmt.Errorf("not implemented yet")
	//return driver.ResultNoRows, nil
}

func (s *Session) readCall(outputFields []*parameterField) (*callResult, []locatorID, error) {
	cr := &callResult{outputFields: outputFields}

	//var qrs []*QueryResult
	var qr *queryResult
	var ids []locatorID
	outPrms := &outputParameters{}
	meta := &resultMetadata{}
	resSet := &resultset{}
	lobReply := &writeLobReply{}

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkOutputParameters:
			outPrms.outputFields = cr.outputFields
			s.pr.read(outPrms)
			cr.fieldValues = outPrms.fieldValues
		case pkResultMetadata:
			/*
				procedure call with table parameters does return metadata for each table
				sequence: metadata, resultsetID, resultset
				but:
				- resultset might not be provided for all tables
				- so, 'additional' query result is detected by new metadata part
			*/
			qr = &queryResult{}
			cr.qrs = append(cr.qrs, qr)
			s.pr.read(meta)
			qr.fields = meta.resultFields
		case pkResultset:
			resSet.resultFields = qr.fields
			s.pr.read(resSet)
			qr.fieldValues = resSet.fieldValues
			qr.attributes = ph.partAttributes
		case pkResultsetID:
			s.pr.read((*resultsetID)(&qr._rsID))
		case pkWriteLobReply:
			s.pr.read(lobReply)
			ids = lobReply.ids
		}
	}); err != nil {
		return nil, nil, err
	}

	// init fieldValues
	if cr.fieldValues == nil {
		cr.fieldValues = newFieldValues(0)
	}
	for _, qr := range cr.qrs {
		if qr.fieldValues == nil {
			qr.fieldValues = newFieldValues(0)
		}
	}
	return cr, ids, nil
}

// Query executes a query.
func (s *Session) Query(pr *PrepareResult, args []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// allow e.g inserts as query -> handle commit like in exec
	if err := s.pw.write(s.sessionID, mtExecute, !s.inTx, statementID(pr.stmtID), newInputParameters(pr.prmFields, args)); err != nil {
		return nil, err
	}

	qr := &queryResult{fields: pr.resultFields}
	resSet := &resultset{}

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkResultsetID:
			s.pr.read((*resultsetID)(&qr._rsID))
		case pkResultset:
			resSet.resultFields = qr.fields
			s.pr.read(resSet)
			qr.fieldValues = resSet.fieldValues
			qr.attributes = ph.partAttributes
		}
	}); err != nil {
		return nil, err
	}
	if qr._rsID == 0 { // non select query
		return noResult, nil
	}
	return newQueryResultSet(s, qr), nil
}

// FetchNext fetches next chunk in query result set.
func (s *Session) fetchNext(rr rowsResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	qr, err := rr.queryResult()
	if err != nil {
		return err
	}
	if err := s.pw.write(s.sessionID, mtFetchNext, false, resultsetID(qr._rsID), fetchsize(s.cfg.FetchSize())); err != nil {
		return err
	}

	resSet := &resultset{}

	return s.pr.iterateParts(func(ph *partHeader) {
		if ph.partKind == pkResultset {
			resSet.resultFields = qr.fields
			s.pr.read(resSet)
			qr.fieldValues = resSet.fieldValues
			qr.attributes = ph.partAttributes
		}
	})
}

// DropStatementID releases the hdb statement handle.
func (s *Session) DropStatementID(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.pw.write(s.sessionID, mtDropStatementID, false, statementID(id)); err != nil {
		return err
	}
	return s.pr.readSkip()
}

// CloseResultsetID releases the hdb resultset handle.
func (s *Session) CloseResultsetID(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.pw.write(s.sessionID, mtCloseResultset, false, resultsetID(id)); err != nil {
		return err
	}
	return s.pr.readSkip()
}

// Commit executes a database commit.
func (s *Session) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.pw.write(s.sessionID, mtCommit, false); err != nil {
		return err
	}
	if err := s.pr.readSkip(); err != nil {
		return err
	}
	s.inTx = false
	return nil
}

// Rollback executes a database rollback.
func (s *Session) Rollback() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.pw.write(s.sessionID, mtRollback, false); err != nil {
		return err
	}
	if err := s.pr.readSkip(); err != nil {
		return err
	}
	s.inTx = false
	return nil
}

// decodeLobs decodes (reads from db) output lob or result lob parameters.

// read lob reply
// - seems like readLobreply returns only a result for one lob - even if more then one is requested
// --> read single lobs
func (s *Session) decodeLobs(descr *lobOutDescr, wr io.Writer) error {
	var err error

	if descr.isCharBased {
		wrcl := transform.NewWriter(wr, unicode.Cesu8ToUtf8Transformer) // CESU8 transformer
		err = s._decodeLobs(descr, wrcl, func(b []byte) (int64, error) {
			// Caution: hdb counts 4 byte utf-8 encodings (cesu-8 6 bytes) as 2 (3 byte) chars
			numChars := int64(0)
			for len(b) > 0 {
				if !cesu8.FullRune(b) { //
					return 0, fmt.Errorf("lob chunk consists of incomplete CESU-8 runes")
				}
				_, size := cesu8.DecodeRune(b)
				b = b[size:]
				numChars++
				if size == cesu8.CESUMax {
					numChars++
				}
			}
			return numChars, nil
		})
	} else {
		err = s._decodeLobs(descr, wr, func(b []byte) (int64, error) { return int64(len(b)), nil })
	}

	if pw, ok := wr.(*io.PipeWriter); ok { // if the writer is a pipe-end -> close at the end
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}
	return err
}

func (s *Session) _decodeLobs(descr *lobOutDescr, wr io.Writer, countChars func(b []byte) (int64, error)) error {
	lobChunkSize := int64(s.cfg.LobChunkSize())

	chunkSize := func(numChar, ofs int64) int32 {
		chunkSize := numChar - ofs
		if chunkSize > lobChunkSize {
			return int32(lobChunkSize)
		}
		return int32(chunkSize)
	}

	if _, err := wr.Write(descr.b); err != nil {
		return err
	}

	lobRequest := &readLobRequest{}
	lobRequest.id = descr.id

	lobReply := &readLobReply{}

	eof := descr.opt.isLastData()

	ofs, err := countChars(descr.b)
	if err != nil {
		return err
	}

	for !eof {

		lobRequest.ofs += ofs
		lobRequest.chunkSize = chunkSize(descr.numChar, ofs)

		if err := s.pw.write(s.sessionID, mtWriteLob, false, lobRequest); err != nil {
			return err
		}

		if err := s.pr.iterateParts(func(ph *partHeader) {
			if ph.partKind == pkReadLobReply {
				s.pr.read(lobReply)
			}
		}); err != nil {
			return err
		}

		if lobReply.id != lobRequest.id {
			return fmt.Errorf("internal error: invalid lob locator %d - expected %d", lobReply.id, lobRequest.id)
		}

		if _, err := wr.Write(lobReply.b); err != nil {
			return err
		}

		ofs, err = countChars(lobReply.b)
		if err != nil {
			return err
		}
		eof = lobReply.opt.isLastData()
	}
	return nil
}

// encodeLobs encodes (write to db) input lob parameters.
func (s *Session) encodeLobs(cr *callResult, ids []locatorID, inPrmFields []*parameterField, args []driver.NamedValue) error {

	chunkSize := int(s.cfg.LobChunkSize())

	readers := make([]io.Reader, 0, len(ids))
	descrs := make([]*writeLobDescr, 0, len(ids))

	j := 0
	for i, f := range inPrmFields {
		if f.tc.isLob() {
			rd, ok := args[i].Value.(io.Reader)
			if !ok {
				return fmt.Errorf("protocol error: invalid lob parameter %[1]T %[1]v - io.Reader expected", args[i].Value)
			}
			if f.tc.isCharBased() {
				rd = transform.NewReader(rd, unicode.Utf8ToCesu8Transformer) // CESU8 transformer
			}
			if j >= len(ids) {
				return fmt.Errorf("protocol error: invalid number of lob parameter ids %d", len(ids))
			}
			readers = append(readers, rd)
			descrs = append(descrs, &writeLobDescr{id: ids[j]})
			j++
		}
	}

	writeLobRequest := &writeLobRequest{}

	for len(descrs) != 0 {

		if len(descrs) != len(ids) {
			return fmt.Errorf("protocol error: invalid number of lob parameter ids %d - expected %d", len(descrs), len(ids))
		}
		for i, descr := range descrs { // check if ids and descrs are in sync
			if descr.id != ids[i] {
				return fmt.Errorf("protocol error: lob parameter id mismatch %d - expected %d", descr.id, ids[i])
			}
		}

		// TODO check total size limit
		for i, descr := range descrs {
			descr.b = make([]byte, chunkSize)
			size, err := readers[i].Read(descr.b)
			descr.b = descr.b[:size]
			if err != nil && err != io.EOF {
				return err
			}
			descr.ofs = -1 //offset (-1 := append)
			descr.opt = loDataincluded
			if err == io.EOF {
				descr.opt |= loLastdata
			}
		}

		writeLobRequest.descrs = descrs

		if err := s.pw.write(s.sessionID, mtReadLob, false, writeLobRequest); err != nil {
			return err
		}

		lobReply := &writeLobReply{}
		outPrms := &outputParameters{}

		if err := s.pr.iterateParts(func(ph *partHeader) {
			switch ph.partKind {
			case pkOutputParameters:
				outPrms.outputFields = cr.outputFields
				s.pr.read(outPrms)
				cr.fieldValues = outPrms.fieldValues
			case pkWriteLobReply:
				s.pr.read(lobReply)
				ids = lobReply.ids
			}
		}); err != nil {
			return err
		}

		// remove done descr and readers
		j := 0
		for i, descr := range descrs {
			if !descr.opt.isLastData() {
				descrs[j] = descr
				readers[j] = readers[i]
				j++
			}
		}
		descrs = descrs[:j]
		readers = readers[:j]
	}
	return nil
}
