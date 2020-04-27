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

	"github.com/SAP/go-hdb/internal/unicode"
	"github.com/SAP/go-hdb/internal/unicode/cesu8"
)

const (
	mnSCRAMSHA256 = "SCRAMSHA256"
	mnGSS         = "GSS"
	mnSAML        = "SAML"
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

func newSessionConn(ctx context.Context, addr string, timeoutSec int, config *tls.Config) (sessionConn, error) {
	// session recording
	if wr, ok := ctx.Value(sesRecording).(io.Writer); ok {
		conn, err := newDbConn(ctx, addr, timeoutSec, config)
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
	return newDbConn(ctx, addr, timeoutSec, config)
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

func newDbConn(ctx context.Context, addr string, timeoutSec int, config *tls.Config) (*dbConn, error) {
	timeout := time.Duration(timeoutSec) * time.Second
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	// is TLS connection requested?
	if config != nil {
		conn = tls.Client(conn, config)
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
	Dfv() int
	TLSConfig() *tls.Config
	Legacy() bool
}

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

	conn, err := newSessionConn(ctx, cfg.Host(), cfg.Timeout(), cfg.TLSConfig())
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

	pr := newProtocolReader(bufRd) // read downstream
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
	authentication := mnSCRAMSHA256

	switch authentication {
	default:
		return fmt.Errorf("invalid authentication %s", authentication)

	case mnSCRAMSHA256:
		if err := s.authenticateScramsha256(); err != nil {
			return err
		}
	case mnGSS:
		panic("not implemented error")
	case mnSAML:
		panic("not implemented error")
	}

	if s.sessionID <= 0 {
		return fmt.Errorf("invalid session id %d", s.sessionID)
	}

	return nil
}

func (s *Session) authenticateScramsha256() error {
	tr := unicode.Utf8ToCesu8Transformer
	tr.Reset()

	username := make([]byte, cesu8.StringSize(s.cfg.Username()))
	if _, _, err := tr.Transform(username, []byte(s.cfg.Username()), true); err != nil {
		return err // should never happen
	}

	password := make([]byte, cesu8.StringSize(s.cfg.Password()))
	if _, _, err := tr.Transform(password, []byte(s.cfg.Password()), true); err != nil {
		return err //should never happen
	}

	clientChallenge := clientChallenge()

	//initial request
	initialRequest := &scramsha256InitialRequest{username: username, clientChallenge: clientChallenge}
	if err := s.pw.write(s.sessionID, mtAuthenticate, false, initialRequest); err != nil {
		return err
	}

	initialReply, err := s.pr.readScramsha256InitialReply()
	if err != nil {
		return err
	}

	// final request
	finalRequest := &scramsha256FinalRequest{
		username:    username,
		clientProof: clientProof(initialReply.salt, initialReply.serverChallenge, clientChallenge, password),
	}

	id := newClientID()

	co := connectOptions{}
	co.set(coDistributionProtocolVersion, optBooleanType(false))
	co.set(coSelectForUpdateSupported, optBooleanType(false))
	co.set(coSplitBatchCommands, optBooleanType(true))

	dfv := checkDfv(optIntType(s.cfg.Dfv()))
	co.set(coDataFormatVersion2, dfv)

	co.set(coCompleteArrayExecution, optBooleanType(true))
	if s.cfg.Locale() != "" {
		co.set(coClientLocale, optStringType(s.cfg.Locale()))
	}
	co.set(coClientDistributionMode, cdmOff)
	// co.set(coImplicitLobStreaming, optBooleanType(true))

	if err := s.pw.write(s.sessionID, mtConnect, false, finalRequest, id, co); err != nil {
		return err
	}

	_, sessionID, err := s.pr.readScramsha256FinalReply()
	if err != nil {
		return err
	}
	s.sessionID = sessionID

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

	qr, err := s.pr.readQueryDirect()
	if err != nil {
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

	fc, rows, err := s.pr.readExecDirect()
	if err != nil {
		return nil, err
	}
	if fc == fcDDL {
		return driver.ResultNoRows, nil
	}
	return driver.RowsAffected(rows), nil
}

// Prepare prepares a sql statement.
func (s *Session) Prepare(query string) (*PrepareResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.pw.write(s.sessionID, mtPrepare, false, command(query)); err != nil {
		return nil, err
	}

	pr, err := s.pr.readPrepare()
	if err != nil {
		return nil, err
	}
	return pr, nil
}

// Exec executes a sql statement.
func (s *Session) Exec(pr *PrepareResult, args []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.pw.write(s.sessionID, mtExecute, !s.inTx, statementID(pr.stmtID), newInputParameters(pr.prmFields, args)); err != nil {
		return nil, err
	}

	fc, rows, ids, err := s.pr.readExec()
	if err != nil {
		return nil, err
	}

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
	return driver.RowsAffected(rows), nil
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

	cr, ids, err := s.pr.readCall(outPrmFields)
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

	cr, ids, err := s.pr.readCall(outPrmFields)
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

// Query executes a query.
func (s *Session) Query(pr *PrepareResult, args []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// allow e.g inserts as query -> handle commit like in exec
	if err := s.pw.write(s.sessionID, mtExecute, !s.inTx, statementID(pr.stmtID), newInputParameters(pr.prmFields, args)); err != nil {
		return nil, err
	}

	qr, err := s.pr.readQuery(pr)
	if err != nil {
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
	return s.pr.readFetchNext(qr)
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
func (s *Session) decodeLobs(writer chunkWriter) error {
	readLobRequest := &readLobRequest{writer: writer}

	for !writer.eof() {
		if err := s.pw.write(s.sessionID, mtWriteLob, false, readLobRequest); err != nil {
			return err
		}
		if err := s.pr.readReadLobReply(writer); err != nil {
			return err
		}
	}
	return nil
}

// encodeLobs encodes (write to db) input lob parameters.
func (s *Session) encodeLobs(cr *callResult, ids []locatorID, inPrmFields []*parameterField, args []driver.NamedValue) error {

	chunkReaders := make([]chunkReader, 0, len(ids))

	j := 0
	for i, f := range inPrmFields {
		if f.tc.isLob() {
			rd, ok := args[i].Value.(io.Reader)
			if !ok {
				return fmt.Errorf("protocol error: invalid lob parameter %[1]T %[1]v - io.Reader expected", args[i].Value)
			}
			if j >= len(ids) {
				return fmt.Errorf("protocol error: invalid number of lob parameter ids %d", len(ids))
			}
			chRd := newChunkReader(f.tc.isCharBased(), ids[j], int(s.cfg.LobChunkSize()), rd)
			chunkReaders = append(chunkReaders, chRd)
			j++
		}
	}

	writeLobRequest := &writeLobRequest{chunkReaders: chunkReaders}

	if len(chunkReaders) != len(ids) {
		return fmt.Errorf("protocol error: invalid number of lob parameter ids %d - expected %d", len(chunkReaders), len(ids))
	}

	for len(chunkReaders) != 0 {

		if err := s.pw.write(s.sessionID, mtReadLob, false, writeLobRequest); err != nil {
			return err
		}
		ids, err := s.pr.readWriteLobReply(cr)
		if err != nil {
			return err
		}

		// remove done chunkReaders
		i := 0
		for _, chunkReader := range chunkReaders {
			if !chunkReader.eof() {
				chunkReaders[i] = chunkReader
				i++
			}
		}
		chunkReaders = chunkReaders[:i]
		// check ids, chunkReaders consistency
		if len(chunkReaders) != len(ids) {
			return fmt.Errorf("protocol error: invalid number of lob parameter ids %d - expected %d", len(chunkReaders), len(ids))
		}
		for i, id := range ids {
			if id != chunkReaders[i].locatorID() {
				return fmt.Errorf("protocol error: lob parameter id mismatch %d - expected %d", chunkReaders[i].locatorID(), id)
			}
		}
	}
	return nil
}
