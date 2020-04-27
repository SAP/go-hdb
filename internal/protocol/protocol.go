/*
Copyright 2020 SAP SE

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
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/SAP/go-hdb/driver/sqltrace"
	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

type partCache struct {
	clientID            *clientID
	command             *command
	connectOptions      *connectOptions
	topologyInformation *topologyInformation
	rowsAffected        *rowsAffected
	transactionFlags    *transactionFlags
	statementContext    *statementContext
	statementID         *statementID
	parameterMetadata   *parameterMetadata
	inputParameters     *inputParameters
	outputParameters    *outputParameters
	resultMetadata      *resultMetadata
	resultsetID         *resultsetID
	fetchSize           *fetchsize
	resultset           *resultset
	readLobRequest      *readLobRequest
	writeLobRequest     *writeLobRequest
	readLobReply        *readLobReply
	writeLobReply       *writeLobReply
	hdbErrors           *hdbErrors

	parts map[partKind]partReader
}

func newPartCache() *partCache {
	c := &partCache{
		clientID:            &clientID{},
		command:             &command{},
		connectOptions:      &connectOptions{},
		topologyInformation: &topologyInformation{},
		rowsAffected:        &rowsAffected{},
		transactionFlags:    &transactionFlags{},
		statementContext:    &statementContext{},
		statementID:         new(statementID),
		parameterMetadata:   &parameterMetadata{},
		inputParameters:     &inputParameters{},
		outputParameters:    &outputParameters{},
		resultMetadata:      &resultMetadata{},
		resultsetID:         new(resultsetID),
		fetchSize:           new(fetchsize),
		resultset:           &resultset{},
		readLobRequest:      &readLobRequest{},
		writeLobRequest:     &writeLobRequest{},
		readLobReply:        &readLobReply{},
		writeLobReply:       &writeLobReply{},
		hdbErrors:           &hdbErrors{},
	}

	parts := map[partKind]partReader{
		pkClientID:            c.clientID,
		pkCommand:             c.command,
		pkConnectOptions:      c.connectOptions,
		pkTopologyInformation: c.topologyInformation,
		pkRowsAffected:        c.rowsAffected,
		pkTransactionFlags:    c.transactionFlags,
		pkStatementContext:    c.statementContext,
		pkStatementID:         c.statementID,
		pkParameterMetadata:   c.parameterMetadata,
		pkParameters:          c.inputParameters,
		pkOutputParameters:    c.outputParameters,
		pkResultMetadata:      c.resultMetadata,
		pkResultsetID:         c.resultsetID,
		pkFetchSize:           c.fetchSize,
		pkResultset:           c.resultset,
		pkReadLobRequest:      c.readLobRequest,
		pkWriteLobRequest:     c.writeLobRequest,
		pkReadLobReply:        c.readLobReply,
		pkWriteLobReply:       c.writeLobReply,
		pkError:               c.hdbErrors,
	}

	c.parts = parts
	return c
}

func (c *partCache) get(pk partKind) (partReader, bool) {
	part, ok := c.parts[pk]
	return part, ok
}

// rowsResult represents the row resultset of a query or stored procedure (output parameters, call table results).
type rowsResult interface {
	rsID() uint64                         // RsID returns the resultset id.
	columns() []string                    // Columns returns the names of the resultset columns.
	numRow() int                          // NumRow returns the number of rows available in FieldValues.
	closed() bool                         // Closed returnr true if the database resultset is closed (completely read).
	lastPacket() bool                     // LastPacket returns true if the last packet of a resultset was read from database.
	copyRow(idx int, dest []driver.Value) // CopyRow fills the dest value slice with row data at index idx.
	field(idx int) Field                  // Field returns the field descriptor at position idx.
	queryResult() (*queryResult, error)   // Used by fetch next if RowsResult is based on a query (nil for CallResult).
}

var (
	_ rowsResult = (*queryResult)(nil)
	_ rowsResult = (*callResult)(nil)
)

// A PrepareResult represents the result of a prepare statement.
type PrepareResult struct {
	fc           functionCode
	stmtID       uint64
	prmFields    []*parameterField
	resultFields []*resultField
}

// Check checks consistency of the prepare result.
func (pr *PrepareResult) Check(qd *QueryDescr) error {
	call := qd.kind == QkCall
	if call != pr.fc.isProcedureCall() {
		return fmt.Errorf("function code mismatch: query descriptor %s - function code %s", qd.kind, pr.fc)
	}

	if !call {
		// only input parameters allowed
		for _, f := range pr.prmFields {
			if f.Out() {
				return fmt.Errorf("invalid parameter %s", f)
			}
		}
	}
	return nil
}

// StmtID returns the statement id.
func (pr *PrepareResult) StmtID() uint64 {
	return pr.stmtID
}

// IsProcedureCall returns true if the statement is a call statement.
func (pr *PrepareResult) IsProcedureCall() bool {
	return pr.fc.isProcedureCall()
}

// NumField returns the number of parameter fields in a database statement.
func (pr *PrepareResult) NumField() int {
	return len(pr.prmFields)
}

// NumInputField returns the number of input fields in a database statement.
func (pr *PrepareResult) NumInputField() int {
	if !pr.fc.isProcedureCall() {
		return len(pr.prmFields) // only input fields
	}
	numField := 0
	for _, f := range pr.prmFields {
		if f.In() {
			numField++
		}
	}
	return numField
}

// PrmField returns the parameter field at index idx.
func (pr *PrepareResult) PrmField(idx int) Field {
	return pr.prmFields[idx]
}

// A QueryResult represents the resultset of a query.
type queryResult struct {
	_rsID       uint64
	fields      []*resultField
	fieldValues []driver.Value
	attributes  partAttributes
	_columns    []string
}

// RsID implements the RowsResult interface.
func (qr *queryResult) rsID() uint64 {
	return qr._rsID
}

// Field implements the RowsResult interface.
func (qr *queryResult) field(idx int) Field {
	return qr.fields[idx]
}

// NumRow implements the RowsResult interface.
func (qr *queryResult) numRow() int {
	if qr.fieldValues == nil {
		return 0
	}
	return len(qr.fieldValues) / len(qr.fields)
}

// CopyRow implements the RowsResult interface.
func (qr *queryResult) copyRow(idx int, dest []driver.Value) {
	cols := len(qr.fields)
	copy(dest, qr.fieldValues[idx*cols:(idx+1)*cols])
}

// Closed implements the RowsResult interface.
func (qr *queryResult) closed() bool {
	return qr.attributes.ResultsetClosed()
}

// LastPacket implements the RowsResult interface.
func (qr *queryResult) lastPacket() bool {
	return qr.attributes.LastPacket()
}

// Columns implements the RowsResult interface.
func (qr *queryResult) columns() []string {
	if qr._columns == nil {
		numField := len(qr.fields)
		qr._columns = make([]string, numField)
		for i := 0; i < numField; i++ {
			qr._columns[i] = qr.fields[i].Name()
		}
	}
	return qr._columns
}

func (qr *queryResult) queryResult() (*queryResult, error) {
	return qr, nil
}

// A CallResult represents the result (output parameters and values) of a call statement.
type callResult struct { // call output parameters
	outputFields []*parameterField
	fieldValues  []driver.Value
	_columns     []string
	qrs          []*queryResult // table output parameters
}

// RsID implements the RowsResult interface.
func (cr *callResult) rsID() uint64 {
	return 0
}

// Field implements the RowsResult interface.
func (cr *callResult) field(idx int) Field {
	return cr.outputFields[idx]
}

// NumRow implements the RowsResult interface.
func (cr *callResult) numRow() int {
	if cr.fieldValues == nil {
		return 0
	}
	return len(cr.fieldValues) / len(cr.outputFields)
}

// CopyRow implements the RowsResult interface.
func (cr *callResult) copyRow(idx int, dest []driver.Value) {
	cols := len(cr.outputFields)
	copy(dest, cr.fieldValues[idx*cols:(idx+1)*cols])
}

// Closed implements the RowsResult interface.
func (cr *callResult) closed() bool {
	return true
}

// LastPacket implements the RowsResult interface.
func (cr *callResult) lastPacket() bool {
	return true
}

// Columns implements the RowsResult interface.
func (cr *callResult) columns() []string {
	if cr._columns == nil {
		numField := len(cr.outputFields)
		cr._columns = make([]string, numField)
		for i := 0; i < numField; i++ {
			cr._columns[i] = cr.outputFields[i].Name()
		}
	}
	return cr._columns
}

func (cr *callResult) queryResult() (*queryResult, error) {
	return nil, errors.New("cannot use call result as query result")
}

func (cr *callResult) appendTableRefFields() {
	for i, qr := range cr.qrs {
		cr.outputFields = append(cr.outputFields, &parameterField{name: fmt.Sprintf("table %d", i), tc: tcTableRef, mode: pmOut, offset: 0})
		cr.fieldValues = append(cr.fieldValues, encodeID(qr._rsID))
	}
}

func (cr *callResult) appendTableRowsFields(s *Session) {
	for i, qr := range cr.qrs {
		cr.outputFields = append(cr.outputFields, &parameterField{name: fmt.Sprintf("table %d", i), tc: tcTableRows, mode: pmOut, offset: 0})
		cr.fieldValues = append(cr.fieldValues, newQueryResultSet(s, qr))
	}
}

type protocolReader struct {
	dec              *encoding.Decoder
	tracer           traceLogger
	msgIter          *msgIter
	segIter          *segIter
	partIter         *partIter
	errorFlag        bool
	rowsAffectedFlag bool

	*partCache

	// partReader read errors could be
	// - read buffer errors -> buffer Error() and ResetError()
	// - plus other errors (which cannot be ignored, e.g. Lob reader)
	err error
}

func newProtocolReader(rd io.Reader) *protocolReader {
	tracer := newTraceLogger(false)
	dec := encoding.NewDecoder(rd)
	partIter := newPartIter(dec, tracer)
	segIter := newSegIter(partIter, dec, tracer)
	msgIter := newMsgIter(segIter, dec, tracer)
	return &protocolReader{
		dec:       dec,
		tracer:    tracer,
		partCache: newPartCache(),
		partIter:  partIter,
		segIter:   segIter,
		msgIter:   msgIter,
	}
}

func (r *protocolReader) readProlog() error {
	rep := &initReply{}
	if err := rep.decode(r.dec); err != nil {
		return err
	}
	r.tracer.Log(rep)
	return nil
}

func (r *protocolReader) checkError() error {
	defer func() { // init readFlags
		r.errorFlag = false
		r.rowsAffectedFlag = false
		r.err = nil
		r.dec.ResetError()
	}()

	if r.err != nil {
		return r.err
	}

	if err := r.dec.Error(); err != nil {
		return err
	}

	if !r.errorFlag {
		return nil
	}

	if r.rowsAffectedFlag { // link statement to error
		j := 0
		for i, rows := range *r.rowsAffected {
			if rows == raExecutionFailed {
				r.hdbErrors.setStmtNo(j, i)
				j++
			}
		}
	}

	if r.hdbErrors.isWarnings() {
		for _, e := range r.hdbErrors.errors {
			sqltrace.Traceln(e)
		}
		return nil
	}

	return r.hdbErrors
}

func (r *protocolReader) canSkip(pk partKind) bool {
	// errors and rowsAffected needs always to be read
	if pk == pkError || pk == pkRowsAffected {
		return false
	}
	if debug {
		return false
	}
	return true
}

func (r *protocolReader) read(part partReader) {
	pk := r.partIter.partKind()
	switch pk {
	case pkError:
		r.errorFlag = true
	case pkRowsAffected:
		r.rowsAffectedFlag = true
	}
	r.err = r.partIter.read(part)
}

func (r *protocolReader) skip() {
	pk := r.partIter.partKind()
	if r.canSkip(pk) {
		r.partIter.skip()
	} else {
		switch pk {
		case pkError:
			r.errorFlag = true
		case pkRowsAffected:
			r.rowsAffectedFlag = true
		}
		part, ok := r.partCache.get(pk)
		if !ok {
			plog.Fatalf("part cache entry missing: %s", pk)
		}
		r.err = r.partIter.read(part)
	}
}

func (r *protocolReader) readSkip() error {
	r.msgIter.next()
	r.segIter.next()
	for r.partIter.next() {
		r.skip()
	}
	return r.checkError()
}

func (r *protocolReader) readScramsha256InitialReply() (*scramsha256InitialReply, error) {
	var scramsha256InitialReply = &scramsha256InitialReply{}

	r.msgIter.next()
	r.segIter.next()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkAuthentication:
			r.read(scramsha256InitialReply)
		default:
			r.skip()
		}
	}
	return scramsha256InitialReply, r.checkError()
}

func (r *protocolReader) readScramsha256FinalReply() (*scramsha256FinalReply, int64, error) {
	var scramsha256FinalReply = &scramsha256FinalReply{}

	r.msgIter.next()
	r.segIter.next()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkAuthentication:
			r.read(scramsha256FinalReply)
		default:
			r.skip()
		}
	}
	return scramsha256FinalReply, r.msgIter.mh.sessionID, r.checkError()
}

func (r *protocolReader) readExecDirect() (functionCode, int64, error) {
	var fc functionCode
	var rows int64

	r.msgIter.next()
	r.segIter.next()
	fc = r.segIter.functionCode()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkRowsAffected:
			r.read(r.rowsAffected)
			rows = r.rowsAffected.total()
		default:
			r.skip()
		}
	}
	return fc, rows, r.checkError()
}

func (r *protocolReader) readQueryDirect() (*queryResult, error) {
	qr := &queryResult{}

	r.msgIter.next()
	r.segIter.next()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkResultMetadata:
			r.read(r.resultMetadata)
			qr.fields = r.resultMetadata.resultFields
		case pkResultsetID:
			r.read(r.resultsetID)
			qr._rsID = uint64(*r.resultsetID)
		case pkResultset:
			r.resultset.resultFields = qr.fields
			r.read(r.resultset)
			qr.fieldValues = r.resultset.fieldValues
			qr.attributes = r.partIter.ph.partAttributes
		default:
			r.skip()
		}
	}
	return qr, r.checkError()
}

func (r *protocolReader) readQuery(pr *PrepareResult) (*queryResult, error) {
	qr := &queryResult{fields: pr.resultFields}

	r.msgIter.next()
	r.segIter.next()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkResultsetID:
			r.read(r.resultsetID)
			qr._rsID = uint64(*r.resultsetID)
		case pkResultset:
			r.resultset.resultFields = qr.fields
			r.read(r.resultset)
			qr.fieldValues = r.resultset.fieldValues
			qr.attributes = r.partIter.ph.partAttributes
		default:
			r.skip()
		}
	}
	return qr, r.checkError()
}

func (r *protocolReader) readFetchNext(qr *queryResult) error {
	r.msgIter.next()
	r.segIter.next()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkResultset:
			r.resultset.resultFields = qr.fields
			r.read(r.resultset)
			qr.fieldValues = r.resultset.fieldValues
			qr.attributes = r.partIter.ph.partAttributes
		default:
			r.skip()
		}
	}
	return r.checkError()
}

func (r *protocolReader) readPrepare() (*PrepareResult, error) {
	pr := &PrepareResult{}

	r.msgIter.next()
	r.segIter.next()
	pr.fc = r.segIter.functionCode()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkStatementID:
			r.read(r.statementID)
			pr.stmtID = uint64(*r.statementID)
		case pkResultMetadata:
			r.read(r.resultMetadata)
			pr.resultFields = r.resultMetadata.resultFields
		case pkParameterMetadata:
			r.read(r.parameterMetadata)
			pr.prmFields = r.parameterMetadata.parameterFields
		default:
			r.skip()
		}
	}

	return pr, r.checkError()
}

func (r *protocolReader) readExec() (fc functionCode, rows int64, ids []locatorID, err error) {

	r.msgIter.next()
	r.segIter.next()
	fc = r.segIter.functionCode()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkRowsAffected:
			r.read(r.rowsAffected)
			rows = r.rowsAffected.total()
		case pkWriteLobReply:
			r.read(r.writeLobReply)
			ids = r.writeLobReply.ids
		default:
			r.skip()
		}
	}
	return fc, rows, ids, r.checkError()
}

func (r *protocolReader) readCall(outputFields []*parameterField) (*callResult, []locatorID, error) {

	cr := &callResult{outputFields: outputFields}

	//var qrs []*QueryResult
	var qr *queryResult
	var ids []locatorID

	r.msgIter.next()
	r.segIter.next()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkOutputParameters:
			r.outputParameters.outputFields = cr.outputFields
			r.read(r.outputParameters)
			cr.fieldValues = r.outputParameters.fieldValues
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
			r.read(r.resultMetadata)
			qr.fields = r.resultMetadata.resultFields
		case pkResultset:
			r.resultset.resultFields = qr.fields
			r.read(r.resultset)
			qr.fieldValues = r.resultset.fieldValues
			qr.attributes = r.partIter.ph.partAttributes
		case pkResultsetID:
			r.read(r.resultsetID)
			qr._rsID = uint64(*r.resultsetID)
		case pkWriteLobReply:
			r.read(r.writeLobReply)
			ids = r.writeLobReply.ids
		default:
			r.skip()
		}
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
	return cr, ids, r.checkError()
}

func (r *protocolReader) readReadLobReply(writer chunkWriter) error {
	var readLobReply = &readLobReply{writer: writer}

	r.msgIter.next()
	r.segIter.next()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkReadLobReply:
			r.read(readLobReply)
		default:
			r.skip()
		}
	}
	return r.checkError()
}

func (r *protocolReader) readWriteLobReply(cr *callResult) ([]locatorID, error) {
	var writeLobReply = &writeLobReply{}

	r.msgIter.next()
	r.segIter.next()

	for r.partIter.next() {
		switch r.partIter.partKind() {
		case pkOutputParameters:
			r.outputParameters.outputFields = cr.outputFields
			r.read(r.outputParameters)
			cr.fieldValues = r.outputParameters.fieldValues
		case pkWriteLobReply:
			r.read(writeLobReply)
		default:
			r.skip()
		}
	}
	return writeLobReply.ids, r.checkError()
}

//

type partIter struct {
	dec     *encoding.Decoder
	tracer  traceLogger
	msgSize int64
	numPart int
	cnt     int
	ph      *partHeader
}

func newPartIter(dec *encoding.Decoder, tracer traceLogger) *partIter {
	return &partIter{
		dec:    dec,
		tracer: tracer,
		ph:     &partHeader{},
	}
}

func (p *partIter) partKind() partKind {
	return p.ph.partKind
}

func (p *partIter) reset(msgSize int64, numPart int) {
	p.msgSize = msgSize
	p.numPart = numPart
	p.cnt = 0
}

func (p *partIter) next() bool {
	if p.cnt >= p.numPart {
		return false
	}
	p.cnt++
	if err := p.ph.decode(p.dec); err != nil {
		return false
	}
	p.tracer.Log(p.ph)
	return true
}

func (p *partIter) skip() {
	p.dec.Skip(int(p.ph.bufferLength))
	p.tracer.Log("*skipped")

	/*
		hdb protocol
		- in general padding but
		- in some messages the last record sent is not padded
		  - message header varPartLength < segment header segmentLength
		    - msgSize == 0: mh.varPartLength == sh.segmentLength
			- msgSize < 0 : mh.varPartLength < sh.segmentLength
	*/
	if p.cnt != p.numPart || p.msgSize == 0 {
		p.dec.Skip(padBytes(int(p.ph.bufferLength)))
	}
}

func (p *partIter) read(part partReader) error {

	p.dec.ResetCnt()
	if err := part.decode(p.dec, p.ph); err != nil {
		return err // do not ignore partReader errros
	}
	cnt := p.dec.Cnt()
	p.tracer.Log(part)

	bufferLen := int(p.ph.bufferLength)
	switch {
	case cnt < bufferLen: // protocol buffer length > read bytes -> skip the unread bytes

		// TODO enable for debug
		// b := make([]byte, bufferLen-cnt)
		// p.rd.ReadFull(b)
		// println(fmt.Sprintf("%x", b))
		// println(string(b))

		p.dec.Skip(bufferLen - cnt)

	case cnt > bufferLen: // read bytes > protocol buffer length -> should never happen
		panic(fmt.Errorf("protocol error: read bytes %d > buffer length %d", cnt, bufferLen))
	}

	/*
		hdb protocol
		- in general padding but
		- in some messages the last record sent is not padded
		  - message header varPartLength < segment header segmentLength
		    - msgSize == 0: mh.varPartLength == sh.segmentLength
			- msgSize < 0 : mh.varPartLength < sh.segmentLength
	*/
	if p.cnt != p.numPart || p.msgSize == 0 {
		p.dec.Skip(padBytes(int(p.ph.bufferLength)))
	}
	return nil
}

type segIter struct {
	partIter *partIter
	dec      *encoding.Decoder
	tracer   traceLogger
	msgSize  int64
	numSeg   int
	cnt      int
	sh       *segmentHeader
}

func newSegIter(partIter *partIter, dec *encoding.Decoder, tracer traceLogger) *segIter {
	return &segIter{
		partIter: partIter,
		dec:      dec,
		tracer:   tracer,
		sh:       &segmentHeader{},
	}
}

func (s *segIter) functionCode() functionCode {
	return s.sh.functionCode
}

func (s *segIter) reset(msgSize int64, numSeg int) {
	s.msgSize = msgSize
	s.numSeg = numSeg
	s.cnt = 0
}

func (s *segIter) next() bool {
	if s.cnt >= s.numSeg {
		return false
	}
	s.cnt++
	if err := s.sh.decode(s.dec); err != nil {
		return false
	}
	s.tracer.Log(s.sh)
	s.msgSize -= int64(s.sh.segmentLength)
	s.partIter.reset(s.msgSize, int(s.sh.noOfParts))
	return true
}

type msgIter struct {
	segIter *segIter
	dec     *encoding.Decoder
	tracer  traceLogger
	mh      *messageHeader
}

func newMsgIter(segIter *segIter, dec *encoding.Decoder, tracer traceLogger) *msgIter {
	return &msgIter{
		segIter: segIter,
		dec:     dec,
		tracer:  tracer,
		mh:      &messageHeader{},
	}
}

func (m *msgIter) next() bool {
	if err := m.mh.decode(m.dec); err != nil {
		return false
	}
	m.tracer.Log(m.mh)
	m.segIter.reset(int64(m.mh.varPartLength), int(m.mh.noOfSegm))
	return true
}

// protocol writer
type protocolWriter struct {
	wr  *bufio.Writer
	enc *encoding.Encoder

	tracer traceLogger

	// reuse header
	mh *messageHeader
	sh *segmentHeader
	ph *partHeader
}

func newProtocolWriter(wr *bufio.Writer) *protocolWriter {
	return &protocolWriter{
		wr:     wr,
		enc:    encoding.NewEncoder(wr),
		tracer: newTraceLogger(true),
		mh:     new(messageHeader),
		sh:     new(segmentHeader),
		ph:     new(partHeader),
	}
}

const (
	productVersionMajor  = 4
	productVersionMinor  = 20
	protocolVersionMajor = 4
	protocolVersionMinor = 1
)

func (w *protocolWriter) writeProlog() error {
	req := &initRequest{}
	req.product.major = productVersionMajor
	req.product.minor = productVersionMinor
	req.protocol.major = protocolVersionMajor
	req.protocol.minor = protocolVersionMinor
	req.numOptions = 1
	req.endianess = littleEndian
	if err := req.encode(w.enc); err != nil {
		return err
	}
	w.tracer.Log(req)
	return w.wr.Flush()
}

func (w *protocolWriter) write(sessionID int64, messageType messageType, commit bool, writers ...partWriter) error {

	numWriters := len(writers)
	partSize := make([]int, numWriters)
	size := int64(segmentHeaderSize + numWriters*partHeaderSize) //int64 to hold MaxUInt32 in 32bit OS

	for i, part := range writers {
		s := part.size()
		size += int64(s + padBytes(s))
		partSize[i] = s // buffer size (expensive calculation)
	}

	if size > math.MaxUint32 {
		return fmt.Errorf("message size %d exceeds maximum message header value %d", size, int64(math.MaxUint32)) //int64: without cast overflow error in 32bit OS
	}

	bufferSize := size

	w.mh.sessionID = sessionID
	w.mh.varPartLength = uint32(size)
	w.mh.varPartSize = uint32(bufferSize)
	w.mh.noOfSegm = 1

	if err := w.mh.encode(w.enc); err != nil {
		return err
	}
	w.tracer.Log(w.mh)

	if size > math.MaxInt32 {
		return fmt.Errorf("message size %d exceeds maximum part header value %d", size, math.MaxInt32)
	}

	w.sh.messageType = messageType
	w.sh.commit = commit
	w.sh.segmentKind = skRequest
	w.sh.segmentLength = int32(size)
	w.sh.segmentOfs = 0
	w.sh.noOfParts = int16(numWriters)
	w.sh.segmentNo = 1

	if err := w.sh.encode(w.enc); err != nil {
		return err
	}
	w.tracer.Log(w.sh)

	bufferSize -= segmentHeaderSize

	for i, part := range writers {

		size := partSize[i]
		pad := padBytes(size)

		w.ph.partKind = part.kind()
		if err := w.ph.setNumArg(part.numArg()); err != nil {
			return err
		}
		w.ph.bufferLength = int32(size)
		w.ph.bufferSize = int32(bufferSize)

		if err := w.ph.encode(w.enc); err != nil {
			return err
		}
		w.tracer.Log(w.ph)

		if err := part.encode(w.enc); err != nil {
			return err
		}
		w.tracer.Log(part)

		w.enc.Zeroes(pad)

		bufferSize -= int64(partHeaderSize + size + pad)
	}
	return w.wr.Flush()
}
