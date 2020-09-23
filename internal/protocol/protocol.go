// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"bufio"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/SAP/go-hdb/driver/sqltrace"
	"github.com/SAP/go-hdb/internal/container/varmap"
	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

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
	if qr.fieldValues == nil || qr.fields == nil || len(qr.fields) == 0 {
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
	if cr.fieldValues == nil || cr.outputFields == nil || len(cr.outputFields) == 0 {
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
	upStream bool

	// authentication
	step   int
	method string

	dec    *encoding.Decoder
	tracer traceLogger

	mh *messageHeader
	sh *segmentHeader
	ph *partHeader

	msgSize  int64
	numPart  int
	cntPart  int
	partRead bool

	partReaderCache map[partKind]partReader

	lastErrors       *hdbErrors
	lastRowsAffected *rowsAffected

	// partReader read errors could be
	// - read buffer errors -> buffer Error() and ResetError()
	// - plus other errors (which cannot be ignored, e.g. Lob reader)
	err error
}

func newProtocolReader(upStream bool, rd io.Reader) *protocolReader {
	return &protocolReader{
		upStream:        upStream,
		dec:             encoding.NewDecoder(rd),
		tracer:          newTraceLogger(upStream),
		partReaderCache: map[partKind]partReader{},
		mh:              &messageHeader{},
		sh:              &segmentHeader{},
		ph:              &partHeader{},
	}
}

func (r *protocolReader) setDfv(dfv int) {
	r.dec.SetDfv(dfv)
}

func (r *protocolReader) readSkip() error            { return r.iterateParts(nil) }
func (r *protocolReader) sessionID() int64           { return r.mh.sessionID }
func (r *protocolReader) functionCode() functionCode { return r.sh.functionCode }

func (r *protocolReader) readInitRequest() error {
	req := &initRequest{}
	if err := req.decode(r.dec); err != nil {
		return err
	}
	r.tracer.Log(req)
	return nil
}

func (r *protocolReader) readInitReply() error {
	rep := &initReply{}
	if err := rep.decode(r.dec); err != nil {
		return err
	}
	r.tracer.Log(rep)
	return nil
}

func (r *protocolReader) readProlog() error {
	if r.upStream {
		return r.readInitRequest()
	}
	return r.readInitReply()
}

func (r *protocolReader) checkError() error {
	defer func() { // init readFlags
		r.lastErrors = nil
		r.lastRowsAffected = nil
		r.err = nil
		r.dec.ResetError()
	}()

	if r.err != nil {
		return r.err
	}

	if err := r.dec.Error(); err != nil {
		return err
	}

	if r.lastErrors == nil {
		return nil
	}

	if r.lastRowsAffected != nil { // link statement to error
		j := 0
		for i, rows := range *r.lastRowsAffected {
			if rows == raExecutionFailed {
				r.lastErrors.setStmtNo(j, i)
				j++
			}
		}
	}

	if r.lastErrors.isWarnings() {
		for _, e := range r.lastErrors.errors {
			sqltrace.Traceln(e)
		}
		return nil
	}

	return r.lastErrors
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

func (r *protocolReader) read(part partReader) error {
	r.partRead = true

	err := r.readPart(part)
	if err != nil {
		r.err = err
	}

	switch part := part.(type) {
	case *hdbErrors:
		r.lastErrors = part
	case *rowsAffected:
		r.lastRowsAffected = part
	}
	return err
}

func (r *protocolReader) authPart() partReader {
	defer func() { r.step++ }()

	switch {
	case r.upStream && r.step == 0:
		return &authInitReq{}
	case r.upStream:
		return &authFinalReq{}
	case !r.upStream && r.step == 0:
		return &authInitRep{}
	case !r.upStream:
		return &authFinalRep{}
	default:
		panic(fmt.Errorf("invalid auth step in protocol reader %d", r.step))
	}
}

func (r *protocolReader) defaultPart(pk partKind) (partReader, error) {
	part, ok := r.partReaderCache[pk]
	if !ok {
		var err error
		part, err = newPartReader(pk)
		if err != nil {
			return nil, err
		}
		r.partReaderCache[pk] = part
	}
	return part, nil
}

func (r *protocolReader) skip() error {
	pk := r.ph.partKind
	if r.canSkip(pk) {
		return r.skipPart()
	}

	var part partReader
	var err error
	if pk == pkAuthentication {
		part = r.authPart()
	} else {
		part, err = r.defaultPart(pk)
	}
	if err != nil {
		return r.skipPart()
	}
	return r.read(part)
}

func (r *protocolReader) skipPart() error {
	r.dec.Skip(int(r.ph.bufferLength))
	r.tracer.Log("*skipped")

	/*
		hdb protocol
		- in general padding but
		- in some messages the last record sent is not padded
		  - message header varPartLength < segment header segmentLength
		    - msgSize == 0: mh.varPartLength == sh.segmentLength
			- msgSize < 0 : mh.varPartLength < sh.segmentLength
	*/
	if r.cntPart != r.numPart || r.msgSize == 0 {
		r.dec.Skip(padBytes(int(r.ph.bufferLength)))
	}
	return nil
}

func (r *protocolReader) readPart(part partReader) error {

	r.dec.ResetCnt()
	if err := part.decode(r.dec, r.ph); err != nil {
		return err // do not ignore partReader errros
	}
	cnt := r.dec.Cnt()
	r.tracer.Log(part)

	bufferLen := int(r.ph.bufferLength)
	switch {
	case cnt < bufferLen: // protocol buffer length > read bytes -> skip the unread bytes

		// TODO enable for debug
		// b := make([]byte, bufferLen-cnt)
		// p.rd.ReadFull(b)
		// println(fmt.Sprintf("%x", b))
		// println(string(b))

		r.dec.Skip(bufferLen - cnt)

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
	if r.cntPart != r.numPart || r.msgSize == 0 {
		r.dec.Skip(padBytes(int(r.ph.bufferLength)))
	}
	return nil
}

func (r *protocolReader) iterateParts(partCb func(ph *partHeader)) error {
	if err := r.mh.decode(r.dec); err != nil {
		return err
	}
	r.tracer.Log(r.mh)

	r.msgSize = int64(r.mh.varPartLength)

	for i := 0; i < int(r.mh.noOfSegm); i++ {

		if err := r.sh.decode(r.dec); err != nil {
			return err
		}
		r.tracer.Log(r.sh)

		r.msgSize -= int64(r.sh.segmentLength)
		r.numPart = int(r.sh.noOfParts)
		r.cntPart = 0

		for j := 0; j < int(r.sh.noOfParts); j++ {

			if err := r.ph.decode(r.dec); err != nil {
				return err
			}
			r.tracer.Log(r.ph)

			r.cntPart++

			r.partRead = false
			if partCb != nil {
				partCb(r.ph)
			}
			if !r.partRead {
				r.skip()
			}
			if r.err != nil {
				return r.err
			}
		}
	}
	return r.checkError()
}

// protocol writer
type protocolWriter struct {
	wr  *bufio.Writer
	sv  *varmap.VarMap // session variables
	enc *encoding.Encoder

	tracer traceLogger

	// reuse header
	mh *messageHeader
	sh *segmentHeader
	ph *partHeader
}

func newProtocolWriter(wr *bufio.Writer, sv *varmap.VarMap) *protocolWriter {
	return &protocolWriter{
		wr:     wr,
		sv:     sv,
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
	// check on session variables to be send as ClientInfo
	if messageType.clientInfoSupported() && w.sv.HasUpdates() {
		upd, del := w.sv.Delta()
		// TODO: how to delete session variables via clientInfo
		// ...for the time being we set the value to <space>...
		for k := range del {
			upd[k] = ""
		}
		writers = append([]partWriter{clientInfo(upd)}, writers...)
	}

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
