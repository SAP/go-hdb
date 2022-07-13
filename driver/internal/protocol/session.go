// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"bufio"
	"context"
	"database/sql/driver"
	"fmt"
	"io"

	"golang.org/x/text/transform"

	"github.com/SAP/go-hdb/driver/hdb"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
)

//padding
const padding = 8

// DriverVersion holds the version of the driver and is set during go-hdb initialization to driver.DriverVersion value.
var DriverVersion string

// ClientType is the information provided to HDB identifying the driver.
// Previously the driver.DriverName "hdb" was used but we should be more specific in providing a unique client type to HANA backend.
const ClientType = "https://github.com/SAP/go-hdb"

func padBytes(size int) int {
	if r := size % padding; r != 0 {
		return padding - r
	}
	return 0
}

const (
	dfvLevel1        = 1
	defaultSessionID = -1
)

// Session represents a HDB session.
type Session struct {
	cfg *SessionConfig

	sessionID     int64
	serverOptions connectOptions
	hdbVersion    *hdb.Version

	pr *protocolReader
	pw *protocolWriter
}

// NewSession creates a new database session.
func NewSession(ctx context.Context, rw *bufio.ReadWriter, cfg *SessionConfig) (*Session, error) {
	pw := newProtocolWriter(rw.Writer, cfg.CESU8Encoder, cfg.SessionVariables) // write upstream
	if err := pw.writeProlog(); err != nil {
		return nil, err
	}

	pr := newProtocolReader(false, rw.Reader, cfg.CESU8Decoder) // read downstream
	if err := pr.readProlog(); err != nil {
		return nil, err
	}

	s := &Session{cfg: cfg, sessionID: defaultSessionID, pr: pr, pw: pw}

	authStepper := newAuth(cfg)
	var err error
	if s.sessionID, s.serverOptions, err = s.authenticate(authStepper); err != nil {
		return nil, err
	}

	if s.sessionID <= 0 {
		return nil, fmt.Errorf("invalid session id %d", s.sessionID)
	}

	s.hdbVersion = hdb.ParseVersion(s.serverOptions.fullVersionString())
	return s, nil
}

// SessionID returns the session id of the hdb connection.
func (s *Session) SessionID() int64 { return s.sessionID }

// HDBVersion returns the hdb server version.
func (s *Session) HDBVersion() *hdb.Version { return s.hdbVersion }

// DatabaseName returns the database name.
func (s *Session) DatabaseName() string {
	return plainOptions(s.serverOptions).asString(int8(coDatabaseName))
}

func (s *Session) defaultClientOptions() connectOptions {
	co := connectOptions{
		int8(coDistributionProtocolVersion): optBooleanType(false),
		int8(coSelectForUpdateSupported):    optBooleanType(false),
		int8(coSplitBatchCommands):          optBooleanType(true),
		int8(coDataFormatVersion2):          optIntType(s.cfg.Dfv),
		int8(coCompleteArrayExecution):      optBooleanType(true),
		int8(coClientDistributionMode):      cdmOff,
		// int8(coImplicitLobStreaming):        optBooleanType(true),
	}
	if s.cfg.Locale != "" {
		co[int8(coClientLocale)] = optStringType(s.cfg.Locale)
	}
	return co
}

func (s *Session) authenticate(stepper authStepper) (int64, connectOptions, error) {
	var auth partReadWriter
	var err error

	// client context
	clientContext := clientContext(plainOptions{
		int8(ccoClientVersion):            optStringType(DriverVersion),
		int8(ccoClientType):               optStringType(ClientType),
		int8(ccoClientApplicationProgram): optStringType(s.cfg.ApplicationName),
	})

	if auth, err = stepper.next(); err != nil {
		return 0, nil, err
	}
	if err := s.pw.write(s.sessionID, mtAuthenticate, false, clientContext, auth); err != nil {
		return 0, nil, err
	}

	if auth, err = stepper.next(); err != nil {
		return 0, nil, err
	}
	if err := s.pr.iterateParts(func(ph *partHeader) {
		if ph.partKind == pkAuthentication {
			s.pr.read(auth)
		}
	}); err != nil {
		return 0, nil, err
	}

	if auth, err = stepper.next(); err != nil {
		return 0, nil, err
	}
	id := newClientID()
	co := s.defaultClientOptions()
	if err := s.pw.write(s.sessionID, mtConnect, false, auth, id, co); err != nil {
		return 0, nil, err
	}

	if auth, err = stepper.next(); err != nil {
		return 0, nil, err
	}
	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkAuthentication:
			s.pr.read(auth)
		case pkConnectOptions:
			s.pr.read(&co)
			// set data format version
			// TODO generalize for sniffer
			s.pr.setDfv(int(co[int8(coDataFormatVersion2)].(optIntType)))
		}
	}); err != nil {
		return 0, nil, err
	}

	return s.pr.sessionID(), co, nil
}

// QueryDirect executes a query without query parameters.
func (s *Session) QueryDirect(query string, commit bool) (driver.Rows, error) {
	// allow e.g inserts as query -> handle commit like in ExecDirect
	if err := s.pw.write(s.sessionID, mtExecuteDirect, commit, command(query)); err != nil {
		return nil, err
	}

	qr := &queryResult{session: s}
	meta := &resultMetadata{}
	resSet := &resultset{}

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkResultMetadata:
			s.pr.read(meta)
			qr.fields = meta.resultFields
		case pkResultsetID:
			s.pr.read((*resultsetID)(&qr.rsID))
		case pkResultset:
			resSet.resultFields = qr.fields
			s.pr.read(resSet)
			qr.fieldValues = resSet.fieldValues
			qr.decodeErrors = resSet.decodeErrors
			qr.attributes = ph.partAttributes
		}
	}); err != nil {
		return nil, err
	}
	if qr.rsID == 0 { // non select query
		return noResult, nil
	}
	return qr, nil
}

// ExecDirect executes a sql statement without statement parameters.
func (s *Session) ExecDirect(query string, commit bool) (driver.Result, error) {
	if err := s.pw.write(s.sessionID, mtExecuteDirect, commit, command(query)); err != nil {
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
	if err := s.pw.write(s.sessionID, mtPrepare, false, command(query)); err != nil {
		return nil, err
	}

	pr := &PrepareResult{session: s}
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
			pr.parameterFields = prmMeta.parameterFields
		}
	}); err != nil {
		return nil, err
	}
	pr.fc = s.pr.functionCode()
	return pr, nil
}

// fetchFirstLobChunk reads the first LOB data ckunk.
func (s *Session) fetchFirstLobChunk(nvargs []driver.NamedValue) (bool, error) {
	chunkSize := s.cfg.LobChunkSize
	hasNext := false

	for _, arg := range nvargs {
		if lobInDescr, ok := arg.Value.(*lobInDescr); ok {
			last, err := lobInDescr.fetchNext(chunkSize)
			if !last {
				hasNext = true
			}
			if err != nil {
				return hasNext, err
			}
		}
	}
	return hasNext, nil
}

/*
Exec executes a sql statement.

Bulk insert containing LOBs:
- Precondition:
  .Sending more than one row with partial LOB data.
- Observations:
  .In hdb version 1 and 2 'piecewise' LOB writing does work.
  .Same does not work in case of geo fields which are LOBs en,- decoded as well.
  .In hana version 4 'piecewise' LOB writing seems not to work anymore at all.
- Server implementation (not documented):
  .'piecewise' LOB writing is only suppoerted for the last row of a 'bulk insert'.
- Current implementation:
  One server call in case of
    - 'non bulk' execs or
    - 'bulk' execs without LOBs
  else potential several server calls (split into packages).
  Package invariant:
  - For all packages except the last one, the last row contains 'incomplete' LOB data ('piecewise' writing)
*/
func (s *Session) Exec(pr *PrepareResult, nvargs []driver.NamedValue, commit bool) (driver.Result, error) {
	hasLob := func() bool {
		for _, f := range pr.parameterFields {
			if f.tc.isLob() {
				return true
			}
		}
		return false
	}()

	// no split needed: no LOB or only one row
	if !hasLob || len(pr.parameterFields) == len(nvargs) {
		return s.exec(pr, nvargs, hasLob, commit)
	}

	// args need to be potentially splitted (piecewise LOB handling)
	numColumns := len(pr.parameterFields)
	numRows := len(nvargs) / numColumns
	totRowsAffected := int64(0)
	lastFrom := 0

	for i := 0; i < numRows; i++ { // row-by-row

		from := i * numColumns
		to := from + numColumns

		hasNext, err := s.fetchFirstLobChunk(nvargs[from:to])
		if err != nil {
			return nil, err
		}

		/*
			trigger server call (exec) if piecewise lob handling is needed
			or we did reach the last row
		*/
		if hasNext || i == (numRows-1) {
			r, err := s.exec(pr, nvargs[lastFrom:to], true, commit)
			if err != nil {
				return driver.RowsAffected(totRowsAffected), err
			}
			if rowsAffected, err := r.RowsAffected(); err != nil {
				totRowsAffected += rowsAffected
			}
			if err != nil {
				return driver.RowsAffected(totRowsAffected), err
			}
			lastFrom = to
		}
	}
	return driver.RowsAffected(totRowsAffected), nil
}

// exec executes an exec server call.
func (s *Session) exec(pr *PrepareResult, nvargs []driver.NamedValue, hasLob, commit bool) (driver.Result, error) {
	inputParameters, err := newInputParameters(pr.parameterFields, nvargs, hasLob)
	if err != nil {
		return nil, err
	}
	if err := s.pw.write(s.sessionID, mtExecute, commit, statementID(pr.stmtID), inputParameters); err != nil {
		return nil, err
	}

	rows := &rowsAffected{}
	var ids []locatorID
	lobReply := &writeLobReply{}
	var rowsAffected int64

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkRowsAffected:
			s.pr.read(rows)
			rowsAffected = rows.total()
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
		if err := s.encodeLobs(nil, ids, pr.parameterFields, nvargs); err != nil {
			return nil, err
		}
	}

	if fc == fcDDL {
		return driver.ResultNoRows, nil
	}
	return driver.RowsAffected(rowsAffected), nil
}

// QueryCall executes a stored procecure (by Query).
func (s *Session) QueryCall(pr *PrepareResult, nvargs []driver.NamedValue) (driver.Rows, error) {
	/*
		only in args
		invariant: #inPrmFields == #args
	*/
	var inPrmFields, outPrmFields []*ParameterField
	hasInLob := false
	for _, f := range pr.parameterFields {
		if f.In() {
			inPrmFields = append(inPrmFields, f)
			if f.tc.isLob() {
				hasInLob = true
			}
		}
		if f.Out() {
			outPrmFields = append(outPrmFields, f)
		}
	}

	if hasInLob {
		if _, err := s.fetchFirstLobChunk(nvargs); err != nil {
			return nil, err
		}
	}
	inputParameters, err := newInputParameters(inPrmFields, nvargs, hasInLob)
	if err != nil {
		return nil, err
	}
	if err := s.pw.write(s.sessionID, mtExecute, false, statementID(pr.stmtID), inputParameters); err != nil {
		return nil, err
	}

	/*
		call without lob input parameters:
		--> callResult output parameter values are set after read call
		call with lob input parameters:
		--> callResult output parameter values are set after last lob input write
	*/

	cr, ids, _, err := s.readCall(outPrmFields) // ignore numRow
	if err != nil {
		return nil, err
	}

	if len(ids) != 0 {
		/*
			writeLobParameters:
			- chunkReaders
			- cr (callResult output parameters are set after all lob input parameters are written)
		*/
		if err := s.encodeLobs(cr, ids, inPrmFields, nvargs); err != nil {
			return nil, err
		}
	}

	// legacy mode?
	if s.cfg.Legacy {
		cr.appendTableRefFields() // TODO review
		for _, qr := range cr.qrs {
			// add to cache
			QueryResultCache.set(qr.rsID, qr)
		}
	} else {
		cr.appendTableRowsFields()
	}
	return cr, nil
}

// ExecCall executes a stored procecure (by Exec).
func (s *Session) ExecCall(pr *PrepareResult, nvargs []driver.NamedValue) (driver.Result, error) {
	/*
		in,- and output args
		invariant: #prmFields == #args
	*/
	var inPrmFields, outPrmFields []*ParameterField
	var inArgs, outArgs []driver.NamedValue
	hasInLob := false
	for i, f := range pr.parameterFields {
		if f.In() {
			inPrmFields = append(inPrmFields, f)
			inArgs = append(inArgs, nvargs[i])
			if f.tc.isLob() {
				hasInLob = true
			}
		}
		if f.Out() {
			outPrmFields = append(outPrmFields, f)
			outArgs = append(outArgs, nvargs[i])
		}
	}

	// TODO release v1.0.0 - assign output parameters
	if len(outPrmFields) != 0 {
		return nil, fmt.Errorf("stmt.Exec: support of output parameters not implemented yet")
	}

	if hasInLob {
		if _, err := s.fetchFirstLobChunk(inArgs); err != nil {
			return nil, err
		}
	}
	inputParameters, err := newInputParameters(inPrmFields, inArgs, hasInLob)
	if err != nil {
		return nil, err
	}
	if err := s.pw.write(s.sessionID, mtExecute, false, statementID(pr.stmtID), inputParameters); err != nil {
		return nil, err
	}

	/*
		call without lob input parameters:
		--> callResult output parameter values are set after read call
		call with lob output parameters:
		--> callResult output parameter values are set after last lob input write
	*/

	cr, ids, numRow, err := s.readCall(outPrmFields)
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
	return driver.RowsAffected(numRow), nil
}

func (s *Session) readCall(outputFields []*ParameterField) (*callResult, []locatorID, int64, error) {
	cr := &callResult{session: s, outputFields: outputFields}

	//var qrs []*QueryResult
	var qr *queryResult
	rows := &rowsAffected{}
	var ids []locatorID
	outPrms := &outputParameters{}
	meta := &resultMetadata{}
	resSet := &resultset{}
	lobReply := &writeLobReply{}
	var numRow int64

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkRowsAffected:
			s.pr.read(rows)
			numRow = rows.total()
		case pkOutputParameters:
			outPrms.outputFields = cr.outputFields
			s.pr.read(outPrms)
			cr.fieldValues = outPrms.fieldValues
			cr.decodeErrors = outPrms.decodeErrors
		case pkResultMetadata:
			/*
				procedure call with table parameters does return metadata for each table
				sequence: metadata, resultsetID, resultset
				but:
				- resultset might not be provided for all tables
				- so, 'additional' query result is detected by new metadata part
			*/
			qr = &queryResult{session: s}
			cr.qrs = append(cr.qrs, qr)
			s.pr.read(meta)
			qr.fields = meta.resultFields
		case pkResultset:
			resSet.resultFields = qr.fields
			s.pr.read(resSet)
			qr.fieldValues = resSet.fieldValues
			qr.decodeErrors = resSet.decodeErrors
			qr.attributes = ph.partAttributes
		case pkResultsetID:
			s.pr.read((*resultsetID)(&qr.rsID))
		case pkWriteLobReply:
			s.pr.read(lobReply)
			ids = lobReply.ids
		}
	}); err != nil {
		return nil, nil, 0, err
	}
	return cr, ids, numRow, nil
}

// Query executes a query.
func (s *Session) Query(pr *PrepareResult, nvargs []driver.NamedValue, commit bool) (driver.Rows, error) {
	// allow e.g inserts as query -> handle commit like in exec

	hasLob := func() bool {
		for _, f := range pr.parameterFields {
			if f.tc.isLob() {
				return true
			}
		}
		return false
	}()

	if hasLob {
		if _, err := s.fetchFirstLobChunk(nvargs); err != nil {
			return nil, err
		}
	}
	inputParameters, err := newInputParameters(pr.parameterFields, nvargs, hasLob)
	if err != nil {
		return nil, err
	}
	if err := s.pw.write(s.sessionID, mtExecute, commit, statementID(pr.stmtID), inputParameters); err != nil {
		return nil, err
	}

	qr := &queryResult{session: s, fields: pr.resultFields}
	resSet := &resultset{}

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkResultsetID:
			s.pr.read((*resultsetID)(&qr.rsID))
		case pkResultset:
			resSet.resultFields = qr.fields
			s.pr.read(resSet)
			qr.fieldValues = resSet.fieldValues
			qr.decodeErrors = resSet.decodeErrors
			qr.attributes = ph.partAttributes
		}
	}); err != nil {
		return nil, err
	}
	if qr.rsID == 0 { // non select query
		return noResult, nil
	}
	return qr, nil
}

// FetchNext fetches next chunk in query result set.
func (s *Session) fetchNext(qr *queryResult) error {
	if err := s.pw.write(s.sessionID, mtFetchNext, false, resultsetID(qr.rsID), fetchsize(s.cfg.FetchSize)); err != nil {
		return err
	}

	resSet := &resultset{resultFields: qr.fields, fieldValues: qr.fieldValues} // reuse field values

	return s.pr.iterateParts(func(ph *partHeader) {
		if ph.partKind == pkResultset {
			s.pr.read(resSet)
			qr.fieldValues = resSet.fieldValues
			qr.decodeErrors = resSet.decodeErrors
			qr.attributes = ph.partAttributes
		}
	})
}

// DropStatementID releases the hdb statement handle.
func (s *Session) DropStatementID(id uint64) error {
	if err := s.pw.write(s.sessionID, mtDropStatementID, false, statementID(id)); err != nil {
		return err
	}
	return s.pr.readSkip()
}

// CloseResultsetID releases the hdb resultset handle.
func (s *Session) CloseResultsetID(id uint64) error {
	if err := s.pw.write(s.sessionID, mtCloseResultset, false, resultsetID(id)); err != nil {
		return err
	}
	return s.pr.readSkip()
}

// Commit executes a database commit.
func (s *Session) Commit() error {
	if err := s.pw.write(s.sessionID, mtCommit, false); err != nil {
		return err
	}
	if err := s.pr.readSkip(); err != nil {
		return err
	}
	return nil
}

// Rollback executes a database rollback.
func (s *Session) Rollback() error {
	if err := s.pw.write(s.sessionID, mtRollback, false); err != nil {
		return err
	}
	if err := s.pr.readSkip(); err != nil {
		return err
	}
	return nil
}

// Disconnect disconnects the session.
func (s *Session) Disconnect() error {
	if err := s.pw.write(s.sessionID, mtDisconnect, false); err != nil {
		return err
	}
	/*
		Do not read server reply as on slow connections the TCP/IP connection is closed (by Server)
		before the reply can be read completely.

		// if err := s.pr.readSkip(); err != nil {
		// 	return err
		// }

	*/
	return nil
}

// DBConnectInfo provided hdb connection information.
func (s *Session) DBConnectInfo(databaseName string) (*hdb.DBConnectInfo, error) {
	ci := dbConnectInfo{int8(ciDatabaseName): optStringType(databaseName)}
	if err := s.pw.write(s.sessionID, mtDBConnectInfo, false, ci); err != nil {
		return nil, err
	}

	if err := s.pr.iterateParts(func(ph *partHeader) {
		switch ph.partKind {
		case pkDBConnectInfo:
			s.pr.read(&ci)
		}
	}); err != nil {
		return nil, err
	}

	return &hdb.DBConnectInfo{
		DatabaseName: databaseName,
		Host:         plainOptions(ci).asString(int8(ciHost)),
		Port:         plainOptions(ci).asInt(int8(ciPort)),
		IsConnected:  plainOptions(ci).asBool(int8(ciIsConnected)),
	}, nil
}

// decodeLobs decodes (reads from db) output lob or result lob parameters.

// read lob reply
// - seems like readLobreply returns only a result for one lob - even if more then one is requested
// --> read single lobs
func (s *Session) decodeLobs(descr *lobOutDescr, wr io.Writer) error {
	var err error

	if descr.isCharBased {
		wrcl := transform.NewWriter(wr, s.cfg.CESU8Decoder()) // CESU8 transformer
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
	lobChunkSize := int64(s.cfg.LobChunkSize)

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
func (s *Session) encodeLobs(cr *callResult, ids []locatorID, inPrmFields []*ParameterField, nvargs []driver.NamedValue) error {

	chunkSize := s.cfg.LobChunkSize

	descrs := make([]*writeLobDescr, 0, len(ids))

	numInPrmField := len(inPrmFields)

	j := 0
	for i, arg := range nvargs { // range over args (mass / bulk operation)
		f := inPrmFields[i%numInPrmField]
		if f.tc.isLob() {
			lobInDescr, ok := arg.Value.(*lobInDescr)
			if !ok {
				return fmt.Errorf("protocol error: invalid lob parameter %[1]T %[1]v - *lobInDescr expected", arg)
			}
			if j >= len(ids) {
				return fmt.Errorf("protocol error: invalid number of lob parameter ids %d", len(ids))
			}
			descrs = append(descrs, &writeLobDescr{lobInDescr: lobInDescr, id: ids[j]})
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
		for _, descr := range descrs {
			if err := descr.fetchNext(chunkSize); err != nil {
				return err
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
				cr.decodeErrors = outPrms.decodeErrors
			case pkWriteLobReply:
				s.pr.read(lobReply)
				ids = lobReply.ids
			}
		}); err != nil {
			return err
		}

		// remove done descr
		j := 0
		for _, descr := range descrs {
			if !descr.opt.isLastData() {
				descrs[j] = descr
				j++
			}
		}
		descrs = descrs[:j]
	}
	return nil
}
