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

// TODO Sniffer
/*
sniffer:
- complete for go-hdb: especially call with table parameters
- delete caches for statement and result
- don't ignore part read error
  - example: read scramsha256InitialReply got silently stuck because methodname check failed
- test with python client and handle surprises
  - analyze for not ignoring part read errors
*/

import (
	"bufio"
	"io"
	"net"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

// A Sniffer is a simple proxy for logging hdb protocol requests and responses.
type Sniffer struct {
	conn   net.Conn
	dbConn net.Conn

	//client
	clRd *bufio.Reader
	clWr *bufio.Writer
	//database
	dbRd *bufio.Reader
	dbWr *bufio.Writer

	// reader
	upRd   *sniffUpReader
	downRd *sniffDownReader
}

// NewSniffer creates a new sniffer instance. The conn parameter is the net.Conn connection, where the Sniffer
// is listening for hdb protocol calls. The dbAddr is the hdb host port address in "host:port" format.
func NewSniffer(conn net.Conn, dbConn net.Conn) *Sniffer {

	//TODO - review setting values here
	trace = true
	debug = true

	s := &Sniffer{
		conn:   conn,
		dbConn: dbConn,
		// buffered write to client
		clWr: bufio.NewWriter(conn),
		// buffered write to db
		dbWr: bufio.NewWriter(dbConn),
	}

	//read from client connection and write to db buffer
	s.clRd = bufio.NewReader(io.TeeReader(conn, s.dbWr))
	//read from db and write to client connection buffer
	s.dbRd = bufio.NewReader(io.TeeReader(dbConn, s.clWr))

	s.upRd = newSniffUpReader(s.clRd)
	s.downRd = newSniffDownReader(s.dbRd, s.upRd)

	return s
}

// Do starts the protocol request and response logging.
func (s *Sniffer) Do() error {
	defer s.dbConn.Close()
	defer s.conn.Close()

	s.upRd.readInitRequest()
	if err := s.dbWr.Flush(); err != nil {
		return err
	}
	s.downRd.readInitReply()
	if err := s.clWr.Flush(); err != nil {
		return err
	}

	for {
		//up stream
		if err := s.upRd.readMsg(); err != nil {
			return err // err == io.EOF: connection closed by client
		}
		if err := s.dbWr.Flush(); err != nil {
			return err
		}
		//down stream
		if err := s.downRd.readMsg(); err != nil {
			if _, ok := err.(*hdbErrors); !ok { //if hdbErrors continue
				return err
			}
		}
		if err := s.clWr.Flush(); err != nil {
			return err
		}
	}
}

type sniffReader struct {
	dec      *encoding.Decoder
	tracer   traceLogger
	msgIter  *msgIter
	segIter  *segIter
	partIter *partIter

	*partCache

	step int
}

func newSniffReader(upStream bool, rd *bufio.Reader) *sniffReader {
	tracer := newTraceLogger(upStream)
	dec := encoding.NewDecoder(rd)
	partIter := newPartIter(dec, tracer)
	segIter := newSegIter(partIter, dec, tracer)
	msgIter := newMsgIter(segIter, dec, tracer)
	return &sniffReader{
		dec:       dec,
		tracer:    tracer,
		partCache: newPartCache(),
		partIter:  partIter,
		segIter:   segIter,
		msgIter:   msgIter,
	}
}

type sniffUpReader struct {
	*sniffReader
	mt   messageType
	rsID uint64
}

func newSniffUpReader(rd *bufio.Reader) *sniffUpReader {
	return &sniffUpReader{sniffReader: newSniffReader(true, rd)}
}

type sniffDownReader struct {
	*sniffReader

	upRd *sniffUpReader
}

func newSniffDownReader(rd *bufio.Reader, upRd *sniffUpReader) *sniffDownReader {
	return &sniffDownReader{sniffReader: newSniffReader(false, rd), upRd: upRd}
}

func (r *sniffUpReader) readMsg() error {
	if !r.msgIter.next() {
		return r.dec.ResetError()
	}
	r.segIter.next()
	if r.segIter.sh.segmentKind != skRequest {
		panic("segment type request expected")
	}

	var resultFields []*resultField

	for r.partIter.next() {
		switch r.partIter.partKind() {

		case pkResultMetadata:
			r.read(r.resultMetadata)
			resultFields = r.resultMetadata.resultFields

		case pkResultset:
			r.resultset.resultFields = resultFields

			for _, f := range resultFields {
				println(f.String())
			}

			r.read(r.resultset)

		default:
			r.skip()
		}
	}
	return r.dec.ResetError()
}

func (r *sniffDownReader) readMsg() error {
	if !r.msgIter.next() {
		return r.dec.ResetError()
	}
	r.segIter.next()
	fc := r.segIter.functionCode()
	switch fc {
	// TODO - check fc

	}

	var resultFields []*resultField

	for r.partIter.next() {
		switch r.partIter.partKind() {

		case pkResultMetadata:
			r.read(r.resultMetadata)
			resultFields = r.resultMetadata.resultFields

		case pkResultset:
			r.resultset.resultFields = resultFields

			for _, f := range resultFields {
				println(f.String())
			}

			println("R E A D  R E S U L T")

			r.read(r.resultset)

		default:
			r.skip()
		}
	}
	return r.dec.ResetError()
}

func (r *sniffUpReader) readInitRequest() {
	req := &initRequest{}
	req.decode(r.dec)
	r.tracer.Log(req)
}

func (r *sniffDownReader) readInitReply() {
	rep := &initReply{}
	rep.decode(r.dec)
	r.tracer.Log(rep)
}

func (r *sniffReader) read(part partReader) {
	r.partIter.read(part)
}

func (r *sniffReader) skip() {
	pk := r.partIter.partKind()

	skip := pk == pkWriteLobRequest ||
		pk == pkReadLobRequest ||
		pk == pkReadLobReply ||
		// pkParameterMetadata ||
		// pk == pkParameters ||
		// pk == pkResultMetadata ||
		// pk == pkResultset ||
		// pk == pkOutputParameters ||
		pk == pkStatementContext //TODO python client sends TinyInts (not supported yet)

	if skip {
		r.partIter.skip()
	} else {
		part, ok := r.partCache.get(pk)
		if ok {
			r.partIter.read(part)
		} else {
			r.partIter.skip()
		}
	}
}
