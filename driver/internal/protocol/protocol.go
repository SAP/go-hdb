package protocol

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"golang.org/x/text/transform"
)

const (
	clientPrefix = "→"
	dbPrefix     = "←"
)

// padding
const padding = 8

func padBytes(size int) int {
	if r := size % padding; r != 0 {
		return padding - r
	}
	return 0
}

// Reader is the protocol reader interface.
type Reader interface {
	ReadProlog() error
	IterateParts(partFn func(ph *PartHeader)) error
	Read(part partReader) error
	ReadSkip() error
	SessionID() int64
	FunctionCode() FunctionCode
}

// Writer is the protocol writer interface.
type Writer interface {
	WriteProlog() error
	Write(sessionID int64, messageType MessageType, commit bool, writers ...partWriter) error
}

type baseReader struct {
	tracer      *log.Logger
	tracePrefix string

	dec *encoding.Decoder

	mh *messageHeader
	sh *segmentHeader
	ph *PartHeader

	readBytes int64
	numPart   int
	cntPart   int
	partRead  bool

	partReaderCache map[PartKind]partReader

	lastErrors       *HdbErrors
	lastRowsAffected *RowsAffected

	// partReader read errors could be
	// - read buffer errors -> buffer Error() and ResetError()
	// - plus other errors (which cannot be ignored, e.g. Lob reader)
	err error
}

func newBaseReader(rd io.Reader, tracer *log.Logger, tracePrefix string, decoder func() transform.Transformer) *baseReader {
	return &baseReader{
		tracer:          tracer,
		tracePrefix:     tracePrefix,
		dec:             encoding.NewDecoder(rd, decoder),
		partReaderCache: map[PartKind]partReader{},
		mh:              &messageHeader{},
		sh:              &segmentHeader{},
		ph:              &PartHeader{},
	}
}

type dbReader struct {
	*baseReader
}
type clientReader struct {
	*baseReader
}

// NewDBReader returns an instance of a database protocol reader.
func NewDBReader(rd io.Reader, tracer *log.Logger, decoder func() transform.Transformer) Reader {
	return &dbReader{baseReader: newBaseReader(rd, tracer, dbPrefix, decoder)}
}

// NewClientReader returns an instance of a client protocol reader.
func NewClientReader(rd io.Reader, tracer *log.Logger, decoder func() transform.Transformer) Reader {
	return &clientReader{baseReader: newBaseReader(rd, tracer, clientPrefix, decoder)}
}

func (r *baseReader) ReadSkip() error            { return r.IterateParts(nil) }
func (r *baseReader) SessionID() int64           { return r.mh.sessionID }
func (r *baseReader) FunctionCode() FunctionCode { return r.sh.functionCode }

func (r *dbReader) ReadProlog() error {
	rep := &initReply{}
	if err := rep.decode(r.dec); err != nil {
		return err
	}
	if r.tracer != nil {
		r.tracer.Printf(fmt.Sprintf("%sINI %s", r.tracePrefix, rep))
	}
	return nil
}
func (r *clientReader) ReadProlog() error {
	req := &initRequest{}
	if err := req.decode(r.dec); err != nil {
		return err
	}
	if r.tracer != nil {
		r.tracer.Printf(fmt.Sprintf("%sINI %s", r.tracePrefix, req))
	}
	return nil
}

func (r *baseReader) checkError() error {
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
		for i, rows := range r.lastRowsAffected.rows {
			if rows == RaExecutionFailed {
				r.lastErrors.setStmtNo(j, r.lastRowsAffected.Ofs+i)
				j++
			}
		}
	}
	return r.lastErrors
}

func (r *baseReader) Read(part partReader) error {
	r.partRead = true

	err := r.readPart(part)
	if err != nil {
		r.err = err
	}

	switch part := part.(type) {
	case *HdbErrors:
		r.lastErrors = part
	case *RowsAffected:
		r.lastRowsAffected = part
	}
	return err
}

func (r *baseReader) skip() error {
	pk := r.ph.PartKind

	// if trace is on or mandatory parts need to be read we cannot skip
	if !(r.tracer != nil || pk == PkError || pk == PkRowsAffected) {
		return r.skipPart()
	}

	// check part cache
	if part, ok := r.partReaderCache[pk]; ok {
		return r.Read(part)
	}

	part := newGenPartReader(pk)
	if part == nil { // part cannot be instantiated generically -> skip
		return r.skipPart()
	}

	// cache part
	r.partReaderCache[pk] = part

	return r.Read(part)
}

func (r *baseReader) skipPadding() int64 {
	if r.cntPart != r.numPart { // padding if not last part
		padBytes := padBytes(int(r.ph.bufferLength))
		r.dec.Skip(padBytes)
		return int64(padBytes)
	}

	// last part:
	// skip difference between real read bytes and message header var part length
	padBytes := int64(r.mh.varPartLength) - r.readBytes
	switch {
	case padBytes < 0:
		panic(fmt.Errorf("protocol error: bytes read %d > variable part length %d", r.readBytes, r.mh.varPartLength))
	case padBytes > 0:
		r.dec.Skip(int(padBytes))
	}
	return padBytes
}

func (r *baseReader) skipPart() error {
	r.dec.ResetCnt()
	r.dec.Skip(int(r.ph.bufferLength))
	if r.tracer != nil {
		r.tracer.Printf(fmt.Sprintf("*skipped %s", r.ph.PartKind))
	}
	r.readBytes += int64(r.dec.Cnt())
	r.readBytes += r.skipPadding()
	return nil
}

func (r *baseReader) readPart(part partReader) error {
	r.dec.ResetCnt()
	err := part.decode(r.dec, r.ph) // do not return here in case of error -> read stream would be broken
	cnt := r.dec.Cnt()

	if r.tracer != nil {
		r.tracer.Printf(fmt.Sprintf("     %s", part))
	}

	bufferLen := int(r.ph.bufferLength)
	switch {
	case cnt < bufferLen: // protocol buffer length > read bytes -> skip the unread bytes
		r.dec.Skip(bufferLen - cnt)
	case cnt > bufferLen: // read bytes > protocol buffer length -> should never happen
		panic(fmt.Errorf("protocol error: read bytes %d > buffer length %d", cnt, bufferLen))
	}

	r.readBytes += int64(r.dec.Cnt())
	r.readBytes += r.skipPadding()
	return err
}

func (r *baseReader) IterateParts(partFn func(ph *PartHeader)) error {
	if err := r.mh.decode(r.dec); err != nil {
		return err
	}
	r.readBytes = 0 // header bytes are not calculated in header varPartBytes: start with zero
	if r.tracer != nil {
		r.tracer.Printf(fmt.Sprintf("%sMSG %s", r.tracePrefix, r.mh))
	}

	for i := 0; i < int(r.mh.noOfSegm); i++ {
		if err := r.sh.decode(r.dec); err != nil {
			return err
		}

		r.readBytes += segmentHeaderSize

		if r.tracer != nil {
			r.tracer.Printf(fmt.Sprintf(" SEG %s", r.sh))
		}

		r.numPart = int(r.sh.noOfParts)
		r.cntPart = 0

		for j := 0; j < int(r.sh.noOfParts); j++ {

			if err := r.ph.decode(r.dec); err != nil {
				return err
			}

			r.readBytes += partHeaderSize

			if r.tracer != nil {
				r.tracer.Printf(fmt.Sprintf(" PAR %s", r.ph))
			}

			r.cntPart++

			r.partRead = false
			if partFn != nil {
				partFn(r.ph)
			}
			if !r.partRead {
				r.skip()
			}
		}
	}
	return r.checkError()
}

// writer represents a protocol writer.
type writer struct {
	tracer *log.Logger

	wr  *bufio.Writer
	enc *encoding.Encoder

	sv     map[string]string
	svSent bool

	// reuse header
	mh *messageHeader
	sh *segmentHeader
	ph *PartHeader
}

// NewWriter returns an instance of a protocol writer.
func NewWriter(wr *bufio.Writer, tracer *log.Logger, encoder func() transform.Transformer, sv map[string]string) Writer {
	return &writer{
		tracer: tracer,
		wr:     wr,
		sv:     sv,
		enc:    encoding.NewEncoder(wr, encoder),
		mh:     new(messageHeader),
		sh:     new(segmentHeader),
		ph:     new(PartHeader),
	}
}

const (
	productVersionMajor  = 4
	productVersionMinor  = 20
	protocolVersionMajor = 4
	protocolVersionMinor = 1
)

func (w *writer) WriteProlog() error {
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
	if w.tracer != nil {
		w.tracer.Printf(fmt.Sprintf("%sINI %s", clientPrefix, req))
	}
	return w.wr.Flush()
}

func (w *writer) Write(sessionID int64, messageType MessageType, commit bool, writers ...partWriter) error {
	// check on session variables to be send as ClientInfo
	if w.sv != nil && !w.svSent && messageType.ClientInfoSupported() {
		writers = append([]partWriter{clientInfo(w.sv)}, writers...)
		w.svSent = true
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
	if w.tracer != nil {
		w.tracer.Printf(fmt.Sprintf("%sMSG %s", clientPrefix, w.mh))
	}

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
	if w.tracer != nil {
		w.tracer.Printf(fmt.Sprintf(" SEG %s", w.sh))
	}

	bufferSize -= segmentHeaderSize

	for i, part := range writers {

		size := partSize[i]
		pad := padBytes(size)

		w.ph.PartKind = part.kind()
		if err := w.ph.setNumArg(part.numArg()); err != nil {
			return err
		}
		w.ph.bufferLength = int32(size)
		w.ph.bufferSize = int32(bufferSize)

		if err := w.ph.encode(w.enc); err != nil {
			return err
		}
		if w.tracer != nil {
			w.tracer.Printf(fmt.Sprintf(" PAR %s", w.ph))
		}

		if err := part.encode(w.enc); err != nil {
			return err
		}
		if w.tracer != nil {
			w.tracer.Printf(fmt.Sprintf("     %s", part))
		}

		w.enc.Zeroes(pad)

		bufferSize -= int64(partHeaderSize + size + pad)
	}
	return w.wr.Flush()
}
