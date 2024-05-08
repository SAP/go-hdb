package protocol

import (
	"bufio"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"math"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"golang.org/x/text/transform"
)

const (
	traceMsg = "PROT"

	prefixDB     = "←"
	prefixClient = "→"

	textIni    = "INI"
	textMsgHdr = "MSH"
	textSegHdr = "SGH"
	textParHdr = "PRH"
	textPar    = "PRT"
	textSkip   = "*skipped"
)

// padding.
const padding = 8

func padBytes(size int) int {
	if r := size % padding; r != 0 {
		return padding - r
	}
	return 0
}

type partCache map[PartKind]Part

func (c *partCache) get(kind PartKind) (Part, bool) {
	if part, ok := (*c)[kind]; ok {
		return part, true
	}
	part := newGenPartReader(kind)
	if part == nil { // part cannot be instantiated generically
		return nil, false
	}
	(*c)[kind] = part
	return part, true
}

// Reader represents a protocol reader.
type Reader struct {
	dec       *encoding.Decoder
	readFn    lobReadFn
	protTrace bool
	logger    *slog.Logger

	prefix string
	// ReadProlog reads the protocol prolog.
	ReadProlog func(ctx context.Context) error

	mh *messageHeader
	sh *segmentHeader
	ph *partHeader

	partCache partCache
}

func newReader(dec *encoding.Decoder, readFn lobReadFn, protTrace bool, logger *slog.Logger) *Reader {
	return &Reader{
		dec:       dec,
		readFn:    readFn,
		protTrace: protTrace,
		logger:    logger,
		partCache: partCache{},
		mh:        &messageHeader{},
		sh:        &segmentHeader{},
		ph:        &partHeader{},
	}
}

// NewDBReader returns an instance of a database protocol reader.
func NewDBReader(dec *encoding.Decoder, readFn lobReadFn, protTrace bool, logger *slog.Logger) *Reader {
	reader := newReader(dec, readFn, protTrace, logger)
	reader.ReadProlog = reader.readPrologDB
	reader.prefix = prefixDB
	return reader
}

// NewClientReader returns an instance of a client protocol reader.
func NewClientReader(dec *encoding.Decoder, readFn lobReadFn, protTrace bool, logger *slog.Logger) *Reader {
	reader := newReader(dec, readFn, protTrace, logger)
	reader.ReadProlog = reader.readPrologClient
	reader.prefix = prefixClient
	return reader
}

// SkipParts reads and discards all protocol parts.
func (r *Reader) SkipParts(ctx context.Context) error { return r.IterateParts(ctx, nil) }

// SessionID returns the session ID.
func (r *Reader) SessionID() int64 { return r.mh.sessionID }

// FunctionCode returns the function code of the protocol.
func (r *Reader) FunctionCode() FunctionCode { return r.sh.functionCode }

func (r *Reader) readPrologDB(ctx context.Context) error {
	rep := &initReply{}
	if err := rep.decode(r.dec); err != nil {
		return err
	}
	if r.protTrace {
		r.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(r.prefix+textIni, rep.String()))
	}
	return nil
}
func (r *Reader) readPrologClient(ctx context.Context) error {
	req := &initRequest{}
	if err := req.decode(r.dec); err != nil {
		return err
	}
	if r.protTrace {
		r.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(r.prefix+textIni, req.String()))
	}
	return nil
}

func (r *Reader) skipPadding() int {
	padBytes := padBytes(int(r.ph.bufferLength))
	r.dec.Skip(padBytes)
	return padBytes
}

func (r *Reader) skipPaddingLastPart(numReadByte int64) {
	// last part:
	// skip difference between real read bytes and message header var part length
	padBytes := int64(r.mh.varPartLength) - numReadByte
	switch {
	case padBytes < 0:
		panic(fmt.Errorf("protocol error: bytes read %d > variable part length %d", numReadByte, r.mh.varPartLength))
	case padBytes > 0:
		r.dec.Skip(int(padBytes))
	}
}

func (r *Reader) readPart(ctx context.Context, part Part) error {
	cntBefore := r.dec.Cnt()

	var err error
	switch part := part.(type) {
	// do not return here in case of error -> read stream would be broken
	case partDecoder:
		err = part.decode(r.dec)
	case bufLenPartDecoder:
		err = part.decodeBufLen(r.dec, r.ph.bufLen())
	case numArgPartDecoder:
		err = part.decodeNumArg(r.dec, r.ph.numArg())
	case resultPartDecoder:
		err = part.decodeResult(r.dec, r.ph.numArg(), r.readFn)
	default:
		panic(fmt.Errorf("invalid part decoder %[1]T %[1]v", part))
	}
	// do not return here in case of error -> read stream would be broken

	cnt := r.dec.Cnt() - cntBefore

	if r.protTrace {
		r.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(r.prefix+textPar, part.String()))
	}

	bufferLen := int(r.ph.bufferLength)
	switch {
	case cnt < bufferLen: // protocol buffer length > read bytes -> skip the unread bytes
		r.dec.Skip(bufferLen - cnt)
	case cnt > bufferLen: // read bytes > protocol buffer length -> should never happen
		panic(fmt.Errorf("protocol error: read bytes %d > buffer length %d", cnt, bufferLen))
	}
	return err
}

// IterateParts iterates through all protocol parts.
func (r *Reader) IterateParts(ctx context.Context, fn func(kind PartKind, attrs PartAttributes, read func(part Part))) error {
	var lastErrors *HdbErrors
	var lastRowsAffected *RowsAffected

	if err := r.mh.decode(r.dec); err != nil {
		return err
	}

	var numReadByte int64 = 0 // header bytes are not calculated in header varPartBytes: start with zero
	if r.protTrace {
		r.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(r.prefix+textMsgHdr, r.mh.String()))
	}

	for i := 0; i < int(r.mh.noOfSegm); i++ {
		if err := r.sh.decode(r.dec); err != nil {
			return err
		}

		numReadByte += segmentHeaderSize

		if r.protTrace {
			r.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(r.prefix+textSegHdr, r.sh.String()))
		}

		lastPart := int(r.sh.noOfParts) - 1
		for j := 0; j <= lastPart; j++ {
			if err := r.ph.decode(r.dec); err != nil {
				return err
			}
			kind := r.ph.partKind

			numReadByte += partHeaderSize

			if r.protTrace {
				r.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(r.prefix+textParHdr, r.ph.String()))
			}

			cntBefore := r.dec.Cnt()

			partRequested := false
			if kind != PkError && fn != nil { // caller must not handle hdb errors
				var err error
				fn(kind, r.ph.partAttributes, func(part Part) {
					partRequested = true
					err = r.readPart(ctx, part)
					if part.kind() == PkRowsAffected {
						lastRowsAffected = part.(*RowsAffected)
					}
				})
				if err != nil {
					return err
				}
			}
			if !partRequested {
				// if trace is on or mandatory parts need to be read we cannot skip
				if !(r.protTrace || kind == PkError || kind == PkRowsAffected) {
					r.dec.Skip(int(r.ph.bufferLength))
				} else {
					if part, ok := r.partCache.get(kind); ok {
						if err := r.readPart(ctx, part); err != nil {
							return err
						}
						switch kind {
						case PkError:
							lastErrors = part.(*HdbErrors)
						case PkRowsAffected:
							lastRowsAffected = part.(*RowsAffected)
						}
					} else {
						r.dec.Skip(int(r.ph.bufferLength))
						if r.protTrace {
							r.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(r.prefix+textSkip, kind.String()))
						}
					}
				}
			}

			numReadByte += int64(r.dec.Cnt()) - int64(cntBefore)

			if j != lastPart { // not last part
				numReadByte += int64(r.skipPadding())
			}

		}
	}

	r.skipPaddingLastPart(numReadByte)

	if err := r.dec.Error(); err != nil {
		r.dec.ResetError()
		return err
	}

	if lastErrors == nil {
		return nil
	}

	if lastRowsAffected != nil { // link statement to error
		j := 0
		for i, rows := range lastRowsAffected.rows {
			if rows == RaExecutionFailed {
				lastErrors.setStmtNo(j, lastRowsAffected.Ofs+i)
				j++
			}
		}
	}
	if lastErrors.onlyWarnings {
		for _, err := range lastErrors.errs {
			r.logger.LogAttrs(ctx, slog.LevelWarn, err.Error())
		}
		return nil
	}
	return lastErrors
}

// Writer represents a protocol writer.
type Writer struct {
	protTrace bool
	logger    *slog.Logger

	wr  *bufio.Writer
	enc *encoding.Encoder

	sv     map[string]string
	svSent bool

	// reuse header
	mh *messageHeader
	sh *segmentHeader
	ph *partHeader
}

// NewWriter returns an instance of a protocol writer.
func NewWriter(wr *bufio.Writer, enc *encoding.Encoder, protTrace bool, logger *slog.Logger, encoder func() transform.Transformer, sv map[string]string) *Writer {
	return &Writer{
		protTrace: protTrace,
		logger:    logger,
		wr:        wr,
		sv:        sv,
		enc:       enc,
		mh:        new(messageHeader),
		sh:        new(segmentHeader),
		ph:        new(partHeader),
	}
}

const (
	productVersionMajor  = 4
	productVersionMinor  = 20
	protocolVersionMajor = 4
	protocolVersionMinor = 1
)

// WriteProlog writes the protocol prolog.
func (w *Writer) WriteProlog(ctx context.Context) error {
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
	if w.protTrace {
		w.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(prefixClient+textIni, req.String()))
	}
	return w.wr.Flush()
}

func (w *Writer) _write(ctx context.Context, sessionID int64, messageType MessageType, commit bool, parts ...partEncoder) error {
	// check on session variables to be send as ClientInfo
	if w.sv != nil && !w.svSent && messageType.ClientInfoSupported() {
		parts = append([]partEncoder{(*clientInfo)(&w.sv)}, parts...)
		w.svSent = true
	}

	numPart := len(parts)
	partSize := make([]int, numPart)
	size := int64(segmentHeaderSize + numPart*partHeaderSize) // int64 to hold MaxUInt32 in 32bit OS

	for i, part := range parts {
		s := part.size()
		size += int64(s + padBytes(s))
		partSize[i] = s // buffer size (expensive calculation)
	}

	if size > math.MaxUint32 {
		return fmt.Errorf("message size %d exceeds maximum message header value %d", size, int64(math.MaxUint32)) // int64: without cast overflow error in 32bit OS
	}

	bufferSize := size

	w.mh.sessionID = sessionID
	w.mh.varPartLength = uint32(size)
	w.mh.varPartSize = uint32(bufferSize)
	w.mh.noOfSegm = 1

	if err := w.mh.encode(w.enc); err != nil {
		return err
	}
	if w.protTrace {
		w.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(prefixClient+textMsgHdr, w.mh.String()))
	}

	if size > math.MaxInt32 {
		return fmt.Errorf("message size %d exceeds maximum part header value %d", size, math.MaxInt32)
	}

	w.sh.messageType = messageType
	w.sh.commit = commit
	w.sh.segmentKind = skRequest
	w.sh.segmentLength = int32(size)
	w.sh.segmentOfs = 0
	w.sh.noOfParts = int16(numPart)
	w.sh.segmentNo = 1

	if err := w.sh.encode(w.enc); err != nil {
		return err
	}
	if w.protTrace {
		w.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(prefixClient+textSegHdr, w.sh.String()))
	}

	bufferSize -= segmentHeaderSize

	for i, part := range parts {
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
		if w.protTrace {
			w.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(prefixClient+textParHdr, w.ph.String()))
		}

		if err := part.encode(w.enc); err != nil {
			return err
		}
		if w.protTrace {
			w.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(prefixClient+textPar, part.String()))
		}

		w.enc.Zeroes(pad)

		bufferSize -= int64(partHeaderSize + size + pad)
	}
	return w.wr.Flush()
}

func (w *Writer) Write(ctx context.Context, sessionID int64, messageType MessageType, commit bool, parts ...partEncoder) error {
	if err := w._write(ctx, sessionID, messageType, commit, parts...); err != nil {
		return errors.Join(err, driver.ErrBadConn)
	}
	return nil
}
