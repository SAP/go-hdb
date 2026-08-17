package protocol

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"math"
	"reflect"
	"slices"

	"github.com/SAP/go-hdb/driver/compress"
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

// Compression thresholds for outbound packets. Mirror the SAP HANA C++
// client (hdbcli, SocketCommunication.cpp).
const (
	// minCompressBlockSize: packets with a varpart smaller than this are
	// sent uncompressed. Mirrors MIN_COMPRESS_PKT_LEN
	// (SocketCommunication.cpp:56, = 10 KiB) — avoids the cost of
	// compressing tiny payloads where the savings would be negligible.
	minCompressBlockSize = 10 * 1024

	// minCompressionSizePercent: maximum allowed compressed size as a
	// percentage of the input. Mirrors MIN_COMPRESSION_SIZE_PCT
	// (SocketCommunication.cpp:376, = 95). The destination buffer is
	// sized to 95% of input; if LZ4 cannot fit the output into that
	// bound, CompressBlock returns lz4.ErrShortBuffer and we send
	// uncompressed. Compression must save at least 5% to be used.
	minCompressionSizePercent = 95
)

type partCache map[PartKind]Part

func (c *partCache) get(kind PartKind) (Part, bool) {
	if part, ok := (*c)[kind]; ok {
		return part, true
	}
	if kind == PkAuthentication {
		return nil, false // cannot instantiate generically
	}
	pt, ok := genPartTypeMap[kind]
	if !ok {
		// whether part cannot be instantiated generically or
		// part is not (yet) known to the driver
		return nil, false
	}
	// create instance
	part, ok := reflect.TypeAssert[Part](reflect.New(pt))
	if !ok {
		panic("part kind does not implement part reader interface") // should never happen
	}
	if part, ok := part.(initer); ok {
		part.init()
	}
	(*c)[kind] = part
	return part, true
}

// ReaderAttrs holds reader attributes.
type ReaderAttrs struct {
	protTrace       bool
	logger          *slog.Logger
	tr              transform.Transformer
	lobChunkSize    int
	emptyDateAsNull bool
	compressor      compress.Compressor
	alphanumDfv1    bool
}

// NewReaderAttrs returns a new ReaderAttrs instance.
func NewReaderAttrs(protTrace bool, logger *slog.Logger, tr transform.Transformer, lobChunkSize int, emptyDateAsNull bool, compressor compress.Compressor) *ReaderAttrs {
	return &ReaderAttrs{
		protTrace:       protTrace,
		logger:          logger,
		tr:              tr,
		lobChunkSize:    lobChunkSize,
		emptyDateAsNull: emptyDateAsNull,
		compressor:      compressor,
	}
}

// SetAlphanumDfv1 sets alphanumDfv1.
func (a *ReaderAttrs) SetAlphanumDfv1(b bool) {
	a.alphanumDfv1 = b
}

// Reader represents a protocol reader.
type Reader struct {
	rd io.Reader

	attrs *ReaderAttrs

	readPrologFn func(ctx context.Context) error
	protTraceFn  func(ctx context.Context, text string, part fmt.Stringer)

	mh *messageHeader
	sh *segmentHeader

	tmpBuf  []byte
	scratch []byte

	partCache partCache
	partInfo  *PartInfo
}

func newReader(rd io.Reader, attrs *ReaderAttrs, readFromDB bool) *Reader {
	partInfo := &PartInfo{Header: &PartHeader{}}

	r := &Reader{
		rd:        rd,
		attrs:     attrs,
		mh:        &messageHeader{},
		sh:        &segmentHeader{},
		scratch:   make([]byte, 32),
		partCache: partCache{},
		partInfo:  partInfo,
	}

	if readFromDB {
		r.readPrologFn = r.readPrologDB
		if attrs.protTrace {
			r.protTraceFn = r.protTraceDB
		}
	} else {
		r.readPrologFn = r.readPrologClient
		if attrs.protTrace {
			r.protTraceFn = r.protTraceClient
		}
	}

	partInfo.ReadHDBErrors = r.readHDBErrors
	partInfo.ReadPart = r.readPart
	partInfo.ReadResultPart = r.readResultPart
	partInfo.SkipPart = r.skipPart

	return r
}

// NewDBReader returns an instance of a database protocol reader.
func NewDBReader(rd io.Reader, attrs *ReaderAttrs) *Reader { return newReader(rd, attrs, true) }

// NewClientReader returns an instance of a client protocol reader.
func NewClientReader(rd io.Reader, attrs *ReaderAttrs) *Reader { return newReader(rd, attrs, false) }

// SessionID returns the session ID.
func (r *Reader) SessionID() int64 { return r.mh.sessionID }

// FunctionCode returns the function code of the protocol.
func (r *Reader) FunctionCode() FunctionCode { return r.sh.functionCode }

// ReadProlog reads the protocol prolog.
func (r *Reader) ReadProlog(ctx context.Context) error {
	return r.readPrologFn(ctx)
}

// SkipParts reads and discards all protocol parts.
func (r *Reader) SkipParts(ctx context.Context) error {
	for pi, err := range r.Parts(ctx) {
		if err != nil {
			return err
		}
		switch pi.Header.Kind() {
		case PkError:
			err = r.readHDBErrors(ctx)
		default:
			err = r.skipPart(ctx)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Parts iterates through all protocol parts.
func (r *Reader) Parts(ctx context.Context) iter.Seq2[*PartInfo, error] {

	readHeader := func(ctx context.Context) error {

		dec := encoding.Decoder(r.scratch[:messageHeaderSize])
		if _, err := io.ReadFull(r.rd, dec); err != nil {
			return err
		}
		r.mh.decode(&dec)

		if r.protTraceFn != nil {
			r.protTraceFn(ctx, textMsgHdr, r.mh)
		}

		dec = encoding.Decoder(r.scratch[:segmentHeaderSize])
		if _, err := io.ReadFull(r.rd, dec); err != nil {
			return err
		}
		r.sh.decode(&dec)
		return nil
	}

	// fillBuffer allocates a fresh buffer per read. Decoded string and []byte
	// values alias the buffer directly (no copy), and such values can be retained
	// past this read (lazy column scan, lob chunk assembly, multiple stored
	// procedure output tables). A reused buffer would overwrite data still
	// referenced by the caller, so each read gets its own buffer whose lifetime
	// the GC manages.
	fillBuffer := func() ([]byte, error) {
		numWireByte := int(r.mh.varPartLength) - segmentHeaderSize

		buf := make([]byte, numWireByte)
		_, err := io.ReadFull(r.rd, buf)
		return buf, err
	}

	fillBufferCompressed := func() ([]byte, error) {
		numWireByte := int(r.mh.varPartLength) - segmentHeaderSize
		numDecompressByte := int(r.mh.compressionVarPartLength) - segmentHeaderSize

		r.tmpBuf = slices.Grow(r.tmpBuf, numWireByte)
		r.tmpBuf = r.tmpBuf[:numWireByte]

		// fresh buffer per read - see fillBuffer.
		buf := make([]byte, numDecompressByte)

		if _, err := io.ReadFull(r.rd, r.tmpBuf); err != nil {
			return nil, err
		}

		compressor := r.attrs.compressor
		if compressor == nil {
			panic("compressor misssing") // should never happen
		}

		_, err := compressor.Decompress(r.tmpBuf, buf)
		return buf, err
	}

	return func(yield func(*PartInfo, error) bool) {
		if err := readHeader(ctx); err != nil {
			yield(nil, err)
			return
		}

		var buf []byte
		var err error
		if r.mh.packetOptions.isCompressed() {
			buf, err = fillBufferCompressed()
		} else {
			buf, err = fillBuffer()
		}
		if err != nil {
			yield(nil, err)
			return
		}

		for i := range int(r.mh.noOfSegm) {
			if i != 0 {
				dec := encoding.Decoder(buf[:segmentHeaderSize])
				buf = buf[segmentHeaderSize:]
				r.sh.decode(&dec)
			}

			if r.protTraceFn != nil {
				r.protTraceFn(ctx, textSegHdr, r.sh)
			}

			numPart := int(r.sh.noOfParts)
			lastPart := numPart - 1

			for j := range numPart {
				ph := r.partInfo.Header

				dec := encoding.Decoder(buf[:partHeaderSize])
				buf = buf[partHeaderSize:]
				ph.decode(&dec)

				if r.protTraceFn != nil {
					r.protTraceFn(ctx, textParHdr, ph)
				}

				dec = encoding.Decoder(buf[:ph.bufferLength])
				bufAdvance := int(ph.bufferLength)
				if j != lastPart {
					bufAdvance += padBytes(int(ph.bufferLength))
				}
				buf = buf[bufAdvance:]

				r.partInfo.Dec = &dec

				if !yield(r.partInfo, nil) {
					return
				}
			}
		}
	}
}

func (r *Reader) readPrologDB(ctx context.Context) error {
	rep := &initReply{}
	dec := encoding.Decoder(r.scratch[:initReplySize])
	if _, err := io.ReadFull(r.rd, dec); err != nil {
		return err
	}
	if err := rep.decode(&dec); err != nil {
		return err
	}
	if r.protTraceFn != nil {
		r.protTraceFn(ctx, textIni, rep)
	}
	return nil
}

func (r *Reader) readPrologClient(ctx context.Context) error {
	req := &initRequest{}
	dec := encoding.Decoder(r.scratch[:initRequestSize])
	if _, err := io.ReadFull(r.rd, dec); err != nil {
		return err
	}
	if err := req.decode(&dec); err != nil {
		return err
	}
	if r.protTraceFn != nil {
		r.protTraceFn(ctx, textIni, req)
	}
	return nil
}

func (r *Reader) protTraceDB(ctx context.Context, text string, p fmt.Stringer) {
	r.attrs.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(prefixDB+text, p.String()))
}

func (r *Reader) protTraceClient(ctx context.Context, text string, p fmt.Stringer) {
	r.attrs.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(prefixClient+text, p.String()))
}

func (r *Reader) readHDBErrors(ctx context.Context) error {
	hdbErrors := new(HdbErrors)

	if err := r.readPart(ctx, hdbErrors); err != nil {
		return err
	}
	if hdbErrors.onlyWarnings {
		for _, err := range hdbErrors.errs {
			r.attrs.logger.LogAttrs(ctx, slog.LevelWarn, err.Error())
		}
		return nil
	}
	return hdbErrors
}

func (r *Reader) readPart(ctx context.Context, part PartDecoder) error {
	err := part.decode(r.partInfo.Dec, r.partInfo.Header, r.attrs)
	if r.protTraceFn != nil {
		r.protTraceFn(ctx, textPar, part)
	}
	return err
}

func (r *Reader) skipPart(ctx context.Context) error {
	// if trace is on or mandatory parts need to be read we cannot skip
	if r.protTraceFn == nil {
		return nil
	}

	kind := r.partInfo.Header.Kind()
	if part, ok := r.partCache.get(kind); ok {
		if part, ok := part.(PartDecoder); ok {
			return r.readPart(ctx, part)
		}
	}
	// generic trace.
	r.protTraceFn(ctx, textSkip, kind)
	return nil
}

const defaultSessionID = -1

// WriterAttrs holds writer attributes.
type WriterAttrs struct {
	protTrace           bool
	logger              *slog.Logger
	tr                  transform.Transformer
	sv                  map[string]string
	compressor          compress.Compressor
	compressEnableWrite bool
}

// NewWriterAttrs returns a WriterAttrs instance.
func NewWriterAttrs(protTrace bool, logger *slog.Logger, tr transform.Transformer, sv map[string]string, compressor compress.Compressor) *WriterAttrs {
	return &WriterAttrs{
		protTrace:  protTrace,
		logger:     logger,
		tr:         tr,
		sv:         sv,
		compressor: compressor,
	}
}

// SetCompressEnableWrite sets compressEnableWrite.
func (a *WriterAttrs) SetCompressEnableWrite(b bool) {
	a.compressEnableWrite = b
}

// Writer represents a protocol writer.
type Writer struct {
	wr io.Writer

	attrs *WriterAttrs

	svSent bool

	sessionID int64

	// reuse header
	mh *messageHeader
	sh *segmentHeader
	ph *PartHeader

	buf     []byte
	tmpBuf  []byte
	scratch []byte

	hasError bool
}

// NewWriter returns an instance of a protocol writer.
func NewWriter(wr io.Writer, attrs *WriterAttrs) *Writer {
	return &Writer{
		wr:        wr,
		attrs:     attrs,
		sessionID: defaultSessionID,
		mh:        new(messageHeader),
		sh:        new(segmentHeader),
		ph:        new(PartHeader),
		buf:       make([]byte, 0, 1024),
		scratch:   make([]byte, 0, 32),
	}
}

const (
	productVersionMajor  = 4
	productVersionMinor  = 20
	protocolVersionMajor = 4
	protocolVersionMinor = 1
)

// HasError returns true if writing raised an error, false otherwise.
func (w *Writer) HasError() bool { return w.hasError }

// WriteProlog writes the protocol prolog.
func (w *Writer) WriteProlog(ctx context.Context) error {
	enc := encoding.Encoder(w.scratch[:0])

	req := &initRequest{}
	req.product.major = productVersionMajor
	req.product.minor = productVersionMinor
	req.protocol.major = protocolVersionMajor
	req.protocol.minor = protocolVersionMinor
	req.numOptions = 1
	req.endianness = littleEndian
	if err := req.encode(&enc); err != nil {
		return err
	}
	if w.attrs.protTrace {
		w.protTrace(ctx, textIni, req)
	}
	_, err := w.wr.Write(enc)
	return err
}

// SetSessionID sets the session ID after a successful authentication.
func (w *Writer) SetSessionID(sessionID int64) { w.sessionID = sessionID }

func (w *Writer) Write(ctx context.Context, messageType MessageType, commit bool, parts ...PartEncoder) error {
	err := w._write(ctx, messageType, commit, parts...)
	if err != nil {
		w.hasError = true
	}
	return err
}

func compressBuffer(compressor compress.Compressor, buf, tmpBuf []byte) (bool, []byte, error) {
	uncompressedSize := len(buf)
	if uncompressedSize < minCompressBlockSize {
		return false, buf, nil
	}

	compressBound := compressor.CompressBound(uncompressedSize)
	// A valid LZ4 bound is always >= the input (incompressible data expands).
	// A smaller value means the compressor is misbehaving: either a custom
	// implementation computing the bound wrong, or the reference C
	// LZ4_compressBound returning 0 for input above LZ4_MAX_INPUT_SIZE
	// (0x7E000000, ~2.11 GB). A packet varpart is a uint32 (max ~4 GiB) so it
	// could in theory exceed that, but real HANA packets are orders of
	// magnitude smaller, so this is unreachable in practice. Either way, treat
	// the bound as invalid and send the packet uncompressed.
	if compressBound < uncompressedSize {
		return false, buf, nil
	}
	tmpBuf = slices.Grow(tmpBuf, compressBound)
	tmpBuf = tmpBuf[:compressBound]
	compressedSize, err := compressor.Compress(buf, tmpBuf)
	if err != nil {
		return false, buf, err
	}
	// uncompressedSize always > 0
	if (100 - float64(compressedSize)*100/float64(uncompressedSize)) < minCompressionSizePercent {
		return false, buf, nil
	}
	return true, tmpBuf[:compressedSize], nil
}

func (w *Writer) _write(ctx context.Context, messageType MessageType, commit bool, parts ...PartEncoder) error {
	// check on session variables to be sent as ClientInfo
	if w.attrs.sv != nil && !w.svSent && messageType.ClientInfoSupported() {
		parts = append([]PartEncoder{(*clientInfo)(&w.attrs.sv)}, parts...)
		w.svSent = true
	}

	numPart := len(parts)
	partSize := make([]int, numPart)
	totalSize := int64(segmentHeaderSize + numPart*partHeaderSize) // int64 to hold MaxUInt32 in 32bit OS

	partEnc := encoding.Encoder(w.buf[:0])

	// encode parts and calculate total size
	for i, part := range parts {

		partEnc.Zeroes(partHeaderSize)

		pos := len(partEnc)
		if err := part.encode(&partEnc, w.attrs.tr); err != nil {
			return err
		}
		size := len(partEnc) - pos
		pad := padBytes(size)
		partEnc.Zeroes(pad)

		totalSize += int64(size + pad)
		partSize[i] = size
	}

	if totalSize > math.MaxUint32 {
		return fmt.Errorf("message size %d exceeds maximum message header value %d", totalSize, int64(math.MaxUint32)) // int64: without cast overflow error in 32bit OS
	}

	// patch part headers
	bufferSize := totalSize - segmentHeaderSize

	pos := 0
	for i, part := range parts {

		size := partSize[i]
		pad := padBytes(size)

		w.ph.partKind = part.kind()
		if err := w.ph.setNumArg(part.numArg()); err != nil {
			return err
		}
		w.ph.bufferLength = int32(size)     //nolint: gosec
		w.ph.bufferSize = int32(bufferSize) //nolint: gosec

		enc := encoding.Encoder(w.scratch[:0])
		if err := w.ph.encode(&enc); err != nil {
			return err
		}
		if w.attrs.protTrace {
			w.protTrace(ctx, textParHdr, w.ph)
		}

		// patch
		copy(partEnc[pos:], enc[:partHeaderSize])
		pos += partHeaderSize + size + pad

		// part prot trace
		if w.attrs.protTrace {
			w.protTrace(ctx, textPar, part)
		}

		bufferSize -= int64(partHeaderSize + size + pad)
	}

	compress, partBuf := false, partEnc
	if w.attrs.compressEnableWrite {
		var err error
		if compress, partBuf, err = compressBuffer(w.attrs.compressor, partEnc, w.tmpBuf); err != nil {
			return err
		}
	}

	// start writing
	if compress {
		w.mh.packetOptions = poIsCompressed
		w.mh.compressionVarPartLength = uint32(totalSize)             //nolint: gosec
		w.mh.varPartLength = uint32(segmentHeaderSize + len(partBuf)) //nolint: gosec
	} else {
		w.mh.packetOptions = 0
		w.mh.compressionVarPartLength = 0
		w.mh.varPartLength = uint32(totalSize) //nolint: gosec
	}

	w.mh.sessionID = w.sessionID
	w.mh.varPartSize = uint32(totalSize) //nolint: gosec
	w.mh.noOfSegm = 1

	enc := encoding.Encoder(w.scratch[:0])
	if err := w.mh.encode(&enc); err != nil {
		return err
	}
	if _, err := w.wr.Write(enc); err != nil {
		return err
	}

	if w.attrs.protTrace {
		w.protTrace(ctx, textMsgHdr, w.mh)
	}

	w.sh.messageType = messageType
	w.sh.commit = commit
	w.sh.segmentKind = skRequest
	w.sh.segmentLength = int32(totalSize) //nolint: gosec
	w.sh.segmentOfs = 0
	w.sh.noOfParts = int16(numPart) //nolint: gosec
	w.sh.segmentNo = 1

	enc = encoding.Encoder(w.scratch[:0])
	if err := w.sh.encode(&enc); err != nil {
		return err
	}
	if _, err := w.wr.Write(enc); err != nil {
		return err
	}
	if w.attrs.protTrace {
		w.protTrace(ctx, textSegHdr, w.sh)
	}

	_, err := w.wr.Write(partBuf)
	return err
}

func (w *Writer) protTrace(ctx context.Context, text string, p fmt.Stringer) {
	w.attrs.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String(prefixClient+text, p.String()))
}
