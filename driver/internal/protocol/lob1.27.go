//go:build go1.27

package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/internal/trace"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
	"golang.org/x/text/transform"
)

// LobOutDescr represents a lob output descriptor.
type LobOutDescr struct {
	isCharLob bool
	decoded   bool
	/*
		HDB does not return lob type code but undefined only
		--> ltc is always ltcUndefined
		--> use isCharBased instead of type code check
	*/
	ltc     lobTypecode
	Opt     LobOptions
	numChar int64
	numByte int64
	id      LocatorID
	b       []byte

	decodedNumChar int
	decodeErr      error
}

func (d *LobOutDescr) String() string {
	return fmt.Sprintf("typecode %s options %s numChar %d numByte %d id %d bytes %s", d.ltc, d.Opt, d.numChar, d.numByte, d.id, trace.Cut(d.b))
}

// decodeLobOutDescr decodes a lob output descriptor, or returns a real nil for a null value.
// Result is used as driver.Value: a typed nil *LobOutDescr would not be recognized as nil.
func decodeLobOutDescr(dec *encoding.Decoder, isCharLob bool) (any, error) {
	ltc := lobTypecode(dec.Int8())
	opt := LobOptions(dec.Int8())
	if opt.isNull() {
		return nil, nil
	}
	d := &LobOutDescr{isCharLob: isCharLob, ltc: ltc, Opt: opt}
	dec.Skip(2)
	d.numChar = dec.Int64()
	d.numByte = dec.Int64()
	d.id = LocatorID(dec.Uint64())
	size := int(dec.Int32())
	d.b = dec.Bytes(size)
	return d, nil
}

// LocatorID returns the lob locator id.
func (d *LobOutDescr) LocatorID() LocatorID { return d.id }

// NewReadLobReply creates a lob read reply part with the descriptor's locator id
// and lob type.
func (d *LobOutDescr) NewReadLobReply() *ReadLobReply {
	return &ReadLobReply{id: d.id, isCharLob: d.isCharLob}
}

// Bytes returns the initial chunk and its protocol offset count. Character LOBs
// are decoded from CESU-8 in place once; subsequent calls return the cached result.
func (d *LobOutDescr) Bytes(tr transform.Transformer) ([]byte, int, error) {
	if !d.isCharLob {
		return d.b, len(d.b), nil
	}
	if !d.decoded {
		d.b, d.decodedNumChar, d.decodeErr = decodeLobChunk(tr, d.b)
		// Cache errors too: a failed conversion may have modified the bytes.
		d.decoded = true
	}
	return d.b, d.decodedNumChar, d.decodeErr
}

// ReadLobReply represents a lob read reply part.
type ReadLobReply struct {
	id        LocatorID
	Opt       LobOptions
	isCharLob bool
	b         []byte
}

func (r *ReadLobReply) String() string {
	return fmt.Sprintf("id %d options %s bytes %s", r.id, r.Opt, trace.Cut(r.b))
}

// NewReadLobReply creates a lob read reply part for the given locator id.
func NewReadLobReply(id LocatorID) *ReadLobReply {
	return &ReadLobReply{id: id}
}

// Bytes returns the reply chunk and its protocol offset count, decoding
// character LOBs from CESU-8 in place. Call once per reply.
func (r *ReadLobReply) Bytes(tr transform.Transformer) ([]byte, int, error) {
	if !r.isCharLob {
		return r.b, len(r.b), nil
	}
	return decodeLobChunk(tr, r.b)
}

func (r *ReadLobReply) decode(dec *encoding.Decoder, header *PartHeader, attrs *ReaderAttrs) error {
	if header.numArg() != 1 {
		panic("numArg == 1 expected")
	}
	id := LocatorID(dec.Uint64())
	if id != r.id {
		return fmt.Errorf("invalid locator id %d - expected %d", id, r.id)
	}
	r.Opt = LobOptions(dec.Int8())
	size := int(dec.Int32())
	dec.Skip(3)
	r.b = dec.Bytes(size)
	return nil
}

// decodeLobChunk decodes a CESU-8 chunk in place and returns its UTF-8 bytes
// and HANA character count.
func decodeLobChunk(tr transform.Transformer, b []byte) ([]byte, int, error) {
	var nDst, numChar int
	var err error
	tr.Reset()
	if ntr, ok := tr.(cesu8.NumCharTransformer); ok { // fasttrack
		nDst, _, numChar, err = ntr.TransformNumChar(b, b, false) // cesu8 -> utf8 (always enough space)
	} else { // slow
		nDst, _, err = tr.Transform(b, b, false) // cesu8 -> utf8 (always enough space)
		numChar = cesu8.NumChar(b[:nDst])
	}
	if err != nil && err != transform.ErrShortSrc { //nolint: errorlint
		return b[:nDst], numChar, err
	}
	return b[:nDst], numChar, nil
}
