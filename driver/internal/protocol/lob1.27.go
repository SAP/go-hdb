//go:build go1.27

package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

// LobOutDescr represents a lob output descriptor.
type LobOutDescr struct {
	IsCharLob bool
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
	Bytes   []byte
}

func newLobOutDescr(isCharLob bool) *LobOutDescr {
	return &LobOutDescr{IsCharLob: isCharLob}
}

func (d *LobOutDescr) String() string {
	return fmt.Sprintf("typecode %s options %s numChar %d numByte %d id %d bytes %v", d.ltc, d.Opt, d.numChar, d.numByte, d.id, d.Bytes)
}

func (d *LobOutDescr) decode(dec *encoding.Decoder) bool {
	d.ltc = lobTypecode(dec.Int8())
	d.Opt = LobOptions(dec.Int8())
	if d.Opt.isNull() {
		return true
	}
	dec.Skip(2)
	d.numChar = dec.Int64()
	d.numByte = dec.Int64()
	d.id = LocatorID(dec.Uint64())
	size := int(dec.Int32())
	d.Bytes = dec.Bytes(size)
	return false
}

// LocatorID returns the lob locator id.
func (d *LobOutDescr) LocatorID() LocatorID { return d.id }

// ReadLobReply represents a lob read reply part.
type ReadLobReply struct {
	id    LocatorID
	Opt   LobOptions
	Bytes []byte
}

func (r *ReadLobReply) String() string {
	return fmt.Sprintf("id %d options %s bytes %v", r.id, r.Opt, r.Bytes)
}

// NewReadLobReply creates a lob read reply part for the given locator id.
func NewReadLobReply(id LocatorID) *ReadLobReply {
	return &ReadLobReply{id: id}
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
	r.Bytes = dec.Bytes(size)
	return nil
}
