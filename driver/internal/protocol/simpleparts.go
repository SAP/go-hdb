package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"golang.org/x/text/transform"
)

// ClientID represents a client id part.
type ClientID []byte

func (id ClientID) String() string { return string(id) }
func (id *ClientID) decode(dec *encoding.Decoder, header *PartHeader, attrs *ReaderAttrs) error {
	*id = dec.Bytes(header.bufLen())
	return nil
}
func (id ClientID) encode(enc *encoding.Encoder, _ transform.Transformer) error {
	enc.Bytes(id)
	return nil
}

// Command represents a command part with cesu8 content.
type Command []byte

func (c Command) String() string { return string(c) }
func (c *Command) decode(dec *encoding.Decoder, header *PartHeader, attrs *ReaderAttrs) error {
	var err error
	*c, err = dec.CESU8Bytes(attrs.Tr, header.bufLen())
	if err != nil {
		return err
	}
	return nil
}
func (c Command) encode(enc *encoding.Encoder, tr transform.Transformer) error {
	_, err := enc.CESU8Bytes(tr, c)
	return err
}

// Fetchsize represents a fetch size part.
type Fetchsize int32

func (s Fetchsize) String() string { return fmt.Sprintf("fetchsize %d", s) }
func (s *Fetchsize) decode(dec *encoding.Decoder, _ *PartHeader, _ *ReaderAttrs) error {
	*s = Fetchsize(dec.Int32())
	return nil
}
func (s Fetchsize) encode(enc *encoding.Encoder, _ transform.Transformer) error {
	enc.Int32(int32(s))
	return nil
}

// StatementID represents the statement id part type.
type StatementID uint64

func (id StatementID) String() string { return fmt.Sprintf("%d", id) }

// Decode implements the partDecoder interface.
func (id *StatementID) decode(dec *encoding.Decoder, _ *PartHeader, _ *ReaderAttrs) error {
	*id = StatementID(dec.Uint64())
	return nil
}

// Encode implements the partEncoder interface.
func (id StatementID) encode(enc *encoding.Encoder, _ transform.Transformer) error {
	enc.Uint64(uint64(id))
	return nil
}
