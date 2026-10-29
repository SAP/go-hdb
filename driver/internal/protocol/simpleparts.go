package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/internal/unsafe"
)

// ClientID represents a client id part.
type ClientID []byte

func (id ClientID) String() string { return string(id) }
func (id *ClientID) decode(dec *encoding.Decoder, header *PartHeader, attrs *ReaderAttrs) error {
	*id = dec.Bytes(header.bufLen())
	return nil
}
func (id ClientID) encode(enc *encoding.Encoder) error {
	enc.Bytes(id)
	return nil
}

// Command represents a command part with cesu8 content.
type Command struct {
	cmd      []byte
	traceCmd *string // if non-nil, shown in the protocol trace instead of the raw command
}

// NewCommand returns a command part for cmd.
func NewCommand(cmd string, traceCmd *string) Command {
	return Command{cmd: unsafe.String2ByteSlice(cmd), traceCmd: traceCmd}
}

func (c Command) String() string {
	if c.traceCmd != nil {
		return *c.traceCmd
	}
	return unsafe.ByteSlice2String(c.cmd)
}
func (c *Command) decode(dec *encoding.Decoder, header *PartHeader, attrs *ReaderAttrs) error {
	var err error
	c.cmd, err = dec.CESU8Bytes(header.bufLen())
	if err != nil {
		return err
	}
	return nil
}
func (c Command) encode(enc *encoding.Encoder) error {
	_, err := enc.CESU8Bytes(c.cmd)
	return err
}

// Fetchsize represents a fetch size part.
type Fetchsize int32

func (s Fetchsize) String() string { return fmt.Sprintf("fetchsize %d", s) }
func (s *Fetchsize) decode(dec *encoding.Decoder, _ *PartHeader, _ *ReaderAttrs) error {
	*s = Fetchsize(dec.Int32())
	return nil
}
func (s Fetchsize) encode(enc *encoding.Encoder) error {
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
func (id StatementID) encode(enc *encoding.Encoder) error {
	enc.Uint64(uint64(id))
	return nil
}
