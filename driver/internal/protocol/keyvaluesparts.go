package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"golang.org/x/text/transform"
)

type clientInfo map[string]string

func (c clientInfo) String() string { return fmt.Sprintf("%v", map[string]string(c)) }
func (c clientInfo) numArg() int    { return len(c) }
func (c *clientInfo) decode(dec *encoding.Decoder, header *PartHeader, attrs *ReaderAttrs) error {
	*c = clientInfo{} // no reuse of maps - create new one

	for range header.numArg() {
		k, err := dec.Cesu8Field(attrs.Tr)
		if err != nil {
			return err
		}
		v, err := dec.Cesu8Field(attrs.Tr)
		if err != nil {
			return err
		}
		// set key value
		(*c)[string(k.([]byte))] = string(v.([]byte))
	}
	return nil
}
func (c clientInfo) encode(enc *encoding.Encoder, tr transform.Transformer) error {
	for k, v := range c {
		if err := enc.Cesu8Field(tr, k); err != nil {
			return err
		}
		if err := enc.Cesu8Field(tr, v); err != nil {
			return err
		}
	}
	return nil
}
