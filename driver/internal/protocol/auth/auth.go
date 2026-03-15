// Package auth provides authentication methods.
package auth

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/internal/unsafe"
)

/*
authentication method types supported by the driver:
  - basic authentication (username, password based) (whether SCRAMSHA256 or SCRAMPBKDF2SHA256) and
  - X509 (client certificate) authentication and
  - JWT (token) authentication
*/
const (
	MtSCRAMSHA256       = "SCRAMSHA256"       // password
	MtSCRAMPBKDF2SHA256 = "SCRAMPBKDF2SHA256" // password pbkdf2
	MtX509              = "X509"              // client certificate
	MtJWT               = "JWT"               // json web token
	MtSessionCookie     = "SessionCookie"     // session cookie
	MtLDAP              = "LDAP"              // LDAP authentication
)

// authentication method orders.
const (
	MoSessionCookie byte = iota
	MoX509
	MoJWT
	MoSCRAMPBKDF2SHA256
	MoSCRAMSHA256
	MoLDAP
)

// A Method defines the interface for an authentication method.
type Method interface {
	fmt.Stringer
	Typ() string
	Order() byte
	PrepareInitReq(prms *Prms) error
	InitRepDecode(d *Decoder) error
	PrepareFinalReq(prms *Prms) error
	FinalRepDecode(d *Decoder) error
}

// Methods defines a collection of methods.
type Methods map[string]Method // key equals authentication method type.

// Order returns an ordered method slice.
func (m Methods) Order() []Method {
	methods := make([]Method, 0, len(m))
	for _, e := range m {
		methods = append(methods, e)
	}
	slices.SortFunc(methods, func(m1, m2 Method) int { return cmp.Compare(m1.Order(), m2.Order()) })
	return methods
}

// CookieGetter is implemented by authentication methods supporting cookies to reconnect.
type CookieGetter interface {
	Cookie() (logonname string, cookie []byte)
}

var (
	_ Method = (*SCRAMSHA256)(nil)
	_ Method = (*SCRAMPBKDF2SHA256)(nil)
	_ Method = (*JWT)(nil)
	_ Method = (*X509)(nil)
	_ Method = (*SessionCookie)(nil)
	_ Method = (*LDAP)(nil)
)

/*
Field size methods (used for decoding) of
- bytes, string, unicode string

- a size <= 250 encoded in one byte or
- an unsigned 2 byte integer size encoded in three bytes
  . first byte equals 255
  . second and third byte is an big endian encoded uint16

See also "SAP HANA SQL Command Network Protocol Reference" version 1.2 chapter 2.3.7.20.

Weirdly enough:
- encoding follows the standard rules for length/size indicators
- see prms on details
*/

const (
	maxFieldSize1ByteLen    = 250
	fieldSize2ByteIndicator = 255
)

func fieldSize(size int) int {
	if size > maxFieldSize1ByteLen {
		return 3
	}
	return 1
}

func encodeFieldSize(e *encoding.Encoder, size int) error {
	switch {
	case size <= maxFieldSize1ByteLen:
		e.Byte(byte(size))
	case size <= math.MaxUint16:
		e.Byte(fieldSize2ByteIndicator)
		e.Uint16ByteOrder(uint16(size), binary.BigEndian)
	default:
		return fmt.Errorf("invalid field size %d - maximum %d", size, math.MaxUint16)
	}
	return nil
}

func decodeFieldSize(d *encoding.Decoder) int {
	b := d.Byte()
	switch {
	case b <= maxFieldSize1ByteLen:
		return int(b)
	case b == fieldSize2ByteIndicator:
		return int(d.Uint16ByteOrder(binary.BigEndian))
	default:
		panic("invalid sub parameter size indicator")
	}
}

// Decoder represents an authentication decoder.
type Decoder struct {
	d *encoding.Decoder
}

// NewDecoder returns a new decoder instance.
func NewDecoder(d *encoding.Decoder) *Decoder {
	return &Decoder{d: d}
}

// NumPrm ckecks the number of parameters and returns an error if not equal expected, nil otherwise.
func (d *Decoder) NumPrm(expected int) error {
	numPrm := int(d.d.Int16())
	if numPrm != expected {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, expected)
	}
	return nil
}

func (d *Decoder) String() string {
	size := decodeFieldSize(d.d)
	if size == 0 {
		return ""
	}
	b := make([]byte, size)
	d.d.Bytes(b)
	return unsafe.ByteSlice2String(b)
}

func (d *Decoder) cesu8String() (string, error) {
	size := decodeFieldSize(d.d)
	if size == 0 {
		return "", nil
	}
	b, err := d.d.CESU8Bytes(size)
	if err != nil {
		return "", err
	}
	return unsafe.ByteSlice2String(b), nil
}

func (d *Decoder) bytes() []byte {
	size := decodeFieldSize(d.d)
	if size == 0 {
		return nil
	}
	b := make([]byte, size)
	d.d.Bytes(b)
	return b
}

func (d *Decoder) bigUint32() (uint32, error) {
	size := d.d.Byte()
	if size != encoding.IntegerFieldSize { // 4 bytes
		return 0, fmt.Errorf("invalid auth uint32 size %d - expected %d", size, encoding.IntegerFieldSize)
	}
	return d.d.Uint32ByteOrder(binary.BigEndian), nil // big endian coded (e.g. rounds param)
}

func (d *Decoder) subSize() int {
	return decodeFieldSize(d.d)
}

// Prms represents authentication parameters.
type Prms struct {
	prms []any
}

func (p *Prms) String() string { return fmt.Sprintf("%v", p.prms) }

// AddCESU8String adds a CESU8 string parameter.
func (p *Prms) AddCESU8String(s string) { p.prms = append(p.prms, s) } // unicode string
func (p *Prms) addEmpty()               { p.prms = append(p.prms, []byte{}) }
func (p *Prms) addBytes(b []byte)       { p.prms = append(p.prms, b) }
func (p *Prms) addString(s string)      { p.prms = append(p.prms, []byte(s)) } // treat like bytes to distinguisch from unicode string
func (p *Prms) addPrms() *Prms {
	prms := &Prms{}
	p.prms = append(p.prms, prms)
	return prms
}

// Size returns the size in bytes of the parameters.
func (p *Prms) Size() int {
	size := encoding.SmallintFieldSize // no of parameters (2 bytes)
	for _, prm := range p.prms {
		switch prm := prm.(type) {
		case []byte, string:
			size += encoding.VarFieldSize(prm)
		case *Prms:
			subSize := prm.Size()
			size += (subSize + fieldSize(subSize))
		default:
			panic("invalid parameter") // should not happen
		}
	}
	return size
}

// Encode encodes the parameters.
func (p *Prms) Encode(enc *encoding.Encoder) error {
	numPrms := len(p.prms)
	if numPrms > math.MaxInt16 {
		return fmt.Errorf("invalid number of parameters %d - maximum %d", numPrms, math.MaxInt16)
	}
	enc.Int16(int16(numPrms))

	for _, e := range p.prms {
		switch e := e.(type) {
		case []byte:
			if err := enc.LIBytes(e); err != nil {
				return err
			}
		case string:
			if err := enc.CESU8LIString(e); err != nil {
				return err
			}
		case *Prms:
			subSize := e.Size()
			if err := encodeFieldSize(enc, subSize); err != nil {
				return err
			}
			if err := e.Encode(enc); err != nil {
				return err
			}
		default:
			panic("invalid parameter") // should not happen
		}
	}
	return nil
}

// Decode decodes the parameters.
func (p *Prms) Decode(dec *encoding.Decoder) error {
	numPrms := int(dec.Int16())
	for range numPrms {

	}
	return nil
}

func checkAuthMethodType(mt, expected string) error {
	if mt != expected {
		return fmt.Errorf("invalid method %s - expected %s", mt, expected)
	}
	return nil
}
