// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
)

// AuthOptions - currently supported are:
// - basic authentication (username, password based) (whether SCRAMSHA256 or SCRAMPBKDF2SHA256) and
// - X509 (client certificate) authentication and
// - JWT (token) authentication

const (
	mnSCRAMSHA256       = "SCRAMSHA256"       // password
	mnSCRAMPBKDF2SHA256 = "SCRAMPBKDF2SHA256" // password pbkdf2
	mnX509              = "X509"              // client certificate
	mnJWT               = "JWT"               // json web token
)

func checkAuthMethodName(name, expected string) error {
	if name != expected {
		return fmt.Errorf("invalid method name %s - expected %s", name, expected)
	}
	return nil
}

const (
	int16Size  = 2
	uint32Size = 4
)

type authStepper interface {
	next() (partReadWriter, error)
}

// subPrmsSize is the type used to encode and decode the size of sub parameters.
// The hana protocoll supports whether:
// - a size <= 245 encoded in one byte or
// - an unsigned 2 byte integer size encoded in three bytes
//   . first byte equals 255
//   . second and third byte is an big endian encoded uint16
type subPrmsSize int

const (
	maxSubPrmsSize1ByteLen    = 245
	subPrmsSize2ByteIndicator = 255
)

func (s subPrmsSize) fieldSize() int {
	if s > maxSubPrmsSize1ByteLen {
		return 3
	}
	return 1
}

func (s subPrmsSize) encode(e *encoding.Encoder) error {
	switch {
	case s <= maxSubPrmsSize1ByteLen:
		e.Byte(byte(s))
	case s <= math.MaxUint16:
		e.Byte(subPrmsSize2ByteIndicator)
		e.Uint16ByteOrder(uint16(s), binary.BigEndian) // big endian
	default:
		return fmt.Errorf("invalid subparameter size %d - maximum %d", s, 42)
	}
	return nil
}

func (s *subPrmsSize) decode(d *encoding.Decoder) {
	b := d.Byte()
	switch {
	case b <= maxSubPrmsSize1ByteLen:
		*s = subPrmsSize(b)
	case b == subPrmsSize2ByteIndicator:
		*s = subPrmsSize(d.Uint16ByteOrder(binary.BigEndian))
	default:
		panic(fmt.Sprintf("invalid sub parameter size indicator %d", b))
	}
}

type authDecoder struct {
	d *encoding.Decoder
}

func newAuthDecoder(d *encoding.Decoder) *authDecoder {
	return &authDecoder{d: d}
}

func (d *authDecoder) numPrm(expected int) error {
	numPrm := int(d.d.Int16())
	if numPrm != expected {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, expected)
	}
	return nil
}

func (d *authDecoder) string() (string, error)      { return decodeVarString(d.d) }
func (d *authDecoder) cesu8String() (string, error) { return decodeCESU8String(d.d) }
func (d *authDecoder) bytes() ([]byte, error)       { return decodeVarBytes(d.d) }
func (d *authDecoder) bigUint32() (uint32, error) {
	size := d.d.Byte()
	if size != uint32Size {
		return 0, fmt.Errorf("invalid auth uint32 size %d - expected %d", size, uint32Size)
	}
	return d.d.Uint32ByteOrder(binary.BigEndian), nil // big endian coded (e.g. rounds param)
}
func (d *authDecoder) subSize() int {
	var subSize subPrmsSize
	(&subSize).decode(d.d)
	return int(subSize)
}

type authPrms struct {
	prms []interface{}
}

func (p *authPrms) addEmpty()               { p.prms = append(p.prms, []byte{}) }
func (p *authPrms) addBytes(b []byte)       { p.prms = append(p.prms, b) }
func (p *authPrms) addString(s string)      { p.prms = append(p.prms, []byte(s)) } // treat like bytes to distinguisch from unicode string
func (p *authPrms) addCESU8String(s string) { p.prms = append(p.prms, s) }         // unicode string
func (p *authPrms) addPrms() *authPrms {
	prms := &authPrms{}
	p.prms = append(p.prms, prms)
	return prms
}

func (p *authPrms) size() int {
	size := int16Size // no of parameters
	for _, e := range p.prms {
		switch e := e.(type) {
		case []byte:
			size += varBytesSize(len(e))
		case string:
			size += varBytesSize(cesu8.StringSize(e))
		case *authPrms:
			subSize := subPrmsSize(e.size())
			size += (int(subSize) + subSize.fieldSize())
		default:
			panic(fmt.Sprintf("invalid parameter %[1]v %[1]t", e)) // should not happen
		}
	}
	return size
}

func (p *authPrms) encode(enc *encoding.Encoder) error {
	numPrms := len(p.prms)
	if numPrms > math.MaxInt16 {
		return fmt.Errorf("invalid number of parameters %d - maximum %d", numPrms, math.MaxInt16)
	}
	enc.Int16(int16(numPrms))

	for _, e := range p.prms {
		switch e := e.(type) {
		case []byte:
			if err := encodeVarBytes(enc, e); err != nil {
				return err
			}
		case string:
			if err := encodeCESU8String(enc, e); err != nil {
				return err
			}
		case *authPrms:
			subSize := subPrmsSize(e.size())
			if err := subSize.encode(enc); err != nil {
				return err
			}
			if err := e.encode(enc); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("invalid parameter %[1]v %[1]t", e)) // should not happen
		}
	}
	return nil
}

type authMethod interface {
	fmt.Stringer
	methodName() string
	prepareInitReq(prms *authPrms)
	initRepDecode(d *authDecoder) error
	prepareFinalReq(prms *authPrms) error
	finalRepDecode(d *authDecoder) error
}

type authMethods struct {
	methods []authMethod // set of supported authentication methods
	method  authMethod   // selected authentication method
}

func (m *authMethods) String() string {
	if m.method == nil {
		return fmt.Sprintf("methods %v", m.methods)
	}
	return fmt.Sprintf("methods %v selected method %s", m.methods, m.method.methodName())
}

func (m *authMethods) add(method authMethod) { m.methods = append(m.methods, method) }

func (m *authMethods) setMethod(methodName string) error {
	for _, method := range m.methods {
		if method.methodName() == methodName {
			m.method = method
			return nil
		}
	}
	return fmt.Errorf("invalid or not supported authentication method %s", methodName)
}

type authInitReq struct {
	username string
	methods  *authMethods
	prms     *authPrms
}

func (r *authInitReq) String() string { return fmt.Sprintf("username %s %s", r.username, r.methods) }

func (r *authInitReq) prepare() {
	r.prms = &authPrms{}

	r.prms.addCESU8String(r.username)
	for _, m := range r.methods.methods {
		m.prepareInitReq(r.prms)
	}
}

func (r *authInitReq) size() int                                          { return r.prms.size() }
func (r *authInitReq) decode(dec *encoding.Decoder, ph *partHeader) error { panic("not implemented") }
func (r *authInitReq) encode(enc *encoding.Encoder) error                 { return r.prms.encode(enc) }

type authInitRep struct {
	methods *authMethods
}

func (r *authInitRep) String() string                     { return r.methods.String() }
func (r *authInitRep) size() int                          { panic("not implemented") }
func (r *authInitRep) encode(enc *encoding.Encoder) error { panic("not implemented") }

func (r *authInitRep) decode(dec *encoding.Decoder, ph *partHeader) error {
	d := newAuthDecoder(dec)

	if err := d.numPrm(2); err != nil {
		return err
	}
	methodName, err := d.string()
	if err != nil {
		return err
	}
	if err := r.methods.setMethod(methodName); err != nil {
		return err
	}
	return r.methods.method.initRepDecode(d)
}

type authFinalReq struct {
	method authMethod
	prms   *authPrms
}

func (r *authFinalReq) String() string { return fmt.Sprintf("method %s", r.method) }

func (r *authFinalReq) prepare() error {
	r.prms = &authPrms{}
	if err := r.method.prepareFinalReq(r.prms); err != nil {
		return err
	}
	return nil
}
func (r *authFinalReq) size() int                                          { return r.prms.size() }
func (r *authFinalReq) decode(dec *encoding.Decoder, ph *partHeader) error { panic("not implemented") }
func (r *authFinalReq) encode(enc *encoding.Encoder) error                 { return r.prms.encode(enc) }

type authFinalRep struct {
	method authMethod
}

func (r *authFinalRep) String() string                     { return fmt.Sprintf("method %s", r.method) }
func (r *authFinalRep) size() int                          { panic("not implemented") }
func (r *authFinalRep) encode(enc *encoding.Encoder) error { panic("not implemented") }
func (r *authFinalRep) decode(dec *encoding.Decoder, ph *partHeader) error {
	return r.method.finalRepDecode(newAuthDecoder(dec))
}

type auth struct {
	step     int
	username string
	methods  *authMethods
}

func newAuth(cfg *SessionConfig) *auth {
	methods := &authMethods{}
	if cfg.ClientCert != nil && cfg.ClientKey != nil {
		methods.add(newAuthX509(cfg.ClientCert, cfg.ClientKey))
	}
	if cfg.Token != "" {
		methods.add(newAuthJWT(cfg.Token))
	}
	// mimic standard drivers and use password as token if user is empty
	if cfg.Token == "" && cfg.Username == "" && cfg.Password != "" {
		methods.add(newAuthJWT(cfg.Password))
	}
	if cfg.Password != "" {
		methods.add(newAuthSCRAMPBKDF2SHA256(cfg.Username, cfg.Password))
		methods.add(newAuthSCRAMSHA256(cfg.Username, cfg.Password))
	}
	return &auth{username: cfg.Username, methods: methods}
}

func (a *auth) next() (partReadWriter, error) {
	defer func() { a.step++ }()

	switch a.step {
	case 0:
		initReq := &authInitReq{username: a.username, methods: a.methods}
		initReq.prepare()
		return initReq, nil
	case 1:
		return &authInitRep{methods: a.methods}, nil
	case 2:
		finalReq := &authFinalReq{method: a.methods.method}
		if err := finalReq.prepare(); err != nil {
			return nil, err
		}
		return finalReq, nil
	case 3:
		return &authFinalRep{method: a.methods.method}, nil
	default:
		panic("should never happen")
	}
}
