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

// authentication method types supported by the driver:
// - basic authentication (username, password based) (whether SCRAMSHA256 or SCRAMPBKDF2SHA256) and
// - X509 (client certificate) authentication and
// - JWT (token) authentication
const (
	amtSCRAMSHA256       = "SCRAMSHA256"       // password
	amtSCRAMPBKDF2SHA256 = "SCRAMPBKDF2SHA256" // password pbkdf2
	amtX509              = "X509"              // client certificate
	amtJWT               = "JWT"               // json web token
	amtSessionCookie     = "SessionCookie"     // session cookie
)

// authentication method orders.
const (
	amoSessionCookie byte = iota
	amoX509
	amoJWT
	amoSCRAMPBKDF2SHA256
	amoSCRAMSHA256
)

// A AuthFailedError is returned if the last authorization step would fail with a hdb error 10 (authorization failed).
type AuthFailedError struct {
	err error
	mt  string
}

func (e *AuthFailedError) Error() string {
	return fmt.Sprintf("authentication failed method type: %s", e.mt)
}

// MethodType returns the authentication method type.
func (e *AuthFailedError) MethodType() string { return e.mt }

// Unwrap returns the nested error.
func (e *AuthFailedError) Unwrap() error { return e.err }

func newAuthFailedError(mt string, err error) *AuthFailedError {
	return &AuthFailedError{mt: mt, err: err}
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
	if size != integerFieldSize { // 4 bytes
		return 0, fmt.Errorf("invalid auth uint32 size %d - expected %d", size, integerFieldSize)
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
	size := smallintFieldSize // no of parameters (2 bytes)
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

func checkAuthMethodType(mt, expected string) error {
	if mt != expected {
		return fmt.Errorf("invalid method %s - expected %s", mt, expected)
	}
	return nil
}

// AuthPasswordSetter is implemented by authentication methods supporting password updates.
type AuthPasswordSetter interface {
	SetPassword(string)
}

// AuthTokenSetter is implemented by authentication methods supporting token updates.
type AuthTokenSetter interface {
	SetToken(string)
}

// AuthCertKeySetter is implemented by authentication methods supporting certificate and key updates.
type AuthCertKeySetter interface {
	SetCertKey(cert, key []byte)
}

// AuthCookieGetter is implemented by authentication methods supporting cookies to reconnect.
type AuthCookieGetter interface {
	Cookie() (logonname string, cookie []byte)
}

// an authMethod defines the interface for an authentication method.
type authMethod interface {
	fmt.Stringer
	typ() string
	order() byte
	prepareInitReq(prms *authPrms)
	initRepDecode(d *authDecoder) error
	prepareFinalReq(prms *authPrms) error
	finalRepDecode(d *authDecoder) error
}

type authMethods map[string]authMethod // key equals authentication method type.

// Auth holds the client authentication methods dependant on the driver.Connector attributes.
type Auth struct {
	logonname string
	methods   authMethods
	method    authMethod // selected method
}

// NewAuth creates a new Auth instance.
func NewAuth(logonname string) *Auth { return &Auth{logonname: logonname, methods: authMethods{}} }

func (a *Auth) String() string { return fmt.Sprintf("logonname: %s", a.logonname) }

// AddSessionCookie adds session cookie authentication method.
func (a *Auth) AddSessionCookie(cookie []byte, clientID string) {
	a.methods[amtSessionCookie] = newAuthSessionCookie(cookie, clientID)
	traceAuthf("add session cookie: cookie %v clientID %s", cookie, clientID)
}

// AddBasic adds basic authentication methods.
func (a *Auth) AddBasic(username, password string) {
	a.methods[amtSCRAMPBKDF2SHA256] = newAuthSCRAMPBKDF2SHA256(username, password)
	a.methods[amtSCRAMSHA256] = newAuthSCRAMSHA256(username, password)
}

// AddJWT adds JWT authentication method.
func (a *Auth) AddJWT(token string) { a.methods[amtJWT] = newAuthJWT(token) }

// AddX509 adds X509 authentication method.
func (a *Auth) AddX509(cert, key []byte) { a.methods[amtX509] = newAuthX509(cert, key) }

// Method returns the selected authentication method.
func (a *Auth) Method() interface{} { return a.method }

func (a *Auth) setMethod(mt string) error {
	var ok bool

	traceAuthf("selected method: %s", mt)

	if a.method, ok = a.methods[mt]; !ok {
		return fmt.Errorf("invalid method type: %s", mt)
	}
	return nil
}

func (a *Auth) step0() (partReadWriter, error) {
	traceAuth("step0")
	prms := &authPrms{}
	prms.addCESU8String(a.logonname)
	for _, m := range a.methods.order() {
		m.prepareInitReq(prms)
	}
	return &authInitReq{prms: prms}, nil
}
func (a *Auth) step1() (partReadWriter, error) { traceAuth("step1"); return &authInitRep{auth: a}, nil }
func (a *Auth) step2() (partReadWriter, error) {
	traceAuth("step2")
	prms := &authPrms{}
	if err := a.method.prepareFinalReq(prms); err != nil {
		return nil, err
	}
	return &authFinalReq{prms}, nil
}
func (a *Auth) step3() (partReadWriter, error) {
	traceAuth("step3")
	return &authFinalRep{method: a.method}, nil
}

type authInitReq struct {
	prms *authPrms
}

func (r *authInitReq) String() string                                     { return fmt.Sprintf("parameters %v", r.prms) }
func (r *authInitReq) size() int                                          { return r.prms.size() }
func (r *authInitReq) decode(dec *encoding.Decoder, ph *partHeader) error { panic("not implemented") }
func (r *authInitReq) encode(enc *encoding.Encoder) error                 { return r.prms.encode(enc) }

type authInitRep struct {
	auth *Auth
}

func (r *authInitRep) String() string                     { return fmt.Sprintf("auth: %s", r.auth) }
func (r *authInitRep) size() int                          { panic("not implemented") }
func (r *authInitRep) encode(enc *encoding.Encoder) error { panic("not implemented") }

func (r *authInitRep) decode(dec *encoding.Decoder, ph *partHeader) error {
	d := newAuthDecoder(dec)

	if err := d.numPrm(2); err != nil {
		return err
	}
	mt, err := d.string()
	if err != nil {
		return err
	}

	if err := r.auth.setMethod(mt); err != nil {
		return err
	}
	return r.auth.method.initRepDecode(d)
}

type authFinalReq struct {
	prms *authPrms
}

func (r *authFinalReq) String() string                                     { return fmt.Sprintf("parameters %v", r.prms) }
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
