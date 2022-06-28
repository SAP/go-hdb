package protocol

import (
	"fmt"
	"strings"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
)

const mnJWT = "JWT"

func checkMethodJWT(method string) error {
	if !strings.EqualFold(mnJWT, method) {
		return fmt.Errorf("unexpected method %s - expected %s", method, mnJWT)
	}

	return nil
}

type authMethodJWT struct {
	token string
}

func (m *authMethodJWT) String() string {
	return fmt.Sprintf("method %s token %s", mnJWT, m.token)
}

func (m *authMethodJWT) size() int {
	size := 1 // len byte for method name
	size += len(mnJWT)

	size += varBytesSize(len(m.token)) // token can be longer than bytesLenIndSmall bytes

	return size
}

func (m *authMethodJWT) decode(dec *encoding.Decoder, ph *partHeader) error {
	panic("not implemented")
}

func (m *authMethodJWT) encode(enc *encoding.Encoder) error {
	if err := authShortBytes.encode(enc, []byte(mnJWT)); err != nil {
		return err
	}

	return encodeVarBytes(enc, []byte(m.token))
}

type authInitReqJWT struct {
	method *authMethodJWT
}

func (r *authInitReqJWT) String() string {
	return fmt.Sprintf("emptyusername method %v", r.method)
}

func (r *authInitReqJWT) size() int {
	size := int16Size // no of parameters

	size++ // len byte for empty username

	size += r.method.size()

	return size
}

func (r *authInitReqJWT) encode(enc *encoding.Encoder) error {
	enc.Int16(int16(1 + 1*2)) // empty username + one method with two fields (method name + token)

	// empty user name
	if err := authShortCESU8String.encode(enc, ""); err != nil {
		return err
	}

	return r.method.encode(enc)
}

func (r *authInitReqJWT) decode(dec *encoding.Decoder, ph *partHeader) error {
	panic("not implemented")
}

type authInitRepJWT struct {
	username string
}

func (r *authInitRepJWT) String() string {
	return fmt.Sprintf("method %s user %v", mnJWT, r.username)
}

func (r *authInitRepJWT) size() int {
	panic("not implemented")
}

func (r *authInitRepJWT) encode(enc *encoding.Encoder) error {
	panic("not implemented")
}

func (r *authInitRepJWT) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	if numPrm != 2 {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, 2)
	}

	method := string(authShortBytes.decode(dec))
	if err := checkMethodJWT(method); err != nil {
		return err
	}

	var err error
	r.username, err = authShortCESU8String.decode(dec)

	return err
}

type authFinalReqJWT struct {
	username string
}

func (r *authFinalReqJWT) String() string {
	return fmt.Sprintf("username %s method %s emptyparam", r.username, mnJWT)
}

func (r *authFinalReqJWT) size() int {
	size := int16Size // no of parameters

	// username
	size++ // len byte username
	size += cesu8.StringSize(r.username)

	// method name
	size++ // len byte method name
	size += len(mnJWT)

	size++ // len byte empty parameter

	return size
}

func (r *authFinalReqJWT) decode(dec *encoding.Decoder, ph *partHeader) error {
	panic("not implemented")
}

func (r *authFinalReqJWT) encode(enc *encoding.Encoder) error {
	enc.Int16(3) // username, method, empty parameter

	if err := authShortCESU8String.encode(enc, r.username); err != nil {
		return err
	}

	if err := authShortBytes.encode(enc, []byte(mnJWT)); err != nil {
		return err
	}

	if err := authShortBytes.encode(enc, []byte{}); err != nil {
		return err
	}

	return nil
}

type authFinalRepJWT struct {
	cookie []byte
}

func (r *authFinalRepJWT) String() string {
	return fmt.Sprintf("method %s cookie %v", mnJWT, r.cookie)
}

func (r *authFinalRepJWT) size() int {
	panic("not implemented")
}

func (r *authFinalRepJWT) encode(enc *encoding.Encoder) error {
	panic("not implemented")
}

func (r *authFinalRepJWT) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	if numPrm != 2 {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, 2)
	}

	method := string(authShortBytes.decode(dec))
	if err := checkMethodJWT(method); err != nil {
		return err
	}

	cookieSize, null := decodeVarBytesSize(dec)
	if !null {
		r.cookie = make([]byte, cookieSize) // should we do something with the cookie?
		dec.Bytes(r.cookie)
	}

	return nil
}

type authJWT struct {
	step    int
	method  *authMethodJWT
	initRep *authInitRepJWT
}

func newAuthJWT(token string) *authJWT {
	return &authJWT{
		step:    0,
		method:  &authMethodJWT{token: token},
		initRep: &authInitRepJWT{},
	}
}

func (a *authJWT) next() (partReadWriter, error) {
	defer func() { a.step++ }()

	switch a.step {
	case 0:
		return &authInitReqJWT{method: a.method}, nil
	case 1:
		return a.initRep, nil
	case 2:
		return &authFinalReqJWT{username: a.initRep.username}, nil
	case 3:
		return &authFinalRepJWT{}, nil
	}
	panic("should never happen")
}
