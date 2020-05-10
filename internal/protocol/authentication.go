/*
Copyright 2020 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package protocol

//Salted Challenge Response Authentication Mechanism (SCRAM)

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"golang.org/x/crypto/pbkdf2"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
	"github.com/SAP/go-hdb/internal/unicode/cesu8"
)

const (
	mnSCRAMSHA256       = "SCRAMSHA256"       // password
	mnSCRAMPBKDF2SHA256 = "SCRAMPBKDF2SHA256" // pbkdf2
)

const (
	clientChallengeSize = 64
	serverChallengeSize = 48
	saltSize            = 16
	clientProofSize     = 32
)

const (
	int16Size  = 2
	uint32Size = 4
)

type authStepper interface {
	next() (partReadWriter, error)
}

type authMethod struct {
	method          string
	clientChallenge []byte
}

func (m *authMethod) String() string {
	return fmt.Sprintf("method %s clientChallenge %v", m.method, m.clientChallenge)
}

func (m *authMethod) size() int {
	size := 2 // number of parameters
	size += len(m.method)
	size += len(m.clientChallenge)
	return size
}

func (r *authMethod) decode(dec *encoding.Decoder, ph *partHeader) error {
	r.method = string(decodeShortBytes(dec))
	r.clientChallenge = decodeShortBytes(dec)
	return nil
}

func (r *authMethod) encode(enc *encoding.Encoder) error {
	if err := encodeShortBytes(enc, []byte(r.method)); err != nil {
		return err
	}
	if err := encodeShortBytes(enc, r.clientChallenge); err != nil {
		return err
	}
	return nil
}

type authInitReq struct {
	username string
	methods  []*authMethod
}

func (r *authInitReq) String() string {
	return fmt.Sprintf("username %s methods %v", r.username, r.methods)
}

func (r *authInitReq) size() int {
	size := int16Size // no of parameters
	size++            // len byte username
	size += cesu8.StringSize(r.username)
	for _, m := range r.methods {
		size += m.size()
	}
	return size
}

func (r *authInitReq) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	r.username = decodeShortCESU8String(dec)
	numMethod := (numPrm - 1) / 2
	r.methods = make([]*authMethod, numMethod)
	for i := 0; i < len(r.methods); i++ {
		authMethod := &authMethod{}
		r.methods[i] = authMethod
		if err := authMethod.decode(dec, ph); err != nil {
			return err
		}
	}
	return nil
}

func (r *authInitReq) encode(enc *encoding.Encoder) error {
	enc.Int16(int16(1 + len(r.methods)*2)) // username + methods á each two fields
	if err := encodeShortCESU8String(enc, r.username); err != nil {
		return err
	}
	for _, m := range r.methods {
		m.encode(enc)
	}
	return nil
}

type authInitSCRAMSHA256Rep struct {
	salt, serverChallenge []byte
}

func (r *authInitSCRAMSHA256Rep) String() string {
	return fmt.Sprintf("salt %v serverChallenge %v", r.salt, r.serverChallenge)
}

func (r *authInitSCRAMSHA256Rep) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	if numPrm != 2 {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, 2)
	}
	r.salt = decodeShortBytes(dec)
	r.serverChallenge = decodeShortBytes(dec)
	return nil
}

type authInitSCRAMPBKDF2SHA256Rep struct {
	salt, serverChallenge []byte
	rounds                uint32
}

func (r *authInitSCRAMPBKDF2SHA256Rep) String() string {
	return fmt.Sprintf("salt %v serverChallenge %v rounds %d", r.salt, r.serverChallenge, r.rounds)
}

func (r *authInitSCRAMPBKDF2SHA256Rep) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	if numPrm != 3 {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, 3)
	}
	r.salt = decodeShortBytes(dec)
	r.serverChallenge = decodeShortBytes(dec)
	size := dec.Byte()
	if size != uint32Size {
		return fmt.Errorf("invalid auth uint32 size %d - expected %d", size, uint32Size)
	}
	r.rounds = dec.Uint32ByteOrder(binary.BigEndian) // big endian coded (e.g. rounds param)
	return nil
}

type authInitRep struct {
	method string
	prms   partDecoder
}

func (r *authInitRep) String() string                     { return fmt.Sprintf("method %s parameters %v", r.method, r.prms) }
func (r *authInitRep) size() int                          { panic("not implemented") }
func (r *authInitRep) encode(enc *encoding.Encoder) error { panic("not implemented") }

func (r *authInitRep) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	if numPrm != 2 {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, 2)
	}
	r.method = string(decodeShortBytes(dec))

	dec.Byte() // sub parameter lenght

	switch r.method {
	case mnSCRAMSHA256:
		r.prms = &authInitSCRAMSHA256Rep{}
		return r.prms.decode(dec, ph)
	case mnSCRAMPBKDF2SHA256:
		r.prms = &authInitSCRAMPBKDF2SHA256Rep{}
		return r.prms.decode(dec, ph)
	default:
		return fmt.Errorf("invalid or not supported authentication method %s", r.method)
	}
}

type authClientProofReq struct {
	clientProof []byte
}

func (r *authClientProofReq) String() string { return fmt.Sprintf("clientProof %v", r.clientProof) }

func (r *authClientProofReq) size() int {
	size := int16Size // no of parameters
	size += len(r.clientProof) + 1
	return size
}

func (r *authClientProofReq) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	if numPrm != 1 {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, 1)
	}
	r.clientProof = decodeShortBytes(dec)
	return nil
}

func (r *authClientProofReq) encode(enc *encoding.Encoder) error {
	enc.Int16(1)
	if err := encodeShortBytes(enc, r.clientProof); err != nil {
		return err
	}
	return nil
}

type authFinalReq struct {
	username, method string
	prms             partDecodeEncoder
}

func (r *authFinalReq) String() string {
	return fmt.Sprintf("username %s methods %s parameter %v", r.username, r.method, r.prms)
}

func (r *authFinalReq) size() int {
	size := int16Size // no of parameters
	size += cesu8.StringSize(r.username) + 1
	size += len(r.method) + 1
	size++ // len sub parameters
	size += r.prms.size()
	return size
}

func (r *authFinalReq) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	if numPrm != 3 {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, 3)
	}
	r.username = decodeShortCESU8String(dec)
	r.method = string(decodeShortBytes(dec))
	dec.Byte() // sub parameters
	r.prms = &authClientProofReq{}
	return r.prms.decode(dec, ph)
}

func (r *authFinalReq) encode(enc *encoding.Encoder) error {
	enc.Int16(3)
	if err := encodeShortCESU8String(enc, r.username); err != nil {
		return err
	}
	if err := encodeShortBytes(enc, []byte(r.method)); err != nil {
		return err
	}
	enc.Byte(byte(r.prms.size()))
	return r.prms.encode(enc)
}

type authServerProofRep struct {
	serverProof []byte
}

func (r *authServerProofRep) String() string { return fmt.Sprintf("serverProof %v", r.serverProof) }

func (r *authServerProofRep) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	if numPrm != 1 {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, 1)
	}
	r.serverProof = decodeShortBytes(dec)
	return nil
}

func (r *authServerProofRep) encode(enc *encoding.Encoder) error {
	enc.Int16(1)
	if err := encodeShortBytes(enc, r.serverProof); err != nil {
		return err
	}
	return nil
}

type authFinalRep struct {
	method string
	prms   partDecoder
}

func (r *authFinalRep) String() string                     { return fmt.Sprintf("method %s parameter %v", r.method, r.prms) }
func (r *authFinalRep) size() int                          { panic("not implemented") }
func (r *authFinalRep) encode(enc *encoding.Encoder) error { panic("not implemented") }

func (r *authFinalRep) decode(dec *encoding.Decoder, ph *partHeader) error {
	numPrm := int(dec.Int16())
	if numPrm != 2 {
		return fmt.Errorf("invalid number of parameters %d - expected %d", numPrm, 2)
	}
	r.method = string(decodeShortBytes(dec))
	dec.Byte() // sub parameters
	r.prms = &authServerProofRep{}
	return r.prms.decode(dec, ph)
}

type auth struct {
	step               int
	username, password string
	methods            []*authMethod
	initRep            *authInitRep
}

func newAuth(username, password string) *auth {
	return &auth{
		username: username,
		password: password,
		methods: []*authMethod{
			&authMethod{method: mnSCRAMPBKDF2SHA256, clientChallenge: clientChallenge()},
			&authMethod{method: mnSCRAMSHA256, clientChallenge: clientChallenge()},
		},
		initRep: &authInitRep{},
	}
}

func (a *auth) clientChallenge(method string) []byte {
	for _, m := range a.methods {
		if m.method == method {
			return m.clientChallenge
		}
	}
	panic("should never happen")
}

func (a *auth) next() (partReadWriter, error) {
	defer func() { a.step++ }()

	switch a.step {
	case 0:
		for _, m := range a.methods {
			if len(m.clientChallenge) != clientChallengeSize {
				return nil, fmt.Errorf("invalid client challenge size %d - expected %d", len(m.clientChallenge), clientChallengeSize)
			}
		}
		return &authInitReq{username: a.username, methods: a.methods}, nil
	case 1:
		return a.initRep, nil
	case 2:
		var clientProof []byte

		switch a.initRep.method {
		case mnSCRAMSHA256:
			prms := a.initRep.prms.(*authInitSCRAMSHA256Rep)
			if len(prms.salt) != saltSize {
				return nil, fmt.Errorf("invalid salt size %d - expected %d", len(prms.salt), saltSize)
			}
			if len(prms.serverChallenge) != serverChallengeSize {
				return nil, fmt.Errorf("invalid server challenge size %d - expected %d", len(prms.serverChallenge), serverChallengeSize)
			}
			clientProof = clientProofSCRAMSHA256(prms.salt, prms.serverChallenge, a.clientChallenge(a.initRep.method), []byte(a.password))
		case mnSCRAMPBKDF2SHA256:
			prms := a.initRep.prms.(*authInitSCRAMPBKDF2SHA256Rep)
			if len(prms.salt) != saltSize {
				return nil, fmt.Errorf("invalid salt size %d - expected %d", len(prms.salt), saltSize)
			}
			if len(prms.serverChallenge) != serverChallengeSize {
				return nil, fmt.Errorf("invalid server challenge size %d - expected %d", len(prms.serverChallenge), serverChallengeSize)
			}
			clientProof = clientProofSCRAMPBKDF2SHA256(prms.salt, prms.serverChallenge, prms.rounds, a.clientChallenge(a.initRep.method), []byte(a.password))
		default:
			panic("should never happen")
		}
		if len(clientProof) != clientProofSize {
			return nil, fmt.Errorf("invalid client proof size %d - expected %d", len(clientProof), clientProofSize)
		}
		return &authFinalReq{username: a.username, method: a.initRep.method, prms: &authClientProofReq{clientProof: clientProof}}, nil
	case 3:
		return &authFinalRep{}, nil
	}
	panic("should never happen")
}

func clientChallenge() []byte {
	r := make([]byte, clientChallengeSize)
	if _, err := rand.Read(r); err != nil {
		plog.Fatalf("client challenge fatal error")
	}
	return r
}

func clientProofSCRAMSHA256(salt, serverChallenge, clientChallenge, password []byte) []byte {
	buf := make([]byte, 0, len(salt)+len(serverChallenge)+len(clientChallenge))
	buf = append(buf, salt...)
	buf = append(buf, serverChallenge...)
	buf = append(buf, clientChallenge...)

	key := _sha256(_hmac(password, salt))
	sig := _hmac(_sha256(key), buf)

	proof := xor(sig, key)
	return proof
}

func clientProofSCRAMPBKDF2SHA256(salt, serverChallenge []byte, rounds uint32, clientChallenge, password []byte) []byte {
	buf := make([]byte, 0, len(salt)+len(serverChallenge)+len(clientChallenge))
	buf = append(buf, salt...)
	buf = append(buf, serverChallenge...)
	buf = append(buf, clientChallenge...)

	key := _sha256(pbkdf2.Key(password, salt, int(rounds), clientProofSize, sha256.New))
	sig := _hmac(_sha256(key), buf)

	proof := xor(sig, key)
	return proof
}

func _sha256(p []byte) []byte {
	hash := sha256.New()
	hash.Write(p)
	s := hash.Sum(nil)
	return s
}

func _hmac(key, p []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(p)
	s := hash.Sum(nil)
	return s
}

func xor(sig, key []byte) []byte {
	r := make([]byte, len(sig))

	for i, v := range sig {
		r[i] = v ^ key[i]
	}
	return r
}

// helper decode / encode
func decodeShortCESU8String(dec *encoding.Decoder) string {
	size := dec.Byte()
	return string(dec.CESU8Bytes(int(size)))
}

func encodeShortCESU8String(enc *encoding.Encoder, s string) error {
	size := cesu8.StringSize(s)
	if size > math.MaxUint8 {
		return fmt.Errorf("invalid auth parameter lenght %d", size)
	}
	enc.Byte(byte(size))
	enc.CESU8String(s)
	return nil
}

func decodeShortBytes(dec *encoding.Decoder) []byte {
	size := dec.Byte()
	b := make([]byte, size)
	dec.Bytes(b)
	return b
}

func encodeShortBytes(enc *encoding.Encoder, b []byte) error {
	size := len(b)
	if size > math.MaxUint8 {
		return fmt.Errorf("invalid auth parameter lenght %d", size)
	}
	enc.Byte(byte(size))
	enc.Bytes(b)
	return nil
}
