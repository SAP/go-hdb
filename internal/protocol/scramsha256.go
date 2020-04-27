/*
Copyright 2014 SAP SE

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
	"fmt"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

const (
	clientChallengeSize     = 64
	serverChallengeDataSize = 68
	clientProofDataSize     = 35
	clientProofSize         = 32
)

type scramsha256InitialRequest struct {
	username        []byte
	clientChallenge []byte
}

func (r *scramsha256InitialRequest) String() string {
	return fmt.Sprintf("username %s type %s clientChallenge %v", r.username, mnSCRAMSHA256, r.clientChallenge)
}

func (r *scramsha256InitialRequest) size() int {
	return 2 + authFieldSize(r.username) + authFieldSize([]byte(mnSCRAMSHA256)) + authFieldSize(r.clientChallenge)
}

func (r *scramsha256InitialRequest) decode(dec *encoding.Decoder, ph *partHeader) error {
	dec.Int16() // cnt

	size := dec.Byte()
	r.username = make([]byte, size)
	dec.Bytes(r.username)

	size = dec.Byte()
	dec.Skip(int(size)) // mnSCRAMSHA256

	size = dec.Byte()
	r.clientChallenge = make([]byte, size)
	dec.Bytes(r.clientChallenge)

	return dec.Error()
}

func (r *scramsha256InitialRequest) encode(enc *encoding.Encoder) error {
	enc.Int16(3)
	encodeAuthField(enc, r.username)
	encodeAuthField(enc, []byte(mnSCRAMSHA256))
	encodeAuthField(enc, r.clientChallenge)
	return nil
}

type scramsha256InitialReply struct {
	salt            []byte
	serverChallenge []byte
}

func (r *scramsha256InitialReply) String() string {
	return fmt.Sprintf("salt %v serverChallenge %v", r.salt, r.serverChallenge)
}

func (r *scramsha256InitialReply) decode(dec *encoding.Decoder, ph *partHeader) error {
	dec.Int16() // cnt
	if err := readMethodName(dec); err != nil {
		println("read method name error")

		return err
	}
	size := dec.Byte()

	//TODO check: python client gives different challenge data size
	// disable check

	// if size != serverChallengeDataSize {
	// 	println("server challenge data size error")

	// 	return fmt.Errorf("invalid server challenge data size %d - %d expected", size, serverChallengeDataSize)
	// }

	//server challenge data

	cnt := dec.Int16()

	// println("data field count")
	// println(cnt)

	if cnt != 2 {
		//println("invalid server challenge data field count")
		return fmt.Errorf("invalid server challenge data field count %d - %d expected", cnt, 2)
	}

	size = dec.Byte()
	r.salt = make([]byte, size)
	dec.Bytes(r.salt)

	size = dec.Byte()
	r.serverChallenge = make([]byte, size)
	dec.Bytes(r.serverChallenge)

	return dec.Error()
}

type scramsha256FinalRequest struct {
	username    []byte
	clientProof []byte
}

func (r *scramsha256FinalRequest) String() string {
	return fmt.Sprintf("username %s type %s clientProof %v", r.username, mnSCRAMSHA256, r.clientProof)
}

func (r *scramsha256FinalRequest) size() int {
	return 2 + authFieldSize(r.username) + authFieldSize([]byte(mnSCRAMSHA256)) + authFieldSize(r.clientProof)
}

func (r *scramsha256FinalRequest) decode(dec *encoding.Decoder, ph *partHeader) error {
	dec.Int16() // cnt

	size := dec.Byte()
	r.username = make([]byte, size)
	dec.Bytes(r.username)

	size = dec.Byte()
	dec.Skip(int(size)) // mnSCRAMSHA256

	size = dec.Byte()
	r.clientProof = make([]byte, size)
	dec.Bytes(r.clientProof)

	return nil
}

func (r *scramsha256FinalRequest) encode(enc *encoding.Encoder) error {
	enc.Int16(3)
	encodeAuthField(enc, r.username)
	encodeAuthField(enc, []byte(mnSCRAMSHA256))
	encodeAuthField(enc, r.clientProof)
	return nil
}

type scramsha256FinalReply struct {
	serverProof []byte
}

func (r *scramsha256FinalReply) String() string {
	return fmt.Sprintf("serverProof %v", r.serverProof)
}

func (r *scramsha256FinalReply) decode(dec *encoding.Decoder, ph *partHeader) error {
	cnt := dec.Int16()
	if cnt != 2 {
		return fmt.Errorf("invalid final reply field count %d - %d expected", cnt, 2)
	}
	if err := readMethodName(dec); err != nil {
		return err
	}

	//serverProof
	size := dec.Byte()
	serverProof := make([]byte, size)
	dec.Bytes(serverProof)

	return dec.Error()
}

//helper
func authFieldSize(f []byte) int {
	size := len(f)
	if size >= 250 {
		// - different indicators compared to db field handling
		// - 1-5 bytes? but only 1 resp 3 bytes explained
		panic("not implemented error")
	}
	return size + 1 //length indicator size := 1
}

func encodeAuthField(enc *encoding.Encoder, f []byte) {
	size := len(f)
	if size >= 250 {
		// - different indicators compared to db field handling
		// - 1-5 bytes? but only 1 resp 3 bytes explained
		panic("not implemented error")
	}

	enc.Byte(byte(size))
	enc.Bytes(f)
}

func readMethodName(dec *encoding.Decoder) error {
	size := dec.Byte()
	methodName := make([]byte, size)
	dec.Bytes(methodName)

	// println("methodname")
	// println(string(methodName))

	//TODO - python client
	// python client: database response with SCRAMPBKDF2SHA256
	// --> disable check

	// if string(methodName) != mnSCRAMSHA256 {
	// 	return fmt.Errorf("invalid authentication method %s - %s expected", methodName, mnSCRAMSHA256)
	// }

	return nil
}

func clientChallenge() []byte {
	r := make([]byte, clientChallengeSize)
	if _, err := rand.Read(r); err != nil {
		plog.Fatalf("client challenge fatal error")
	}
	return r
}

func clientProof(salt, serverChallenge, clientChallenge, password []byte) []byte {

	clientProof := make([]byte, clientProofDataSize)

	buf := make([]byte, 0, len(salt)+len(serverChallenge)+len(clientChallenge))
	buf = append(buf, salt...)
	buf = append(buf, serverChallenge...)
	buf = append(buf, clientChallenge...)

	key := _sha256(_hmac(password, salt))
	sig := _hmac(_sha256(key), buf)

	proof := xor(sig, key)
	//actual implementation: only one salt value?
	clientProof[0] = 0
	clientProof[1] = 1
	clientProof[2] = clientProofSize
	copy(clientProof[3:], proof)
	return clientProof
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
