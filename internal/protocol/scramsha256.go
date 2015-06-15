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

	"github.com/SAP/go-hdb/internal/bufio"
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

func newScramsha256InitialRequest() *scramsha256InitialRequest {
	return &scramsha256InitialRequest{}
}

func (r *scramsha256InitialRequest) kind() partKind {
	return pkAuthentication
}

func (r *scramsha256InitialRequest) size() (int, error) {
	return 2 + authFieldSize(r.username) + authFieldSize([]byte(mnSCRAMSHA256)) + authFieldSize(r.clientChallenge), nil
}

func (r *scramsha256InitialRequest) numArg() int {
	return 1
}

func (r *scramsha256InitialRequest) write(wr *bufio.Writer) error {
	if err := wr.WriteInt16(3); err != nil { //field count
		return err
	}
	if err := writeAuthField(wr, r.username); err != nil {
		return err
	}
	if err := writeAuthField(wr, []byte(mnSCRAMSHA256)); err != nil {
		return err
	}
	if err := writeAuthField(wr, r.clientChallenge); err != nil {
		return err
	}
	return nil
}

type scramsha256InitialReply struct {
	salt            []byte
	serverChallenge []byte
}

func newScramsha256InitialReply() *scramsha256InitialReply {
	return &scramsha256InitialReply{}
}

func (r *scramsha256InitialReply) kind() partKind {
	return pkAuthentication
}

func (r *scramsha256InitialReply) setNumArg(int) {
	//not needed
}

func (r *scramsha256InitialReply) read(rd *bufio.Reader) error {
	cnt, err := rd.ReadInt16()
	if err != nil {
		return err
	}

	if err := readMethodName(rd); err != nil {
		return err
	}

	size, err := rd.ReadByte()
	if err != nil {
		return err
	}
	if size != serverChallengeDataSize {
		return fmt.Errorf("invalid server challenge data size %d - %d expected", size, serverChallengeDataSize)
	}

	//server challenge data

	cnt, err = rd.ReadInt16()
	if err != nil {
		return err
	}
	if cnt != 2 {
		return fmt.Errorf("invalid server challenge data field count %d - %d expected", cnt, 2)
	}

	size, err = rd.ReadByte()
	if err != nil {
		return err
	}
	if trace {
		logger.Printf("salt size %d", size)
	}

	r.salt = make([]byte, size)
	if err := rd.ReadFull(r.salt); err != nil {
		return err
	}
	if trace {
		logger.Printf("salt %v", r.salt)
	}

	size, err = rd.ReadByte()
	if err != nil {
		return err
	}

	r.serverChallenge = make([]byte, size)
	if err := rd.ReadFull(r.serverChallenge); err != nil {
		return err
	}
	if trace {
		logger.Printf("server challenge %v", r.serverChallenge)
	}

	return nil
}

type scramsha256FinalRequest struct {
	username    []byte
	clientProof []byte
}

func newScramsha256FinalRequest() *scramsha256FinalRequest {
	return &scramsha256FinalRequest{}
}

func (r *scramsha256FinalRequest) kind() partKind {
	return pkAuthentication
}

func (r *scramsha256FinalRequest) size() (int, error) {
	return 2 + authFieldSize(r.username) + authFieldSize([]byte(mnSCRAMSHA256)) + authFieldSize(r.clientProof), nil
}

func (r *scramsha256FinalRequest) numArg() int {
	return 1
}

func (r *scramsha256FinalRequest) write(wr *bufio.Writer) error {
	if err := wr.WriteInt16(3); err != nil { //field count
		return err
	}
	if err := writeAuthField(wr, r.username); err != nil {
		return err
	}
	if err := writeAuthField(wr, []byte(mnSCRAMSHA256)); err != nil {
		return err
	}
	if err := writeAuthField(wr, r.clientProof); err != nil {
		return err
	}
	return nil
}

type scramsha256FinalReply struct {
	serverProof []byte
}

func newScramsha256FinalReply() *scramsha256FinalReply {
	return &scramsha256FinalReply{}
}

func (r *scramsha256FinalReply) kind() partKind {
	return pkAuthentication
}

func (r *scramsha256FinalReply) setNumArg(int) {
	//not needed
}

func (r *scramsha256FinalReply) read(rd *bufio.Reader) error {
	cnt, err := rd.ReadInt16()
	if err != nil {
		return err
	}
	if cnt != 2 {
		return fmt.Errorf("invalid final reply field count %d - %d expected", cnt, 2)
	}

	if err := readMethodName(rd); err != nil {
		return err
	}

	//serverProof
	size, err := rd.ReadByte()
	if err != nil {
		return err
	}

	serverProof := make([]byte, size)
	if err := rd.ReadFull(serverProof); err != nil {
		return err
	}

	return nil
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

func writeAuthField(wr *bufio.Writer, f []byte) error {
	size := len(f)
	if size >= 250 {
		// - different indicators compared to db field handling
		// - 1-5 bytes? but only 1 resp 3 bytes explained
		panic("not implemented error")
	}

	if err := wr.WriteByte(byte(size)); err != nil {
		return err
	}

	if _, err := wr.Write(f); err != nil {
		return err
	}

	return nil
}

func readMethodName(rd *bufio.Reader) error {
	size, err := rd.ReadByte()
	if err != nil {
		return err
	}
	methodName := make([]byte, size)
	if err := rd.ReadFull(methodName); err != nil {
		return err
	}
	if string(methodName) != mnSCRAMSHA256 {
		return fmt.Errorf("invalid authentication method %s - %s expected", methodName, mnSCRAMSHA256)
	}
	return nil
}

func clientChallenge() []byte {
	r := make([]byte, clientChallengeSize)
	if _, err := rand.Read(r); err != nil {
		logger.Fatal("client challenge fatal error")
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
	if trace {
		logger.Printf("sha length %d value %v", len(s), s)
	}
	return s
}

func _hmac(key, p []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(p)
	s := hash.Sum(nil)
	if trace {
		logger.Printf("hmac length %d value %v", len(s), s)
	}
	return s
}

func xor(sig, key []byte) []byte {
	r := make([]byte, len(sig))

	for i, v := range sig {
		r[i] = v ^ key[i]
	}
	return r
}
