// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

// Salted Challenge Response Authentication Mechanism (SCRAM)

import (
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

// authSCRAMPBKDF2SHA256 implements SCRAMPBKDF2SHA256 authentication.
type authSCRAMPBKDF2SHA256 struct {
	username, password       string
	clientChallenge          []byte
	salt, serverChallenge    []byte
	clientProof, serverProof []byte
	rounds                   uint32
}

// newAuthSCRAMPBKDF2SHA256 creates a new authSCRAMPBKDF2SHA256 instance.
func newAuthSCRAMPBKDF2SHA256(username, password string) *authSCRAMPBKDF2SHA256 {
	return &authSCRAMPBKDF2SHA256{username: username, password: password, clientChallenge: clientChallenge()}
}

func (a *authSCRAMPBKDF2SHA256) String() string {
	return fmt.Sprintf("method type %s clientChallenge %v", a.typ(), a.clientChallenge)
}

// SetPassword implenets the AuthPasswordSetter interface.
func (a *authSCRAMPBKDF2SHA256) SetPassword(password string) { a.password = password }

func (a *authSCRAMPBKDF2SHA256) typ() string { return amtSCRAMPBKDF2SHA256 }

func (a *authSCRAMPBKDF2SHA256) order() byte { return amoSCRAMPBKDF2SHA256 }

func (a *authSCRAMPBKDF2SHA256) prepareInitReq(prms *authPrms) {
	prms.addString(a.typ())
	prms.addBytes(a.clientChallenge)
}

func (a *authSCRAMPBKDF2SHA256) initRepDecode(d *authDecoder) error {
	d.subSize() // sub parameters
	if err := d.numPrm(3); err != nil {
		return err
	}
	var err error
	if a.salt, err = d.bytes(); err != nil {
		return err
	}
	if a.serverChallenge, err = d.bytes(); err != nil {
		return err
	}
	if err := checkSalt(a.salt); err != nil {
		return err
	}
	if err := checkServerChallenge(a.serverChallenge); err != nil {
		return err
	}
	if a.rounds, err = d.bigUint32(); err != nil {
		return err
	}
	return nil
}

func (a *authSCRAMPBKDF2SHA256) prepareFinalReq(prms *authPrms) error {
	key := scrampbkdf2sha256Key([]byte(a.password), a.salt, int(a.rounds))
	a.clientProof = clientProof(key, a.salt, a.serverChallenge, a.clientChallenge)
	if err := checkClientProof(a.clientProof); err != nil {
		return err
	}

	prms.addCESU8String(a.username)
	prms.addString(a.typ())
	subPrms := prms.addPrms()
	subPrms.addBytes(a.clientProof)

	return nil
}

func (a *authSCRAMPBKDF2SHA256) finalRepDecode(d *authDecoder) error {
	if err := d.numPrm(2); err != nil {
		return err
	}
	mt, err := d.string()
	if err != nil {
		return err
	}
	if err := checkAuthMethodType(mt, a.typ()); err != nil {
		return err
	}
	d.subSize()
	if err := d.numPrm(1); err != nil {
		return err
	}
	a.serverProof, err = d.bytes()
	return err
}

func scrampbkdf2sha256Key(password, salt []byte, rounds int) []byte {
	return _sha256(pbkdf2.Key(password, salt, rounds, clientProofSize, sha256.New))
}
