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

// authSCRAMPBKDF2SHA256 implements mnSCRAMPBKDF2SHA256.
type authSCRAMPBKDF2SHA256 struct {
	_methodName              string
	username, password       string
	clientChallenge          []byte
	salt, serverChallenge    []byte
	clientProof, serverProof []byte
	rounds                   uint32
}

func newAuthSCRAMPBKDF2SHA256(username, password string) authMethod {
	return &authSCRAMPBKDF2SHA256{_methodName: mnSCRAMPBKDF2SHA256, username: username, password: password, clientChallenge: clientChallenge()}
}

func (a *authSCRAMPBKDF2SHA256) String() string {
	return fmt.Sprintf("method %s clientChallenge %v", a.methodName(), a.clientChallenge)
}

func (a *authSCRAMPBKDF2SHA256) methodName() string { return a._methodName }

func (a *authSCRAMPBKDF2SHA256) prepareInitReq(prms *authPrms) {
	prms.addString(a.methodName())
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
	prms.addString(a.methodName())
	subPrms := prms.addPrms()
	subPrms.addBytes(a.clientProof)

	return nil
}

func (a *authSCRAMPBKDF2SHA256) finalRepDecode(d *authDecoder) error {
	if err := d.numPrm(2); err != nil {
		return err
	}
	methodName, err := d.string()
	if err != nil {
		return err
	}
	if err := checkAuthMethodName(methodName, a.methodName()); err != nil {
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
