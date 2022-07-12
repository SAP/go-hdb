// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

// Salted Challenge Response Authentication Mechanism (SCRAM)

import (
	"fmt"
)

// authSCRAMSHA256 implements mnSCRAMSHA256.
type authSCRAMSHA256 struct {
	username, password       string
	clientChallenge          []byte
	salt, serverChallenge    []byte
	clientProof, serverProof []byte
}

func newAuthSCRAMSHA256(username, password string) authMethod {
	return &authSCRAMSHA256{username: username, password: password, clientChallenge: clientChallenge()}
}

func (a *authSCRAMSHA256) String() string {
	return fmt.Sprintf("method %s clientChallenge %v", a.methodName(), a.clientChallenge)
}

func (a *authSCRAMSHA256) methodName() string { return mnSCRAMSHA256 }

func (a *authSCRAMSHA256) prepareInitReq(prms *authPrms) {
	prms.addString(a.methodName())
	prms.addBytes(a.clientChallenge)
}

func (a *authSCRAMSHA256) initRepDecode(d *authDecoder) error {
	d.subSize() // sub parameters
	if err := d.numPrm(2); err != nil {
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
	return nil
}

func (a *authSCRAMSHA256) prepareFinalReq(prms *authPrms) error {
	key := scramsha256Key([]byte(a.password), a.salt)
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

func (a *authSCRAMSHA256) finalRepDecode(d *authDecoder) error {
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
	if d.subSize() == 0 { // mnSCRAMSHA256: server does not return server proof parameter
		return nil
	}
	if err := d.numPrm(1); err != nil {
		return err
	}
	a.serverProof, err = d.bytes()
	return err
}

func scramsha256Key(password, salt []byte) []byte {
	return _sha256(_hmac(password, salt))
}
