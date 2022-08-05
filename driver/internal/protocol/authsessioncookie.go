// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"
)

// authSessionCookie implements session cookie authentication.
type authSessionCookie struct {
	cookie   []byte
	clientID string
}

// newAuthSessionCookie creates a new authSessionCookie instance.
func newAuthSessionCookie(cookie []byte, clientID string) *authSessionCookie {
	return &authSessionCookie{cookie: cookie, clientID: clientID}
}

func (a *authSessionCookie) String() string {
	return fmt.Sprintf("method %s cookie %v", a.typ(), a.cookie)
}

func (a *authSessionCookie) typ() string { return amtSessionCookie }

func (a *authSessionCookie) order() byte { return amoSessionCookie }

func (a *authSessionCookie) prepareInitReq(prms *authPrms) {
	prms.addString(a.typ())
	prms.addBytes(append(a.cookie, a.clientID...)) //cookie + clientID !!!
}

func (a *authSessionCookie) initRepDecode(d *authDecoder) error {
	return nil
}

func (a *authSessionCookie) prepareFinalReq(prms *authPrms) error {
	prms.addString(a.typ())
	prms.addEmpty() // empty parameter
	return nil
}

func (a *authSessionCookie) finalRepDecode(d *authDecoder) error {
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
	if _, err := d.bytes(); err != nil { // second parameter seems to be empty
		return err
	}
	return err
}
