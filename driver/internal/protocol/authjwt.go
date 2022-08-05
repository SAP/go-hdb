// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"
)

// authJWT implements JWT authentication.
type authJWT struct {
	token     string
	logonname string
	_cookie   []byte
}

// newAuthJWT creates a new authJWT instance.
func newAuthJWT(token string) *authJWT { return &authJWT{token: token} }

func (a *authJWT) String() string { return fmt.Sprintf("method type %s token %s", a.typ(), a.token) }

// SetToken implements the AuthTokenSetter interface.
func (a *authJWT) SetToken(token string) { a.token = token }

// Cookie implements the CookieGetter interface.
func (a *authJWT) Cookie() (string, []byte) { return a.logonname, a._cookie }

func (a *authJWT) typ() string { return amtJWT }

func (a *authJWT) order() byte { return amoJWT }

func (a *authJWT) prepareInitReq(prms *authPrms) {
	prms.addString(a.typ())
	prms.addString(a.token)
}

func (a *authJWT) initRepDecode(d *authDecoder) error {
	var err error
	a.logonname, err = d.string()
	traceAuthf("JWT auth - logonname: %v", a.logonname)
	return err
}

func (a *authJWT) prepareFinalReq(prms *authPrms) error {
	prms.addCESU8String(a.logonname)
	prms.addString(a.typ())
	prms.addEmpty() // empty parameter
	return nil
}

func (a *authJWT) finalRepDecode(d *authDecoder) error {
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
	a._cookie, err = d.bytes()
	traceAuthf("JWT auth - cookie: %v", a._cookie)
	return err
}
