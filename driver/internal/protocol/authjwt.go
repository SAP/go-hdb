// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"
	"strings"
)

// authJWT implements mnJWT.
type authJWT struct {
	token    string
	username string
	cookie   []byte
}

func newAuthJWT(token string) authMethod {
	return &authJWT{token: token}
}

func (a *authJWT) String() string {
	return fmt.Sprintf("method %s token %s", a.methodName(), a.token)
}

func (a *authJWT) methodName() string { return mnJWT }

func (a *authJWT) prepareInitReq(prms *authPrms) {
	prms.addString(a.methodName())
	prms.addString(a.token)
}

func (a *authJWT) initRepDecode(d *authDecoder) error {
	var err error
	a.username, err = d.string()
	return err
}

func (a *authJWT) prepareFinalReq(prms *authPrms) error {
	prms.addCESU8String(a.username)
	prms.addString(a.methodName())
	prms.addEmpty() // empty parameter
	return nil
}

func (a *authJWT) finalRepDecode(d *authDecoder) error {
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
	a.cookie, err = d.bytes()
	return err
}

func isJWTToken(token string) bool {
	return strings.HasPrefix(token, "ey")
}
