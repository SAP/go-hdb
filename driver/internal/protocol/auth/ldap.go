// SPDX-FileCopyrightText: 2014-2024 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/auth/ldap"
)

// LDAP implements LDAP authentication.
type LDAP struct {
	username string
	password string

	clientChallenge *ldap.ClientChallenge
	serverChallenge *ldap.ServerChallenge
}

// NewLDAP creates a new LDAP authentication instance.
func NewLDAP(username, password string) *LDAP {
	return &LDAP{
		username: username,
		password: password,
	}
}

func (a *LDAP) String() string {
	return fmt.Sprintf("method type %s username %s", a.Typ(), a.username)
}
func (a *LDAP) Typ() string { return MtLDAP }
func (a *LDAP) Order() byte { return MoLDAP }

func (a *LDAP) PrepareInitReq(prms *Prms) error {
	a.clientChallenge = ldap.NewClientChallenge()

	prms.addString(a.Typ())

	// Add sub-parameters: clientNonce and capabilities
	subPrms := prms.addPrms()
	subPrms.addBytes(a.clientChallenge.ClientNonce[:])
	subPrms.addBytes(a.clientChallenge.Capabilities[:])

	return nil
}

func (a *LDAP) InitRepDecode(d *Decoder) error {
	d.subSize()
	if err := d.NumPrm(4); err != nil {
		return fmt.Errorf("LDAP authentication: %w", err)
	}

	sc, err := ldap.NewServerChallenge(d.bytes(), d.bytes(), d.bytes(), d.bytes())
	if err != nil {
		return fmt.Errorf("LDAP authentication: %w", err)
	}
	if sc.ClientNonce != a.clientChallenge.ClientNonce {
		return fmt.Errorf("LDAP authentication: client nonce mismatch")
	}

	a.serverChallenge = sc
	return nil
}

func (a *LDAP) PrepareFinalReq(prms *Prms) error {
	clientProof, err := ldap.NewClientProof(a.password, a.serverChallenge)
	if err != nil {
		return fmt.Errorf("LDAP authentication: %w", err)
	}

	prms.AddCESU8String(a.username)
	prms.addString(a.Typ())

	subPrms := prms.addPrms()
	subPrms.addBytes(clientProof.EncryptedSessionKey)
	subPrms.addBytes(clientProof.EncryptedPassword)

	return nil
}

func (a *LDAP) FinalRepDecode(d *Decoder) error {
	if err := d.NumPrm(2); err != nil {
		return fmt.Errorf("LDAP authentication: %w", err)
	}

	fr, err := ldap.NewFinalResponse(d.String(), d.bytes())
	if err != nil {
		return fmt.Errorf("LDAP authentication: %w", err)
	}
	if err := checkAuthMethodType(fr.MethodName, a.Typ()); err != nil {
		return err
	}
	return nil
}
