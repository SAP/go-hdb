// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"bytes"
	"strings"
	"sync"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// authAttrs is holding authentication relevant attributes.
type authAttrs struct {
	mu                      sync.RWMutex
	_username, _password    string // basic authentication
	_clientCert, _clientKey []byte // X509
	_token                  string // JWT
	_logonname              string // session cookie login does need logon name provided by JWT authentication.
	_sessionCookie          []byte // authentication via session cookie (HDB currently does support only SAML and JWT - go-hdb JWT)
	_refreshPassword        func() (password string, ok bool)
	_refreshClientCert      func() (clientCert, clientKey []byte, ok bool)
	_refreshToken           func() (token string, ok bool)
}

func (a *authAttrs) username() string { a.mu.RLock(); defer a.mu.RUnlock(); return a._username }
func (a *authAttrs) password() string { a.mu.RLock(); defer a.mu.RUnlock(); return a._password }
func (a *authAttrs) setPassword(password string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._password = password
}
func (a *authAttrs) clientCert() (clientCert, clientKey []byte) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a._clientCert, a._clientKey
}
func (a *authAttrs) setClientCert(clientCert, clientKey []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._clientCert = clientCert
	a._clientKey = clientKey
}
func (a *authAttrs) token() string         { a.mu.RLock(); defer a.mu.RUnlock(); return a._token }
func (a *authAttrs) setToken(token string) { a.mu.Lock(); defer a.mu.Unlock(); a._token = token }
func (a *authAttrs) setSessionCookie(logonname string, cookie []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._logonname = logonname
	a._sessionCookie = cookie
}
func (a *authAttrs) refreshPassword() func() (password string, ok bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a._refreshPassword
}
func (a *authAttrs) setRefreshPassword(refreshPassword func() (password string, ok bool)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._refreshPassword = refreshPassword
}
func (a *authAttrs) refreshClientCert() func() (clientCert, clientKey []byte, ok bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a._refreshClientCert
}
func (a *authAttrs) setRefreshClientCert(refreshClientCert func() (clientCert, clientKey []byte, ok bool)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._refreshClientCert = refreshClientCert
}
func (a *authAttrs) refreshToken() func() (token string, ok bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a._refreshToken
}
func (a *authAttrs) setRefreshToken(refreshToken func() (token string, ok bool)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._refreshToken = refreshToken
}

func isJWTToken(token string) bool { return strings.HasPrefix(token, "ey") }

func (a *authAttrs) cookieAuth() *p.Auth {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a._sessionCookie == nil {
		return nil
	}

	auth := p.NewAuth(a._logonname)                                 // important: for session cookie auth we do need the logonname from JWT auth.
	auth.AddSessionCookie(a._sessionCookie, a._logonname, clientID) // And for HANA onPrem the final session cookie req needs the logonname as well.
	return auth
}

func (a *authAttrs) auth() *p.Auth {
	a.mu.RLock()
	defer a.mu.RUnlock()

	auth := p.NewAuth(a._username) // use username as logonname
	if a._clientCert != nil && a._clientKey != nil {
		auth.AddX509(a._clientCert, a._clientKey)
	}
	if a._token != "" {
		auth.AddJWT(a._token)
	}
	// mimic standard drivers and use password as token if user is empty
	if a._token == "" && a._username == "" && isJWTToken(a._password) {
		auth.AddJWT(a._password)
	}
	if a._password != "" {
		auth.AddBasic(a._username, a._password)
	}
	return auth
}

func (a *authAttrs) refresh(auth *p.Auth) bool {
	switch method := auth.Method().(type) {

	case p.AuthPasswordSetter:
		if fn := a.refreshPassword(); fn != nil {
			if password, ok := fn(); ok && a._password != password {
				a.setPassword(password)
				method.SetPassword(password)
				return true
			}
		}
	case p.AuthTokenSetter:
		if fn := a.refreshToken(); fn != nil {
			if token, ok := fn(); ok && a._token != token {
				a.setToken(token)
				method.SetToken(token)
				return true
			}
		}
	case p.AuthCertKeySetter:
		if fn := a.refreshClientCert(); fn != nil {
			if clientCert, clientKey, ok := fn(); ok && !(bytes.Equal(a._clientCert, clientCert) && bytes.Equal(a._clientKey, clientKey)) {
				a.setClientCert(clientCert, clientKey)
				method.SetCertKey(clientCert, clientKey)
				return true
			}
		}
	}
	return false
}
