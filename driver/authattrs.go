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
	hasCookie               atomicBool
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

/*
keep c as the instance name, so that the generated help does have the same variable name when object is
included in connector
*/

func isJWTToken(token string) bool { return strings.HasPrefix(token, "ey") }

func (c *authAttrs) cookieAuth() *p.Auth {
	if !c.hasCookie.Load() { // fastpath without lock
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	auth := p.NewAuth(c._logonname)                                 // important: for session cookie auth we do need the logonname from JWT auth.
	auth.AddSessionCookie(c._sessionCookie, c._logonname, clientID) // And for HANA onPrem the final session cookie req needs the logonname as well.
	return auth
}

func (c *authAttrs) auth() *p.Auth {
	c.mu.RLock()
	defer c.mu.RUnlock()

	auth := p.NewAuth(c._username) // use username as logonname
	if c._clientCert != nil && c._clientKey != nil {
		auth.AddX509(c._clientCert, c._clientKey)
	}
	if c._token != "" {
		auth.AddJWT(c._token)
	}
	// mimic standard drivers and use password as token if user is empty
	if c._token == "" && c._username == "" && isJWTToken(c._password) {
		auth.AddJWT(c._password)
	}
	if c._password != "" {
		auth.AddBasic(c._username, c._password)
	}
	return auth
}

func (c *authAttrs) refresh(auth *p.Auth) bool {
	switch method := auth.Method().(type) {

	case p.AuthPasswordSetter:
		if fn := c._refreshPassword; fn != nil {
			if password, ok := fn(); ok && c._password != password {
				c.mu.Lock()
				c._password = password
				c.mu.Unlock()
				method.SetPassword(password)
				return true
			}
		}
	case p.AuthTokenSetter:
		if fn := c._refreshToken; fn != nil {
			if token, ok := fn(); ok && c._token != token {
				c.mu.Lock()
				c._token = token
				c.mu.Unlock()
				method.SetToken(token)
				return true
			}
		}
	case p.AuthCertKeySetter:
		if fn := c._refreshClientCert; fn != nil {
			if clientCert, clientKey, ok := fn(); ok && !(bytes.Equal(c._clientCert, clientCert) && bytes.Equal(c._clientKey, clientKey)) {
				c.mu.Lock()
				c._clientCert = clientCert
				c._clientKey = clientKey
				c.mu.Unlock()
				method.SetCertKey(clientCert, clientKey)
				return true
			}
		}
	}
	return false
}

func (c *authAttrs) invalidateCookie() { c.hasCookie.Store(false) }

func (c *authAttrs) setCookie(logonname string, sessionCookie []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hasCookie.Store(true)
	c._logonname = logonname
	c._sessionCookie = sessionCookie
}

// Username returns the username of the connector.
func (c *authAttrs) Username() string { c.mu.RLock(); defer c.mu.RUnlock(); return c._username }

// Password returns the basic authentication password of the connector.
func (c *authAttrs) Password() string { c.mu.RLock(); defer c.mu.RUnlock(); return c._password }

// SetPassword sets the basic authentication password of the connector.
func (c *authAttrs) SetPassword(password string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c._password = password
}

// RefreshPassword returns the callback function for basic authentication password refresh.
func (c *authAttrs) RefreshPassword() func() (password string, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c._refreshPassword
}

// SetRefreshPassword sets the callback function for basic authentication password refresh.
func (c *authAttrs) SetRefreshPassword(refreshPassword func() (password string, ok bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c._refreshPassword = refreshPassword
}

// ClientCert returns the X509 authentication client certificate and key of the connector.
func (c *authAttrs) ClientCert() (clientCert, clientKey []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c._clientCert, c._clientKey
}

// RefreshClientCert returns the callback function for X509 authentication client certificate and key refresh.
func (c *authAttrs) RefreshClientCert() func() (clientCert, clientKey []byte, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c._refreshClientCert
}

// SetRefreshClientCert sets the callback function for X509 authentication client certificate and key refresh.
func (c *authAttrs) SetRefreshClientCert(refreshClientCert func() (clientCert, clientKey []byte, ok bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c._refreshClientCert = refreshClientCert
}

// Token returns the JWT authentication token of the connector.
func (c *authAttrs) Token() string { c.mu.RLock(); defer c.mu.RUnlock(); return c._token }

// RefreshToken returns the callback function for JWT authentication token refresh.
func (c *authAttrs) RefreshToken() func() (token string, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c._refreshToken
}

// SetRefreshToken sets the callback function for JWT authentication token refresh.
func (c *authAttrs) SetRefreshToken(refreshToken func() (token string, ok bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c._refreshToken = refreshToken
}
