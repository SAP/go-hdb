package driver

import (
	"strings"
	"sync"
	"sync/atomic"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/internal/protocol/x509"
)

// authAttrs is holding authentication relevant attributes.
type authAttrs struct {
	hasCookie            atomic.Bool
	mu                   sync.RWMutex
	_username, _password string        // basic authentication
	_certKey             *x509.CertKey // X509
	_token               string        // JWT
	_logonname           string        // session cookie login does need logon name provided by JWT authentication.
	_sessionCookie       []byte        // authentication via session cookie (HDB currently does support only SAML and JWT - go-hdb JWT)
	_refreshPassword     func() (password string, ok bool)
	_refreshClientCert   func() (clientCert, clientKey []byte, ok bool)
	_refreshToken        func() (token string, ok bool)
	cbmu                 sync.RWMutex // prevents refresh callbacks from being called in parallel
}

/*
	keep c as the instance name, so that the generated help does have
	the same instance variable name when included in connector
*/

func isJWTToken(token string) bool { return strings.HasPrefix(token, "ey") }

func (c *authAttrs) cookieAuth() *p.Auth {
	if !c.hasCookie.Load() { // fastpath without lock
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	auth := p.NewAuth(c._logonname)                                 // important: for session cookie auth we do need the logonname from JWT auth,
	auth.AddSessionCookie(c._sessionCookie, c._logonname, clientID) // and for HANA onPrem the final session cookie req needs the logonname as well.
	return auth
}

func (c *authAttrs) auth() *p.Auth {
	c.mu.RLock()
	defer c.mu.RUnlock()

	auth := p.NewAuth(c._username) // use username as logonname
	if c._certKey != nil {
		auth.AddX509(c._certKey)
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

func (c *authAttrs) callRefreshPasswordWithLock() (string, bool) {
	refreshPassword := c._refreshPassword // copy within lock
	c.cbmu.Lock()
	defer c.cbmu.Unlock()
	c.mu.Unlock() // unlock attr, so that callback can call attr methods
	defer c.mu.Lock()
	return refreshPassword()
}

func (c *authAttrs) callRefreshTokenWithLock() (string, bool) {
	refreshToken := c._refreshToken // copy within lock
	c.cbmu.Lock()
	defer c.cbmu.Unlock()
	c.mu.Unlock() // unlock attr, so that callback can call attr methods
	defer c.mu.Lock()
	return refreshToken()
}

func (c *authAttrs) callRefreshClientCertWithLock() ([]byte, []byte, bool) {
	refreshClientCert := c._refreshClientCert // copy within lock
	c.cbmu.Lock()
	defer c.cbmu.Unlock()
	c.mu.Unlock() // unlock attr, so that callback can call attr methods
	defer c.mu.Lock()
	return refreshClientCert()
}

func (c *authAttrs) refresh() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c._refreshPassword != nil {
		if password, ok := c.callRefreshPasswordWithLock(); ok {
			if password != c._password {
				c._password = password
			}
		}
	}
	if c._refreshToken != nil {
		if token, ok := c.callRefreshTokenWithLock(); ok {
			if token != c._token {
				c._token = token
			}
		}
	}
	if c._refreshClientCert != nil {
		if clientCert, clientKey, ok := c.callRefreshClientCertWithLock(); ok {
			if !c._certKey.Equal(clientCert, clientKey) {
				certKey, err := x509.NewCertKey(clientCert, clientKey)
				if err != nil {
					return err
				}
				c._certKey = certKey
			}
		}
	}
	return nil
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
// The callback function might be called simultaneously from multiple goroutines only if registered
// for more than one Connector.
func (c *authAttrs) SetRefreshPassword(refreshPassword func() (password string, ok bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c._refreshPassword = refreshPassword
}

// ClientCert returns the X509 authentication client certificate and key of the connector.
func (c *authAttrs) ClientCert() (clientCert, clientKey []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c._certKey.Cert(), c._certKey.Key()
}

// RefreshClientCert returns the callback function for X509 authentication client certificate and key refresh.
func (c *authAttrs) RefreshClientCert() func() (clientCert, clientKey []byte, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c._refreshClientCert
}

// SetRefreshClientCert sets the callback function for X509 authentication client certificate and key refresh.
// The callback function might be called simultaneously from multiple goroutines only if registered
// for more than one Connector.
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
// The callback function might be called simultaneously from multiple goroutines only if registered
// for more than one Connector.
func (c *authAttrs) SetRefreshToken(refreshToken func() (token string, ok bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c._refreshToken = refreshToken
}
