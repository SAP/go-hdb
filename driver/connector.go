// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"os"
	"time"

	"github.com/SAP/go-hdb/driver/dial"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"golang.org/x/text/transform"
)

/*
SessionVariables maps session variables to their values.
All defined session variables will be set once after a database connection is opened.
*/
type SessionVariables map[string]string

/*
A Connector represents a hdb driver in a fixed configuration.
A Connector can be passed to sql.OpenDB (starting from go 1.10) allowing users to bypass a string based data source name.
*/
type Connector struct {
	metrics      *metrics
	connAttrs    *connAttrs
	authAttrs    *authAttrs
	sessionAttrs *p.SessionAttrs
}

// NewConnector returns a new Connector instance with default values.
func NewConnector() *Connector {
	return &Connector{
		metrics:      newMetrics(hdbDriver.metrics),
		connAttrs:    newConnAttrs(),
		authAttrs:    &authAttrs{},
		sessionAttrs: p.NewSessionAttrs(),
	}
}

// NewBasicAuthConnector creates a connector for basic authentication.
func NewBasicAuthConnector(host, username, password string) *Connector {
	c := NewConnector()
	c.connAttrs._host = host
	c.authAttrs._username = username
	c.authAttrs._password = password
	return c
}

// NewX509AuthConnector creates a connector for X509 (client certificate) authentication.
func NewX509AuthConnector(host string, clientCert, clientKey []byte) *Connector {
	c := NewConnector()
	c.connAttrs._host = host
	c.authAttrs._clientCert = clientCert
	c.authAttrs._clientKey = clientKey
	return c
}

// NewX509AuthConnectorByFiles creates a connector for X509 (client certificate) authentication
// based on client certificate and client key files.
func NewX509AuthConnectorByFiles(host, clientCertFile, clientKeyFile string) (*Connector, error) {
	clientCert, err := os.ReadFile(clientCertFile)
	if err != nil {
		return nil, err
	}
	clientKey, err := os.ReadFile(clientKeyFile)
	if err != nil {
		return nil, err
	}
	return NewX509AuthConnector(host, clientCert, clientKey), nil
}

// NewJWTAuthConnector creates a connector for token (JWT) based authentication.
func NewJWTAuthConnector(host, token string) *Connector {
	c := NewConnector()
	c.connAttrs._host = host
	c.authAttrs._token = token
	return c
}

func newDSNConnector(dsn *DSN) (*Connector, error) {
	c := NewConnector()
	c.connAttrs._host = dsn.host
	c.connAttrs._defaultSchema = dsn.defaultSchema
	c.connAttrs._pingInterval = dsn.pingInterval
	c.connAttrs._setTimeout(dsn.timeout)
	if dsn.tls != nil {
		if err := c.connAttrs._setTLS(dsn.tls.ServerName, dsn.tls.InsecureSkipVerify, dsn.tls.RootCAFiles); err != nil {
			return nil, err
		}
	}
	c.authAttrs._username = dsn.username
	c.authAttrs._password = dsn.password
	return c, nil
}

// NewDSNConnector creates a connector from a data source name.
func NewDSNConnector(dsnStr string) (*Connector, error) {
	dsn, err := parseDSN(dsnStr)
	if err != nil {
		return nil, err
	}
	return newDSNConnector(dsn)
}

// Host returns the host of the connector.
func (c *Connector) Host() string { return c.connAttrs.host() }

// Timeout returns the timeout of the connector.
func (c *Connector) Timeout() time.Duration { return c.connAttrs.timeout() }

/*
SetTimeout sets the timeout of the connector.

For more information please see DSNTimeout.
*/
func (c *Connector) SetTimeout(timeout time.Duration) { c.connAttrs.setTimeout(timeout) }

// PingInterval returns the connection ping interval of the connector.
func (c *Connector) PingInterval() time.Duration { return c.connAttrs.pingInterval() }

/*
SetPingInterval sets the connection ping interval value of the connector.

If the ping interval is greater than zero, the driver pings all open
connections (active or idle in connection pool) periodically.
Parameter d defines the time between the pings in milliseconds.
*/
func (c *Connector) SetPingInterval(d time.Duration) { c.connAttrs.setPingInterval(d) }

// BufferSize returns the bufferSize of the connector.
func (c *Connector) BufferSize() int { return c.connAttrs.bufferSize() }

/*
SetBufferSize sets the bufferSize of the connector.
*/
func (c *Connector) SetBufferSize(bufferSize int) { c.connAttrs.setBufferSize(bufferSize) }

// BulkSize returns the bulkSize of the connector.
func (c *Connector) BulkSize() int { return c.connAttrs.bulkSize() }

// SetBulkSize sets the bulkSize of the connector.
func (c *Connector) SetBulkSize(bulkSize int) { c.connAttrs.setBulkSize(bulkSize) }

// TCPKeepAlive returns the tcp keep-alive value of the connector.
func (c *Connector) TCPKeepAlive() time.Duration { return c.connAttrs.tcpKeepAlive() }

/*
SetTCPKeepAlive sets the tcp keep-alive value of the connector.

For more information please see net.Dialer structure.
*/
func (c *Connector) SetTCPKeepAlive(tcpKeepAlive time.Duration) {
	c.connAttrs.setTCPKeepAlive(tcpKeepAlive)
}

// DefaultSchema returns the database default schema of the connector.
func (c *Connector) DefaultSchema() string { return c.connAttrs.defaultSchema() }

// SetDefaultSchema sets the database default schema of the connector.
func (c *Connector) SetDefaultSchema(schema string) { c.connAttrs.setDefaultSchema(schema) }

// TLSConfig returns the TLS configuration of the connector.
func (c *Connector) TLSConfig() *tls.Config { return c.connAttrs.tlsConfig() }

// SetTLS sets the TLS configuration of the connector with given parameters. An existing connector TLS configuration is replaced.
func (c *Connector) SetTLS(serverName string, insecureSkipVerify bool, rootCAFiles ...string) error {
	return c.connAttrs.setTLS(serverName, insecureSkipVerify, rootCAFiles)
}

// SetTLSConfig sets the TLS configuration of the connector.
func (c *Connector) SetTLSConfig(tlsConfig *tls.Config) { c.connAttrs.setTLSConfig(tlsConfig) }

// Dialer returns the dialer object of the connector.
func (c *Connector) Dialer() dial.Dialer { return c.connAttrs.dialer() }

// SetDialer sets the dialer object of the connector.
func (c *Connector) SetDialer(dialer dial.Dialer) { c.connAttrs.setDialer(dialer) }

// Username returns the username of the connector.
func (c *Connector) Username() string { return c.authAttrs.username() }

// Password returns the basic authentication password of the connector.
func (c *Connector) Password() string { return c.authAttrs.password() }

// RefreshPassword returns the callback function for basic authentication password refresh.
func (c *Connector) RefreshPassword() func() (password string, ok bool) {
	return c.authAttrs.refreshPassword()
}

// SetRefreshPassword sets the callback function for basic authentication password refresh.
func (c *Connector) SetRefreshPassword(refreshPassword func() (password string, ok bool)) {
	c.authAttrs.setRefreshPassword(refreshPassword)
}

// ClientCert returns the X509 authentication client certificate and key of the connector.
func (c *Connector) ClientCert() (clientCert, clientKey []byte) { return c.authAttrs.clientCert() }

// RefreshClientCert returns the callback function for X509 authentication client certificate and key refresh.
func (c *Connector) RefreshClientCert() func() (clientCert, clientKey []byte, ok bool) {
	return c.authAttrs.refreshClientCert()
}

// SetRefreshClientCert sets the callback function for X509 authentication client certificate and key refresh.
func (c *Connector) SetRefreshClientCert(refreshClientCert func() (clientCert, clientKey []byte, ok bool)) {
	c.authAttrs.setRefreshClientCert(refreshClientCert)
}

// Token returns the JWT authentication token of the connector.
func (c *Connector) Token() string { return c.authAttrs.token() }

// RefreshToken returns the callback function for JWT authentication token refresh.
func (c *Connector) RefreshToken() func() (token string, ok bool) { return c.authAttrs.refreshToken() }

// SetRefreshToken sets the callback function for JWT authentication token refresh.
func (c *Connector) SetRefreshToken(refreshToken func() (token string, ok bool)) {
	c.authAttrs.setRefreshToken(refreshToken)
}

// ApplicationName returns the locale of the connector.
func (c *Connector) ApplicationName() string { return c.sessionAttrs.ApplicationName() }

// SetApplicationName sets the application name of the connector.
func (c *Connector) SetApplicationName(name string) { c.sessionAttrs.SetApplicationName(name) }

// SessionVariables returns the session variables stored in connector.
func (c *Connector) SessionVariables() SessionVariables { return c.sessionAttrs.SessionVariables() }

// SetSessionVariables sets the session varibles of the connector.
func (c *Connector) SetSessionVariables(sessionVariables SessionVariables) {
	c.sessionAttrs.SetSessionVariables(sessionVariables)
}

// Locale returns the locale of the connector.
func (c *Connector) Locale() string { return c.sessionAttrs.Locale() }

/*
SetLocale sets the locale of the connector.

For more information please see DSNLocale.
*/
func (c *Connector) SetLocale(locale string) { c.sessionAttrs.SetLocale(locale) }

// FetchSize returns the fetchSize of the connector.
func (c *Connector) FetchSize() int { return c.sessionAttrs.FetchSize() }

/*
SetFetchSize sets the fetchSize of the connector.

For more information please see DSNFetchSize.
*/
func (c *Connector) SetFetchSize(fetchSize int) { c.sessionAttrs.SetFetchSize(fetchSize) }

// LobChunkSize returns the lobChunkSize of the connector.
func (c *Connector) LobChunkSize() int { return c.sessionAttrs.LobChunkSize() }

// SetLobChunkSize sets the lobChunkSize of the connector.
func (c *Connector) SetLobChunkSize(lobChunkSize int) { c.sessionAttrs.SetLobChunkSize(lobChunkSize) }

// Dfv returns the client data format version of the connector.
func (c *Connector) Dfv() int { return c.sessionAttrs.Dfv() }

// SetDfv sets the client data format version of the connector.
func (c *Connector) SetDfv(dfv int) { c.sessionAttrs.SetDfv(dfv) }

// Legacy returns the connector legacy flag.
func (c *Connector) Legacy() bool { return c.sessionAttrs.Legacy() }

// SetLegacy sets the connector legacy flag.
func (c *Connector) SetLegacy(b bool) { c.sessionAttrs.SetLegacy(b) }

// CESU8Decoder returns the CESU-8 decoder of the connector.
func (c *Connector) CESU8Decoder() func() transform.Transformer { return c.sessionAttrs.CESU8Decoder() }

// SetCESU8Decoder sets the CESU-8 decoder of the connector.
func (c *Connector) SetCESU8Decoder(cesu8Decoder func() transform.Transformer) {
	c.sessionAttrs.SetCESU8Decoder(cesu8Decoder)
}

// CESU8Encoder returns the CESU-8 encoder of the connector.
func (c *Connector) CESU8Encoder() func() transform.Transformer { return c.sessionAttrs.CESU8Encoder() }

// SetCESU8Encoder sets the CESU-8 encoder of the connector.
func (c *Connector) SetCESU8Encoder(cesu8Encoder func() transform.Transformer) {
	c.sessionAttrs.SetCESU8Encoder(cesu8Encoder)
}

// NativeDriver returns the concrete underlying Driver of the Connector.
func (c *Connector) NativeDriver() *Driver { return hdbDriver }

// Connect implements the database/sql/driver/Connector interface.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	sessionAttrs := c.sessionAttrs.Clone()

	c.connAttrs.mu.RLock()
	defer c.connAttrs.mu.RUnlock()

	// can we connect via cookie?
	if auth := c.authAttrs.cookieAuth(); auth != nil {
		conn, err := newConn(ctx, c.metrics, c.connAttrs, sessionAttrs, auth)
		if err == nil {
			return conn, nil
		}
		if _, ok := err.(*p.AuthFailedError); !ok {
			return nil, err
		}
	}

	auth := c.authAttrs.auth()
	retries := 1
	for {
		conn, err := newConn(ctx, c.metrics, c.connAttrs, sessionAttrs, auth)
		if err == nil {
			if method, ok := auth.Method().(p.AuthCookieGetter); ok {
				c.authAttrs.setSessionCookie(method.Cookie())
			}
			return conn, nil
		}
		if _, ok := err.(*p.AuthFailedError); !ok {
			return nil, err
		}
		if retries < 1 || !c.authAttrs.refresh(auth) {
			return nil, err
		}
		retries--
	}
}

// Driver implements the database/sql/driver/Connector interface.
func (c *Connector) Driver() driver.Driver { return hdbDriver }

// Stats returns connector statistics.
func (c *Connector) Stats() Stats { return c.metrics.stats() }
