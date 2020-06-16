/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"sync"
	"time"

	p "github.com/SAP/go-hdb/internal/protocol"
)

// Data Format Version values.
// Driver does currently support DfvLevel1, DfvLevel4 and DfvLevel6.
const (
	DfvLevel0 = 0 // base data format
	DfvLevel1 = 1 // eval types support all data types
	DfvLevel2 = 2 // reserved, broken, do not use
	DfvLevel3 = 3 // additional types Longdate, Secondate, Daydate, Secondtime supported for NGAP
	DfvLevel4 = 4 // generic support for new date/time types
	DfvLevel5 = 5 // spatial types in ODBC on request
	DfvLevel6 = 6 // BINTEXT
	DfvLevel7 = 7 // with boolean support
	DfvLevel8 = 8 // with FIXED8/12/16 support
)

// var supportedDfvs = map[int]bool{DfvLevel1: true, DfvLevel4: true, DfvLevel6: true, DfvLevel8: true}
var supportedDfvs = map[int]bool{DfvLevel1: true, DfvLevel4: true, DfvLevel6: true}

// Connector default values.
const (
	DefaultDfv          = DfvLevel6        // Default data version format level.
	DefaultTimeout      = 300              // Default value connection timeout (300 seconds = 5 minutes).
	DefaultTCPKeepAlive = 15 * time.Second // Default TCP keep-alive value (copied from net.dial.go)
	DefaultFetchSize    = 128              // Default value fetchSize.
	DefaultBulkSize     = 1000             // Default value bulkSize.
	DefaultLobChunkSize = 4096             // Default value lobChunkSize.
	DefaultLegacy       = true             // Default value legacy.
)

// Connector minimal values.
const (
	minTimeout      = 0   // Minimal timeout value.
	minFetchSize    = 1   // Minimal fetchSize value.
	minBulkSize     = 1   // Minimal bulkSize value.
	minLobChunkSize = 128 // Minimal lobChunkSize
	// TODO check maxLobChunkSize
	maxLobChunkSize = 1 << 14 // Maximal lobChunkSize
)

// check if Connector implements session parameter interface.
var _ p.SessionConfig = (*Connector)(nil)

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
	mu                              sync.RWMutex
	host, username, password        string
	locale                          string
	bufferSize, fetchSize, bulkSize int
	lobChunkSize                    int32
	timeout, dfv                    int
	tcpKeepAlive                    time.Duration // see net.Dialer
	tlsConfig                       *tls.Config
	sessionVariables                SessionVariables
	defaultSchema                   Identifier
	legacy                          bool
}

func newConnector() *Connector {
	return &Connector{
		fetchSize:    DefaultFetchSize,
		bulkSize:     DefaultBulkSize,
		lobChunkSize: DefaultLobChunkSize,
		timeout:      DefaultTimeout,
		tcpKeepAlive: DefaultTCPKeepAlive,
		dfv:          DefaultDfv,
		legacy:       DefaultLegacy,
	}
}

// NewBasicAuthConnector creates a connector for basic authentication.
func NewBasicAuthConnector(host, username, password string) *Connector {
	c := newConnector()
	c.host = host
	c.username = username
	c.password = password
	return c
}

const parseDSNErrorText = "parse dsn error"

// ParseDSNError is the error returned in case DSN is invalid.
type ParseDSNError struct{ err error }

func (e ParseDSNError) Error() string {
	if err := errors.Unwrap(e.err); err != nil {
		return fmt.Sprintf("%s: %s", parseDSNErrorText, err.Error())
	}
	return parseDSNErrorText
}

// Unwrap returns the nested error.
func (e ParseDSNError) Unwrap() error { return e.err }

// NewDSNConnector creates a connector from a data source name.
func NewDSNConnector(dsn string) (*Connector, error) {
	c := newConnector()

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, &ParseDSNError{err}
	}

	c.host = u.Host

	if u.User != nil {
		c.username = u.User.Username()
		c.password, _ = u.User.Password()
	}

	var certPool *x509.CertPool

	for k, v := range u.Query() {
		switch k {

		default:
			return nil, fmt.Errorf("URL parameter %s is not supported", k)

		case DSNFetchSize:
			if len(v) == 0 {
				continue
			}
			fetchSize, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, fmt.Errorf("failed to parse fetchSize: %s", v[0])
			}
			if fetchSize < minFetchSize {
				c.fetchSize = minFetchSize
			} else {
				c.fetchSize = fetchSize
			}

		case DSNTimeout:
			if len(v) == 0 {
				continue
			}
			timeout, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, fmt.Errorf("failed to parse timeout: %s", v[0])
			}
			if timeout < minTimeout {
				c.timeout = minTimeout
			} else {
				c.timeout = timeout
			}

		case DSNLocale:
			if len(v) == 0 {
				continue
			}
			c.locale = v[0]

		case DSNTLSServerName:
			if len(v) == 0 {
				continue
			}
			if c.tlsConfig == nil {
				c.tlsConfig = &tls.Config{}
			}
			c.tlsConfig.ServerName = v[0]

		case DSNTLSInsecureSkipVerify:
			if len(v) == 0 {
				continue
			}
			var err error
			b := true
			if v[0] != "" {
				b, err = strconv.ParseBool(v[0])
				if err != nil {
					return nil, fmt.Errorf("failed to parse InsecureSkipVerify (bool): %s", v[0])
				}
			}
			if c.tlsConfig == nil {
				c.tlsConfig = &tls.Config{}
			}
			c.tlsConfig.InsecureSkipVerify = b

		case DSNTLSRootCAFile:
			for _, fn := range v {
				rootPEM, err := ioutil.ReadFile(fn)
				if err != nil {
					return nil, err
				}
				if certPool == nil {
					certPool = x509.NewCertPool()
				}
				if ok := certPool.AppendCertsFromPEM(rootPEM); !ok {
					return nil, fmt.Errorf("failed to parse root certificate - filename: %s", fn)
				}
			}
			if certPool != nil {
				if c.tlsConfig == nil {
					c.tlsConfig = &tls.Config{}
				}
				c.tlsConfig.RootCAs = certPool
			}
		}
	}
	return c, nil
}

// Host returns the host of the connector.
func (c *Connector) Host() string { return c.host }

// Username returns the username of the connector.
func (c *Connector) Username() string { return c.username }

// Password returns the password of the connector.
func (c *Connector) Password() string { return c.password }

// Locale returns the locale of the connector.
func (c *Connector) Locale() string { c.mu.RLock(); defer c.mu.RUnlock(); return c.locale }

/*
SetLocale sets the locale of the connector.

For more information please see DSNLocale.
*/
func (c *Connector) SetLocale(locale string) { c.mu.Lock(); c.locale = locale; c.mu.Unlock() }

// BufferSize returns the bufferSize of the connector.
func (c *Connector) BufferSize() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.bufferSize }

// FetchSize returns the fetchSize of the connector.
func (c *Connector) FetchSize() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.fetchSize }

/*
SetFetchSize sets the fetchSize of the connector.

For more information please see DSNFetchSize.
*/
func (c *Connector) SetFetchSize(fetchSize int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fetchSize < minFetchSize {
		fetchSize = minFetchSize
	}
	c.fetchSize = fetchSize
	return nil
}

// BulkSize returns the bulkSize of the connector.
func (c *Connector) BulkSize() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.bulkSize }

/*
SetBulkSize sets the bulkSize of the connector.
*/
func (c *Connector) SetBulkSize(bulkSize int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if bulkSize < minBulkSize {
		bulkSize = minBulkSize
	}
	c.bulkSize = bulkSize
	return nil
}

// LobChunkSize returns the lobChunkSize of the connector.
func (c *Connector) LobChunkSize() int32 { c.mu.RLock(); defer c.mu.RUnlock(); return c.lobChunkSize }

// Timeout returns the timeout of the connector.
func (c *Connector) Timeout() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.timeout }

/*
SetTimeout sets the timeout of the connector.

For more information please see DSNTimeout.
*/
func (c *Connector) SetTimeout(timeout int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if timeout < minTimeout {
		timeout = minTimeout
	}
	c.timeout = timeout
	return nil
}

// TCPKeepAlive returns the tcp keep-alive value of the connector.
func (c *Connector) TCPKeepAlive() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tcpKeepAlive
}

/*
SetTCPKeepAlive sets the tcp keep-alive value of the connector.

For more information please see net.Dialer structure.
*/
func (c *Connector) SetTCPKeepAlive(tcpKeepAlive time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tcpKeepAlive = tcpKeepAlive
	return nil
}

// Dfv returns the client data format version of the connector.
func (c *Connector) Dfv() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.dfv }

// SetDfv sets the client data format version of the connector.
func (c *Connector) SetDfv(dfv int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := supportedDfvs[dfv]; ok {
		c.dfv = dfv
	} else {
		c.dfv = DefaultDfv
	}
	return nil
}

// TLSConfig returns the TLS configuration of the connector.
func (c *Connector) TLSConfig() *tls.Config { c.mu.RLock(); defer c.mu.RUnlock(); return c.tlsConfig }

// SetTLSConfig sets the TLS configuration of the connector.
func (c *Connector) SetTLSConfig(tlsConfig *tls.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tlsConfig = tlsConfig
	return nil
}

// SessionVariables returns the session variables stored in connector.
func (c *Connector) SessionVariables() SessionVariables {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessionVariables
}

// SetSessionVariables sets the session varibles of the connector.
func (c *Connector) SetSessionVariables(sessionVariables SessionVariables) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionVariables = make(SessionVariables, len(sessionVariables))
	for k, v := range sessionVariables {
		c.sessionVariables[k] = v
	}
	return nil
}

// DefaultSchema returns the database default schema of the connector.
func (c *Connector) DefaultSchema() Identifier {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.defaultSchema
}

// SetDefaultSchema sets the database default schema of the connector.
func (c *Connector) SetDefaultSchema(schema Identifier) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.defaultSchema = schema
	return nil
}

// Legacy returns the connector legacy flag.
func (c *Connector) Legacy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.legacy
}

// SetLegacy sets the connector legacy flag.
func (c *Connector) SetLegacy(b bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.legacy = b
	return nil
}

// BasicAuthDSN return the connector DSN for basic authentication.
func (c *Connector) BasicAuthDSN() string {
	values := url.Values{}
	if c.locale != "" {
		values.Set(DSNLocale, c.locale)
	}
	if c.fetchSize != 0 {
		values.Set(DSNFetchSize, fmt.Sprintf("%d", c.fetchSize))
	}
	if c.timeout != 0 {
		values.Set(DSNTimeout, fmt.Sprintf("%d", c.timeout))
	}
	return (&url.URL{
		Scheme:   DriverName,
		User:     url.UserPassword(c.username, c.password),
		Host:     c.host,
		RawQuery: values.Encode(),
	}).String()
}

// Connect implements the database/sql/driver/Connector interface.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) { return newConn(ctx, c) }

// Driver implements the database/sql/driver/Connector interface.
func (c *Connector) Driver() driver.Driver { return drv }
