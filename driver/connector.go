// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/SAP/go-hdb/driver/dial"
	"github.com/SAP/go-hdb/driver/internal/container/vermap"
	"github.com/SAP/go-hdb/driver/internal/dsn"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
	"golang.org/x/text/transform"
)

// Data format version values.
const (
	DfvLevel0 int = 0 // base data format
	DfvLevel1 int = 1 // eval types support all data types
	DfvLevel2 int = 2 // reserved, broken, do not use
	DfvLevel3 int = 3 // additional types Longdate, Secondate, Daydate, Secondtime supported for NGAP
	DfvLevel4 int = 4 // generic support for new date/time types
	DfvLevel5 int = 5 // spatial types in ODBC on request
	DfvLevel6 int = 6 // BINTEXT
	DfvLevel7 int = 7 // with boolean support
	DfvLevel8 int = 8 // with FIXED8/12/16 support
)

// IsSupportedDfv returns true if the data format version dfv is supported by the driver, false otherwise.
func IsSupportedDfv(dfv int) bool {
	return dfv == DfvLevel1 || dfv == DfvLevel4 || dfv == DfvLevel6 || dfv == DfvLevel8
}

// SupportedDfvs returns a slice of data format versions supported by the driver.
var SupportedDfvs = []int{DfvLevel1, DfvLevel4, DfvLevel6, DfvLevel8}

// Connector default values.
const (
	DefaultDfv          = DfvLevel8         // Default data version format level.
	DefaultTimeout      = 300 * time.Second // Default value connection timeout (300 seconds = 5 minutes).
	DefaultTCPKeepAlive = 15 * time.Second  // Default TCP keep-alive value (copied from net.dial.go)
	DefaultBufferSize   = 16276             // Default value bufferSize.
	DefaultFetchSize    = 128               // Default value fetchSize.
	DefaultBulkSize     = 10000             // Default value bulkSize.
	DefaultLobChunkSize = 8192              // Default value lobChunkSize.
	DefaultLegacy       = false             // Default value legacy.
)

// Connector minimal / maximal values.
const (
	minTimeout      = 0 * time.Second // Minimal timeout value.
	minFetchSize    = 1               // Minimal fetchSize value.
	minBulkSize     = 1               // Minimal bulkSize value.
	MaxBulkSize     = p.MaxNumArg     // Maximum bulk size.
	minLobChunkSize = 128             // Minimal lobChunkSize
	maxLobChunkSize = 1 << 14         // Maximal lobChunkSize (TODO check)
)

/*
SessionVariables maps session variables to their values.
All defined session variables will be set once after a database connection is opened.
*/
type SessionVariables map[string]string

/*
A Connector represents a hdb driver in a fixed configuration.
A Connector can be passed to sql.OpenDB (starting from go 1.10) allowing users to bypass a string based data source name.
After the connector has been passed to sql.OpenDB it must not be modified.
*/
type Connector struct {
	mu                   sync.RWMutex
	host                 string
	bufferSize, bulkSize int
	timeout              time.Duration
	pingInterval         time.Duration
	tcpKeepAlive         time.Duration // see net.Dialer
	tlsConfig            *tls.Config
	defaultSchema        string
	dialer               dial.Dialer
	// session relevant attributes
	username, password            string
	clientCertFile, clientKeyFile string
	token                         string // JWT
	applicationName               string
	sessionVariables              *vermap.VerMap
	locale                        string
	fetchSize, lobChunkSize       int
	dfv                           int
	legacy                        bool
	cesu8Decoder                  func() transform.Transformer
	cesu8Encoder                  func() transform.Transformer
}

// NewConnector returns a new Connector instance with default values.
func NewConnector() *Connector {
	return &Connector{
		bufferSize:       DefaultBufferSize,
		bulkSize:         DefaultBulkSize,
		timeout:          DefaultTimeout,
		tcpKeepAlive:     DefaultTCPKeepAlive,
		dialer:           dial.DefaultDialer,
		applicationName:  defaultApplicationName,
		sessionVariables: vermap.NewVerMap(),
		fetchSize:        DefaultFetchSize,
		lobChunkSize:     DefaultLobChunkSize,
		dfv:              DefaultDfv,
		legacy:           DefaultLegacy,
		cesu8Decoder:     cesu8.DefaultDecoder,
		cesu8Encoder:     cesu8.DefaultEncoder,
	}
}

// NewBasicAuthConnector creates a connector for basic authentication.
func NewBasicAuthConnector(host, username, password string) *Connector {
	c := NewConnector()
	c.host = host
	c.username = username
	c.password = password
	return c
}

// NewX509AuthConnector creates a connector for X509 (client certificate) authentication.
func NewX509AuthConnector(host, username, clientCertFile, clientKeyFile string) *Connector {
	c := NewConnector()
	c.host = host
	c.username = username
	c.clientCertFile = clientCertFile
	c.clientKeyFile = clientKeyFile
	return c
}

// NewJWTAuthConnector creates a connector for token (JWT) based authentication.
func NewJWTAuthConnector(host, username, token string) *Connector {
	c := NewConnector()
	c.host = host
	c.username = username
	c.token = token
	return c
}

// NewDSNConnector creates a connector from a data source name.
func NewDSNConnector(dsnStr string) (*Connector, error) {
	dsn, err := dsn.Parse(dsnStr)
	if err != nil {
		return nil, err
	}
	c := NewConnector()
	c.host = dsn.Host
	c.username = dsn.Username
	c.password = dsn.Password
	c.defaultSchema = dsn.DefaultSchema
	c.locale = dsn.Locale
	c.setFetchSize(dsn.FetchSize)
	c.setTimeout(dsn.Timeout)
	c.pingInterval = dsn.PingInterval
	if dsn.TLS != nil {
		c.setTLS(dsn.TLS.ServerName, dsn.TLS.InsecureSkipVerify, dsn.TLS.RootCAFiles)
	}
	return c, nil
}

// sessionConfig returns the session relevant connector attributes.
func (c *Connector) sessionConfig() *p.SessionConfig {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &p.SessionConfig{
		DriverVersion:    DriverVersion,
		DriverName:       DriverName,
		Username:         c.username,
		Password:         c.password,
		ClientCertFile:   c.clientCertFile,
		ClientKeyFile:    c.clientKeyFile,
		Token:            c.token,
		ApplicationName:  c.applicationName,
		SessionVariables: c.sessionVariables, //TODO clone
		Locale:           c.locale,
		FetchSize:        c.fetchSize,
		LobChunkSize:     c.lobChunkSize,
		Dfv:              c.dfv,
		Legacy:           c.legacy,
		CESU8Decoder:     c.cesu8Decoder,
		CESU8Encoder:     c.cesu8Encoder,
	}
}

// Host returns the host of the connector.
func (c *Connector) Host() string { c.mu.RLock(); defer c.mu.RUnlock(); return c.host }

// Username returns the username of the connector.
func (c *Connector) Username() string { c.mu.RLock(); defer c.mu.RUnlock(); return c.username }

// Password returns the password of the connector.
func (c *Connector) Password() string { c.mu.RLock(); defer c.mu.RUnlock(); return c.password }

// Locale returns the locale of the connector.
func (c *Connector) Locale() string { c.mu.RLock(); defer c.mu.RUnlock(); return c.locale }

/*
SetLocale sets the locale of the connector.

For more information please see DSNLocale.
*/
func (c *Connector) SetLocale(locale string) { c.mu.Lock(); defer c.mu.Unlock(); c.locale = locale }

// DriverVersion returns the driver version of the connector.
func (c *Connector) DriverVersion() string { return DriverVersion }

// DriverName returns the driver name of the connector.
func (c *Connector) DriverName() string { return DriverName }

// ApplicationName returns the locale of the connector.
func (c *Connector) ApplicationName() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.applicationName
}

// SetApplicationName sets the application name of the connector.
func (c *Connector) SetApplicationName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applicationName = name
}

// BufferSize returns the bufferSize of the connector.
func (c *Connector) BufferSize() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.bufferSize }

/*
SetBufferSize sets the bufferSize of the connector.
*/
func (c *Connector) SetBufferSize(bufferSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bufferSize = bufferSize
}

// FetchSize returns the fetchSize of the connector.
func (c *Connector) FetchSize() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.fetchSize }

func (c *Connector) setFetchSize(fetchSize int) {
	if fetchSize < minFetchSize {
		fetchSize = minFetchSize
	}
	c.fetchSize = fetchSize
}

/*
SetFetchSize sets the fetchSize of the connector.

For more information please see DSNFetchSize.
*/
func (c *Connector) SetFetchSize(fetchSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setFetchSize(fetchSize)
}

// BulkSize returns the bulkSize of the connector.
func (c *Connector) BulkSize() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.bulkSize }

func (c *Connector) setBulkSize(bulkSize int) {
	switch {
	case bulkSize < minBulkSize:
		bulkSize = minBulkSize
	case bulkSize > MaxBulkSize:
		bulkSize = MaxBulkSize
	}
	c.bulkSize = bulkSize
}

// SetBulkSize sets the bulkSize of the connector.
func (c *Connector) SetBulkSize(bulkSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setBulkSize(bulkSize)
}

// LobChunkSize returns the lobChunkSize of the connector.
func (c *Connector) LobChunkSize() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.lobChunkSize }

// SetLobChunkSize sets the lobChunkSize of the connector.
func (c *Connector) SetLobChunkSize(lobChunkSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case lobChunkSize < minLobChunkSize:
		lobChunkSize = minLobChunkSize
	case lobChunkSize > maxLobChunkSize:
		lobChunkSize = maxLobChunkSize
	}
	c.lobChunkSize = lobChunkSize
}

// Dialer returns the dialer object of the connector.
func (c *Connector) Dialer() dial.Dialer { c.mu.RLock(); defer c.mu.RUnlock(); return c.dialer }

// SetDialer sets the dialer object of the connector.
func (c *Connector) SetDialer(dialer dial.Dialer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if dialer == nil {
		dialer = dial.DefaultDialer
	}
	c.dialer = dialer
}

// CESU8Decoder returns the CESU-8 decoder of the connector.
func (c *Connector) CESU8Decoder() func() transform.Transformer {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cesu8Decoder
}

// SetCESU8Decoder sets the CESU-8 decoder of the connector.
func (c *Connector) SetCESU8Decoder(cesu8Decoder func() transform.Transformer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cesu8Decoder = cesu8Decoder
}

// CESU8Encoder returns the CESU-8 encoder of the connector.
func (c *Connector) CESU8Encoder() func() transform.Transformer {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cesu8Encoder
}

// SetCESU8Encoder sets the CESU-8 encoder of the connector.
func (c *Connector) SetCESU8Encoder(cesu8Encoder func() transform.Transformer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cesu8Encoder = cesu8Encoder
}

// Timeout returns the timeout of the connector.
func (c *Connector) Timeout() time.Duration { c.mu.RLock(); defer c.mu.RUnlock(); return c.timeout }

func (c *Connector) setTimeout(timeout time.Duration) {
	if timeout < minTimeout {
		timeout = minTimeout
	}
	c.timeout = timeout
}

/*
SetTimeout sets the timeout of the connector.

For more information please see DSNTimeout.
*/
func (c *Connector) SetTimeout(timeout time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setTimeout(timeout)
}

// PingInterval returns the connection ping interval of the connector.
func (c *Connector) PingInterval() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pingInterval
}

/*
SetPingInterval sets the connection ping interval value of the connector.

If the ping interval is greater than zero, the driver pings all open
connections (active or idle in connection pool) periodically.
Parameter d defines the time between the pings.
*/
func (c *Connector) SetPingInterval(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pingInterval = d
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
func (c *Connector) SetTCPKeepAlive(tcpKeepAlive time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tcpKeepAlive = tcpKeepAlive
}

// Dfv returns the client data format version of the connector.
func (c *Connector) Dfv() int { c.mu.RLock(); defer c.mu.RUnlock(); return c.dfv }

// SetDfv sets the client data format version of the connector.
func (c *Connector) SetDfv(dfv int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !IsSupportedDfv(dfv) {
		dfv = DefaultDfv
	}
	c.dfv = dfv
}

// TLSConfig returns the TLS configuration of the connector.
func (c *Connector) TLSConfig() *tls.Config { c.mu.RLock(); defer c.mu.RUnlock(); return c.tlsConfig }

func (c *Connector) setTLS(serverName string, insecureSkipVerify bool, rootCAFiles []string) error {
	c.tlsConfig = &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkipVerify,
	}
	var certPool *x509.CertPool
	for _, fn := range rootCAFiles {
		rootPEM, err := os.ReadFile(fn)
		if err != nil {
			return err
		}
		if certPool == nil {
			certPool = x509.NewCertPool()
		}
		if ok := certPool.AppendCertsFromPEM(rootPEM); !ok {
			return fmt.Errorf("failed to parse root certificate - filename: %s", fn)
		}
	}
	if certPool != nil {
		c.tlsConfig.RootCAs = certPool
	}
	return nil
}

// SetTLS sets the TLS configuration of the connector with given parameters. An existing connector TLS configuration is replaced.
func (c *Connector) SetTLS(serverName string, insecureSkipVerify bool, rootCAFiles ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setTLS(serverName, insecureSkipVerify, rootCAFiles)
}

// SetTLSConfig sets the TLS configuration of the connector.
func (c *Connector) SetTLSConfig(tlsConfig *tls.Config) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tlsConfig = tlsConfig.Clone()
}

// SessionVariables returns the session variables stored in connector.
func (c *Connector) SessionVariables() SessionVariables {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return SessionVariables(c.sessionVariables.Load())
}

// SetSessionVariables sets the session varibles of the connector.
func (c *Connector) SetSessionVariables(sessionVariables SessionVariables) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionVariables.Store(map[string]string(sessionVariables))
}

// DefaultSchema returns the database default schema of the connector.
func (c *Connector) DefaultSchema() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.defaultSchema
}

// SetDefaultSchema sets the database default schema of the connector.
func (c *Connector) SetDefaultSchema(schema string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.defaultSchema = schema
}

// Legacy returns the connector legacy flag.
func (c *Connector) Legacy() bool { c.mu.RLock(); defer c.mu.RUnlock(); return c.legacy }

// SetLegacy sets the connector legacy flag.
func (c *Connector) SetLegacy(b bool) { c.mu.Lock(); defer c.mu.Unlock(); c.legacy = b }

// Connect implements the database/sql/driver/Connector interface.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return newConn(ctx, c)
}

// Driver implements the database/sql/driver/Connector interface.
func (c *Connector) Driver() driver.Driver { return hdbDriver }
