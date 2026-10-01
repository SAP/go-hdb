package driver

// Deprecated connector surface (starting with v1.19.0), frozen
// indefinitely: no new features, no removals. New code uses
// driver.ConnectorConfig (see docs/CONFIG.md): configure via ConnectorConfig fields,
// construct with NewConfigConnector, read back with Connector.Config().
// Also carries notes on simplifications that become possible if the
// deprecated surface is ever retired (see below).
//
// This file collects every deprecated Connector method (getters, setters).
//
// RETIREMENT NOTES (no date; only if a future release drops this surface):
//   - drop updateCfg and the atomic ConnectorConfig pointer: with no writers left,
//     cfg becomes a plain immutable field again and all locking around
//     pure-config access goes away with it.
// (No renames needed: NewConfigConnector already carries the clean name.
// connAttrs stays regardless, as the credential-free session view.)

import (
	"crypto/tls"
	"log/slog"
	"maps"
	"net"
	"path"
	"time"
	"unique"

	"github.com/SAP/go-hdb/driver/compress"
	"github.com/SAP/go-hdb/driver/dial"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/internal/protocol/auth"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
	"golang.org/x/text/transform"
)

// updateCfg replaces the stored configuration with a mutated copy.
// It is the only post-construction cfg writer (this file's setters).
// The lock serializes concurrent setters against each other (no lost
// generations); memory safety comes from the atomic swap itself.
// Readers keep seeing their generation untouched.
func (c *Connector) updateCfg(fn func(*ConnectorConfig)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *c.cfg.Load()
	fn(&cp)
	c.cfg.Store(&cp)
}

/*
SessionVariables maps session variables to their values.
All defined session variables will be set once after a database connection is opened.

Deprecated: use map[string]string with ConnectorConfig.SessionVariables instead.
Only the deprecated accessors below still use this type.
*/
type SessionVariables map[string]string

// NewConnector returns a new Connector instance with default values.
//
// Deprecated: build a ConnectorConfig with NewConnectorConfig and construct with
// NewConfigConnector.
func NewConnector() *Connector {
	return newConnector(NewConnectorConfig())
}

// NewBasicAuthConnector creates a connector for basic authentication.
//
// Deprecated: build a ConnectorConfig (NewConnectorConfig, set Username/Password) and
// construct with NewConfigConnector.
func NewBasicAuthConnector(host, username, password string) *Connector {
	cfg := NewConnectorConfig()
	cfg.Host = host
	cfg.Username = username
	cfg.Password = password
	return newConnector(cfg)
}

// NewX509AuthConnector creates a connector for X509 (client certificate) authentication.
// Parameters clientCert and clientKey in PEM format, clientKey not password encrypted.
//
// Deprecated: build a ConnectorConfig (NewConnectorConfig, set ClientCert/ClientKey) and
// construct with NewConfigConnector.
func NewX509AuthConnector(host string, clientCert, clientKey []byte) (*Connector, error) {
	cfg := NewConnectorConfig()
	cfg.Host = host
	cfg.ClientCert = clientCert
	cfg.ClientKey = clientKey
	// No validation (main parity): fail fast on unparsable input only.
	c := newConnector(cfg)
	var err error
	if c._certKey, err = auth.NewCertKey(unique.Make(string(clientCert)), unique.Make(string(clientKey))); err != nil {
		return nil, err
	}
	return c, nil
}

// NewX509AuthConnectorByFiles creates a connector for X509 (client certificate) authentication
// based on client certificate and client key files.
// Parameters clientCertFile and clientKeyFile in PEM format, clientKeyFile not password encrypted.
//
// Deprecated: build a ConnectorConfig (NewConnectorConfig, set ClientCertFile/ClientKeyFile) and
// construct with NewConfigConnector.
func NewX509AuthConnectorByFiles(host, clientCertFile, clientKeyFile string) (*Connector, error) {
	cfg := NewConnectorConfig()
	cfg.Host = host
	cfg.ClientCertFile = path.Clean(clientCertFile)
	cfg.ClientKeyFile = path.Clean(clientKeyFile)
	certHandle, keyHandle, err := readCertKeyFiles(cfg.ClientCertFile, cfg.ClientKeyFile)
	if err != nil {
		return nil, err
	}
	// No validation (main parity): fail fast on unreadable files or
	// unparsable input only.
	c := newConnector(cfg)
	if c._certKey, err = auth.NewCertKey(certHandle, keyHandle); err != nil {
		return nil, err
	}
	return c, nil
}

// NewJWTAuthConnector creates a connector for token (JWT) based authentication.
//
// Deprecated: build a ConnectorConfig (NewConnectorConfig, set Token) and construct
// with NewConfigConnector.
func NewJWTAuthConnector(host, token string) *Connector {
	cfg := NewConnectorConfig()
	cfg.Host = host
	cfg.Token = token
	return newConnector(cfg)
}

// NewDSNConnector creates a connector from a data source name.
//
// Deprecated: parse with ParseDSNConfig and construct with
// NewConfigConnector.
func NewDSNConnector(dsnStr string) (*Connector, error) {
	cfg, err := ParseDSNConfig(dsnStr)
	if err != nil {
		return nil, err
	}
	// No validation (main parity): an empty host yields a connector
	// that fails at connect time, as before.
	return newConnector(cfg), nil
}

// Host returns the host of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Host() string { return c.cfg.Load().Host }

// DatabaseName returns the tenant database name of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) DatabaseName() string {
	return c.cfg.Load().DatabaseName
}

// TCPKeepAliveConfig returns the tcp keep-alive config value of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) TCPKeepAliveConfig() net.KeepAliveConfig {
	return c.cfg.Load().TCPKeepAliveConfig
}

// SetTCPKeepAliveConfig sets the tcp keep-alive config value of the connector.
//
// Deprecated: configure via ConnectorConfig.TCPKeepAliveConfig (see its field
// documentation).
func (c *Connector) SetTCPKeepAliveConfig(tcpKeepAliveConfig net.KeepAliveConfig) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.TCPKeepAliveConfig = tcpKeepAliveConfig
	})
}

// Timeout returns the dial timeout of the connector (legacy name;
// all budgets are equal unless the new fields were set directly).
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Timeout() time.Duration {
	return c.cfg.Load().DialTimeout
}

// SetTimeout sets the timeout of the connector: DialTimeout, ReadTimeout
// and WriteTimeout are all set at once, preserving the legacy
// single-setting semantics.
//
// Deprecated: configure via ConnectorConfig dial/read/write timeouts.
func (c *Connector) SetTimeout(timeout time.Duration) {
	c.updateCfg(func(cp *ConnectorConfig) {
		if timeout < minTimeout {
			timeout = minTimeout
		}
		cp.DialTimeout = timeout
		cp.ReadTimeout = timeout
		cp.WriteTimeout = timeout
	})
}

// PingInterval returns the connection ping interval of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) PingInterval() time.Duration {
	return c.cfg.Load().PingInterval
}

// SetPingInterval sets the connection ping interval value of the connector.
//
// Deprecated: configure via ConnectorConfig.PingInterval (see its field
// documentation).
func (c *Connector) SetPingInterval(d time.Duration) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.PingInterval = d
	})
}

// BufferSize returns the bufferSize of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) BufferSize() int {
	return c.cfg.Load().BufferSize
}

// SetBufferSize sets the bufferSize of the connector.
//
// Deprecated: configure via ConnectorConfig.BufferSize.
func (c *Connector) SetBufferSize(bufferSize int) {
	c.updateCfg(func(cp *ConnectorConfig) {
		if bufferSize < minBufferSize {
			bufferSize = minBufferSize
		}
		cp.BufferSize = bufferSize
	})
}

// BulkSize returns the bulkSize of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) BulkSize() int { return c.cfg.Load().BulkSize }

// SetBulkSize sets the bulkSize of the connector.
//
// Deprecated: configure via ConnectorConfig.BulkSize.
func (c *Connector) SetBulkSize(bulkSize int) {
	c.updateCfg(func(cp *ConnectorConfig) {
		switch {
		case bulkSize < minBulkSize:
			bulkSize = minBulkSize
		case bulkSize > maxBulkSize:
			bulkSize = maxBulkSize
		}
		cp.BulkSize = bulkSize
	})
}

// TCPKeepAlive returns the tcp keep-alive value of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
// Mirrors net.Dialer: zero uses the net default (15s), negative disables.
func (c *Connector) TCPKeepAlive() time.Duration {
	return c.cfg.Load().TCPKeepAlive
}

// SetTCPKeepAlive sets the tcp keep-alive value of the connector.
//
// Mirrors net.Dialer: zero uses the net default (15s), negative disables.
//
// For more information please see net.Dialer structure.
//
// Deprecated: configure via ConnectorConfig.TCPKeepAlive.
func (c *Connector) SetTCPKeepAlive(tcpKeepAlive time.Duration) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.TCPKeepAlive = tcpKeepAlive
	})
}

// DefaultSchema returns the database default schema of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) DefaultSchema() string {
	return c.cfg.Load().DefaultSchema
}

// SetDefaultSchema sets the database default schema of the connector.
//
// Deprecated: configure via ConnectorConfig.DefaultSchema.
func (c *Connector) SetDefaultSchema(schema string) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.DefaultSchema = schema
	})
}

// TLSConfig returns the TLS configuration of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) TLSConfig() *tls.Config {
	return c.cfg.Load().TLSConfig.Clone()
}

// SetTLS sets the TLS configuration of the connector with given parameters. An existing connector TLS configuration is replaced.
//
// Deprecated: configure via ConnectorConfig.TLSConfig, built with NewTLSConfig.
func (c *Connector) SetTLS(serverName string, insecureSkipVerify bool, rootCAFiles ...string) error {
	tlsConfig, err := NewTLSConfig(serverName, insecureSkipVerify, rootCAFiles...)
	if err != nil {
		return err
	}
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.TLSConfig = tlsConfig
	})
	return nil
}

// SetTLSConfig sets the TLS configuration of the connector.
//
// Deprecated: configure via ConnectorConfig.TLSConfig.
func (c *Connector) SetTLSConfig(tlsConfig *tls.Config) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.TLSConfig = tlsConfig.Clone()
	})
}

// Dialer returns the dialer object of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Dialer() dial.Dialer {
	return c.cfg.Load().Dialer
}

// SetDialer sets the dialer object of the connector.
//
// Deprecated: configure via ConnectorConfig.Dialer.
func (c *Connector) SetDialer(dialer dial.Dialer) {
	c.updateCfg(func(cp *ConnectorConfig) {
		if dialer == nil {
			dialer = dial.DefaultDialer
		}
		cp.Dialer = dialer
	})
}

// ApplicationName returns the application name of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) ApplicationName() string {
	return c.cfg.Load().ApplicationName
}

// SetApplicationName sets the application name of the connector.
//
// Deprecated: configure via ConnectorConfig.ApplicationName.
func (c *Connector) SetApplicationName(name string) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.ApplicationName = name
	})
}

// SessionVariables returns the session variables stored in connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) SessionVariables() SessionVariables {
	return maps.Clone(c.cfg.Load().SessionVariables)
}

// SetSessionVariables sets the session variables of the connector.
//
// Deprecated: configure via ConnectorConfig.SessionVariables.
func (c *Connector) SetSessionVariables(sessionVariables SessionVariables) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.SessionVariables = maps.Clone(sessionVariables)
	})
}

// Locale returns the locale of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Locale() string { return c.cfg.Load().Locale }

// SetLocale sets the locale of the connector.
//
// Deprecated: configure via ConnectorConfig.Locale (see its field documentation).
func (c *Connector) SetLocale(locale string) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.Locale = locale
	})
}

// FetchSize returns the fetchSize of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) FetchSize() int {
	return c.cfg.Load().FetchSize
}

// SetFetchSize sets the fetchSize of the connector.
//
// Deprecated: configure via ConnectorConfig.FetchSize.
func (c *Connector) SetFetchSize(fetchSize int) {
	c.updateCfg(func(cp *ConnectorConfig) {
		if fetchSize < minFetchSize {
			fetchSize = minFetchSize
		}
		cp.FetchSize = fetchSize
	})
}

// LobChunkSize returns the lobChunkSize of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) LobChunkSize() int {
	return c.cfg.Load().LobChunkSize
}

// SetLobChunkSize sets the lobChunkSize of the connector.
//
// Deprecated: configure via ConnectorConfig.LobChunkSize.
func (c *Connector) SetLobChunkSize(lobChunkSize int) {
	c.updateCfg(func(cp *ConnectorConfig) {
		switch {
		case lobChunkSize < minLobChunkSize:
			lobChunkSize = minLobChunkSize
		case lobChunkSize > maxLobChunkSize:
			lobChunkSize = maxLobChunkSize
		}
		cp.LobChunkSize = lobChunkSize
	})
}

// Dfv returns the client data format version of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Dfv() int { return c.cfg.Load().Dfv }

// SetDfv sets the client data format version of the connector.
//
// Deprecated: configure via ConnectorConfig.Dfv.
func (c *Connector) SetDfv(dfv int) {
	c.updateCfg(func(cp *ConnectorConfig) {
		if !p.IsSupportedDfv(dfv) {
			dfv = defaultDfv
		}
		cp.Dfv = dfv
	})
}

// CESU8Decoder returns the CESU-8 decoder constructor of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead (see
// its field documentation).
func (c *Connector) CESU8Decoder() func() transform.Transformer {
	return c.cfg.Load().CESU8Decoder
}

// SetCESU8Decoder sets the CESU-8 decoder constructor of the connector.
// A nil constructor resets to the default decoder.
//
// Deprecated: configure via ConnectorConfig.CESU8Decoder.
func (c *Connector) SetCESU8Decoder(cesu8DecoderFn func() transform.Transformer) {
	c.updateCfg(func(cp *ConnectorConfig) {
		if cesu8DecoderFn == nil {
			cesu8DecoderFn = cesu8.DefaultDecoder
		}
		cp.CESU8Decoder = cesu8DecoderFn
	})
}

// CESU8Encoder returns the CESU-8 encoder constructor of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead (see
// its field documentation).
func (c *Connector) CESU8Encoder() func() transform.Transformer {
	return c.cfg.Load().CESU8Encoder
}

// SetCESU8Encoder sets the CESU-8 encoder constructor of the connector.
// A nil constructor resets to the default encoder.
//
// Deprecated: configure via ConnectorConfig.CESU8Encoder.
func (c *Connector) SetCESU8Encoder(cesu8EncoderFn func() transform.Transformer) {
	c.updateCfg(func(cp *ConnectorConfig) {
		if cesu8EncoderFn == nil {
			cesu8EncoderFn = cesu8.DefaultEncoder
		}
		cp.CESU8Encoder = cesu8EncoderFn
	})
}

// EmptyDateAsNull returns NULL for empty dates ('0000-00-00') if true.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead (see
// its field documentation).
func (c *Connector) EmptyDateAsNull() bool {
	return c.cfg.Load().EmptyDateAsNull
}

// SetEmptyDateAsNull sets the EmptyDateAsNull flag of the connector.
//
// Deprecated: configure via ConnectorConfig.EmptyDateAsNull.
func (c *Connector) SetEmptyDateAsNull(emptyDateAsNull bool) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.EmptyDateAsNull = emptyDateAsNull
	})
}

// Compressor returns the lz4 compressor of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Compressor() compress.Compressor {
	return c.cfg.Load().Compressor
}

// SetCompressor sets the lz4 compressor of the connector.
//
// Deprecated: configure via ConnectorConfig.Compressor.
func (c *Connector) SetCompressor(compressor compress.Compressor) {
	c.updateCfg(func(cp *ConnectorConfig) {
		if compressor == nil {
			compressor = compress.DefaultCompressor
		}
		cp.Compressor = compressor
	})
}

// ConnectionRouting reports whether the client requests connection routing.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead (see
// its field documentation).
func (c *Connector) ConnectionRouting() bool {
	return c.cfg.Load().ConnectionRouting
}

// SetConnectionRouting sets the client's request for connection routing.
//
// Deprecated: configure via ConnectorConfig.ConnectionRouting (see its field
// documentation).
func (c *Connector) SetConnectionRouting(connectionRouting bool) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.ConnectionRouting = connectionRouting
	})
}

// Logger returns the Logger instance of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Logger() *slog.Logger {
	return c.cfg.Load().Logger
}

// SetLogger sets the Logger instance of the connector.
//
// Deprecated: configure via ConnectorConfig.Logger.
func (c *Connector) SetLogger(logger *slog.Logger) {
	c.updateCfg(func(cp *ConnectorConfig) {
		if logger == nil {
			logger = slog.Default()
		}
		cp.Logger = logger
	})
}

// Username returns the username of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Username() string {
	return c.cfg.Load().Username
}

// Password returns the basic authentication password of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Password() string {
	c.mu.RLock()
	password := c._password
	c.mu.RUnlock()
	return password
}

// SetPassword sets the basic authentication password of the connector.
// It writes through to the stored configuration so Config() gives it
// back; refresh-driven updates stay runtime-local (see ConnectorConfig).
//
// Deprecated: configure via ConnectorConfig.Password.
func (c *Connector) SetPassword(password string) {
	// Inline instead of updateCfg: cfg and the runtime mirror must swap
	// under the same hold, or a concurrent authHnd (single RLock over
	// both) can pair the new cfg.Password with the old _password.
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *c.cfg.Load()
	cp.Password = password
	c.cfg.Store(&cp)
	c._password = password
}

// RefreshPassword returns the callback function for basic authentication password refresh.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) RefreshPassword() func() (password string, ok bool) {
	return c.cfg.Load().RefreshPassword
}

// SetRefreshPassword sets the callback function for basic authentication password refresh.
//
// Deprecated: configure via ConnectorConfig.RefreshPassword (see the ConnectorConfig
// field documentation for concurrency).
func (c *Connector) SetRefreshPassword(refreshPasswordFn func() (password string, ok bool)) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.RefreshPassword = refreshPasswordFn
	})
}

// ClientCert returns the X509 authentication client certificate and key of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) ClientCert() (clientCert, clientKey []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c._certKey == nil {
		return nil, nil
	}
	return c._certKey.Cert(), c._certKey.Key()
}

// RefreshClientCert returns the callback function for X509 authentication client certificate and key refresh.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) RefreshClientCert() func() (clientCert, clientKey []byte, ok bool) {
	return c.cfg.Load().RefreshClientCert
}

// SetRefreshClientCert sets the callback function for X509 authentication client certificate and key refresh.
//
// Deprecated: configure via ConnectorConfig.RefreshClientCert (see the ConnectorConfig
// field documentation for concurrency).
func (c *Connector) SetRefreshClientCert(refreshClientCertFn func() (clientCert, clientKey []byte, ok bool)) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.RefreshClientCert = refreshClientCertFn
	})
}

// Token returns the JWT authentication token of the connector.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) Token() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c._token
}

// RefreshToken returns the callback function for JWT authentication token refresh.
//
// Deprecated: read from own ConnectorConfig or Connector.Config() instead.
func (c *Connector) RefreshToken() func() (token string, ok bool) {
	return c.cfg.Load().RefreshToken
}

// SetRefreshToken sets the callback function for JWT authentication token refresh.
//
// Deprecated: configure via ConnectorConfig.RefreshToken (see the ConnectorConfig field
// documentation for concurrency).
func (c *Connector) SetRefreshToken(refreshTokenFn func() (token string, ok bool)) {
	c.updateCfg(func(cp *ConnectorConfig) {
		cp.RefreshToken = refreshTokenFn
	})
}

// WithDatabase returns a new Connector supporting tenant database connections via database name.
//
// Deprecated: set ConnectorConfig.DatabaseName when constructing the connector;
// the workaround of adding it afterwards is no longer needed.
func (c *Connector) WithDatabase(databaseName string) *Connector {
	nc := c.clone()
	nc.updateCfg(func(cp *ConnectorConfig) {
		cp.DatabaseName = databaseName
	})
	nc._routing = new(routing)
	return nc
}
