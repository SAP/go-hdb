package driver

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unique"

	"github.com/SAP/go-hdb/driver/compress"
	"github.com/SAP/go-hdb/driver/dial"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/internal/protocol/auth"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
	"golang.org/x/text/transform"
)

// ConnectorConfig default values.
const (
	defaultBufferSize    = 1 << 14           // 16384 - default value bufferSize.
	defaultBulkSize      = 10000             // default value bulkSize.
	defaultTimeout       = 300 * time.Second // default value for DialTimeout, ReadTimeout and WriteTimeout (300 seconds = 5 minutes).
	defaultCancelTimeout = 2 * time.Second   // default value for CancelTimeout.
)

// Minimal / maximal values enforced by validate.
const (
	minTimeout    = 0 * time.Second // minimal timeout value.
	minBufferSize = 1 << 11         // 2048 - minimal bufferSize value.
	minBulkSize   = 1               // minimal bulkSize value.
	maxBulkSize   = p.MaxNumArg     // maximum bulk size.
)

const (
	defaultFetchSize    = 128         // Default value fetchSize.
	defaultLobChunkSize = 1 << 16     // Default value lobChunkSize.
	defaultDfv          = p.DfvLevel8 // Default data version format level.
)

const (
	minFetchSize    = 1             // Minimal fetchSize value.
	minLobChunkSize = 128           // Minimal lobChunkSize
	maxLobChunkSize = math.MaxInt32 // Maximal lobChunkSize
)

var defaultTCPKeepAliveConfig = net.KeepAliveConfig{Enable: true}

// ConnectorConfig holds the static connector configuration. A ConnectorConfig may be
// freely mutated until it is handed to NewConfigConnector,
// which validates and deep-copies it: later mutations affect
// nothing. Start from NewConnectorConfig, which fills in defaults;
// a hand-built ConnectorConfig must set every required field (validate
// rejects nil function/interface fields and out-of-range numbers).
type ConnectorConfig struct {
	// Endpoint.
	Host         string
	DatabaseName string

	// Authentication. Password doubles as external-ticket carrier
	// when Username is empty (hdbcli parity: ticket prefix selects
	// the method); Token is the explicit JWT door. Refresh callbacks
	// may run concurrently when shared across Connectors.
	Username        string
	Password        string
	RefreshPassword func() (password string, ok bool)
	Token           string
	RefreshToken    func() (token string, ok bool)

	// X509 client identity (PEM-encoded, key not password encrypted).
	// Static bytes and files are mutually exclusive: set one pair,
	// not both (validate rejects both). Files are read at construction
	// (fail fast) and re-read on refresh while RefreshClientCert is nil.
	ClientCert        []byte                                         // cert bytes, with ClientKey
	ClientKey         []byte                                         // key bytes, with ClientCert
	ClientCertFile    string                                         // cert file path, with ClientKeyFile
	ClientKeyFile     string                                         // key file path, with ClientCertFile
	RefreshClientCert func() (clientCert, clientKey []byte, ok bool) // explicit refresh override, nil unless set

	// Network.
	DialTimeout  time.Duration // budgets establishment (dial + TLS handshake); zero disables
	ReadTimeout  time.Duration // budgets socket reads; zero disables deadlines
	WriteTimeout time.Duration // budgets socket writes; zero disables deadlines
	// CancelTimeout budgets the synchronous CANCEL plus victim drain;
	// the caller's return waits up to this budget. Zero keeps the
	// legacy async-DISCONNECT behavior (immediate sever).
	CancelTimeout time.Duration
	// PingInterval is the time between connection validity checks.
	// Pinging detects broken connections: if the ping fails, another
	// connection out of the pool is used automatically instead of
	// returning an error. Zero disables pinging; otherwise a ping
	// runs when an idle pooled connection is reused and the time
	// since its last use reaches the interval.
	PingInterval       time.Duration
	TCPKeepAlive       time.Duration       // see net.Dialer: zero uses the net default (15s), negative disables
	TCPKeepAliveConfig net.KeepAliveConfig // see net.Dialer
	TLSConfig          *tls.Config
	Dialer             dial.Dialer // required; NewConnectorConfig provides the default dialer

	// Session.
	DefaultSchema string
	// SessionVariables maps session variables to their values. All
	// defined session variables will be set once after a database
	// connection is opened.
	SessionVariables map[string]string
	ApplicationName  string
	// Locale follows "SAP HANA SQL Command Network Protocol".
	Locale string

	// Transfer tuning: FetchSize is rows; the rest are bytes/count.
	BufferSize   int
	FetchSize    int
	LobChunkSize int
	BulkSize     int
	Dfv          int // client data format version, see protocol.SupportedDfvs

	// Protocol behavior. All function/interface fields are required:
	// NewConnectorConfig provides the defaults.
	// CESU8Decoder is the CESU-8 decoder constructor, called once per
	// connection: it is a factory (not a decoder) because one
	// transformer is created per connection.
	CESU8Decoder func() transform.Transformer
	// CESU8Encoder is the CESU-8 encoder constructor, called once per
	// connection: it is a factory (not an encoder) because one
	// transformer is created per connection.
	CESU8Encoder func() transform.Transformer
	// EmptyDateAsNull returns NULL for empty dates ('0000-00-00').
	// For data format version 1 the backend returns the NULL
	// indicator for empty date fields; for other versions (field
	// type daydate) it does not and the value reads 0. Since 1 means
	// '0001-01-01' (the minimal valid date), leaving this unset
	// yields '0000-12-31' for empty dates, keeping NULL, empty, and
	// valid dates distinct.
	//
	// https://help.sap.com/docs/HANA_SERVICE_CF/7c78579ce9b14a669c1f3295b0d8ca16/3f81ccc7e35d44cbbc595c7d552c202a.html?locale=en-US
	EmptyDateAsNull bool
	Compressor      compress.Compressor // nil disables compression
	// ConnectionRouting requests connection routing by the client.
	// The server may not support it; the effective routing state
	// depends on the value negotiated during authentication.
	ConnectionRouting bool

	// Observability.
	Logger *slog.Logger // required; NewConnectorConfig provides the default logger

	// SQLTrace controls per-statement logging.
	SQLTrace SQLTraceConfig
	// ProtTrace controls protocol tracing.
	ProtTrace ProtTraceConfig
}

// SQLTraceConfig controls per-statement logging. Zero value logs nothing.
type SQLTraceConfig struct {
	// Enabled logs every statement at Info (prefilled from the
	// global sqlTrace flag in NewConnectorConfig). Statements reaching a
	// threshold below are logged at Warn instead, traced or not.
	Enabled bool
	// ServerThreshold trips on server processing time
	// from the reply StatementContext; 0 leaves this leg quiet.
	ServerThreshold time.Duration
	// TotalThreshold trips on client elapsed (network + fetch included);
	// 0 leaves this leg quiet.
	TotalThreshold time.Duration
}

// ProtTraceConfig controls protocol tracing. Zero value traces nothing.
type ProtTraceConfig struct {
	// Enabled dumps protocol parts (credentials redacted); snapshotted
	// from the global flag in NewConnectorConfig.
	Enabled bool
}

// NewTLSConfig builds a TLS configuration with the given server name,
// skip-verify flag and root CA files: with no files the system root
// store is used, with files only the file CAs are trusted.
func NewTLSConfig(serverName string, insecureSkipVerify bool, rootCAFiles ...string) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkipVerify, //nolint:gosec
	}
	var certPool *x509.CertPool
	for _, fn := range rootCAFiles {
		rootPEM, err := os.ReadFile(path.Clean(fn))
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
		tlsConfig.RootCAs = certPool
	}
	return tlsConfig, nil
}

// NewConnectorConfig returns a ConnectorConfig with driver defaults.
func NewConnectorConfig() *ConnectorConfig {
	return &ConnectorConfig{
		DialTimeout:        defaultTimeout,
		ReadTimeout:        defaultTimeout,
		WriteTimeout:       defaultTimeout,
		CancelTimeout:      defaultCancelTimeout,
		BufferSize:         defaultBufferSize,
		BulkSize:           defaultBulkSize,
		TCPKeepAliveConfig: defaultTCPKeepAliveConfig,
		Dialer:             dial.DefaultDialer,
		ApplicationName:    defaultApplicationName,
		FetchSize:          defaultFetchSize,
		LobChunkSize:       defaultLobChunkSize,
		Dfv:                defaultDfv,
		CESU8Decoder:       cesu8.DefaultDecoder,
		CESU8Encoder:       cesu8.DefaultEncoder,
		Compressor:         compress.DefaultCompressor,
		Logger:             slog.Default(),
		SQLTrace:           SQLTraceConfig{Enabled: sqlTrace.Load()},
		ProtTrace:          ProtTraceConfig{Enabled: protTrace.Load()},
	}
}

// clone returns a deep copy of c: mutable references (TLSConfig,
// SessionVariables, cert bytes) are copied so no state leaks between
// generations, callers, or the connector. Stateless handles (Logger,
// Dialer, Compressor, callbacks) stay shared by design.
func (cfg *ConnectorConfig) clone() *ConnectorConfig {
	cp := *cfg
	cp.TLSConfig = cfg.TLSConfig.Clone()
	cp.SessionVariables = maps.Clone(cfg.SessionVariables)
	cp.ClientCert = bytes.Clone(cfg.ClientCert)
	cp.ClientKey = bytes.Clone(cfg.ClientKey)
	return &cp
}

// validate checks cfg and reports all invalid values as a joined
// error. Unlike the legacy setters it never silently corrects
// input: out-of-range numbers and missing required fields are
// errors, never filled in.
func (cfg *ConnectorConfig) validate() error {
	certSet, keySet := len(cfg.ClientCert) > 0, len(cfg.ClientKey) > 0
	certFileSet, keyFileSet := cfg.ClientCertFile != "", cfg.ClientKeyFile != ""
	var errs []error
	if cfg.Host == "" {
		errs = append(errs, errors.New("host must not be empty"))
	}
	if certSet != keySet {
		errs = append(errs, errors.New("client cert and key must be set as a pair"))
	}
	if certFileSet != keyFileSet {
		errs = append(errs, errors.New("client cert file and key file must be set as a pair"))
	}
	if certSet && certFileSet {
		errs = append(errs, errors.New("client cert bytes and cert files are mutually exclusive"))
	}
	if cfg.DialTimeout < minTimeout {
		errs = append(errs, fmt.Errorf("invalid dial timeout %s: must not be negative", cfg.DialTimeout))
	}
	if cfg.ReadTimeout < minTimeout {
		errs = append(errs, fmt.Errorf("invalid read timeout %s: must not be negative", cfg.ReadTimeout))
	}
	if cfg.WriteTimeout < minTimeout {
		errs = append(errs, fmt.Errorf("invalid write timeout %s: must not be negative", cfg.WriteTimeout))
	}
	if cfg.CancelTimeout < minTimeout {
		errs = append(errs, fmt.Errorf("invalid cancel timeout %s: must not be negative", cfg.CancelTimeout))
	}
	if cfg.BufferSize < minBufferSize {
		errs = append(errs, fmt.Errorf("invalid buffer size %d: minimum %d", cfg.BufferSize, minBufferSize))
	}
	if cfg.BulkSize < minBulkSize || cfg.BulkSize > maxBulkSize {
		errs = append(errs, fmt.Errorf("invalid bulk size %d: range %d..%d", cfg.BulkSize, minBulkSize, maxBulkSize))
	}
	if cfg.FetchSize < minFetchSize {
		errs = append(errs, fmt.Errorf("invalid fetch size %d: minimum %d", cfg.FetchSize, minFetchSize))
	}
	if cfg.LobChunkSize < minLobChunkSize || cfg.LobChunkSize > maxLobChunkSize {
		errs = append(errs, fmt.Errorf("invalid lob chunk size %d: range %d..%d", cfg.LobChunkSize, minLobChunkSize, maxLobChunkSize))
	}
	if !p.IsSupportedDfv(cfg.Dfv) {
		errs = append(errs, fmt.Errorf("invalid data format version %d", cfg.Dfv))
	}
	if cfg.Dialer == nil {
		errs = append(errs, errors.New("dialer must be set (NewConnectorConfig provides the default dialer)"))
	}
	if cfg.CESU8Decoder == nil {
		errs = append(errs, errors.New("CESU8 decoder must be set (NewConnectorConfig provides the default decoder)"))
	}
	if cfg.CESU8Encoder == nil {
		errs = append(errs, errors.New("CESU8 encoder must be set (NewConnectorConfig provides the default encoder)"))
	}
	// No check for Compressor: nil legitimately disables compression
	// (the default without the liblz4 build tag; session.go guards it).
	if cfg.Logger == nil {
		errs = append(errs, errors.New("logger must be set (NewConnectorConfig provides the default logger)"))
	}
	if cfg.SQLTrace.ServerThreshold < minTimeout {
		errs = append(errs, fmt.Errorf("invalid sql trace server threshold %s: must not be negative", cfg.SQLTrace.ServerThreshold))
	}
	if cfg.SQLTrace.TotalThreshold < minTimeout {
		errs = append(errs, fmt.Errorf("invalid sql trace total threshold %s: must not be negative", cfg.SQLTrace.TotalThreshold))
	}
	return errors.Join(errs...)
}

type redirectCacheKey struct {
	host, databaseName string
}

var redirectCache sync.Map

func redirectHost(host, databaseName string) (string, bool) {
	redirectHost, ok := redirectCache.Load(redirectCacheKey{host: host, databaseName: databaseName})
	if !ok || redirectHost == nil {
		return "", false
	}
	return redirectHost.(string), true
}

func setRedirectHost(host, databaseName, redirectHost string) {
	redirectCache.Store(redirectCacheKey{host: host, databaseName: databaseName}, redirectHost)
}

func deleteRedirectHost(host, databaseName string) {
	redirectCache.Delete(redirectCacheKey{host: host, databaseName: databaseName})
}

// connAttrs is holding connection relevant attributes. It is a
// credential-free view: no auth fields ride into session code.
type connAttrs struct {
	dialTimeout        time.Duration
	readTimeout        time.Duration
	writeTimeout       time.Duration
	cancelTimeout      time.Duration
	pingInterval       time.Duration
	bufferSize         int
	bulkSize           int
	tcpKeepAlive       time.Duration       // see net.Dialer
	tcpKeepAliveConfig net.KeepAliveConfig // see net.Dialer
	tlsConfig          *tls.Config
	defaultSchema      string
	dialer             dial.Dialer
	applicationName    string
	sessionVariables   map[string]string
	locale             string
	fetchSize          int
	lobChunkSize       int
	dfv                int
	cesu8DecoderFn     func() transform.Transformer
	cesu8EncoderFn     func() transform.Transformer
	emptyDateAsNull    bool
	compressor         compress.Compressor
	connectionRouting  bool
	logger             *slog.Logger
	sqlTrace           SQLTraceConfig
	protTrace          ProtTraceConfig
}

func (c *connAttrs) dialContext(ctx context.Context, host string) (net.Conn, error) {
	return c.dialer.DialContext(ctx, host, dial.DialerOptions{Timeout: c.dialTimeout, TCPKeepAlive: c.tcpKeepAlive, TCPKeepAliveConfig: c.tcpKeepAliveConfig})
}

// newConnAttrs builds the session view off one immutable configuration
// generation. Maps and TLS config are shared by reference, not re-cloned:
// cfg is already a deep-copied, immutable generation (see clone) and
// downstream code only reads it (tls.Client documents tls.Config reuse
// as safe), so sharing is sound across generations.
func newConnAttrs(cfg *ConnectorConfig) *connAttrs {
	return &connAttrs{
		dialTimeout:        cfg.DialTimeout,
		readTimeout:        cfg.ReadTimeout,
		writeTimeout:       cfg.WriteTimeout,
		cancelTimeout:      cfg.CancelTimeout,
		pingInterval:       cfg.PingInterval,
		bufferSize:         cfg.BufferSize,
		bulkSize:           cfg.BulkSize,
		tcpKeepAlive:       cfg.TCPKeepAlive,
		tcpKeepAliveConfig: cfg.TCPKeepAliveConfig,
		tlsConfig:          cfg.TLSConfig,
		defaultSchema:      cfg.DefaultSchema,
		dialer:             cfg.Dialer,
		applicationName:    cfg.ApplicationName,
		sessionVariables:   cfg.SessionVariables,
		locale:             cfg.Locale,
		fetchSize:          cfg.FetchSize,
		lobChunkSize:       cfg.LobChunkSize,
		dfv:                cfg.Dfv,
		cesu8DecoderFn:     cfg.CESU8Decoder,
		cesu8EncoderFn:     cfg.CESU8Encoder,
		emptyDateAsNull:    cfg.EmptyDateAsNull,
		compressor:         cfg.Compressor,
		connectionRouting:  cfg.ConnectionRouting,
		logger:             cfg.Logger,
		sqlTrace:           cfg.SQLTrace,
		protTrace:          cfg.ProtTrace,
	}
}

// readCertKeyFiles reads the PEM-encoded client certificate and key
// from disk. Paths are cleaned here so direct field assignment
// needs no setter.
func readCertKeyFiles(certFile, keyFile string) (unique.Handle[string], unique.Handle[string], error) {
	var handle unique.Handle[string]
	cert, err := os.ReadFile(path.Clean(certFile))
	if err != nil {
		return handle, handle, err
	}
	key, err := os.ReadFile(path.Clean(keyFile))
	if err != nil {
		return handle, handle, err
	}
	return unique.Make(string(cert)), unique.Make(string(key)), nil
}

func isJWTToken(token string) bool { return strings.HasPrefix(token, "ey") }

/*
A Connector represents a hdb driver in a fixed configuration.
A Connector can be passed to sql.OpenDB allowing users to bypass a string based data source name.
*/
type Connector struct {
	_routing *routing

	mu sync.RWMutex // guards updateCfg (deprecated setters) and auth mutation

	// cfg is the authoritative configuration, deep-copied at
	// construction into an immutable generation. Deprecated setters
	// replace the generation copy-on-write (see updateCfg); all other
	// readers load the pointer lock-free.
	cfg atomic.Pointer[ConnectorConfig]

	hasCookie      atomic.Bool
	_certKey       *auth.CertKey // X509 identity derived from cfg cert bytes or files
	_password      string        // basic authentication password, refreshed between connects
	_token         string        // JWT token, refreshed between connects
	_logonname     string        // session cookie login does need logon name provided by JWT authentication.
	_sessionCookie []byte        // authentication via session cookie (HDB currently does support only SAML and JWT - go-hdb JWT)
	cbmu           sync.Mutex    // prevents refresh callbacks from being called in parallel

	metrics *metrics

	lifecycle *connLifecycle
}

// newConnector returns a Connector with wiring (routing, lifecycle,
// metrics) holding a deep copy of cfg. It performs no validation and
// no I/O and cannot fail; callers pass NewConnectorConfig defaults
// (NewConnector), validated configs (NewConfigConnector) or
// defaults-based configs whose inputs cannot fail validation
// (legacy wrappers).
func newConnector(cfg *ConnectorConfig) *Connector {
	c := &Connector{
		_routing: new(routing),
		metrics:  stdHdbDriver.metrics, // use default stdHdbDriver metrics
	}
	c.lifecycle = &connLifecycle{connector: c}
	c.cfg.Store(cfg.clone())
	c._password = cfg.Password
	c._token = cfg.Token
	return c
}

// config loads the current immutable configuration generation.
func (c *Connector) config() *ConnectorConfig { return c.cfg.Load() }

// connAttrs builds the session view off the loaded generation.
func (c *Connector) connAttrs() *connAttrs { return newConnAttrs(c.config()) }

// NewConfigConnector validates cfg and returns a Connector holding
// a deep copy of it. Invalid values are returned as error, never
// silently clamped. The X509 identity is established like the legacy
// constructors: files are read first when set, else static bytes;
// file read and key errors fail fast here, never on first connect.
func NewConfigConnector(cfg *ConnectorConfig) (*Connector, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c := newConnector(cfg)
	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		certHandle, keyHandle, err := readCertKeyFiles(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, err
		}
		if c._certKey, err = auth.NewCertKey(certHandle, keyHandle); err != nil {
			return nil, err
		}
	} else if len(cfg.ClientCert) > 0 && len(cfg.ClientKey) > 0 {
		var err error
		if c._certKey, err = auth.NewCertKey(unique.Make(string(cfg.ClientCert)), unique.Make(string(cfg.ClientKey))); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// NativeDriver returns the go-hdb Driver interface of the Connector, exposing the
// go-hdb specific driver functions (Name, Version, Stats). Use Driver for the
// standard database/sql/driver.Driver.
func (c *Connector) NativeDriver() Driver { return stdHdbDriver }

func (c *Connector) fetchRedirectHost(ctx context.Context, databaseName string) (string, error) {
	cfg := c.config()
	conn, err := newConn(ctx, cfg.Host, c.metrics, c._routing, c.connAttrs(), c.lifecycle)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	dbi, err := conn.session.dbConnectInfo(ctx, databaseName)
	if err != nil {
		return "", err
	}
	if dbi.IsConnected { // if databaseName == "SYSTEMDB" and isConnected == true host and port are initial
		return cfg.Host, nil
	}
	return net.JoinHostPort(dbi.Host, strconv.Itoa(dbi.Port)), nil
}

// connect returns a connection or an error; the bool is true if the transport to
// host was reached, so routing state is kept even when authentication fails.
func (c *Connector) connect(ctx context.Context, host string) (driver.Conn, bool, error) {
	// isRealAuthError returns true in case of X509 certificate validation errors or hdb authentication errors, otherwise false.
	isRealAuthError := func(err error) bool {
		if _, ok := errors.AsType[*auth.CertValidationError](err); ok {
			return true
		}
		if hdbErrors, ok := errors.AsType[*p.HdbErrors](err); ok {
			return hdbErrors.Code() == p.HdbErrAuthenticationFailed
		}
		return false
	}

	attrs := c.connAttrs()

	// can we connect via cookie?
	if auth := c.cookieAuth(); auth != nil {
		conn, connErr := newConn(ctx, host, c.metrics, c._routing, attrs, c.lifecycle)
		if connErr != nil {
			return nil, false, connErr
		}
		authErr := conn.authenticate(ctx, host, auth)
		if authErr == nil {
			return conn, true, nil
		}
		conn.Close()
		if !isRealAuthError(authErr) {
			return nil, false, authErr
		}
		c.invalidateCookie() // cookie auth was not successful - do not try again with the same data
	}

	c.cbmu.Lock() // synchronize refresh calls
	defer c.cbmu.Unlock()
	for {
		authHnd := c.authHnd()

		conn, connErr := newConn(ctx, host, c.metrics, c._routing, attrs, c.lifecycle)
		if connErr != nil {
			return nil, false, connErr
		}
		authErr := conn.authenticate(ctx, host, authHnd)
		if authErr == nil {
			if method, ok := authHnd.Selected().(auth.CookieGetter); ok {
				c.setCookie(method.Cookie())
			}
			return conn, true, nil
		}
		conn.Close()
		if !isRealAuthError(authErr) {
			return nil, false, authErr
		}

		// err is auth error - connection itself is ok
		ok, refreshErr := c.refresh()
		if refreshErr != nil {
			return nil, true, refreshErr
		}
		if !ok { // no connection retry in case no refresh took place
			return nil, true, authErr
		}
	}
}

// Connect implements the database/sql/driver/Connector interface.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg := c.config()
	if reuse, ok := c.lifecycle.getConn(ctx); ok { // pooled session is already authenticated: skip dial.
		return reuse, nil
	}
	if cfg.DatabaseName != "" {
		if cached, ok := redirectHost(cfg.Host, cfg.DatabaseName); ok {
			host := c._routing.pick(cached)
			conn, connectSuccess, err := c.connect(ctx, host)
			if !connectSuccess {
				deleteRedirectHost(cfg.Host, cfg.DatabaseName)
			}
			c._routing.setReachable(host, connectSuccess)
			return conn, err
		}
		redirectHost, err := c.fetchRedirectHost(ctx, cfg.DatabaseName)
		if err != nil {
			return nil, err
		}
		conn, connectSuccess, err := c.connect(ctx, redirectHost)
		if connectSuccess {
			setRedirectHost(cfg.Host, cfg.DatabaseName, redirectHost)
		}
		c._routing.setReachable(redirectHost, connectSuccess)
		return conn, err
	}
	host := c._routing.pick(cfg.Host)
	conn, connectSuccess, err := c.connect(ctx, host)
	c._routing.setReachable(host, connectSuccess)
	return conn, err
}

// Driver implements the database/sql/driver/Connector interface.
func (c *Connector) Driver() driver.Driver { return stdHdbDriver }

func (c *Connector) clone() *Connector {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nc := &Connector{
		_routing: c._routing,

		_certKey:  c._certKey,
		_password: c._password,
		_token:    c._token,

		metrics: c.metrics,
	}
	nc.cfg.Store(c.config().clone())

	nc.lifecycle = &connLifecycle{connector: nc}
	return nc
}

// Config returns a deep copy of the connector's user configuration for
// derive-tweak-construct cycles: construction input plus deprecated
// setter mutations. Runtime-refreshed credentials stay connector-local;
// derives inherit the refresh callbacks and re-acquire on first use.
func (c *Connector) Config() ConnectorConfig {
	return *c.config().clone()
}

// auth attributes.
func (c *Connector) cookieAuth() *p.AuthHnd {
	if !c.hasCookie.Load() { // fastpath without lock
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	auth := p.NewAuthHnd(c._logonname)                              // important: for session cookie auth we do need the logonname from JWT auth,
	auth.AddSessionCookie(c._sessionCookie, c._logonname, clientID) // and for HANA onPrem the final session cookie req needs the logonname as well.
	return auth
}

func (c *Connector) authHnd() *p.AuthHnd {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cfg := c.config()
	authHnd := p.NewAuthHnd(cfg.Username) // use username as logonname
	if c._certKey != nil {
		authHnd.AddX509(c._certKey)
	}
	if c._token != "" {
		authHnd.AddJWT(c._token)
	}
	// mimic standard drivers and use password as token if user is empty
	if c._token == "" && cfg.Username == "" && isJWTToken(c._password) {
		authHnd.AddJWT(c._password)
	}
	if c._password != "" {
		authHnd.AddBasic(cfg.Username, c._password)
		authHnd.AddLDAP(cfg.Username, c._password)
	}
	return authHnd
}

func (c *Connector) refresh() (bool, error) {
	refreshed := false

	callRefreshPassword := func(refreshPassword func() (string, bool)) (string, bool) {
		defer c.mu.Lock() // finally lock attr again
		c.mu.Unlock()     // unlock attr, so that callback can call attr methods
		return refreshPassword()
	}

	callRefreshToken := func(refreshToken func() (token string, ok bool)) (string, bool) {
		defer c.mu.Lock() // finally lock attr again
		c.mu.Unlock()     // unlock attr, so that callback can call attr methods
		return refreshToken()
	}

	callRefreshClientCert := func(refreshClientCert func() (clientCert, clientKey []byte, ok bool)) (unique.Handle[string], unique.Handle[string], bool) {
		var handle unique.Handle[string]
		defer c.mu.Lock() // finally lock attr again
		c.mu.Unlock()     // unlock attr, so that callback can call attr methods
		clientCert, clientKey, ok := refreshClientCert()
		if !ok {
			return handle, handle, false
		}
		return unique.Make(string(clientCert)), unique.Make(string(clientKey)), true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cfg := c.config()

	if cfg.RefreshPassword != nil {
		if password, ok := callRefreshPassword(cfg.RefreshPassword); ok {
			if password != c._password {
				c._password = password
				refreshed = true
			}
		}
	}
	if cfg.RefreshToken != nil {
		if token, ok := callRefreshToken(cfg.RefreshToken); ok {
			if token != c._token {
				c._token = token
				refreshed = true
			}
		}
	}
	if cfg.RefreshClientCert != nil {
		if certHandle, keyHandle, ok := callRefreshClientCert(cfg.RefreshClientCert); ok {
			if c._certKey == nil || !c._certKey.Equal(certHandle, keyHandle) {
				certKey, err := auth.NewCertKey(certHandle, keyHandle)
				if err != nil {
					return refreshed, err
				}
				c._certKey = certKey
				refreshed = true
			}
		}
	} else if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		if certHandle, keyHandle, err := readCertKeyFiles(cfg.ClientCertFile, cfg.ClientKeyFile); err == nil {
			if c._certKey == nil || !c._certKey.Equal(certHandle, keyHandle) {
				certKey, err := auth.NewCertKey(certHandle, keyHandle)
				if err != nil {
					return refreshed, err
				}
				c._certKey = certKey
				refreshed = true
			}
		}
	}
	return refreshed, nil
}

func (c *Connector) invalidateCookie() { c.hasCookie.Store(false) }

func (c *Connector) setCookie(logonname string, sessionCookie []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hasCookie.Store(true)
	c._logonname = logonname
	c._sessionCookie = sessionCookie
}
