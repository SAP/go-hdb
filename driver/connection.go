package driver

import (
	"bufio"
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SAP/go-hdb/driver/dial"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/internal/protocol/auth"
	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	hdbreflect "github.com/SAP/go-hdb/driver/internal/reflect"
)

// ErrUnsupportedIsolationLevel is the error raised if a transaction is started with a not supported isolation level.
var ErrUnsupportedIsolationLevel = errors.New("unsupported isolation level")

// ErrNestedTransaction is the error raised if a transaction is created within a transaction as this is not supported by hdb.
var ErrNestedTransaction = errors.New("nested transactions are not supported")

// ErrNestedQuery is the error raised if a new sql statement is sent to the database server before the resultset
// processing of a previous sql query statement is finalized.
// Currently this only can happen if connections are used concurrently and if stream enabled fields (LOBs) are part
// of the resultset.
// This error can be avoided in whether using a transaction or a dedicated connection (sql.Tx or sql.Conn).
var ErrNestedQuery = errors.New("nested sql queries are not supported")

// queries.
const (
	dummyQuery                      = "select 1 from dummy"
	setIsolationLevelReadCommitted  = "set transaction isolation level read committed"
	setIsolationLevelRepeatableRead = "set transaction isolation level repeatable read"
	setIsolationLevelSerializable   = "set transaction isolation level serializable"
	setAccessModeReadOnly           = "set transaction read only"
	setAccessModeReadWrite          = "set transaction read write"
	setDefaultSchema                = "set schema"
)

var (
	// register as var to execute even before init() funcs are called.
	_ = p.RegisterScanType(p.DtBytes, hdbreflect.TypeFor[[]byte](), hdbreflect.TypeFor[NullBytes]())
	_ = p.RegisterScanType(p.DtDecimal, hdbreflect.TypeFor[Decimal](), hdbreflect.TypeFor[NullDecimal]())
	_ = p.RegisterScanType(p.DtLob, hdbreflect.TypeFor[Lob](), hdbreflect.TypeFor[NullLob]())
)

// dbConn wraps the database tcp connection. It sets timeouts and handles driver ErrBadConn behavior.
type dbConn struct {
	metrics   *metrics
	conn      net.Conn
	timeout   time.Duration
	logger    *slog.Logger
	lastRead  time.Time
	lastWrite time.Time
}

func (c *dbConn) deadline() (deadline time.Time) {
	if c.timeout == 0 {
		return
	}
	return time.Now().Add(c.timeout)
}

func (c *dbConn) close() error { return c.conn.Close() }

// Read implements the io.Reader interface.
func (c *dbConn) Read(b []byte) (int, error) {
	// set timeout
	if err := c.conn.SetReadDeadline(c.deadline()); err != nil {
		return 0, fmt.Errorf("%w: %w", driver.ErrBadConn, err)
	}
	c.lastRead = time.Now()
	n, err := c.conn.Read(b)
	c.metrics.msgCh <- timeMsg{idx: timeRead, d: time.Since(c.lastRead)}
	c.metrics.msgCh <- counterMsg{idx: counterBytesRead, v: uint64(n)}
	if err != nil {
		c.logger.LogAttrs(context.Background(), slog.LevelError, "DB conn read error", slog.String("error", err.Error()), slog.String("local address", c.conn.LocalAddr().String()), slog.String("remote address", c.conn.RemoteAddr().String()))
		// wrap error in driver.ErrBadConn
		return n, fmt.Errorf("%w: %w", driver.ErrBadConn, err)
	}
	return n, nil
}

// Write implements the io.Writer interface.
func (c *dbConn) Write(b []byte) (int, error) {
	// set timeout
	if err := c.conn.SetWriteDeadline(c.deadline()); err != nil {
		return 0, fmt.Errorf("%w: %w", driver.ErrBadConn, err)
	}
	c.lastWrite = time.Now()
	n, err := c.conn.Write(b)
	c.metrics.msgCh <- timeMsg{idx: timeWrite, d: time.Since(c.lastWrite)}
	c.metrics.msgCh <- counterMsg{idx: counterBytesWritten, v: uint64(n)}
	if err != nil {
		c.logger.LogAttrs(context.Background(), slog.LevelError, "DB conn write error", slog.String("error", err.Error()), slog.String("local address", c.conn.LocalAddr().String()), slog.String("remote address", c.conn.RemoteAddr().String()))
		// wrap error in driver.ErrBadConn
		return n, fmt.Errorf("%w: %w", driver.ErrBadConn, err)
	}
	return n, nil
}

// check if conn implements all required interfaces.
var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ Conn                      = (*conn)(nil) // go-hdb enhancements
)

// connHook is a hook for testing.
var connHook func(c *conn, op int)

// connection hook operations.
const (
	choNone = iota
	choStmtExec
)

var errCancelled = fmt.Errorf("%w: %w", driver.ErrBadConn, errors.New("db call cancelled"))

// Conn enhances a connection with go-hdb specific connection functions.
type Conn interface {
	HDBVersion() *Version
	DatabaseName() string
	DBConnectInfo(ctx context.Context, databaseName string) (*DBConnectInfo, error)
}

var stdConnTracker = &connTracker{}

type connTracker struct {
	mu      sync.Mutex
	_callDB *sql.DB
	numConn int64
}

func (t *connTracker) add() { t.mu.Lock(); t.numConn++; t.mu.Unlock() }

func (t *connTracker) remove() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.numConn--
	if t.numConn > 0 {
		return
	}
	t.numConn = 0
	if t._callDB != nil {
		t._callDB.Close()
		t._callDB = nil
	}
}

func (t *connTracker) callDB() *sql.DB {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t._callDB == nil {
		t._callDB = sql.OpenDB(new(callConnector))
	}
	return t._callDB
}

// Conn is the implementation of the database/sql/driver Conn interface.
type conn struct {
	attrs   *connAttrs
	metrics *metrics

	sqlTrace bool
	logger   *slog.Logger

	dbConn *dbConn

	wg        sync.WaitGroup // wait for concurrent db calls when closing connections
	inTx      bool           // in transaction
	lastError error          // last error
	sessionID int64

	serverOptions *p.ConnectOptions
	hdbVersion    *Version

	dec *encoding.Decoder
	pr  *p.Reader
	pw  *p.Writer
}

// isAuthError returns true in case of X509 certificate validation errrors or hdb authentication errors, else otherwise.
func isAuthError(err error) bool {
	var certValidationError *auth.CertValidationError
	if errors.As(err, &certValidationError) {
		return true
	}
	var hdbErrors *p.HdbErrors
	if !errors.As(err, &hdbErrors) {
		return false
	}
	return hdbErrors.Code() == p.HdbErrAuthenticationFailed
}

func connect(ctx context.Context, host string, metrics *metrics, connAttrs *connAttrs, authAttrs *authAttrs) (driver.Conn, error) {
	// can we connect via cookie?
	if auth := authAttrs.cookieAuth(); auth != nil {
		conn, err := newSession(ctx, host, metrics, connAttrs, auth)
		if err == nil {
			return conn, nil
		}
		if !isAuthError(err) {
			return nil, err
		}
		authAttrs.invalidateCookie() // cookie auth was not successful - do not try again with the same data
	}

	lastVersion := authAttrs.version.Load()
	for {
		authHnd := authAttrs.authHnd()

		conn, err := newSession(ctx, host, metrics, connAttrs, authHnd)
		if err == nil {
			if method, ok := authHnd.Selected().(auth.CookieGetter); ok {
				authAttrs.setCookie(method.Cookie())
			}
			return conn, nil
		}
		if !isAuthError(err) {
			return nil, err
		}

		if err := authAttrs.refresh(); err != nil {
			return nil, err
		}

		version := authAttrs.version.Load()
		if version == lastVersion { // no connection retry in case no new version available
			return nil, err
		}
		lastVersion = version
	}
}

var (
	protTrace atomic.Bool
	sqlTrace  atomic.Bool
)

func init() {
	setTrace := func(b *atomic.Bool, s string) error {
		v, err := strconv.ParseBool(s)
		if err == nil {
			b.Store(v)
		}
		return err
	}
	flag.BoolFunc("hdb.protTrace", "enabling hdb protocol trace", func(s string) error { return setTrace(&protTrace, s) })
	flag.BoolFunc("hdb.sqlTrace", "enabling hdb sql trace", func(s string) error { return setTrace(&sqlTrace, s) })
}

// SQLTrace returns true if sql tracing output is active, false otherwise.
func SQLTrace() bool { return sqlTrace.Load() }

// SetSQLTrace sets sql tracing output active or inactive.
func SetSQLTrace(on bool) { sqlTrace.Store(on) }

// unique connection number.
var connNo atomic.Uint64

func newConn(ctx context.Context, host string, metrics *metrics, attrs *connAttrs) (*conn, error) {
	netConn, err := attrs._dialer.DialContext(ctx, host, dial.DialerOptions{Timeout: attrs._timeout, TCPKeepAlive: attrs._tcpKeepAlive})
	if err != nil {
		return nil, err
	}

	metrics.lazyInit()

	// is TLS connection requested?
	if attrs._tlsConfig != nil {
		netConn = tls.Client(netConn, attrs._tlsConfig)
	}

	logger := attrs._logger.With(slog.Uint64("conn", connNo.Add(1)))

	dbConn := &dbConn{metrics: metrics, conn: netConn, timeout: attrs._timeout, logger: logger}
	// buffer connection
	rw := bufio.NewReadWriter(bufio.NewReaderSize(dbConn, attrs._bufferSize), bufio.NewWriterSize(dbConn, attrs._bufferSize))

	protTrace := protTrace.Load()

	enc := encoding.NewEncoder(rw.Writer, attrs._cesu8Encoder)
	dec := encoding.NewDecoder(rw.Reader, attrs._cesu8Decoder)

	c := &conn{attrs: attrs, metrics: metrics, dbConn: dbConn, sqlTrace: sqlTrace.Load(), logger: logger, dec: dec, sessionID: defaultSessionID}
	c.pw = p.NewWriter(rw.Writer, enc, protTrace, logger, attrs._cesu8Encoder, attrs._sessionVariables) // write upstream
	c.pr = p.NewDBReader(dec, c.readLob, protTrace, logger)                                             // read downstream

	if err := c.pw.WriteProlog(ctx); err != nil {
		dbConn.close()
		return nil, err
	}

	if err := c.pr.ReadProlog(ctx); err != nil {
		dbConn.close()
		return nil, err
	}

	stdConnTracker.add()

	c.metrics.msgCh <- gaugeMsg{idx: gaugeConn, v: 1} // increment open connections.
	return c, nil
}

func fetchRedirectHost(ctx context.Context, host, databaseName string, metrics *metrics, attrs *connAttrs) (string, error) {
	c, err := newConn(ctx, host, metrics, attrs)
	if err != nil {
		return "", err
	}
	defer c.Close()
	dbi, err := c.dbConnectInfo(ctx, databaseName)
	if err != nil {
		return "", err
	}
	if dbi.IsConnected { // if databaseName == "SYSTEMDB" and isConnected == true host and port are initial
		return host, nil
	}
	return net.JoinHostPort(dbi.Host, strconv.Itoa(dbi.Port)), nil
}

func newSession(ctx context.Context, host string, metrics *metrics, attrs *connAttrs, authHnd *p.AuthHnd) (driver.Conn, error) {
	c, err := newConn(ctx, host, metrics, attrs)
	if err != nil {
		return nil, err
	}
	if err := c.initSession(ctx, attrs, authHnd); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *conn) initSession(ctx context.Context, attrs *connAttrs, authHnd *p.AuthHnd) (err error) {
	if c.sessionID, c.serverOptions, err = c.authenticate(ctx, authHnd, attrs); err != nil {
		return err
	}
	if c.sessionID <= 0 {
		return fmt.Errorf("invalid session id %d", c.sessionID)
	}

	c.hdbVersion = parseVersion(c.versionString())
	c.dec.SetAlphanumDfv1(c.serverOptions.DataFormatVersion2OrZero() == p.DfvLevel1)
	c.dec.SetEmptyDateAsNull(attrs._emptyDateAsNull)

	if attrs._defaultSchema != "" {
		if _, err := c.ExecContext(ctx, strings.Join([]string{setDefaultSchema, Identifier(attrs._defaultSchema).String()}, " "), nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) versionString() (version string) { return c.serverOptions.FullVersionOrZero() }

// ResetSession implements the driver.SessionResetter interface.
func (c *conn) ResetSession(ctx context.Context) error {
	if c.isBad() {
		return driver.ErrBadConn
	}

	c.lastError = nil

	if c.attrs._pingInterval == 0 || c.dbConn.lastRead.IsZero() || time.Since(c.dbConn.lastRead) < c.attrs._pingInterval {
		return nil
	}

	if _, err := c.queryDirect(ctx, dummyQuery, !c.inTx); err != nil {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) isBad() bool { return errors.Is(c.lastError, driver.ErrBadConn) }

// IsValid implements the driver.Validator interface.
func (c *conn) IsValid() bool { return !c.isBad() }

// Ping implements the driver.Pinger interface.
func (c *conn) Ping(ctx context.Context) error {
	if c.sqlTrace {
		defer c.logSQLTrace(ctx, time.Now(), dummyQuery, nil)
	}

	done := make(chan struct{})
	var err error
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		_, err = c.queryDirect(ctx, dummyQuery, !c.inTx)
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.lastError = errCancelled
		return ctx.Err()
	case <-done:
		c.lastError = err
		return err
	}
}

// PrepareContext implements the driver.ConnPrepareContext interface.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.sqlTrace {
		defer c.logSQLTrace(ctx, time.Now(), query, nil)
	}

	done := make(chan struct{})
	var stmt driver.Stmt
	var err error
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		var pr *prepareResult

		if pr, err = c.prepare(ctx, query); err == nil {
			stmt = newStmt(c, query, pr)
			if stmtMetadata, ok := ctx.Value(stmtMetadataCtxKey).(*StmtMetadata); ok {
				*stmtMetadata = pr
			}
		}

		close(done)
	}()

	select {
	case <-ctx.Done():
		c.lastError = errCancelled
		return nil, ctx.Err()
	case <-done:
		c.lastError = err
		return stmt, err
	}
}

// Close implements the driver.Conn interface.
func (c *conn) Close() error {
	c.wg.Wait()                                        // wait until concurrent db calls are finalized
	c.metrics.msgCh <- gaugeMsg{idx: gaugeConn, v: -1} // decrement open connections.
	// do not disconnect if isBad or invalid sessionID
	if !c.isBad() && c.sessionID != defaultSessionID {
		c.disconnect(context.Background()) //nolint:errcheck
	}
	err := c.dbConn.close()
	stdConnTracker.remove()
	return err
}

// BeginTx implements the driver.ConnBeginTx interface.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.inTx {
		return nil, ErrNestedTransaction
	}

	var isolationLevelQuery string
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault, sql.LevelReadCommitted:
		isolationLevelQuery = setIsolationLevelReadCommitted
	case sql.LevelRepeatableRead:
		isolationLevelQuery = setIsolationLevelRepeatableRead
	case sql.LevelSerializable:
		isolationLevelQuery = setIsolationLevelSerializable
	default:
		return nil, ErrUnsupportedIsolationLevel
	}

	done := make(chan struct{})
	var tx driver.Tx
	var err error
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// set isolation level
		if _, err = c.execDirect(ctx, isolationLevelQuery, !c.inTx); err != nil {
			goto done
		}
		// set access mode
		if opts.ReadOnly {
			_, err = c.execDirect(ctx, setAccessModeReadOnly, !c.inTx)
		} else {
			_, err = c.execDirect(ctx, setAccessModeReadWrite, !c.inTx)
		}
		if err != nil {
			goto done
		}
		c.inTx = true
		tx = newTx(c)
	done:
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.lastError = errCancelled
		return nil, ctx.Err()
	case <-done:
		c.lastError = err
		return tx, err
	}
}

var callStmt = regexp.MustCompile(`(?i)^\s*call\s+.*`) // sql statement beginning with call

// QueryContext implements the driver.QueryerContext interface.
func (c *conn) QueryContext(ctx context.Context, query string, nvargs []driver.NamedValue) (driver.Rows, error) {
	if callStmt.MatchString(query) {
		return nil, fmt.Errorf("invalid procedure call %s - please use Exec instead", query)
	}
	if len(nvargs) != 0 {
		return nil, driver.ErrSkip // fast path not possible (prepare needed)
	}
	if c.sqlTrace {
		defer c.logSQLTrace(ctx, time.Now(), query, nvargs)
	}

	done := make(chan struct{})
	var rows driver.Rows
	var err error
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		rows, err = c.queryDirect(ctx, query, !c.inTx)
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.lastError = errCancelled
		return nil, ctx.Err()
	case <-done:
		c.lastError = err
		return rows, err
	}
}

// ExecContext implements the driver.ExecerContext interface.
func (c *conn) ExecContext(ctx context.Context, query string, nvargs []driver.NamedValue) (driver.Result, error) {
	if len(nvargs) != 0 {
		return nil, driver.ErrSkip // fast path not possible (prepare needed)
	}
	if c.sqlTrace {
		defer c.logSQLTrace(ctx, time.Now(), query, nvargs)
	}

	done := make(chan struct{})
	var result driver.Result
	var err error
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// handle procesure call without parameters here as well
		result, err = c.execDirect(ctx, query, !c.inTx)
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.lastError = errCancelled
		return nil, ctx.Err()
	case <-done:
		c.lastError = err
		return result, err
	}
}

// CheckNamedValue implements the NamedValueChecker interface.
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	// - called by sql driver for ExecContext and QueryContext
	// - no check needs to be performed as ExecContext and QueryContext provided
	//   with parameters will force the 'prepare way' (driver.ErrSkip)
	// - Anyway, CheckNamedValue must be implemented to avoid default sql driver checks
	//   which would fail for custom arg types like Lob
	return nil
}

// Conn Raw access methods

// HDBVersion implements the Conn interface.
func (c *conn) HDBVersion() *Version { return c.hdbVersion }

// DatabaseName implements the Conn interface.
func (c *conn) DatabaseName() string { return c.serverOptions.DatabaseNameOrZero() }

// DBConnectInfo implements the Conn interface.
func (c *conn) DBConnectInfo(ctx context.Context, databaseName string) (*DBConnectInfo, error) {
	done := make(chan struct{})
	var ci *DBConnectInfo
	var err error
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ci, err = c.dbConnectInfo(ctx, databaseName)
		close(done)
	}()

	select {
	case <-ctx.Done():
		c.lastError = errCancelled
		return nil, ctx.Err()
	case <-done:
		c.lastError = err
		return ci, err
	}
}

func (c *conn) logSQLTrace(ctx context.Context, start time.Time, query string, nvargs []driver.NamedValue) {
	const maxArg = 5 // limit the number of arguments to 5
	l := len(nvargs)

	if l == 0 {
		c.logger.LogAttrs(ctx, slog.LevelInfo, "SQL", slog.String("query", query), slog.Int64("ms", time.Since(start).Milliseconds()))
		return
	}

	var attrs []slog.Attr
	for i := 0; i < min(l, maxArg); i++ {
		name := nvargs[i].Name
		if name == "" {
			name = strconv.Itoa(nvargs[i].Ordinal)
		}
		attrs = append(attrs, slog.String(name, fmt.Sprintf("%v", nvargs[i].Value)))
	}
	if l > maxArg {
		attrs = append(attrs, slog.Int("numArgSkip", l-maxArg))
	}
	c.logger.LogAttrs(ctx, slog.LevelInfo, "SQL", slog.String("query", query), slog.Int64("ms", time.Since(start).Milliseconds()), slog.Any("arg", slog.GroupValue(attrs...)))
}

func (c *conn) addTimeValue(start time.Time, k int) {
	c.metrics.msgCh <- timeMsg{idx: k, d: time.Since(start)}
}

func (c *conn) addSQLTimeValue(start time.Time, k int) {
	c.metrics.msgCh <- sqlTimeMsg{idx: k, d: time.Since(start)}
}

// transaction.

// check if tx implements all required interfaces.
var (
	_ driver.Tx = (*tx)(nil)
)

type tx struct {
	conn   *conn
	closed bool
}

func newTx(conn *conn) *tx {
	conn.metrics.msgCh <- gaugeMsg{idx: gaugeTx, v: 1} // increment number of transactions.
	return &tx{conn: conn}
}

func (t *tx) Commit() error   { return t.close(false) }
func (t *tx) Rollback() error { return t.close(true) }

func (t *tx) close(rollback bool) error {
	c := t.conn

	c.metrics.msgCh <- gaugeMsg{idx: gaugeTx, v: -1} // decrement number of transactions.

	if c.isBad() {
		return driver.ErrBadConn
	}
	if t.closed {
		return nil
	}
	t.closed = true

	c.inTx = false

	if rollback {
		return c.rollback(context.Background())
	}
	return c.commit(context.Background())
}

const defaultSessionID = -1

func (c *conn) dbConnectInfo(ctx context.Context, databaseName string) (*DBConnectInfo, error) {
	ci := &p.DBConnectInfo{}
	ci.SetDatabaseName(databaseName)
	if err := c.pw.Write(ctx, c.sessionID, p.MtDBConnectInfo, false, ci); err != nil {
		return nil, err
	}

	if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		if kind == p.PkDBConnectInfo {
			read(ci)
		}
	}); err != nil {
		return nil, err
	}

	return &DBConnectInfo{
		DatabaseName: databaseName,
		Host:         ci.HostOrZero(),
		Port:         ci.PortOrZero(),
		IsConnected:  ci.IsConnectedOrZero(),
	}, nil
}

func (c *conn) authenticate(ctx context.Context, authHnd *p.AuthHnd, attrs *connAttrs) (int64, *p.ConnectOptions, error) {
	defer c.addTimeValue(time.Now(), timeAuth)

	// client context
	clientContext := &p.ClientContext{}
	clientContext.SetVersion(DriverVersion)
	clientContext.SetType(clientType)
	clientContext.SetApplicationProgram(attrs._applicationName)

	initRequest, err := authHnd.InitRequest()
	if err != nil {
		return 0, nil, err
	}
	if err := c.pw.Write(ctx, c.sessionID, p.MtAuthenticate, false, clientContext, initRequest); err != nil {
		return 0, nil, err
	}

	initReply, err := authHnd.InitReply()
	if err != nil {
		return 0, nil, err
	}
	if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		if kind == p.PkAuthentication {
			read(initReply)
		}
	}); err != nil {
		return 0, nil, err
	}

	finalRequest, err := authHnd.FinalRequest()
	if err != nil {
		return 0, nil, err
	}

	co := &p.ConnectOptions{}
	co.SetDataFormatVersion2(attrs._dfv)
	co.SetClientDistributionMode(p.CdmOff)
	// co.SetClientDistributionMode(p.CdmConnectionStatement)
	// co.SetSelectForUpdateSupported(true) // doesn't seem to make a difference
	/*
		p.CoSplitBatchCommands:          true,
		p.CoCompleteArrayExecution:      true,
	*/

	if attrs._locale != "" {
		co.SetClientLocale(attrs._locale)
	}

	if err := c.pw.Write(ctx, c.sessionID, p.MtConnect, false, finalRequest, p.ClientID(clientID), co); err != nil {
		return 0, nil, err
	}

	finalReply, err := authHnd.FinalReply()
	if err != nil {
		return 0, nil, err
	}

	ti := new(p.TopologyInformation)

	if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		switch kind {
		case p.PkAuthentication:
			read(finalReply)
		case p.PkConnectOptions:
			read(co)
		case p.PkTopologyInformation:
			read(ti)
		}
	}); err != nil {
		return 0, nil, err
	}
	// log.Printf("co: %s", co)
	// log.Printf("ti: %s", ti)
	return c.pr.SessionID(), co, nil
}

func (c *conn) queryDirect(ctx context.Context, query string, commit bool) (driver.Rows, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimeQuery)

	// allow e.g inserts as query -> handle commit like in _execDirect
	if err := c.pw.Write(ctx, c.sessionID, p.MtExecuteDirect, commit, p.Command(query)); err != nil {
		return nil, err
	}

	qr := &queryResult{conn: c}
	meta := &p.ResultMetadata{}
	resSet := &p.Resultset{}

	if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		switch kind {
		case p.PkResultMetadata:
			read(meta)
			qr.fields = meta.ResultFields
		case p.PkResultsetID:
			read((*p.ResultsetID)(&qr.rsID))
		case p.PkResultset:
			resSet.ResultFields = qr.fields
			read(resSet)
			qr.fieldValues = resSet.FieldValues
			qr.decodeErrors = resSet.DecodeErrors
			qr.attrs = attrs
		}
	}); err != nil {
		return nil, err
	}
	if qr.rsID == 0 { // non select query
		return noResult, nil
	}
	return qr, nil
}

func (c *conn) execDirect(ctx context.Context, query string, commit bool) (driver.Result, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimeExec)

	if err := c.pw.Write(ctx, c.sessionID, p.MtExecuteDirect, commit, p.Command(query)); err != nil {
		return nil, err
	}

	rows := &p.RowsAffected{}
	var numRow int64
	if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		if kind == p.PkRowsAffected {
			read(rows)
			numRow = rows.Total()
		}
	}); err != nil {
		return nil, err
	}
	if c.pr.FunctionCode() == p.FcDDL {
		return driver.ResultNoRows, nil
	}
	return driver.RowsAffected(numRow), nil
}

func (c *conn) prepare(ctx context.Context, query string) (*prepareResult, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimePrepare)

	if err := c.pw.Write(ctx, c.sessionID, p.MtPrepare, false, p.Command(query)); err != nil {
		return nil, err
	}

	pr := &prepareResult{}
	resMeta := &p.ResultMetadata{}
	prmMeta := &p.ParameterMetadata{}

	if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		switch kind {
		case p.PkStatementID:
			read((*p.StatementID)(&pr.stmtID))
		case p.PkResultMetadata:
			read(resMeta)
			pr.resultFields = resMeta.ResultFields
		case p.PkParameterMetadata:
			read(prmMeta)
			pr.parameterFields = prmMeta.ParameterFields
		}
	}); err != nil {
		return nil, err
	}
	pr.fc = c.pr.FunctionCode()
	return pr, nil
}

func (c *conn) query(ctx context.Context, pr *prepareResult, nvargs []driver.NamedValue, commit bool) (driver.Rows, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimeQuery)

	// allow e.g inserts as query -> handle commit like in exec

	if err := convertQueryArgs(pr.parameterFields, nvargs, c.attrs._cesu8Encoder(), c.attrs._lobChunkSize); err != nil {
		return nil, err
	}
	inputParameters, err := p.NewInputParameters(pr.parameterFields, nvargs)
	if err != nil {
		return nil, err
	}
	if err := c.pw.Write(ctx, c.sessionID, p.MtExecute, commit, p.StatementID(pr.stmtID), inputParameters); err != nil {
		return nil, err
	}

	qr := &queryResult{conn: c, fields: pr.resultFields}
	resSet := &p.Resultset{}

	if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		switch kind {
		case p.PkResultsetID:
			read((*p.ResultsetID)(&qr.rsID))
		case p.PkResultset:
			resSet.ResultFields = qr.fields
			read(resSet)
			qr.fieldValues = resSet.FieldValues
			qr.decodeErrors = resSet.DecodeErrors
			qr.attrs = attrs
		}
	}); err != nil {
		return nil, err
	}
	if qr.rsID == 0 { // non select query
		return noResult, nil
	}
	return qr, nil
}

func (c *conn) exec(ctx context.Context, pr *prepareResult, nvargs []driver.NamedValue, commit bool, ofs int) (driver.Result, error) {
	inputParameters, err := p.NewInputParameters(pr.parameterFields, nvargs)
	if err != nil {
		return nil, err
	}
	if err := c.pw.Write(ctx, c.sessionID, p.MtExecute, commit, p.StatementID(pr.stmtID), inputParameters); err != nil {
		return nil, err
	}

	rows := &p.RowsAffected{Ofs: ofs}
	var ids []p.LocatorID
	lobReply := &p.WriteLobReply{}
	var rowsAffected int64

	if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		switch kind {
		case p.PkRowsAffected:
			read(rows)
			rowsAffected = rows.Total()
		case p.PkWriteLobReply:
			read(lobReply)
			ids = lobReply.IDs
		}
	}); err != nil {
		return nil, err
	}
	fc := c.pr.FunctionCode()

	if len(ids) != 0 {
		/*
			writeLobParameters:
			- chunkReaders
			- nil (no callResult, exec does not have output parameters)
		*/

		/*
			write lob data only for the last record as lob streaming is only available for the last one
		*/
		startLastRec := len(nvargs) - len(pr.parameterFields)
		if err := c.writeLobs(nil, ids, pr.parameterFields, nvargs[startLastRec:]); err != nil {
			return nil, err
		}
	}

	if fc == p.FcDDL {
		return driver.ResultNoRows, nil
	}
	return driver.RowsAffected(rowsAffected), nil
}

func (c *conn) execCall(ctx context.Context, outputFields []*p.ParameterField) (*callResult, []p.LocatorID, int64, error) {
	cr := &callResult{conn: c, outputFields: outputFields}

	var qr *queryResult
	rows := &p.RowsAffected{}
	var ids []p.LocatorID
	outPrms := &p.OutputParameters{}
	meta := &p.ResultMetadata{}
	resSet := &p.Resultset{}
	lobReply := &p.WriteLobReply{}
	var numRow int64
	tableRowIdx := 0

	if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		switch kind {
		case p.PkRowsAffected:
			read(rows)
			numRow = rows.Total()
		case p.PkOutputParameters:
			outPrms.OutputFields = cr.outputFields
			read(outPrms)
			cr.fieldValues = outPrms.FieldValues
			cr.decodeErrors = outPrms.DecodeErrors
		case p.PkResultMetadata:
			/*
				procedure call with table parameters does return metadata for each table
				sequence: metadata, resultsetID, resultset
				but:
				- resultset might not be provided for all tables
				- so, 'additional' query result is detected by new metadata part
			*/
			qr = &queryResult{conn: c}
			cr.outputFields = append(cr.outputFields, p.NewTableRowsParameterField(tableRowIdx))
			cr.fieldValues = append(cr.fieldValues, qr)
			tableRowIdx++
			read(meta)
			qr.fields = meta.ResultFields
		case p.PkResultset:
			resSet.ResultFields = qr.fields
			read(resSet)
			qr.fieldValues = resSet.FieldValues
			qr.decodeErrors = resSet.DecodeErrors
			qr.attrs = attrs
		case p.PkResultsetID:
			read((*p.ResultsetID)(&qr.rsID))
		case p.PkWriteLobReply:
			read(lobReply)
			ids = lobReply.IDs
		}
	}); err != nil {
		return nil, nil, 0, err
	}
	return cr, ids, numRow, nil
}

func (c *conn) fetchNext(ctx context.Context, qr *queryResult) error {
	defer c.addSQLTimeValue(time.Now(), sqlTimeFetch)

	if err := c.pw.Write(ctx, c.sessionID, p.MtFetchNext, false, p.ResultsetID(qr.rsID), p.Fetchsize(c.attrs._fetchSize)); err != nil {
		return err
	}

	resSet := &p.Resultset{ResultFields: qr.fields, FieldValues: qr.fieldValues} // reuse field values

	return c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
		if kind == p.PkResultset {
			read(resSet)
			qr.fieldValues = resSet.FieldValues
			qr.decodeErrors = resSet.DecodeErrors
			qr.attrs = attrs
		}
	})
}

func (c *conn) dropStatementID(ctx context.Context, id uint64) error {
	if err := c.pw.Write(ctx, c.sessionID, p.MtDropStatementID, false, p.StatementID(id)); err != nil {
		return err
	}
	return c.pr.SkipParts(ctx)
}

func (c *conn) closeResultsetID(ctx context.Context, id uint64) error {
	if err := c.pw.Write(ctx, c.sessionID, p.MtCloseResultset, false, p.ResultsetID(id)); err != nil {
		return err
	}
	return c.pr.SkipParts(ctx)
}

func (c *conn) commit(ctx context.Context) error {
	defer c.addSQLTimeValue(time.Now(), sqlTimeCommit)

	if err := c.pw.Write(ctx, c.sessionID, p.MtCommit, false); err != nil {
		return err
	}
	if err := c.pr.SkipParts(ctx); err != nil {
		return err
	}
	return nil
}

func (c *conn) rollback(ctx context.Context) error {
	defer c.addSQLTimeValue(time.Now(), sqlTimeRollback)

	if err := c.pw.Write(ctx, c.sessionID, p.MtRollback, false); err != nil {
		return err
	}
	if err := c.pr.SkipParts(ctx); err != nil {
		return err
	}
	return nil
}

func (c *conn) disconnect(ctx context.Context) error {
	if err := c.pw.Write(ctx, c.sessionID, p.MtDisconnect, false); err != nil {
		return err
	}
	/*
		Do not read server reply as on slow connections the TCP/IP connection is closed (by Server)
		before the reply can be read completely.

		// if err := s.pr.readSkip(); err != nil {
		// 	return err
		// }

	*/
	return nil
}

/*
readLob reads output lob or result lob parameters from db.

read lob reply
  - seems like readLobreply returns only a result for one lob - even if more then one is requested
    --> read single lobs
*/
func (c *conn) readLob(lobRequest *p.ReadLobRequest, lobReply *p.ReadLobReply) error {
	defer c.addSQLTimeValue(time.Now(), sqlTimeFetchLob)

	lobRequest.SetChunkSize(c.attrs._lobChunkSize)
	ctx := context.Background()
	for {
		if err := c.pw.Write(ctx, c.sessionID, p.MtWriteLob, false, lobRequest); err != nil {
			return err
		}

		if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
			if kind == p.PkReadLobReply {
				read(lobReply)
			}
		}); err != nil {
			return err
		}

		numChar, err := lobReply.Write()
		if err != nil {
			return err
		}

		if lobReply.IsLastData() {
			break
		}
		lobRequest.AddOfs(numChar)
	}
	return nil
}

func assertEqual[T comparable](s string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s: %v %v", s, a, b))
	}
}

// writeLobs writes input lob parameters to db.
func (c *conn) writeLobs(cr *callResult, ids []p.LocatorID, inPrmFields []*p.ParameterField, nvargs []driver.NamedValue) error {
	assertEqual("lob streaming can only be done for one (the last) record", len(inPrmFields), len(nvargs))

	descrs := make([]*p.WriteLobDescr, 0, len(ids))
	j := 0
	for i, f := range inPrmFields {
		if f.IsLob() {
			lobInDescr, ok := nvargs[i].Value.(*p.LobInDescr)
			if !ok {
				return fmt.Errorf("protocol error: invalid lob parameter %[1]T %[1]v - *lobInDescr expected", nvargs[i])
			}
			if j > len(ids) {
				return fmt.Errorf("protocol error: invalid number of lob parameter ids %d", len(ids))
			}
			if !lobInDescr.IsLastData() {
				descrs = append(descrs, &p.WriteLobDescr{LobInDescr: lobInDescr, ID: ids[j]})
				j++
			}
		}
	}

	writeLobRequest := &p.WriteLobRequest{}

	ctx := context.Background()

	for len(descrs) != 0 {

		if len(descrs) != len(ids) {
			return fmt.Errorf("protocol error: invalid number of lob parameter ids %d - expected %d", len(descrs), len(ids))
		}
		for i, descr := range descrs { // check if ids and descrs are in sync
			if descr.ID != ids[i] {
				return fmt.Errorf("protocol error: lob parameter id mismatch %d - expected %d", descr.ID, ids[i])
			}
		}

		// TODO check total size limit
		for _, descr := range descrs {
			if err := descr.FetchNext(c.attrs._lobChunkSize); err != nil {
				return err
			}
		}

		writeLobRequest.Descrs = descrs

		if err := c.pw.Write(ctx, c.sessionID, p.MtReadLob, false, writeLobRequest); err != nil {
			return err
		}

		lobReply := &p.WriteLobReply{}
		outPrms := &p.OutputParameters{}

		if err := c.pr.IterateParts(ctx, func(kind p.PartKind, attrs p.PartAttributes, read func(part p.Part)) {
			switch kind {
			case p.PkOutputParameters:
				outPrms.OutputFields = cr.outputFields
				read(outPrms)
				cr.fieldValues = outPrms.FieldValues
				cr.decodeErrors = outPrms.DecodeErrors
			case p.PkWriteLobReply:
				read(lobReply)
				ids = lobReply.IDs
			}
		}); err != nil {
			return err
		}

		// remove done descr
		j := 0
		for _, descr := range descrs {
			if !descr.IsLastData() {
				descrs[j] = descr
				j++
			}
		}
		descrs = descrs[:j]
	}
	return nil
}
