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
	"io"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SAP/go-hdb/driver/dial"
	"github.com/SAP/go-hdb/driver/internal/exp/slog"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/internal/protocol/auth"
	"github.com/SAP/go-hdb/driver/internal/protocol/levenshtein"
	"github.com/SAP/go-hdb/driver/internal/protocol/x509"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
	"golang.org/x/text/transform"
)

// Transaction isolation levels supported by hdb.
const (
	LevelReadCommitted  = "READ COMMITTED"
	LevelRepeatableRead = "REPEATABLE READ"
	LevelSerializable   = "SERIALIZABLE"
)

// Access modes supported by hdb.
const (
	modeReadOnly  = "READ ONLY"
	modeReadWrite = "READ WRITE"
)

// map sql isolation level to hdb isolation level.
var isolationLevel = map[driver.IsolationLevel]string{
	driver.IsolationLevel(sql.LevelDefault):        LevelReadCommitted,
	driver.IsolationLevel(sql.LevelReadCommitted):  LevelReadCommitted,
	driver.IsolationLevel(sql.LevelRepeatableRead): LevelRepeatableRead,
	driver.IsolationLevel(sql.LevelSerializable):   LevelSerializable,
}

// map sql read only flag to hdb access mode.
var readOnly = map[bool]string{
	true:  modeReadOnly,
	false: modeReadWrite,
}

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

// queries
const (
	dummyQuery        = "select 1 from dummy"
	setIsolationLevel = "set transaction isolation level"
	setAccessMode     = "set transaction"
	setDefaultSchema  = "set schema"
)

var (
	// register as var to execute even before init() funcs are called
	_ = p.RegisterScanType(p.DtDecimal, reflect.TypeOf((*Decimal)(nil)).Elem(), reflect.TypeOf((*NullDecimal)(nil)).Elem())
	_ = p.RegisterScanType(p.DtLob, reflect.TypeOf((*Lob)(nil)).Elem(), reflect.TypeOf((*NullLob)(nil)).Elem())
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
	//set timeout
	if err := c.conn.SetReadDeadline(c.deadline()); err != nil {
		return 0, fmt.Errorf("%w: %s", driver.ErrBadConn, err)
	}
	c.lastRead = time.Now()
	n, err := c.conn.Read(b)
	c.metrics.chMsg <- timeMsg{idx: timeRead, d: time.Since(c.lastRead)}
	c.metrics.chMsg <- counterMsg{idx: counterBytesRead, v: uint64(n)}
	if err != nil {
		c.logger.LogAttrs(context.Background(), slog.LevelError, "DB conn read error", slog.String("error", err.Error()), slog.String("local address", c.conn.LocalAddr().String()), slog.String("remote address", c.conn.RemoteAddr().String()))
		// wrap error in driver.ErrBadConn
		return n, fmt.Errorf("%w: %s", driver.ErrBadConn, err)
	}
	return n, nil
}

// Write implements the io.Writer interface.
func (c *dbConn) Write(b []byte) (int, error) {
	//set timeout
	if err := c.conn.SetWriteDeadline(c.deadline()); err != nil {
		return 0, fmt.Errorf("%w: %s", driver.ErrBadConn, err)
	}
	c.lastWrite = time.Now()
	n, err := c.conn.Write(b)
	c.metrics.chMsg <- timeMsg{idx: timeWrite, d: time.Since(c.lastWrite)}
	c.metrics.chMsg <- counterMsg{idx: counterBytesWritten, v: uint64(n)}
	if err != nil {
		c.logger.LogAttrs(context.Background(), slog.LevelError, "DB conn write error", slog.String("error", err.Error()), slog.String("local address", c.conn.LocalAddr().String()), slog.String("remote address", c.conn.RemoteAddr().String()))
		// wrap error in driver.ErrBadConn
		return n, fmt.Errorf("%w: %s", driver.ErrBadConn, err)
	}
	return n, nil
}

// check if conn implements all required interfaces
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

// connection hook operations
const (
	choNone = iota
	choStmtExec
)

var errCancelled = fmt.Errorf("%w: %s", driver.ErrBadConn, errors.New("db call cancelled"))

// Conn enhances a connection with go-hdb specific connection functions.
type Conn interface {
	HDBVersion() *Version
	DatabaseName() string
	DBConnectInfo(ctx context.Context, databaseName string) (*DBConnectInfo, error)
}

const traceMsg = "SQL"

// Conn is the implementation of the database/sql/driver Conn interface.
type conn struct {
	*connAttrs
	metrics *metrics

	sqlTrace bool
	logger   *slog.Logger

	dbConn *dbConn

	inTx      bool  // in transaction
	lastError error // last error
	sessionID int64

	serverOptions p.Options[p.ConnectOption]
	hdbVersion    *Version
	fieldTypeCtx  *p.FieldTypeCtx

	pr p.Reader
	pw p.Writer
}

// isAuthError returns true in case of X509 certificate validation errrors or hdb authentication errors, else otherwise.
func isAuthError(err error) bool {
	var validationError *x509.ValidationError
	if errors.As(err, &validationError) {
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

	refreshed := false
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
		if refreshed {
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

		refreshed = true
	}
}

var (
	protTrace atomicBoolFlag
	sqlTrace  atomicBoolFlag
)

func init() {
	flag.Var(&protTrace, "hdb.protTrace", "enabling hdb protocol trace")
	flag.Var(&sqlTrace, "hdb.sqlTrace", "enabling hdb sql trace")
}

// SQLTrace returns true if sql tracing output is active, false otherwise.
func SQLTrace() bool { return sqlTrace.Load() }

// SetSQLTrace sets sql tracing output active or inactive.
func SetSQLTrace(on bool) { sqlTrace.Store(on) }

type namedValues []driver.NamedValue

// LogValue implements the slog.LogValuer interface.
func (nvs namedValues) LogValue() slog.Value {
	attrs := make([]slog.Attr, len(nvs))
	for i, nv := range nvs {
		if nv.Name != "" {
			attrs[i] = slog.String(nv.Name, fmt.Sprintf("%v", nv.Value))
		} else {
			attrs[i] = slog.String(fmt.Sprintf("%d", nv.Ordinal), fmt.Sprintf("%v", nv.Value))
		}
	}
	return slog.GroupValue(attrs...)
}

// unique connection number.
var connNo atomic.Uint64

func newConn(ctx context.Context, host string, metrics *metrics, attrs *connAttrs) (*conn, error) {
	netConn, err := attrs._dialer.DialContext(ctx, host, dial.DialerOptions{Timeout: attrs._timeout, TCPKeepAlive: attrs._tcpKeepAlive})
	if err != nil {
		return nil, err
	}

	// is TLS connection requested?
	if attrs._tlsConfig != nil {
		netConn = tls.Client(netConn, attrs._tlsConfig)
	}

	logger := attrs._logger.With(slog.Uint64("conn", connNo.Add(1)))

	dbConn := &dbConn{metrics: metrics, conn: netConn, timeout: attrs._timeout, logger: logger}
	// buffer connection
	rw := bufio.NewReadWriter(bufio.NewReaderSize(dbConn, attrs._bufferSize), bufio.NewWriterSize(dbConn, attrs._bufferSize))

	protTrace := protTrace.Load()

	c := &conn{
		metrics:   metrics,
		connAttrs: attrs,
		dbConn:    dbConn,
		sqlTrace:  sqlTrace.Load(),
		logger:    logger,
		pw:        p.NewWriter(rw.Writer, protTrace, logger, attrs._cesu8Encoder, attrs._sessionVariables), // write upstream
		pr:        p.NewDBReader(rw.Reader, protTrace, logger, attrs._cesu8Decoder),                        // read downstream
		sessionID: defaultSessionID,
	}

	if err := c.pw.WriteProlog(ctx); err != nil {
		dbConn.close()
		return nil, err
	}

	if err := c.pr.ReadProlog(ctx); err != nil {
		dbConn.close()
		return nil, err
	}

	c.metrics.chMsg <- gaugeMsg{idx: gaugeConn, v: 1} // increment open connections.
	return c, nil
}

func fetchRedirectHost(ctx context.Context, host, databaseName string, metrics *metrics, attrs *connAttrs) (string, error) {
	c, err := newConn(ctx, host, metrics, attrs)
	if err != nil {
		return "", err
	}
	defer c.Close()
	dbi, err := c._dbConnectInfo(ctx, databaseName)
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
	if c.sessionID, c.serverOptions, err = c._authenticate(ctx, authHnd, attrs); err != nil {
		return err
	}
	if c.sessionID <= 0 {
		return fmt.Errorf("invalid session id %d", c.sessionID)
	}

	c.hdbVersion = parseVersion(c.versionString())
	c.fieldTypeCtx = p.NewFieldTypeCtx(int(c.serverOptions[p.CoDataFormatVersion2].(int32)), attrs._emptyDateAsNull)

	if attrs._defaultSchema != "" {
		if _, err := c.ExecContext(ctx, strings.Join([]string{setDefaultSchema, Identifier(attrs._defaultSchema).String()}, " "), nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) versionString() (version string) {
	v, ok := c.serverOptions[p.CoFullVersionString]
	if !ok {
		return
	}
	if s, ok := v.(string); ok {
		return s
	}
	return
}

// ResetSession implements the driver.SessionResetter interface.
func (c *conn) ResetSession(ctx context.Context) error {
	c.lastError = nil

	if c.connAttrs._pingInterval == 0 || c.dbConn.lastRead.IsZero() || time.Since(c.dbConn.lastRead) < c.connAttrs._pingInterval {
		return nil
	}

	if _, err := c._queryDirect(ctx, dummyQuery, !c.inTx); err != nil {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) isBad() bool { return errors.Is(c.lastError, driver.ErrBadConn) }

// IsValid implements the driver.Validator interface.
func (c *conn) IsValid() bool { return !c.isBad() }

// Ping implements the driver.Pinger interface.
func (c *conn) Ping(ctx context.Context) error {
	if c.isBad() {
		return driver.ErrBadConn
	}
	if c.sqlTrace {
		defer func(start time.Time) {
			c.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String("query", dummyQuery), slog.Int64("ms", time.Since(start).Milliseconds()))
		}(time.Now())
	}

	done := make(chan struct{})
	var err error
	go func() {
		_, err = c._queryDirect(ctx, dummyQuery, !c.inTx)
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
	if c.isBad() {
		return nil, driver.ErrBadConn
	}
	if c.sqlTrace {
		defer func(start time.Time) {
			c.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String("query", query), slog.Int64("ms", time.Since(start).Milliseconds()))
		}(time.Now())
	}

	done := make(chan struct{})
	var stmt driver.Stmt
	var err error
	go func() {
		var pr *prepareResult

		if pr, err = c._prepare(ctx, query); err == nil {
			stmt = newStmt(c, query, pr)
		}

		close(done)
	}()

	select {
	case <-ctx.Done():
		c.lastError = errCancelled
		return nil, ctx.Err()
	case <-done:
		c.metrics.chMsg <- gaugeMsg{idx: gaugeStmt, v: 1} // increment number of statements.
		c.lastError = err
		return stmt, err
	}
}

// Close implements the driver.Conn interface.
func (c *conn) Close() error {
	c.metrics.chMsg <- gaugeMsg{idx: gaugeConn, v: -1} // decrement open connections.
	// do not disconnect if isBad or invalid sessionID
	if !c.isBad() && c.sessionID != defaultSessionID {
		c._disconnect(context.Background()) // ignore error
	}
	return c.dbConn.close()
}

// BeginTx implements the driver.ConnBeginTx interface.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.isBad() {
		return nil, driver.ErrBadConn
	}
	if c.inTx {
		return nil, ErrNestedTransaction
	}

	level, ok := isolationLevel[opts.Isolation]
	if !ok {
		return nil, ErrUnsupportedIsolationLevel
	}

	done := make(chan struct{})
	var tx driver.Tx
	var err error
	go func() {
		// set isolation level
		query := strings.Join([]string{setIsolationLevel, level}, " ")
		if _, err = c._execDirect(ctx, query, !c.inTx); err != nil {
			goto done
		}
		// set access mode
		query = strings.Join([]string{setAccessMode, readOnly[opts.ReadOnly]}, " ")
		if _, err = c._execDirect(ctx, query, !c.inTx); err != nil {
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
		c.metrics.chMsg <- gaugeMsg{idx: gaugeTx, v: 1} // increment number of transactions.
		c.lastError = err
		return tx, err
	}
}

var callStmt = regexp.MustCompile(`(?i)^\s*call\s+.*`) // sql statement beginning with call

// QueryContext implements the driver.QueryerContext interface.
func (c *conn) QueryContext(ctx context.Context, query string, nvargs []driver.NamedValue) (driver.Rows, error) {
	if c.isBad() {
		return nil, driver.ErrBadConn
	}
	if callStmt.MatchString(query) {
		return nil, fmt.Errorf("invalid procedure call %s - please use Exec instead", query)
	}
	if len(nvargs) != 0 {
		return nil, driver.ErrSkip //fast path not possible (prepare needed)
	}
	if c.sqlTrace {
		defer func(start time.Time) {
			c.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String("query", query), slog.Int64("ms", time.Since(start).Milliseconds()), slog.Any("arg", namedValues(nvargs)))
		}(time.Now())
	}

	done := make(chan struct{})
	var rows driver.Rows
	var err error
	go func() {
		rows, err = c._queryDirect(ctx, query, !c.inTx)
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
	if c.isBad() {
		return nil, driver.ErrBadConn
	}
	if len(nvargs) != 0 {
		return nil, driver.ErrSkip //fast path not possible (prepare needed)
	}
	if c.sqlTrace {
		defer func(start time.Time) {
			c.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String("query", query), slog.Int64("ms", time.Since(start).Milliseconds()), slog.Any("arg", namedValues(nvargs)))
		}(time.Now())
	}

	done := make(chan struct{})
	var result driver.Result
	var err error
	go func() {
		// handle procesure call without parameters here as well
		result, err = c._execDirect(ctx, query, !c.inTx)
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
func (c *conn) DatabaseName() string { return c._databaseName() }

// DBConnectInfo implements the Conn interface.
func (c *conn) DBConnectInfo(ctx context.Context, databaseName string) (*DBConnectInfo, error) {
	if c.isBad() {
		return nil, driver.ErrBadConn
	}

	done := make(chan struct{})
	var ci *DBConnectInfo
	var err error
	go func() {
		ci, err = c._dbConnectInfo(ctx, databaseName)
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

func (c *conn) addTimeValue(start time.Time, k int) {
	c.metrics.chMsg <- timeMsg{idx: k, d: time.Since(start)}
}

func (c *conn) addSQLTimeValue(start time.Time, k int) {
	c.metrics.chMsg <- sqlTimeMsg{idx: k, d: time.Since(start)}
}

//transaction

// check if tx implements all required interfaces
var (
	_ driver.Tx = (*tx)(nil)
)

type tx struct {
	conn   *conn
	closed bool
}

func newTx(conn *conn) *tx { return &tx{conn: conn} }

func (t *tx) Commit() error   { return t.close(false) }
func (t *tx) Rollback() error { return t.close(true) }

func (t *tx) close(rollback bool) (err error) {
	c := t.conn

	c.metrics.chMsg <- gaugeMsg{idx: gaugeTx, v: -1} // decrement number of transactions.

	if c.isBad() {
		return driver.ErrBadConn
	}
	if t.closed {
		return nil
	}
	t.closed = true

	c.inTx = false

	if rollback {
		err = c._rollback(context.Background())
	} else {
		err = c._commit(context.Background())
	}
	return
}

// check if statements implements all required interfaces
var (
	_ driver.Stmt              = (*stmt)(nil)
	_ driver.StmtExecContext   = (*stmt)(nil)
	_ driver.StmtQueryContext  = (*stmt)(nil)
	_ driver.NamedValueChecker = (*stmt)(nil)
)

type stmt struct {
	conn  *conn
	query string
	pr    *prepareResult
	// rows: stored procedures with table output parameters
	rows *sql.Rows
}

func newStmt(conn *conn, query string, pr *prepareResult) *stmt {
	return &stmt{conn: conn, query: query, pr: pr}
}

/*
NumInput differs dependent on statement (check is done in QueryContext and ExecContext):
- #args == #param (only in params):    query, exec, exec bulk (non control query)
- #args == #param (in and out params): exec call
- #args == 0:                          exec bulk (control query)
- #args == #input param:               query call
*/
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Close() error {
	c := s.conn

	s.conn.metrics.chMsg <- gaugeMsg{idx: gaugeStmt, v: -1} // decrement number of statements.

	if c.isBad() {
		return driver.ErrBadConn
	}
	if s.rows != nil {
		s.rows.Close()
	}
	return c._dropStatementID(context.Background(), s.pr.stmtID)
}

func (s *stmt) QueryContext(ctx context.Context, nvargs []driver.NamedValue) (driver.Rows, error) {
	c := s.conn
	if c.isBad() {
		return nil, driver.ErrBadConn
	}
	if c.sqlTrace {
		defer func(start time.Time) {
			c.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String("query", s.query), slog.Int64("ms", time.Since(start).Milliseconds()), slog.Any("arg", namedValues(nvargs)))
		}(time.Now())
	}
	if s.pr.isProcedureCall() {
		return nil, fmt.Errorf("invalid procedure call %s - please use Exec instead", s.query)
	}

	done := make(chan struct{})
	var rows driver.Rows
	var err error
	go func() {
		rows, err = s.conn._query(ctx, s.pr, nvargs, !s.conn.inTx)
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

func (s *stmt) ExecContext(ctx context.Context, nvargs []driver.NamedValue) (driver.Result, error) {
	c := s.conn

	if c.isBad() {
		return nil, driver.ErrBadConn
	}
	if connHook != nil {
		connHook(c, choStmtExec)
	}
	if c.sqlTrace {
		defer func(start time.Time) {
			c.logger.LogAttrs(ctx, slog.LevelInfo, traceMsg, slog.String("query", s.query), slog.Int64("ms", time.Since(start).Milliseconds()), slog.Any("arg", namedValues(nvargs)))
		}(time.Now())
	}

	done := make(chan struct{})
	var result driver.Result
	var err error
	go func() {
		if s.pr.isProcedureCall() {
			result, s.rows, err = s.conn._execCall(ctx, s.pr, nvargs)
		} else {
			result, err = s.exec(ctx, nvargs)
		}
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

type totalRowsAffected int64

func (t *totalRowsAffected) add(r driver.Result) {
	if r == nil {
		return
	}
	rows, err := r.RowsAffected()
	if err != nil {
		return
	}
	*t += totalRowsAffected(rows)
}

func (s *stmt) exec(ctx context.Context, nvargs []driver.NamedValue) (driver.Result, error) {
	c := s.conn

	numNVArg, numField := len(nvargs), s.pr.numField()

	if numNVArg == 0 {
		if numField != 0 {
			return nil, fmt.Errorf("invalid number of arguments %d - expected %d", numNVArg, numField)
		}
		return c._execBatch(ctx, s.pr, nvargs, !c.inTx, 0)
	}
	if numNVArg == 1 {
		if _, ok := nvargs[0].Value.(func(args []any) error); ok {
			return s.execFct(ctx, nvargs)
		}
	}
	if numNVArg == numField {
		return c._exec(ctx, s.pr, nvargs, !c.inTx, 0)
	}
	if numNVArg%numField != 0 {
		return nil, fmt.Errorf("invalid number of arguments %d - multiple of %d expected", numNVArg, numField)
	}
	return s.execMany(ctx, nvargs)
}

/*
Non 'atomic' (transactional) operation due to the split in packages (bulkSize),
execMany data might only be written partially to the database in case of hdb stmt errors.
*/
func (s *stmt) execMany(ctx context.Context, nvargs []driver.NamedValue) (driver.Result, error) {
	c := s.conn

	totalRowsAffected := totalRowsAffected(0)
	numField := s.pr.numField()
	numNVArg := len(nvargs)
	numRec := numNVArg / numField
	numBatch := numRec / c._bulkSize
	if numRec%c._bulkSize != 0 {
		numBatch++
	}

	for i := 0; i < numBatch; i++ {
		from := i * numField * c._bulkSize
		to := (i + 1) * numField * c._bulkSize
		if to > numNVArg {
			to = numNVArg
		}
		r, err := c._exec(ctx, s.pr, nvargs[from:to], !c.inTx, i*c._bulkSize)
		totalRowsAffected.add(r)
		if err != nil {
			return driver.RowsAffected(totalRowsAffected), err
		}
	}
	return driver.RowsAffected(totalRowsAffected), nil
}

// ErrEndOfRows is the error to be returned using a function based bulk exec to indicate
// the end of rows.
var ErrEndOfRows = errors.New("end of rows")

/*
Non 'atomic' (transactional) operation due to the split in packages (bulkSize),
execMany data might only be written partially to the database in case of hdb stmt errors.
*/
func (s *stmt) execFct(ctx context.Context, nvargs []driver.NamedValue) (driver.Result, error) {
	c := s.conn

	totalRowsAffected := totalRowsAffected(0)
	args := make([]driver.NamedValue, s.pr.numField())
	scanArgs := make([]any, s.pr.numField())

	fct, ok := nvargs[0].Value.(func(args []any) error)
	if !ok {
		panic("should never happen")
	}

	done := false
	batch := 0
	for !done {
		args = args[:0]
		k := 0
		for i := 0; i < c._bulkSize; i++ {
			err := fct(scanArgs)
			if err == ErrEndOfRows {
				done = true
				break
			}
			if err != nil {
				return driver.RowsAffected(totalRowsAffected), err
			}

			for j, scanArg := range scanArgs {
				size := k + 1
				if size > cap(args) {
					args = append(args, make([]driver.NamedValue, size-len(args))...)
				}
				args = args[:size]
				args[k].Ordinal = j + 1
				if t, ok := scanArg.(sql.NamedArg); ok {
					args[k].Name = t.Name
					args[k].Value = t.Value
				} else {
					args[k].Name = ""
					args[k].Value = scanArg
				}
				k++
			}
		}

		if len(args) != 0 {
			r, err := c._exec(ctx, s.pr, args, !c.inTx, batch*c._bulkSize)
			totalRowsAffected.add(r)
			if err != nil {
				return driver.RowsAffected(totalRowsAffected), err
			}
		}
		batch++
	}
	return driver.RowsAffected(totalRowsAffected), nil
}

// CheckNamedValue implements NamedValueChecker interface.
func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error {
	// check add arguments only
	// conversion is happening as part of the exec, query call
	return nil
}

const defaultSessionID = -1

func (c *conn) _convert(field *p.ParameterField, arg any) (any, error) {
	// let fields with own Value converter convert themselves first (e.g. NullInt64, ...)
	var err error
	if valuer, ok := arg.(driver.Valuer); ok {
		if arg, err = valuer.Value(); err != nil {
			return nil, err
		}
	}
	// convert field
	return field.Convert(c._cesu8Encoder(), arg)
}

// TODO: test
func _reorderNVArgs(pos int, name string, nvargs []driver.NamedValue) {
	for i := pos; i < len(nvargs); i++ {
		if nvargs[i].Name != "" && nvargs[i].Name == name {
			tmp := nvargs[i]
			for j := i; j > pos; j-- {
				nvargs[j] = nvargs[j-1]
			}
			nvargs[pos] = tmp
		}
	}
}

// _mapExecArgs
// - all fields need to be input fields
// - out parameters are not supported
// - named parameters are not supported
func (c *conn) _mapExecArgs(fields []*p.ParameterField, nvargs []driver.NamedValue) ([]int, error) {
	numField := len(fields)
	if (len(nvargs) % numField) != 0 {
		return nil, fmt.Errorf("invalid number of arguments %d - multiple of %d expected", len(nvargs), numField)
	}
	numRow := len(nvargs) / numField
	addLobDataRecs := []int{}

	for i := 0; i < numRow; i++ {
		hasAddLobData := false
		for j, field := range fields {
			nvarg := &nvargs[(i*numField)+j]

			if field.Out() {
				return nil, fmt.Errorf("invalid parameter %s - output not allowed", field)
			}
			if _, ok := nvarg.Value.(sql.Out); ok {
				return nil, fmt.Errorf("invalid argument %v - output not allowed", nvarg)
			}
			if nvarg.Name != "" {
				return nil, fmt.Errorf("invalid argument %s - named parameters not supported", nvarg.Name)
			}
			var err error
			if nvarg.Value, err = c._convert(field, nvarg.Value); err != nil {
				return nil, fmt.Errorf("field %s conversion error - %w", field, err)
			}
			// fetch first lob chunk
			if lobInDescr, ok := nvarg.Value.(*p.LobInDescr); ok {
				if err := lobInDescr.FetchNext(c._lobChunkSize); err != nil {
					return nil, err
				}
				if !lobInDescr.Opt.IsLastData() {
					hasAddLobData = true
				}
			}
		}
		if hasAddLobData || i == numRow-1 {
			addLobDataRecs = append(addLobDataRecs, i)
		}
	}
	return addLobDataRecs, nil
}

// _mapQueryArgs
// - all fields need to be input fields
// - out parameters are not supported
// - named parameters are not supported
func (c *conn) _mapQueryArgs(fields []*p.ParameterField, nvargs []driver.NamedValue) error {
	if len(nvargs) != len(fields) {
		return fmt.Errorf("invalid number of arguments %d - %d expected", len(nvargs), len(fields))
	}

	for i, field := range fields {
		nvarg := &nvargs[i]
		if field.Out() {
			return fmt.Errorf("invalid parameter %s - output not allowed", field)
		}
		if _, ok := nvarg.Value.(sql.Out); ok {
			return fmt.Errorf("invalid argument %v - output not allowed", nvarg)
		}
		if nvarg.Name != "" {
			return fmt.Errorf("invalid argument %s - named parameters not supported", nvarg.Name)
		}
		var err error
		if nvarg.Value, err = c._convert(field, nvarg.Value); err != nil {
			return fmt.Errorf("field %s conversion error - %w", field, err)
		}
		// fetch first lob chunk
		if lobInDescr, ok := nvarg.Value.(*p.LobInDescr); ok {
			if err := lobInDescr.FetchNext(c._lobChunkSize); err != nil {
				return err
			}
		}
	}
	return nil
}

type names []string // lazy init via get

func (n *names) get(fields []*p.ParameterField) []string {
	if *n != nil {
		return *n
	}
	*n = make([]string, 0, len(fields))
	for _, field := range fields {
		*n = append(*n, field.Name())
	}
	return *n
}

// _mapQueryCallArgs
// - fields could be input or output fields
// - number of args needs to be equal to number of fields
// - named parameters are supported

type _callArgs struct {
	inFields, outFields []*p.ParameterField
	inArgs, outArgs     []driver.NamedValue
}

func _newCallArgs() *_callArgs {
	return &_callArgs{
		inFields:  []*p.ParameterField{},
		outFields: []*p.ParameterField{},
		inArgs:    []driver.NamedValue{},
		outArgs:   []driver.NamedValue{},
	}
}

func (c *conn) _mapCallArgs(fields []*p.ParameterField, nvargs []driver.NamedValue) (*_callArgs, error) {

	callArgs := _newCallArgs()
	var names names

	if len(nvargs) < len(fields) { // number of fields needs to match number of args or be greater (add table output args)
		return nil, fmt.Errorf("invalid number of arguments %d - %d expected", len(nvargs), len(fields))
	}

	prmnvargs := nvargs[:len(fields)]

	for i, field := range fields {
		_reorderNVArgs(i, field.Name(), prmnvargs)

		nvarg := &prmnvargs[i]

		if nvarg.Name != "" && nvarg.Name != field.Name() {
			likeName := levenshtein.MinDistance(false, names.get(fields), nvarg.Name)
			return nil, fmt.Errorf("invalid argument name %s - did you mean %s?", nvarg.Name, likeName)
		}

		out, isOut := nvarg.Value.(sql.Out)

		var err error
		if field.In() {
			if isOut {
				if !out.In {
					return nil, fmt.Errorf("argument field %s mismatch - use in argument with out field", field)
				}
				if out.Dest, err = c._convert(field, out.Dest); err != nil {
					return nil, fmt.Errorf("field %s conversion error - %w", field, err)
				}
			} else {
				if nvarg.Value, err = c._convert(field, nvarg.Value); err != nil {
					return nil, fmt.Errorf("field %s conversion error - %w", field, err)
				}
			}
			// fetch first lob chunk
			if lobInDescr, ok := nvarg.Value.(*p.LobInDescr); ok {
				if err := lobInDescr.FetchNext(c._lobChunkSize); err != nil {
					return nil, err
				}
			}
			callArgs.inArgs = append(callArgs.inArgs, *nvarg)
			callArgs.inFields = append(callArgs.inFields, field)
		}

		if field.Out() {
			if !isOut {
				return nil, fmt.Errorf("argument field %s mismatch - use out argument with non-out field", field)
			}
			if _, ok := out.Dest.(*sql.Rows); ok {
				return nil, fmt.Errorf("invalid output parameter type %T", out.Dest)
			}
			callArgs.outArgs = append(callArgs.outArgs, *nvarg)
			callArgs.outFields = append(callArgs.outFields, field)
		}
	}

	// table output args
	for i := len(fields); i < len(nvargs); i++ {
		nvarg := &nvargs[i]
		out, ok := nvarg.Value.(sql.Out)
		if !ok {
			return nil, fmt.Errorf("invalid parameter type %T at %d - output parameter expected", nvarg.Value, i)
		}
		if _, ok := out.Dest.(*sql.Rows); !ok {
			return nil, fmt.Errorf("invalid output parameter %T at %d - sql.Rows expected", out.Dest, i)
		}
		callArgs.outArgs = append(callArgs.outArgs, *nvarg)
	}
	return callArgs, nil
}

func (c *conn) _checkError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if hdbErrors, ok := err.(*p.HdbErrors); ok && hdbErrors.HasOnlyWarnings() {
		hdbErrors.ErrorsFunc(func(err error) {
			c.logger.LogAttrs(ctx, slog.LevelWarn, err.Error())
		})
		return nil
	}
	return err
}

func (c *conn) _databaseName() string {
	return c.serverOptions[p.CoDatabaseName].(string)
}

func (c *conn) _dbConnectInfo(ctx context.Context, databaseName string) (*DBConnectInfo, error) {
	ci := p.Options[p.DBConnectInfoType]{p.CiDatabaseName: databaseName}
	if err := c.pw.Write(ctx, c.sessionID, p.MtDBConnectInfo, false, ci); err != nil {
		return nil, err
	}

	if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		switch ph.PartKind {
		case p.PkDBConnectInfo:
			c.pr.Read(ctx, &ci)
		}
	})); err != nil {
		return nil, err
	}

	host, _ := ci[p.CiHost].(string) // check existence and convert to string
	port, _ := ci[p.CiPort].(int32)  // check existence and convert to integer
	isConnected, _ := ci[p.CiIsConnected].(bool)

	return &DBConnectInfo{
		DatabaseName: databaseName,
		Host:         host,
		Port:         int(port),
		IsConnected:  isConnected,
	}, nil
}

func (c *conn) _authenticate(ctx context.Context, authHnd *p.AuthHnd, attrs *connAttrs) (int64, p.Options[p.ConnectOption], error) {
	defer c.addTimeValue(time.Now(), timeAuth)

	// client context
	clientContext := p.Options[p.ClientContextOption]{
		p.CcoClientVersion:            DriverVersion,
		p.CcoClientType:               clientType,
		p.CcoClientApplicationProgram: attrs._applicationName,
	}

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
	if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		if ph.PartKind == p.PkAuthentication {
			c.pr.Read(ctx, initReply)
		}
	})); err != nil {
		return 0, nil, err
	}

	finalRequest, err := authHnd.FinalRequest()
	if err != nil {
		return 0, nil, err
	}

	co := func() p.Options[p.ConnectOption] {
		co := p.Options[p.ConnectOption]{
			p.CoDistributionProtocolVersion: false,
			p.CoSelectForUpdateSupported:    false,
			p.CoSplitBatchCommands:          true,
			p.CoDataFormatVersion2:          int32(attrs._dfv),
			p.CoCompleteArrayExecution:      true,
			p.CoClientDistributionMode:      int32(p.CdmOff),
		}
		if attrs._locale != "" {
			co[p.CoClientLocale] = attrs._locale
		}
		return co
	}()

	if err := c.pw.Write(ctx, c.sessionID, p.MtConnect, false, finalRequest, p.ClientID(clientID), co); err != nil {
		return 0, nil, err
	}

	finalReply, err := authHnd.FinalReply()
	if err != nil {
		return 0, nil, err
	}
	if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		switch ph.PartKind {
		case p.PkAuthentication:
			c.pr.Read(ctx, finalReply)
		case p.PkConnectOptions:
			c.pr.Read(ctx, &co)
		}
	})); err != nil {
		return 0, nil, err
	}
	return c.pr.SessionID(), co, nil
}

func (c *conn) _queryDirect(ctx context.Context, query string, commit bool) (driver.Rows, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimeQuery)

	// allow e.g inserts as query -> handle commit like in _execDirect
	if err := c.pw.Write(ctx, c.sessionID, p.MtExecuteDirect, commit, p.Command(query)); err != nil {
		return nil, err
	}

	qr := &queryResult{conn: c}
	meta := &p.ResultMetadata{FieldTypeCtx: c.fieldTypeCtx}
	resSet := &p.Resultset{}

	if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		switch ph.PartKind {
		case p.PkResultMetadata:
			c.pr.Read(ctx, meta)
			qr.fields = meta.ResultFields
		case p.PkResultsetID:
			c.pr.Read(ctx, (*p.ResultsetID)(&qr.rsID))
		case p.PkResultset:
			resSet.ResultFields = qr.fields
			c.pr.Read(ctx, resSet)
			qr.fieldValues = resSet.FieldValues
			qr.decodeErrors = resSet.DecodeErrors
			qr.attributes = ph.PartAttributes
		}
	})); err != nil {
		return nil, err
	}
	if qr.rsID == 0 { // non select query
		return noResult, nil
	}
	return qr, nil
}

func (c *conn) _execDirect(ctx context.Context, query string, commit bool) (driver.Result, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimeExec)

	if err := c.pw.Write(ctx, c.sessionID, p.MtExecuteDirect, commit, p.Command(query)); err != nil {
		return nil, err
	}

	rows := &p.RowsAffected{}
	var numRow int64
	if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		if ph.PartKind == p.PkRowsAffected {
			c.pr.Read(ctx, rows)
			numRow = rows.Total()
		}
	})); err != nil {
		return nil, err
	}
	if c.pr.FunctionCode() == p.FcDDL {
		return driver.ResultNoRows, nil
	}
	return driver.RowsAffected(numRow), nil
}

func (c *conn) _prepare(ctx context.Context, query string) (*prepareResult, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimePrepare)

	if err := c.pw.Write(ctx, c.sessionID, p.MtPrepare, false, p.Command(query)); err != nil {
		return nil, err
	}

	pr := &prepareResult{}
	resMeta := &p.ResultMetadata{FieldTypeCtx: c.fieldTypeCtx}
	prmMeta := &p.ParameterMetadata{FieldTypeCtx: c.fieldTypeCtx}

	if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		switch ph.PartKind {
		case p.PkStatementID:
			c.pr.Read(ctx, (*p.StatementID)(&pr.stmtID))
		case p.PkResultMetadata:
			c.pr.Read(ctx, resMeta)
			pr.resultFields = resMeta.ResultFields
		case p.PkParameterMetadata:
			c.pr.Read(ctx, prmMeta)
			pr.parameterFields = prmMeta.ParameterFields
		}
	})); err != nil {
		return nil, err
	}
	pr.fc = c.pr.FunctionCode()
	return pr, nil
}

/*
_exec executes a sql statement.

Bulk insert containing LOBs:
  - Precondition:
    .Sending more than one row with partial LOB data.
  - Observations:
    .In hdb version 1 and 2 'piecewise' LOB writing does work.
    .Same does not work in case of geo fields which are LOBs en,- decoded as well.
    .In hana version 4 'piecewise' LOB writing seems not to work anymore at all.
  - Server implementation (not documented):
    .'piecewise' LOB writing is only supported for the last row of a 'bulk insert'.
  - Current implementation:
    One server call in case of
    .'non bulk' execs or
    .'bulk' execs without LOBs
    else potential several server calls (split into packages).
  - Package invariant:
    .for all packages except the last one, the last row contains 'incomplete' LOB data ('piecewise' writing)
*/
func (c *conn) _exec(ctx context.Context, pr *prepareResult, nvargs []driver.NamedValue, commit bool, ofs int) (driver.Result, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimeExec)

	addLobDataRecs, err := c._mapExecArgs(pr.parameterFields, nvargs)
	if err != nil {
		return driver.ResultNoRows, err
	}

	// piecewise LOB handling
	numColumn := len(pr.parameterFields)
	totalRowsAffected := totalRowsAffected(0)
	from := 0
	for i := 0; i < len(addLobDataRecs); i++ {
		to := (addLobDataRecs[i] + 1) * numColumn

		r, err := c._execBatch(ctx, pr, nvargs[from:to], commit, ofs)
		totalRowsAffected.add(r)
		if err != nil {
			return driver.RowsAffected(totalRowsAffected), err
		}
		from = to
	}
	return driver.RowsAffected(totalRowsAffected), nil
}

func (c *conn) _execBatch(ctx context.Context, pr *prepareResult, nvargs []driver.NamedValue, commit bool, ofs int) (driver.Result, error) {
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

	if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		switch ph.PartKind {
		case p.PkRowsAffected:
			c.pr.Read(ctx, rows)
			rowsAffected = rows.Total()
		case p.PkWriteLobReply:
			c.pr.Read(ctx, lobReply)
			ids = lobReply.IDs
		}
	})); err != nil {
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
		if err := c.encodeLobs(nil, ids, pr.parameterFields, nvargs[startLastRec:]); err != nil {
			return nil, err
		}
	}

	if fc == p.FcDDL {
		return driver.ResultNoRows, nil
	}
	return driver.RowsAffected(rowsAffected), nil
}

func (c *conn) _execCall(ctx context.Context, pr *prepareResult, nvargs []driver.NamedValue) (driver.Result, *sql.Rows, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimeCall)

	callArgs, err := c._mapCallArgs(pr.parameterFields, nvargs)
	if err != nil {
		return nil, nil, err
	}
	inputParameters, err := p.NewInputParameters(callArgs.inFields, callArgs.inArgs)
	if err != nil {
		return nil, nil, err
	}
	if err := c.pw.Write(ctx, c.sessionID, p.MtExecute, false, p.StatementID(pr.stmtID), inputParameters); err != nil {
		return nil, nil, err
	}

	/*
		call without lob input parameters:
		--> callResult output parameter values are set after read call
		call with lob output parameters:
		--> callResult output parameter values are set after last lob input write
	*/

	cr, ids, numRow, err := c._readCall(ctx, callArgs.outFields)
	if err != nil {
		return nil, nil, err
	}

	if len(ids) != 0 {
		/*
			writeLobParameters:
			- chunkReaders
			- cr (callResult output parameters are set after all lob input parameters are written)
		*/
		if err := c.encodeLobs(cr, ids, callArgs.inFields, callArgs.inArgs); err != nil {
			return nil, nil, err
		}
	}

	// no output fields -> done
	if len(cr.outputFields) == 0 {
		return driver.RowsAffected(numRow), nil, nil
	}

	scanArgs := []any{}
	for i := range cr.outputFields {
		scanArgs = append(scanArgs, callArgs.outArgs[i].Value.(sql.Out).Dest)
	}

	// no table output parameters -> QueryRow
	if len(callArgs.outFields) == len(callArgs.outArgs) {
		if err := callConverter.QueryRow("", cr).Scan(scanArgs...); err != nil {
			return nil, nil, err
		}
		return driver.RowsAffected(numRow), nil, nil
	}

	// table output parameters -> Query (needs to kept open)
	rows, err := callConverter.Query("", cr)
	if err != nil {
		return nil, rows, err
	}
	if !rows.Next() {
		return nil, rows, rows.Err()
	}
	if err := rows.Scan(scanArgs...); err != nil {
		return nil, rows, err
	}
	return driver.RowsAffected(numRow), rows, nil
}

func (c *conn) _readCall(ctx context.Context, outputFields []*p.ParameterField) (*callResult, []p.LocatorID, int64, error) {
	cr := &callResult{conn: c, outputFields: outputFields}

	var qr *queryResult
	rows := &p.RowsAffected{}
	var ids []p.LocatorID
	outPrms := &p.OutputParameters{}
	meta := &p.ResultMetadata{FieldTypeCtx: c.fieldTypeCtx}
	resSet := &p.Resultset{}
	lobReply := &p.WriteLobReply{}
	var numRow int64
	tableRowIdx := 0

	if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		switch ph.PartKind {
		case p.PkRowsAffected:
			c.pr.Read(ctx, rows)
			numRow = rows.Total()
		case p.PkOutputParameters:
			outPrms.OutputFields = cr.outputFields
			c.pr.Read(ctx, outPrms)
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
			c.pr.Read(ctx, meta)
			qr.fields = meta.ResultFields
		case p.PkResultset:
			resSet.ResultFields = qr.fields
			c.pr.Read(ctx, resSet)
			qr.fieldValues = resSet.FieldValues
			qr.decodeErrors = resSet.DecodeErrors
			qr.attributes = ph.PartAttributes
		case p.PkResultsetID:
			c.pr.Read(ctx, (*p.ResultsetID)(&qr.rsID))
		case p.PkWriteLobReply:
			c.pr.Read(ctx, lobReply)
			ids = lobReply.IDs
		}
	})); err != nil {
		return nil, nil, 0, err
	}
	return cr, ids, numRow, nil
}

func (c *conn) _query(ctx context.Context, pr *prepareResult, nvargs []driver.NamedValue, commit bool) (driver.Rows, error) {
	defer c.addSQLTimeValue(time.Now(), sqlTimeQuery)

	// allow e.g inserts as query -> handle commit like in exec

	if err := c._mapQueryArgs(pr.parameterFields, nvargs); err != nil {
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

	if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		switch ph.PartKind {
		case p.PkResultsetID:
			c.pr.Read(ctx, (*p.ResultsetID)(&qr.rsID))
		case p.PkResultset:
			resSet.ResultFields = qr.fields
			c.pr.Read(ctx, resSet)
			qr.fieldValues = resSet.FieldValues
			qr.decodeErrors = resSet.DecodeErrors
			qr.attributes = ph.PartAttributes
		}
	})); err != nil {
		return nil, err
	}
	if qr.rsID == 0 { // non select query
		return noResult, nil
	}
	return qr, nil
}

func (c *conn) _fetchNext(ctx context.Context, qr *queryResult) error {
	defer c.addSQLTimeValue(time.Now(), sqlTimeFetch)

	if err := c.pw.Write(ctx, c.sessionID, p.MtFetchNext, false, p.ResultsetID(qr.rsID), p.Fetchsize(c._fetchSize)); err != nil {
		return err
	}

	resSet := &p.Resultset{ResultFields: qr.fields, FieldValues: qr.fieldValues} // reuse field values

	return c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
		if ph.PartKind == p.PkResultset {
			c.pr.Read(ctx, resSet)
			qr.fieldValues = resSet.FieldValues
			qr.decodeErrors = resSet.DecodeErrors
			qr.attributes = ph.PartAttributes
		}
	}))
}

func (c *conn) _dropStatementID(ctx context.Context, id uint64) error {
	if err := c.pw.Write(ctx, c.sessionID, p.MtDropStatementID, false, p.StatementID(id)); err != nil {
		return err
	}
	return c._checkError(ctx, c.pr.ReadSkip(ctx))
}

func (c *conn) _closeResultsetID(ctx context.Context, id uint64) error {
	if err := c.pw.Write(ctx, c.sessionID, p.MtCloseResultset, false, p.ResultsetID(id)); err != nil {
		return err
	}
	return c._checkError(ctx, c.pr.ReadSkip(ctx))
}

func (c *conn) _commit(ctx context.Context) error {
	defer c.addSQLTimeValue(time.Now(), sqlTimeCommit)

	if err := c.pw.Write(ctx, c.sessionID, p.MtCommit, false); err != nil {
		return err
	}
	if err := c._checkError(ctx, c.pr.ReadSkip(ctx)); err != nil {
		return err
	}
	return nil
}

func (c *conn) _rollback(ctx context.Context) error {
	defer c.addSQLTimeValue(time.Now(), sqlTimeRollback)

	if err := c.pw.Write(ctx, c.sessionID, p.MtRollback, false); err != nil {
		return err
	}
	if err := c._checkError(ctx, c.pr.ReadSkip(ctx)); err != nil {
		return err
	}
	return nil
}

func (c *conn) _disconnect(ctx context.Context) error {
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

// decodeLobs decodes (reads from db) output lob or result lob parameters.

// read lob reply
// - seems like readLobreply returns only a result for one lob - even if more then one is requested
// --> read single lobs
func (c *conn) decodeLob(descr *p.LobOutDescr, wr io.Writer) error {
	defer c.addSQLTimeValue(time.Now(), sqlTimeFetchLob)

	var err error

	if descr.IsCharBased {
		wrcl := transform.NewWriter(wr, c._cesu8Decoder()) // CESU8 transformer
		err = c._decodeLob(descr, wrcl, func(b []byte) (size int, numChar int) {
			for len(b) > 0 {
				if !cesu8.FullRune(b) {
					return
				}
				_, width := cesu8.DecodeRune(b)
				size += width
				if width == cesu8.CESUMax {
					numChar += 2 // caution: hdb counts 2 chars in case of surrogate pair
				} else {
					numChar++
				}
				b = b[width:]
			}
			return
		})
	} else {
		err = c._decodeLob(descr, wr, func(b []byte) (int, int) { return len(b), len(b) })
	}

	if pw, ok := wr.(*io.PipeWriter); ok { // if the writer is a pipe-end -> close at the end
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}
	return err
}

func (c *conn) _decodeLob(descr *p.LobOutDescr, wr io.Writer, countChars func(b []byte) (int, int)) error {
	lobChunkSize := int64(c._lobChunkSize)

	chunkSize := func(numChar, ofs int64) int32 {
		chunkSize := numChar - ofs
		if chunkSize > lobChunkSize {
			return int32(lobChunkSize)
		}
		return int32(chunkSize)
	}

	size, numChar := countChars(descr.B)
	if _, err := wr.Write(descr.B[:size]); err != nil {
		return err
	}

	lobRequest := &p.ReadLobRequest{}
	lobRequest.ID = descr.ID

	lobReply := &p.ReadLobReply{}

	eof := descr.Opt.IsLastData()

	ctx := context.Background()

	for !eof {

		lobRequest.Ofs += int64(numChar)
		lobRequest.ChunkSize = chunkSize(descr.NumChar, lobRequest.Ofs)

		if err := c.pw.Write(ctx, c.sessionID, p.MtWriteLob, false, lobRequest); err != nil {
			return err
		}

		if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
			if ph.PartKind == p.PkReadLobReply {
				c.pr.Read(ctx, lobReply)
			}
		})); err != nil {
			return err
		}

		if lobReply.ID != lobRequest.ID {
			return fmt.Errorf("internal error: invalid lob locator %d - expected %d", lobReply.ID, lobRequest.ID)
		}

		size, numChar = countChars(lobReply.B)
		if _, err := wr.Write(lobReply.B[:size]); err != nil {
			return err
		}
		eof = lobReply.Opt.IsLastData()
	}
	return nil
}

func assertEqual[T comparable](s string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s: %v %v", s, a, b))
	}
}

// encodeLobs encodes (write to db) input lob parameters.
func (c *conn) encodeLobs(cr *callResult, ids []p.LocatorID, inPrmFields []*p.ParameterField, nvargs []driver.NamedValue) error {
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
			if !lobInDescr.Opt.IsLastData() {
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
			if err := descr.FetchNext(c._lobChunkSize); err != nil {
				return err
			}
		}

		writeLobRequest.Descrs = descrs

		if err := c.pw.Write(ctx, c.sessionID, p.MtReadLob, false, writeLobRequest); err != nil {
			return err
		}

		lobReply := &p.WriteLobReply{}
		outPrms := &p.OutputParameters{}

		if err := c._checkError(ctx, c.pr.IterateParts(ctx, func(ph *p.PartHeader) {
			switch ph.PartKind {
			case p.PkOutputParameters:
				outPrms.OutputFields = cr.outputFields
				c.pr.Read(ctx, outPrms)
				cr.fieldValues = outPrms.FieldValues
				cr.decodeErrors = outPrms.DecodeErrors
			case p.PkWriteLobReply:
				c.pr.Read(ctx, lobReply)
				ids = lobReply.IDs
			}
		})); err != nil {
			return err
		}

		// remove done descr
		j := 0
		for _, descr := range descrs {
			if !descr.Opt.IsLastData() {
				descrs[j] = descr
				j++
			}
		}
		descrs = descrs[:j]
	}
	return nil
}
