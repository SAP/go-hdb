package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// check if statements implements all required interfaces.
var (
	_ driver.Stmt              = (*stmt)(nil)
	_ driver.StmtExecContext   = (*stmt)(nil)
	_ driver.StmtQueryContext  = (*stmt)(nil)
	_ driver.NamedValueChecker = (*stmt)(nil)
)

type stmt struct {
	session   *session
	wg        *sync.WaitGroup
	attrs     *connAttrs
	metrics   *metrics
	sqlTracer *sqlTracer
	query     string
	pr        *prepareResult
	// rows: stored procedures with table output parameters
	rows *sql.Rows
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

func newStmt(session *session, wg *sync.WaitGroup, attrs *connAttrs, metrics *metrics, sqlTracer *sqlTracer, query string, pr *prepareResult) *stmt {
	metrics.msgCh <- gaugeMsg{idx: gaugeStmt, v: 1} // increment number of statements.
	return &stmt{session: session, wg: wg, attrs: attrs, metrics: metrics, sqlTracer: sqlTracer, query: query, pr: pr}
}

/*
NumInput differs dependent on statement (check is done in QueryContext and ExecContext):
- #args == #param (only in params):    query, exec, exec bulk (non control query)
- #args == #param (in and out params): exec call
- #args == 0:                          exec bulk (control query)
- #args == #input param:               query call.
*/
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Close() error {
	s.metrics.msgCh <- gaugeMsg{idx: gaugeStmt, v: -1} // decrement number of statements.

	if s.rows != nil {
		s.rows.Close()
	}

	if s.session.isBad() {
		return driver.ErrBadConn
	}
	return s.session.dropStatementID(context.Background(), s.pr.stmtID)
}

// CheckNamedValue implements NamedValueChecker interface.
func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error {
	// conversion is happening as part of the exec, query call
	return nil
}

func (s *stmt) QueryContext(ctx context.Context, nvargs []driver.NamedValue) (driver.Rows, error) {
	if s.pr.isProcedureCall() {
		return nil, fmt.Errorf("invalid procedure call %s - please use Exec instead", s.query)
	}

	trace := s.sqlTracer.begin()

	done := make(chan struct{})
	var rows driver.Rows
	s.wg.Add(1)
	var sqlErr error
	go func() {
		defer s.wg.Done()
		s.session.withLock(func(sess *session) {
			if sqlErr = s.session.preventSwitchUser(ctx); sqlErr != nil {
				return
			}
			rows, sqlErr = sess.query(ctx, s.pr, nvargs)
		})
		close(done)
	}()

	select {
	case <-ctx.Done():
		s.session.cancel()
		ctxErr := ctx.Err()
		if trace {
			s.sqlTracer.log(ctx, traceQuery, s.query, ctxErr, nvargs)
		}
		return nil, ctxErr
	case <-done:
		if trace {
			s.sqlTracer.log(ctx, traceQuery, s.query, sqlErr, nvargs)
		}
		return rows, sqlErr
	}
}

func (s *stmt) ExecContext(ctx context.Context, nvargs []driver.NamedValue) (driver.Result, error) {
	trace := s.sqlTracer.begin()

	if hookFn, ok := ctx.Value(connHookCtxKey).(connHookFn); ok {
		hookFn(choStmtExec)
	}

	done := make(chan struct{})
	var result driver.Result
	s.wg.Add(1)
	var sqlErr error
	var rows *sql.Rows // needed to avoid data race in close if context get cancelled.
	go func() {
		defer s.wg.Done()
		s.session.withLock(func(sess *session) {
			if sqlErr = s.session.preventSwitchUser(ctx); sqlErr != nil {
				return
			}
			if s.pr.isProcedureCall() {
				result, rows, sqlErr = s.execCall(ctx, sess, s.pr, nvargs) //nolint:sqlclosecheck
			} else {
				result, sqlErr = s.execDefault(ctx, sess, nvargs)
			}
		})
		close(done)
	}()

	select {
	case <-ctx.Done():
		s.session.cancel()
		ctxErr := ctx.Err()
		if trace {
			s.sqlTracer.log(ctx, traceExec, s.query, ctxErr, nvargs)
		}
		return nil, ctxErr
	case <-done:
		s.rows = rows
		if trace {
			s.sqlTracer.log(ctx, traceExec, s.query, sqlErr, nvargs)
		}
		return result, sqlErr
	}
}

func (s *stmt) execCall(ctx context.Context, session *session, pr *prepareResult, nvargs []driver.NamedValue) (driver.Result, *sql.Rows, error) {
	/*
		call without lob input parameters:
		--> callResult output parameter values are set after read call
		call with lob output parameters:
		--> callResult output parameter values are set after last lob input write
	*/

	cr, callArgs, ids, numRow, err := session.execCall(ctx, pr, nvargs)
	if err != nil {
		return nil, nil, err
	}

	if len(ids) != 0 {
		/*
			writeLobParameters:
			- chunkReaders
			- cr (callResult output parameters are set after all lob input parameters are written)
		*/
		if err := session.writeLobs(ctx, cr, ids, callArgs.inFields, callArgs.inArgs); err != nil {
			return nil, nil, err
		}
	}

	numOutputField := len(cr.outFields)
	// no output fields -> done
	if numOutputField == 0 {
		return driver.RowsAffected(numRow), nil, nil
	}

	scanArgs := make([]any, numOutputField)
	for i := 0; i < numOutputField; i++ {
		scanArgs[i] = callArgs.outArgs[i].Value.(sql.Out).Dest
	}

	// no table output parameters -> QueryRow
	if len(callArgs.outFields) == len(callArgs.outArgs) {
		if err := stdConnTracker.callDB().QueryRow("", cr).Scan(scanArgs...); err != nil {
			return nil, nil, err
		}
		return driver.RowsAffected(numRow), nil, nil
	}

	// table output parameters -> Query (needs to kept open)
	rows, err := stdConnTracker.callDB().Query("", cr)
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

func (s *stmt) execDefault(ctx context.Context, session *session, nvargs []driver.NamedValue) (driver.Result, error) {
	numNVArg, numField := len(nvargs), s.pr.numField()

	if numNVArg == 0 {
		if numField != 0 {
			return nil, fmt.Errorf("invalid number of arguments %d - expected %d", numNVArg, numField)
		}
		return session.exec(ctx, s.pr, nvargs, 0)
	}
	if numNVArg == 1 {
		if _, ok := nvargs[0].Value.(func(args []any) error); ok {
			return s.execFct(ctx, session, nvargs)
		}
	}
	if numNVArg == numField {
		return s.exec(ctx, session, s.pr, nvargs, 0)
	}
	if numNVArg%numField != 0 {
		return nil, fmt.Errorf("invalid number of arguments %d - multiple of %d expected", numNVArg, numField)
	}
	return s.execMany(ctx, session, nvargs)
}

// ErrEndOfRows is the error to be returned using a function based bulk exec to indicate
// the end of rows.
var ErrEndOfRows = errors.New("end of rows")

/*
Non 'atomic' (transactional) operation due to the split in packages (bulkSize),
execMany data might only be written partially to the database in case of hdb stmt errors.
*/
func (s *stmt) execFct(ctx context.Context, session *session, nvargs []driver.NamedValue) (driver.Result, error) {
	totalRowsAffected := totalRowsAffected(0)
	args := make([]driver.NamedValue, 0, s.pr.numField())
	scanArgs := make([]any, s.pr.numField())

	fct, ok := nvargs[0].Value.(func(args []any) error)
	if !ok {
		panic("invalid argument") // should never happen
	}

	done := false
	batch := 0
	for !done {
		args = args[:0]
		for i := 0; i < s.attrs._bulkSize; i++ {
			err := fct(scanArgs)
			if errors.Is(err, ErrEndOfRows) {
				done = true
				break
			}
			if err != nil {
				return driver.RowsAffected(totalRowsAffected), err
			}

			args = slices.Grow(args, len(scanArgs))
			for j, scanArg := range scanArgs {
				nv := driver.NamedValue{Ordinal: j + 1}
				if t, ok := scanArg.(sql.NamedArg); ok {
					nv.Name = t.Name
					nv.Value = t.Value
				} else {
					nv.Name = ""
					nv.Value = scanArg
				}
				args = append(args, nv)
			}
		}

		r, err := s.exec(ctx, session, s.pr, args, batch*s.attrs._bulkSize)
		totalRowsAffected.add(r)
		if err != nil {
			return driver.RowsAffected(totalRowsAffected), err
		}
		batch++
	}
	return driver.RowsAffected(totalRowsAffected), nil
}

/*
Non 'atomic' (transactional) operation due to the split in packages (bulkSize),
execMany data might only be written partially to the database in case of hdb stmt errors.
*/
func (s *stmt) execMany(ctx context.Context, session *session, nvargs []driver.NamedValue) (driver.Result, error) {
	bulkSize := s.attrs._bulkSize

	totalRowsAffected := totalRowsAffected(0)
	numField := s.pr.numField()
	numNVArg := len(nvargs)
	numRec := numNVArg / numField
	numBatch := numRec / bulkSize
	if numRec%bulkSize != 0 {
		numBatch++
	}

	for i := 0; i < numBatch; i++ {
		from := i * numField * bulkSize
		to := (i + 1) * numField * bulkSize
		if to > numNVArg {
			to = numNVArg
		}
		r, err := s.exec(ctx, session, s.pr, nvargs[from:to], i*bulkSize)
		totalRowsAffected.add(r)
		if err != nil {
			return driver.RowsAffected(totalRowsAffected), err
		}
	}
	return driver.RowsAffected(totalRowsAffected), nil
}

/*
exec executes a sql statement.

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
func (s *stmt) exec(ctx context.Context, session *session, pr *prepareResult, nvargs []driver.NamedValue, ofs int) (driver.Result, error) {
	addLobDataRecs, err := convertExecArgs(pr.parameterFields, nvargs, s.attrs._cesu8Encoder(), s.attrs._lobChunkSize)
	if err != nil {
		return driver.ResultNoRows, err
	}

	// piecewise LOB handling
	numColumn := len(pr.parameterFields)
	totalRowsAffected := totalRowsAffected(0)
	from := 0
	for i := 0; i < len(addLobDataRecs); i++ {
		to := (addLobDataRecs[i] + 1) * numColumn

		r, err := session.exec(ctx, pr, nvargs[from:to], ofs)
		totalRowsAffected.add(r)
		if err != nil {
			return driver.RowsAffected(totalRowsAffected), err
		}
		from = to
	}
	return driver.RowsAffected(totalRowsAffected), nil
}
