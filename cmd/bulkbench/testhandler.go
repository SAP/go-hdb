package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/SAP/go-hdb/driver"
)

// Test URL paths.
const (
	testSeq = "/test/Seq"
	testPar = "/test/Par"
)

// testResult is the structure used to provide the JSON based test result response.
type testResult struct {
	Test       string
	Seconds    float64
	BatchCount int
	BatchSize  int
	BulkSize   int
	Duration   time.Duration
	Error      string
}

func (r *testResult) String() string {
	if r.Error != "" {
		return r.Error
	}
	return fmt.Sprintf("%s: insert of %d rows in %f seconds (batchCount %d batchSize %d bulkSize %d)", r.Test, r.BatchCount*r.BatchSize, r.Duration.Seconds(), r.BatchCount, r.BatchSize, r.BulkSize)
}

// newTestResult returns a TestResult based on the JSON load provided by a HTTP response.
func newTestResult(r *http.Response) (*testResult, error) {
	result := &testResult{}
	if err := json.NewDecoder(r.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

type testFunc func(db *sql.DB, batchCount, batchSize int, drop bool, wait time.Duration) (time.Duration, error)

// testHandler implements the http.Handler interface for the tests.
type testHandler struct {
	log       func(format string, v ...any)
	testFuncs map[string]testFunc
}

// newTestHandler returns a new TestHandler instance.
func newTestHandler(log func(format string, v ...any)) (*testHandler, error) {
	h := &testHandler{log: log}
	h.testFuncs = map[string]testFunc{
		testSeq: h.testSeq,
		testPar: h.testPar,
	}
	return h, nil
}

func (h *testHandler) tests() []string {
	// need correct sort order
	return []string{testSeq, testPar}
}

const (
	defBatchCount = 10
	defBatchSize  = 10000
)

func (h *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Try to get a comparable environment for each run
	// by clearing garbage from previous runs.
	runtime.GC()

	q := newURLQuery(r)

	batchCount := q.getInt(urlQueryBatchCount, defBatchCount)
	batchSize := q.getInt(urlQueryBatchSize, defBatchSize)

	waitDuration := time.Duration(wait) * time.Second

	test := r.URL.Path

	result := &testResult{Test: test, BatchCount: batchCount, BatchSize: batchSize}

	defer func() {
		h.log("%s", result)
		e := json.NewEncoder(w)
		e.Encode(result) // ignore error
	}()

	db, bulkSize, err := h.setup(batchSize)
	if err != nil {
		result.Error = err.Error()
		return
	}
	defer h.teardown(db)

	var d time.Duration

	if f, ok := h.testFuncs[test]; ok {
		if err = ensureSchemaTable(db); err == nil {
			d, err = f(db, batchCount, batchSize, drop, waitDuration)
		}
	} else {
		err = fmt.Errorf("invalid test %s", test)
	}

	result.BulkSize = bulkSize
	result.Duration = d
	result.Seconds = d.Seconds()
	if err != nil {
		result.Error = err.Error()
	}
}

func (h *testHandler) testSeq(db *sql.DB, batchCount, batchSize int, drop bool, wait time.Duration) (time.Duration, error) {
	numRow := batchCount * batchSize

	if wait > 0 {
		time.Sleep(wait)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return 0, err
	}

	stmt, err := conn.PrepareContext(context.Background(), prepareQuery)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	var d time.Duration

	i := 0
	t := time.Now()
	_, err = stmt.Exec(func(args []any) error {
		if i >= numRow {
			return driver.ErrEndOfRows
		}
		fillRow(i, args)
		i++
		return nil
	})
	d += time.Since(t)
	if err != nil {
		return d, err
	}

	return d, nil
}

type task struct {
	conn *sql.Conn
	stmt *sql.Stmt
	size int
	err  error
}

func (t *task) close() {
	t.stmt.Close()
	t.conn.Close()
}

func (h *testHandler) createTasks(db *sql.DB, batchCount, batchSize int, bulk, drop bool) ([]*task, error) {
	var err error
	tasks := make([]*task, batchCount)
	for i := 0; i < batchCount; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			return nil, err
		}

		stmt, err := conn.PrepareContext(context.Background(), prepareQuery)
		if err != nil {
			return nil, err
		}

		tasks[i] = &task{conn: conn, stmt: stmt, size: batchSize}
	}
	return tasks, err
}

func (h *testHandler) testPar(db *sql.DB, batchCount, batchSize int, drop bool, wait time.Duration) (time.Duration, error) {
	var wg sync.WaitGroup

	tasks, err := h.createTasks(db, batchCount, batchSize, true, drop)
	if err != nil {
		return 0, err
	}

	if wait > 0 {
		time.Sleep(wait)
	}

	t := time.Now() // Start time.

	for i, t := range tasks { // Start one worker per task.
		wg.Add(1)

		go func(worker int, t *task) {
			defer wg.Done()

			j := 0
			if _, err = t.stmt.Exec(func(args []any) error {
				if j >= t.size {
					return driver.ErrEndOfRows
				}
				fillRow(worker*t.size+j, args)
				j++
				return nil
			}); err != nil {
				t.err = err
			}
		}(i, t)
	}
	wg.Wait()

	d := time.Since(t) // Duration.

	for _, t := range tasks {
		// return last error
		err = t.err
		t.close()
	}

	return d, err
}

func (h *testHandler) setup(batchSize int) (*sql.DB, int, error) {
	// Set bulk size to batchSize.
	ctr, err := driver.NewDSNConnector(dsn)
	if err != nil {
		return nil, 0, err
	}
	ctr.SetBulkSize(batchSize)
	ctr.SetBufferSize(bufferSize)
	if err != nil {
		return nil, 0, err
	}
	return sql.OpenDB(ctr), ctr.BulkSize(), nil
}

func (h *testHandler) teardown(db *sql.DB) {
	db.Close()
}

// fillRow fills a table row with values.
func fillRow(idx int, args []any) {
	args[0] = idx
	args[1] = float64(idx)
}
