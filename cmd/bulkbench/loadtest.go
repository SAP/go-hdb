package main

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/SAP/go-hdb/driver"
)

type loadtestResult struct {
	Sequential bool
	BatchCount int
	BatchSize  int
	BulkSize   int
	Duration   time.Duration
	Err        error
}

func (r *loadtestResult) String() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	return fmt.Sprintf(
		"insert of %d rows in %s (sequential: %t batchCount: %d batchSize: %d bulkSize: %d)",
		r.BatchCount*r.BatchSize,
		r.Duration,
		r.Sequential,
		r.BatchCount,
		r.BatchSize,
		r.BulkSize,
	)
}

func (r *loadtestResult) setError(err error) *loadtestResult {
	r.Err = err
	return r
}

const (
	defSequential = true
	defBatchCount = 10
	defBatchSize  = 10000
)

type loadTest struct {
	dba          *dba
	prepareQuery string
}

func newLoadTest(dba *dba) *loadTest {
	return &loadTest{
		dba:          dba,
		prepareQuery: fmt.Sprintf("insert into %s.%s values (?, ?)", driver.Identifier(dba.schemaName), driver.Identifier(dba.tableName)),
	}
}

func (lt *loadTest) test(sequential bool, batchCount, batchSize int, drop bool) *loadtestResult {
	// Try to get a comparable environment for each run
	// by clearing garbage from previous runs.
	runtime.GC()

	waitDuration := time.Duration(wait) * time.Second
	result := &loadtestResult{Sequential: sequential, BatchCount: batchCount, BatchSize: batchSize}

	db, bulkSize, err := lt.setup(batchSize)
	if err != nil {
		return result.setError(err)
	}
	defer lt.teardown(db)

	if drop {
		lt.dba.dropTable()
	}
	if err := lt.dba.ensureSchemaTable(); err != nil {
		return result.setError(err)
	}

	var d time.Duration
	if sequential {
		d, err = lt.sequential(db, batchCount, batchSize, waitDuration)
	} else {
		d, err = lt.concurrent(db, batchCount, batchSize, waitDuration)
	}

	result.BulkSize = bulkSize
	result.Duration = d
	if err != nil {
		return result.setError(err)
	}
	return result
}

func (lt *loadTest) sequential(db *sql.DB, batchCount, batchSize int, wait time.Duration) (time.Duration, error) {
	numRow := batchCount * batchSize

	if wait > 0 {
		time.Sleep(wait)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return 0, err
	}

	stmt, err := conn.PrepareContext(context.Background(), lt.prepareQuery)
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
	/*
		using a dedicated connection for each task causes hdb closing connections
		if number of connection is approx. 1000
		conn *sql.Conn
	*/
	stmt *sql.Stmt
	size int
	err  error
}

func (t *task) close() {
	t.stmt.Close()
}

func createTasks(db *sql.DB, prepareQuery string, batchCount, batchSize int) ([]*task, error) {
	var err error
	tasks := make([]*task, batchCount)
	for i := 0; i < batchCount; i++ {
		stmt, err := db.PrepareContext(context.Background(), prepareQuery)
		if err != nil {
			return nil, err
		}

		tasks[i] = &task{stmt: stmt, size: batchSize}
	}
	return tasks, err
}

func (lt *loadTest) concurrent(db *sql.DB, batchCount, batchSize int, wait time.Duration) (time.Duration, error) {
	var wg sync.WaitGroup

	tasks, err := createTasks(db, lt.prepareQuery, batchCount, batchSize)
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

func (lt *loadTest) setup(batchSize int) (*sql.DB, int, error) {
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

func (lt *loadTest) teardown(db *sql.DB) {
	db.Close()
}

// fillRow fills a table row with values.
func fillRow(idx int, args []any) {
	args[0] = idx
	args[1] = float64(idx)
}
