// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package sqltrace

import (
	"flag"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

type sqlTrace struct {
	once sync.Once
	flag bool
	on   int32 // true for on != 0 - atomic access
	*log.Logger
}

func newSQLTrace() *sqlTrace {
	return &sqlTrace{
		Logger: log.New(os.Stdout, "hdb ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

var tracer = newSQLTrace()

func init() {
	flag.BoolVar(&tracer.flag, "hdb.sqlTrace", false, "enabling hdb sql trace")
}

func boolToInt32(f bool) int32 {
	if f {
		return 1
	}
	return 0
}

// On returns if tracing methods output is active.
func On() bool {
	tracer.once.Do(func() {
		// init on with flag value
		atomic.StoreInt32(&tracer.on, boolToInt32(tracer.flag))
	})
	return atomic.LoadInt32(&tracer.on) != 0
}

// SetOn sets tracing methods output active or inactive.
func SetOn(on bool) { atomic.StoreInt32(&tracer.on, boolToInt32(on)) }

// Trace calls trace logger Print method to print to the trace logger.
func Trace(v ...interface{}) { tracer.Print(v...) }

// Tracef calls trace logger Printf method to print to the trace logger.
func Tracef(format string, v ...interface{}) { tracer.Printf(format, v...) }

// Traceln calls trace logger Println method to print to the trace logger.
func Traceln(v ...interface{}) { tracer.Println(v...) }
