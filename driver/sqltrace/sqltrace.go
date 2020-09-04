// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package sqltrace

import (
	"flag"
	"log"
	"os"
	"sync"
)

type sqlTrace struct {
	mu sync.RWMutex // protects field on
	on bool
	*log.Logger
}

func newSQLTrace() *sqlTrace {
	return &sqlTrace{
		Logger: log.New(os.Stdout, "hdb ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

var tracer = newSQLTrace()

func init() {
	flag.BoolVar(&tracer.on, "hdb.sqlTrace", false, "enabling hdb sql trace")
}

// On returns if tracing methods output is active.
func On() bool {
	tracer.mu.RLock()
	on := tracer.on
	tracer.mu.RUnlock()
	return on
}

// SetOn sets tracing methods output active or inactive.
func SetOn(on bool) {
	tracer.mu.Lock()
	tracer.on = on
	tracer.mu.Unlock()
}

// Trace calls trace logger Print method to print to the trace logger.
func Trace(v ...interface{}) {
	if On() {
		tracer.Print(v...)
	}
}

// Tracef calls trace logger Printf method to print to the trace logger.
func Tracef(format string, v ...interface{}) {
	if On() {
		tracer.Printf(format, v...)
	}
}

// Traceln calls trace logger Println method to print to the trace logger.
func Traceln(v ...interface{}) {
	if On() {
		tracer.Println(v...)
	}
}
