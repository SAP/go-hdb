// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package sqltrace

import (
	"flag"
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/trace"
)

var std = trace.NewTrace("hdb", "sql")

var traceFlag = trace.NewFlag(std)

func init() {
	flag.Var(traceFlag, "hdb.sqlTrace", "enabling hdb sql trace")
}

// On returns if tracing methods output is active.
func On() bool { return std.On() }

// SetOn sets tracing methods output active or inactive.
func SetOn(on bool) { std.SetOn(on) }

// Trace calls trace logger Print method to print to the trace logger.
func Trace(v ...interface{}) { std.Output(2, fmt.Sprint(v...)) }

// Tracef calls trace logger Printf method to print to the trace logger.
func Tracef(format string, v ...interface{}) { std.Output(2, fmt.Sprintf(format, v...)) }

// Traceln calls trace logger Println method to print to the trace logger.
func Traceln(v ...interface{}) { std.Output(2, fmt.Sprintln(v...)) }
