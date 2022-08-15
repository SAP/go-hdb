// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"flag"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver/internal/trace"
)

var stdTrace = trace.NewTrace(log.Ldate|log.Ltime|log.Lshortfile, "hdb", "auth")

var traceFlag = trace.NewFlag(stdTrace)

func init() {
	flag.Var(traceFlag, "hdb.protocol.auth", "enabling hdb authentication trace")
}

// Trace writes values to the trace output via fmt.Sprint.
func Trace(v ...interface{}) { stdTrace.Output(2, fmt.Sprint(v...)) }

// Tracef writes values to the trace output via fmt.Sprintf.
func Tracef(format string, v ...interface{}) { stdTrace.Output(2, fmt.Sprintf(format, v...)) }
