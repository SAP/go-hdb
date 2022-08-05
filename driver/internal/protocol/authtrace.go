// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"flag"
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/trace"
)

var authTrace = trace.NewTrace("hdb", "auth")

var authTraceFlag = trace.NewFlag(authTrace)

func init() {
	flag.Var(authTraceFlag, "hdb.protocol.auth", "enabling hdb authentication trace")
}

func traceAuth(v ...interface{})                 { authTrace.Output(2, fmt.Sprint(v...)) }
func traceAuthf(format string, v ...interface{}) { authTrace.Output(2, fmt.Sprintf(format, v...)) }
