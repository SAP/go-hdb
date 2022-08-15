// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"flag"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver/internal/trace"
)

var protocolTrace = trace.NewTrace(log.Ldate|log.Ltime, "hdb", "protocol")

var protocolTraceFlag = trace.NewFlag(protocolTrace)

func init() {
	flag.Var(protocolTraceFlag, "hdb.protocol.trace", "enabling hdb protocol trace")
}

const (
	upStreamPrefix   = "→"
	downStreamPrefix = "←"
)

func newTracer() (func(up bool, v interface{}), bool) {

	prefix := func(up bool) string {
		if up {
			return upStreamPrefix
		}
		return downStreamPrefix
	}

	traceNull := func(bool, interface{}) {}

	traceProtocol := func(up bool, v interface{}) {
		var msg string

		switch v.(type) {
		case *initRequest, *initReply:
			msg = fmt.Sprintf("%sINI %s", prefix(up), v)
		case *messageHeader:
			msg = fmt.Sprintf("%sMSG %s", prefix(up), v)
		case *segmentHeader:
			msg = fmt.Sprintf(" SEG %s", v)
		case *PartHeader:
			msg = fmt.Sprintf(" PAR %s", v)
		default:
			msg = fmt.Sprintf("     %s", v)
		}
		protocolTrace.Output(2, msg)
	}

	if protocolTrace.On() {
		return traceProtocol, true
	}
	return traceNull, false
}
