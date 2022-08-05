// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"flag"
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/trace"
)

var protocolTrace = trace.NewTrace("hdb", "protocol")

var protocolTraceFlag = trace.NewFlag(protocolTrace)

func init() {
	flag.Var(protocolTraceFlag, "hdb.protocol.trace", "enabling hdb protocol trace")
}

const (
	upStreamPrefix   = "→"
	downStreamPrefix = "←"
)

func streamPrefix(upStream bool) string {
	if upStream {
		return upStreamPrefix
	}
	return downStreamPrefix
}

func traceProtocol(up bool, v interface{}) {
	prefix := streamPrefix(up)
	var msg string

	switch v.(type) {
	case *initRequest, *initReply:
		msg = fmt.Sprintf("%sINI %s", prefix, v)
	case *messageHeader:
		msg = fmt.Sprintf("%sMSG %s", prefix, v)
	case *segmentHeader:
		msg = fmt.Sprintf(" SEG %s", v)
	case *partHeader:
		msg = fmt.Sprintf(" PAR %s", v)
	default:
		msg = fmt.Sprintf("     %s", v)
	}
	protocolTrace.Output(2, msg)
}
