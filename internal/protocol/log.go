/*
Copyright 2020 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package protocol

import (
	"flag"
	"fmt"
	"log"
	"os"
)

const (
	pPrefix = "hdb.protocol"
)

var (
	debug bool
	trace bool
)

//nolint:gochecknoinits
func init() {
	flag.BoolVar(&debug, fmt.Sprintf("%s.debug", pPrefix), false, "enabling hdb protocol debugging mode")
	flag.BoolVar(&trace, fmt.Sprintf("%s.trace", pPrefix), false, "enabling hdb protocol trace")
}

type pLogger struct {
	log *log.Logger
}

func newPLogger() *pLogger {
	return &pLogger{
		log: log.New(os.Stderr, fmt.Sprintf("%s ", pPrefix), log.Ldate|log.Ltime|log.Lshortfile),
	}
}

func (l *pLogger) Printf(format string, v ...interface{}) {
	l.log.Output(2, fmt.Sprintf(format, v...))
}

func (l *pLogger) Fatalf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	l.log.Output(2, fmt.Sprintf(format, v...))
	if debug {
		panic(s)
	}
	os.Exit(1)
}

var plog = newPLogger()

// store os.Stdout
// executing test examples will override os.Stdout
// and fail consequently if trace output is added
var stdout = os.Stdout

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

type traceLogger interface {
	Log(v interface{})
}

type traceLog struct {
	prefix string
	log    *log.Logger
}

func (l *traceLog) Log(v interface{}) {
	var msg string

	switch v.(type) {
	case *initRequest, *initReply:
		msg = fmt.Sprintf("%sINI %s", l.prefix, v)
	case *messageHeader:
		msg = fmt.Sprintf("%sMSG %s", l.prefix, v)
	case *segmentHeader:
		msg = fmt.Sprintf(" SEG %s", v)
	case *partHeader:
		msg = fmt.Sprintf(" PAR %s", v)
	default:
		msg = fmt.Sprintf("     %s", v)
	}
	l.log.Output(2, msg)
}

type noTraceLog struct{}

func (l *noTraceLog) Log(v interface{}) {}

var noTrace = new(noTraceLog)

func newTraceLogger(upStream bool) traceLogger {
	if !trace {
		return noTrace
	}
	return &traceLog{
		prefix: streamPrefix(upStream),
		log:    log.New(stdout, fmt.Sprintf("%s ", pPrefix), log.Ldate|log.Ltime),
	}
}
