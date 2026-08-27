//go:build !unit && go1.27

package driver

import (
	"log"
	"runtime/pprof"
)

// detectGoroutineLeaks writes the runtime goroutineleak profile (Go 1.27+). It
// reports only goroutines blocked on a concurrency primitive that can never be
// unblocked, so transient runnable goroutines - such as database/sql's
// connectionOpener, which DB.Close() signals but does not join - are not reported.
// The profile is always written (as net/http/pprof does); its "total N" header
// line states the number of leaked goroutines.
func detectGoroutineLeaks() {
	p := pprof.Lookup("goroutineleak")
	if p == nil { // safety net; should exist on go1.27+
		return
	}
	p.WriteTo(log.Writer(), 1) //nolint: errcheck
}
