//go:build !unit && !go1.27

package driver

import (
	"bytes"
	"log"
	"runtime"
)

// copied from runtime/debug.
func stack() []byte {
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, true) // all stacks
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}

// detectGoroutineLeaks reports potential goroutine leaks. On Go 1.26 the runtime
// goroutineleak profile is not available, so fall back to counting goroutine
// blocks in a full stack dump (heuristic; may include benign transient goroutines
// such as database/sql's connectionOpener, which DB.Close() signals but does not join).
func detectGoroutineLeaks() {
	stack := stack()
	numLeaking := bytes.Count(stack, []byte{'\n', '\n'}) // count newlines.
	if numLeaking > 0 {
		log.Printf("\nnumber of leaking go routines: %d\n%s\n", numLeaking, stack)
	}
}
