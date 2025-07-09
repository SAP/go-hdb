//go:build !go1.25

// Package wgroup wraps WaitGroup Go until anly go versions >= 1.25 are going to be supported.
package wgroup

import "sync"

// Go is a wrapper for sync.WaitGroup.Go and will be deleted if only go versions >= 1.25 are supported.
func Go(wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}
