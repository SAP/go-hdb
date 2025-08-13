//go:build go1.25

// Package wgroup provides compatibility on go1.24 and go1.25.
package wgroup

import "sync"

// Go is a wrapper for sync.WaitGroup.Go and will be deleted if only go versions >= 1.25 are supported.
func Go(wg *sync.WaitGroup, f func()) {
	wg.Go(f)
}
