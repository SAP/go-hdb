//go:build !go1.21

// Delete after go1.20 is out of maintenance.

// Package maps provides a compatibility layer until g1.20 is out of maintenance.
package maps

import (
	expsmaps "golang.org/x/exp/maps"
)

// Clone is used as a wrapper to exp/maps/Clone.
func Clone[M ~map[K]V, K comparable, V any](m M) M { return expsmaps.Clone(m) }
