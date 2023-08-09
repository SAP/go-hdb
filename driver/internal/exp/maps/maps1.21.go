//go:build go1.21

// Delete after go1.20 is out of maintenance.

// Package maps provides a compatibility layer until g1.20 is out of maintenance.
package maps

import (
	gomaps "maps"
)

// Clone is used as a wrapper to maps/Clone.
func Clone[M ~map[K]V, K comparable, V any](m M) M { return gomaps.Clone(m) }
