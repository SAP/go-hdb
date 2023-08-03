//go:build !go1.21

// Delete after go1.20 is out of maintenance.

// Package slices provides a compatibility layer until g1.20 is out of maintenance.
package slices

import (
	"golang.org/x/exp/constraints"
	expslices "golang.org/x/exp/slices"
)

// BinarySearch is used as a wrapper to exp/slices/BinarySearch
func BinarySearch[S ~[]E, E constraints.Ordered](x S, target E) (int, bool) {
	return expslices.BinarySearch(x, target)
}

// Sort is used as a wrapper to exp/slices/Sort.
func Sort[S ~[]E, E constraints.Ordered](x S) { expslices.Sort(x) }

// SortFunc is used as a wrapper to exp/slices/SortFunc.
func SortFunc[S ~[]E, E any](x S, cmp func(a, b E) int) { expslices.SortFunc(x, cmp) }

// Compact is used as a wrapper to exp/slices/Compact.
func Compact[S ~[]E, E comparable](s S) S { return expslices.Compact(s) }
