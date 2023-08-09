//go:build go1.21

// Delete after go1.20 is out of maintenance.

// Package slices provides a compatibility layer until g1.20 is out of maintenance.
package slices

import (
	"cmp"
	goslices "slices"

	expslices "golang.org/x/exp/slices"
)

// BinarySearch is used as a wrapper to slices/BinarySearch
func BinarySearch[S ~[]E, E cmp.Ordered](x S, target E) (int, bool) {
	return goslices.BinarySearch(x, target)
}

// Sort is used as a wrapper to slices/Sort.
func Sort[S ~[]E, E cmp.Ordered](x S) { goslices.Sort(x) }

// SortFunc is used as a wrapper to slices/SortFunc.
func SortFunc[S ~[]E, E any](x S, cmp func(a, b E) int) { expslices.SortFunc(x, cmp) }

// Compact is used as a wrapper to slices/Compact.
func Compact[S ~[]E, E comparable](s S) S { return goslices.Compact(s) }
