// Package slices provides generic slice functions.
package slices

// AllFunc returns true if for all elements of a slice fn evaluates to true, false otherwise.
func AllFunc[S ~[]E, E any](x S, fn func(a E) bool) bool {
	for _, e := range x {
		if !fn(e) {
			return false
		}
	}
	return true
}

// AnyFunc returns true if for at least one element of a slice fn evaluates to true, false otherwise.
func AnyFunc[S ~[]E, E any](x S, fn func(a E) bool) bool {
	for _, e := range x {
		if fn(e) {
			return true
		}
	}
	return false
}

// RangeFunc executes function fn on all elements of a slice.
func RangeFunc[S ~[]E, E any](x S, fn func(a E)) {
	for _, e := range x {
		fn(e)
	}
}
