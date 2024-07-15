// Package assert implements assertion functions.
package assert

import "fmt"

// Equal panics in case a does not equal b.
func Equal[T comparable](s string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s: %v %v", s, a, b))
	}
}

// True panics in case b is false.
func True(s string, b bool) {
	if !b {
		panic(fmt.Sprintf("%s: %v - expected %v", s, b, true))
	}
}

// Panic panics.
func Panic(s string) {
	panic(s)
}

// Panicf panics.
func Panicf(format string, a ...any) {
	panic(fmt.Sprintf(format, a...))
}

// TPanicf panics.
func TPanicf[T any](format string, a ...any) T {
	panic(fmt.Sprintf(format, a...))
}

// T2Panicf panics.
func T2Panicf[T1 any, T2 any](format string, a ...any) (T1, T2) {
	panic(fmt.Sprintf(format, a...))
}
