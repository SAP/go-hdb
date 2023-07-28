//go:build !go1.21

// Delete after go1.20 is out of maintenance.

package auth

import (
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func compareByte(x, y byte) int {
	switch {
	case x == y:
		return 0
	case x < y:
		return -1
	default:
		return 1
	}
}

// Order returns an ordered method slice.
func (m Methods) Order() []Method {
	methods := maps.Values(m)
	slices.SortFunc(methods, func(m1, m2 Method) int { return compareByte(m1.Order(), m2.Order()) })
	return methods
}
