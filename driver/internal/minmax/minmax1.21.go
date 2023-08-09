//go:build go1.21

// Package minmax provides a compatibility layer until g1.20 is out of maintenance.
package minmax

// MinInt returns the minimum of a and b.
func MinInt(a, b int) int { return min(a, b) }

// MaxInt returns the maximum of a and b.
func MaxInt(a, b int) int { return max(a, b) }
