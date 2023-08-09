//go:build go1.21

// Delete after go1.20 is out of maintenance.

// Package cmp provides a compatibility layer until g1.20 is out of maintenance.
package cmp

import gocmp "cmp"

// Compare is used as a wrapper to cmp/Compare.
func Compare[T gocmp.Ordered](x, y T) int { return gocmp.Compare(x, y) }
