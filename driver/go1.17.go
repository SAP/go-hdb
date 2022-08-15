//go:build !go1.18
// +build !go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package driver

import (
	"sort"
)

func cloneStringSlice(s []string) []string {
	if s == nil {
		return nil
	}
	return append([]string{}, s...)
}

func cloneStringStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	clone := map[string]string{}
	for k, v := range m {
		clone[k] = v
	}
	return clone
}

/*
	func cloneSliceUint64(s []uint64) []uint64 {
		// Preserve nil in case it matters.
		if s == nil {
			return nil
		}
		return append([]uint64{}, s...)
	}
*/
func sortSliceUint64(s []uint64) { sort.Slice(s, func(i, j int) bool { return s[i] < s[j] }) }
func compactSliceUint64(s []uint64) []uint64 {
	if len(s) == 0 {
		return s
	}
	i := 1
	last := s[0]
	for _, v := range s[1:] {
		if v != last {
			s[i] = v
			i++
			last = v
		}
	}
	return s[:i]
}
func binarySearchSliceUint64(s []uint64, x uint64) int {
	return sort.Search(len(s), func(i int) bool { return s[i] >= x })
}
