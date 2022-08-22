//go:build !go1.18
// +build !go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package driver

import (
	"sort"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// aliase
type connectOptions = p.ConnectOptions
type dbConnectInfo = p.DBConnectInfo
type clientContext = p.ClientContext

// generic alternatives
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
func sortSliceFloat64(s []float64) { sort.Slice(s, func(i, j int) bool { return s[i] < s[j] }) }
func compactSliceFloat64(s []float64) []float64 {
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
func binarySearchSliceFloat64(s []float64, x float64) int {
	return sort.Search(len(s), func(i int) bool { return s[i] >= x })
}
