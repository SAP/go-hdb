//go:build go1.18
// +build go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package driver

import (
	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// aliase
type connectOptions = p.Options[p.ConnectOption]
type dbConnectInfo = p.Options[p.DBConnectInfoType]
type clientContext = p.Options[p.ClientContextOption]

// call generic functions
func cloneStringStringMap(m map[string]string) map[string]string { return maps.Clone(m) }
func sortSliceFloat64(s []float64)                               { slices.Sort(s) }
func compactSliceFloat64(s []float64) []float64                  { return slices.Compact(s) }
func binarySearchSliceFloat64(s []float64, x float64) int {
	i, _ := slices.BinarySearch(s, x)
	return i
}
