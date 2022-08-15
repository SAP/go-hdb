//go:build go1.18
// +build go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package driver

import (
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func cloneStringSlice(s []string) []string                       { return slices.Clone(s) }
func cloneStringStringMap(m map[string]string) map[string]string { return maps.Clone(m) }

// func cloneSliceUint64(s []uint64) []uint64             { return slices.Clone(s) }
func sortSliceUint64(s []uint64)                       { slices.Sort(s) }
func compactSliceUint64(s []uint64) []uint64           { return slices.Compact(s) }
func binarySearchSliceUint64(s []uint64, x uint64) int { i, _ := slices.BinarySearch(s, x); return i }
