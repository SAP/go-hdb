//go:build go1.18
// +build go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package protocol

func resizeSlice[S ~[]E, E any](s S, n int) S {
	switch {
	case s == nil:
		s = make(S, n)
	case n > cap(s):
		s = append(s, make(S, n-cap(s))...)
	}
	return s[:n]
}

func resizeHdbErrorSlice[S ~[]E, E any](s S, n int) S            { return resizeSlice(s, n) }
func resizeTopologyInformationSlice[S ~[]E, E any](s S, n int) S { return resizeSlice(s, n) }
func resizeByteSlice[S ~[]E, E any](s S, n int) S                { return resizeSlice(s, n) }
func resizeFieldValues[S ~[]E, E any](s S, n int) S              { return resizeSlice(s, n) }
