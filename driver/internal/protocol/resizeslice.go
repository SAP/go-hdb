// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

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
