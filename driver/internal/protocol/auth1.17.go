//go:build !go1.18
// +build !go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package protocol

import "sort"

func (m authMethods) order() []authMethod {
	methods := make([]authMethod, 0, len(m))
	for _, method := range m {
		methods = append(methods, method)
	}
	sort.Slice(methods, func(i, j int) bool { return methods[i].order() < methods[j].order() })
	return methods
}
