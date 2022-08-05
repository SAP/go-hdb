//go:build go1.18
// +build go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package protocol

import (
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func (m authMethods) order() []authMethod {
	methods := maps.Values(m)
	slices.SortFunc(methods, func(a, b authMethod) bool { return a.order() < b.order() })
	return methods
}
