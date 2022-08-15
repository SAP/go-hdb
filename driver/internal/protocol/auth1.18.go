//go:build go1.18
// +build go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package protocol

import (
	"github.com/SAP/go-hdb/driver/internal/protocol/auth"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func (m authMethods) order() []auth.Method {
	methods := maps.Values(m)
	slices.SortFunc(methods, func(a, b auth.Method) bool { return a.Order() < b.Order() })
	return methods
}
