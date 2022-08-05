//go:build go1.18
// +build go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package protocol

import (
	"golang.org/x/exp/maps"
)

// cloneSessionVariables returns a shallow clone of a session variables map.
func cloneSessionVariables(sv map[string]string) map[string]string { return maps.Clone(sv) }
