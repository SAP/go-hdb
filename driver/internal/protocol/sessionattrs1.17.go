//go:build !go1.18
// +build !go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package protocol

// cloneSessionVariables returns a shallow clone of a session variables map.
func cloneSessionVariables(sv map[string]string) map[string]string {
	if sv == nil {
		return nil
	}
	clone := map[string]string{}
	for k, v := range sv {
		clone[k] = v
	}
	return clone
}
