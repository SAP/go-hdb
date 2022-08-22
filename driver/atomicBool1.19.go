//go:build go1.19
// +build go1.19

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package driver

import (
	"sync/atomic"
)

// aliase
type atomicBool = atomic.Bool
