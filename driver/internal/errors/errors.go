// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Package errors defines errors used in different driver packages.
package errors

import (
	"errors"
)

// ErrFatal is the fatal error instance to be wrapped into or returned by Is() in case the error is a fatal error.
// A fatalError signals that the connection is broken, so the hdb driver should set the connection in driver.ErrBadConn mode.
var ErrFatal = errors.New("fatal error")
