//go:build !go1.18
// +build !go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package driver

import (
	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// aliase
type connectOptions = p.ConnectOptions
type dbConnectInfo = p.DBConnectInfo
type clientContext = p.ClientContext
