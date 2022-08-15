//go:build go1.18
// +build go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package driver

import (
	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// aliase
type connectOptions = p.Options[p.ConnectOption]
type dbConnectInfo = p.Options[p.DBConnectInfoType]
type clientContext = p.Options[p.ClientContextOption]
