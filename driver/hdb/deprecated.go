// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Package hdb defines hdb specific data structures used by the driver.
package hdb

import "github.com/SAP/go-hdb/driver"

// DBConnectInfo defines the connection information attributes returned by hdb.
//
// Deprecated: Please use driver.DBConnectinfo instead.
type DBConnectInfo = driver.DBConnectInfo

// Version is representing a hdb version.
//
// Deprecated: Please use driver.Version instead.
type Version = driver.Version
