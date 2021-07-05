// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

//go:generate stringer -type=dbConnectInfoType

//dbconnectinfotype
type dbConnectInfoType int8

const (
	ciDatabaseName dbConnectInfoType = 1 // string
	ciHost         dbConnectInfoType = 2 // string
	ciPort         dbConnectInfoType = 3 // int4
	ciIsConnected  dbConnectInfoType = 4 // bool
)
