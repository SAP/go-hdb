// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

// ClientContextOption represents a client context option.
type ClientContextOption int8

// ClientContextOption constants.
const (
	CcoClientVersion            ClientContextOption = 1
	CcoClientType               ClientContextOption = 2
	CcoClientApplicationProgram ClientContextOption = 3
)

// DBConnectInfoType represents a database connect info type.
type DBConnectInfoType int8

// DBConnectInfoType constants.
const (
	CiDatabaseName DBConnectInfoType = 1 // string
	CiHost         DBConnectInfoType = 2 // string
	CiPort         DBConnectInfoType = 3 // int4
	CiIsConnected  DBConnectInfoType = 4 // bool
)

type statementContextType int8

const (
	scStatementSequenceInfo statementContextType = 1
	scServerExecutionTime   statementContextType = 2
)

// transaction flags
type transactionFlagType int8

const (
	tfRolledback                      transactionFlagType = 0
	tfCommited                        transactionFlagType = 1
	tfNewIsolationLevel               transactionFlagType = 2
	tfDDLCommitmodeChanged            transactionFlagType = 3
	tfWriteTransactionStarted         transactionFlagType = 4
	tfNowriteTransactionStarted       transactionFlagType = 5
	tfSessionClosingTransactionError  transactionFlagType = 6
	tfSessionClosingTransactionErrror transactionFlagType = 7
	tfReadOnlyMode                    transactionFlagType = 8
)
