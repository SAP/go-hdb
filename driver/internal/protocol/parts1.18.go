//go:build go1.18
// +build go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"reflect"
)

var (
	typeOfClientContext    = reflect.TypeOf((*Options[ClientContextOption])(nil)).Elem()
	typeOfConnectOptions   = reflect.TypeOf((*Options[ConnectOption])(nil)).Elem()
	typeOfTransactionflags = reflect.TypeOf((*Options[transactionFlagType])(nil)).Elem()
	typeOfStatementContext = reflect.TypeOf((*Options[statementContextType])(nil)).Elem()
	typeOfDBConnectInfo    = reflect.TypeOf((*Options[DBConnectInfoType])(nil)).Elem()
)

func (ops Options[K]) kind() PartKind {
	switch reflect.TypeOf(ops) {
	case typeOfClientContext:
		return PkClientContext
	case typeOfConnectOptions:
		return PkConnectOptions
	case typeOfTransactionflags:
		return PkTransactionFlags
	case typeOfStatementContext:
		return PkStatementContext
	case typeOfDBConnectInfo:
		return PkDBConnectInfo
	default:
		panic("invalid options type") // should never happen
	}
}

// check if part types implement partWriter interface
var _ partWriter = (*Options[ClientContextOption])(nil) // sufficient to check one option.

// check if part types implement partReader interface
var _ partReader = (*Options[ClientContextOption])(nil) // sufficient to check one option.

var optionsPartTypeMap = map[PartKind]reflect.Type{
	PkClientContext:    typeOfClientContext,
	PkConnectOptions:   typeOfConnectOptions,
	PkTransactionFlags: typeOfTransactionflags,
	PkStatementContext: typeOfStatementContext,
	PkDBConnectInfo:    typeOfDBConnectInfo,
}
