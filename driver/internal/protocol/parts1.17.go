//go:build !go1.18
// +build !go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"reflect"
)

func (ClientContext) kind() PartKind    { return PkClientContext }
func (ConnectOptions) kind() PartKind   { return PkConnectOptions }
func (transactionFlags) kind() PartKind { return PkTransactionFlags }
func (statementContext) kind() PartKind { return PkStatementContext }
func (DBConnectInfo) kind() PartKind    { return PkDBConnectInfo }

// check if part types implement partWriter interface
var (
	_ partWriter = (*ClientContext)(nil)
	_ partWriter = (*ConnectOptions)(nil)
	_ partWriter = (*DBConnectInfo)(nil)
)

// check if part types implement partReader interface
var (
	_ partReader = (*ClientContext)(nil)
	_ partReader = (*ConnectOptions)(nil)
	_ partReader = (*transactionFlags)(nil)
	_ partReader = (*statementContext)(nil)
	_ partReader = (*DBConnectInfo)(nil)
)

var optionsPartTypeMap = map[PartKind]reflect.Type{
	PkClientContext:    reflect.TypeOf((*ClientContext)(nil)).Elem(),
	PkConnectOptions:   reflect.TypeOf((*ConnectOptions)(nil)).Elem(),
	PkTransactionFlags: reflect.TypeOf((*transactionFlags)(nil)).Elem(),
	PkStatementContext: reflect.TypeOf((*statementContext)(nil)).Elem(),
	PkDBConnectInfo:    reflect.TypeOf((*DBConnectInfo)(nil)).Elem(),
}
