// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

//go:generate stringer -type=messageType

type messageType int8

const (
	mtNil             messageType = 0
	mtExecuteDirect   messageType = 2
	mtPrepare         messageType = 3
	mtAbapStream      messageType = 4
	mtXAStart         messageType = 5
	mtXAJoin          messageType = 6
	mtExecute         messageType = 13
	mtWriteLob        messageType = 16
	mtReadLob         messageType = 17
	mtFindLob         messageType = 18
	mtAuthenticate    messageType = 65
	mtConnect         messageType = 66
	mtCommit          messageType = 67
	mtRollback        messageType = 68
	mtCloseResultset  messageType = 69
	mtDropStatementID messageType = 70
	mtFetchNext       messageType = 71
	mtFetchAbsolute   messageType = 72
	mtFetchRelative   messageType = 73
	mtFetchFirst      messageType = 74
	mtFetchLast       messageType = 75
	mtDisconnect      messageType = 77
	mtExecuteITab     messageType = 78
	mtFetchNextITab   messageType = 79
	mtInsertNextITab  messageType = 80
	mtBatchPrepare    messageType = 81
	mtDBConnectInfo   messageType = 82
	mtXopenXAStart    messageType = 83
	mtXopenXAEnd      messageType = 84
	mtXopenXAPrepare  messageType = 85
	mtXopenXACommit   messageType = 86
	mtXopenXARollback messageType = 87
	mtXopenXARecover  messageType = 88
	mtXopenXAForget   messageType = 89
)

func (mt messageType) clientInfoSupported() bool {
	/*
		mtConnect is only supported since 2.00.042
		As server version is only available after connect we do not use it
		to support especially version 1.00.122 until maintenance
		will end in sommer 2021

		return mt == mtConnect || mt == mtPrepare || mt == mtExecuteDirect || mt == mtExecute
	*/
	return mt == mtPrepare || mt == mtExecuteDirect || mt == mtExecute
}
