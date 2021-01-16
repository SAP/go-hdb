// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

//go:generate stringer -type=segmentKind

type segmentKind int8

const (
	skInvalid segmentKind = 0
	skRequest segmentKind = 1
	skReply   segmentKind = 2
	skError   segmentKind = 5
)
