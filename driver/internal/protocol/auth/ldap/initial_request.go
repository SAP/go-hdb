// SPDX-FileCopyrightText: 2014-2024 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package ldap

import "crypto/rand"

// Initial LDAP authentication request.
// Taken from "SAP HANA SQL Command Network Protocol Reference" version 1.2 chapter 3.9.2.2
//
// Wire format:
//	Field            Data Type        Description
//	FIELDCOUNT       I2               Number of fields within the request.
//	LENGTHINDICATOR  B1               Length of the USERNAME field.
//	USERNAME         B[DATALENGTH]    Name of the user.
//	LENGTHINDICATOR  B1               Length of the METHODNAME field.
//	METHODNAME       B[DATALENGTH]    Method name "LDAP".
//	LENGTHINDICATOR  B1               Length of the CLIENTCHALLENGE field.
//	CLIENTCHALLENGE  B[DATALENGTH]    Client challenge (see ClientChallenge).
//
// Missing fields are set elsewhere (e.g., when serializing).
type InitialRequest struct {
	ClientChallenge ClientChallenge
}

// LDAP client challenge data.
// Taken from "SAP HANA SQL Command Network Protocol Reference" version 1.2 chapter 3.9.2.2
// The spec does not document the client challenge in detail.
//
// Wire format:
//	Field
//	FIELDCOUNT
//	LENGTHINDICATOR
//	CLIENTNONCE
//	LENGTHINDICATOR
//	CAPABILITIES
//
// The structure is derived from https://github.com/SAP/node-hdb/blob/master/lib/protocol/auth/LDAP.js
type ClientChallenge struct {
	ClientNonce  [64]byte
	Capabilities [8]byte
}

func NewClientChallenge() *ClientChallenge {
	c := &ClientChallenge{}

	rand.Read(c.ClientNonce[:])

	// This magic number is called "default capabilities" in node-hdb.
	c.Capabilities[0] = 0x01

	return c
}
