// SPDX-FileCopyrightText: 2014-2024 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package ldap

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// Initial LDAP authentication reply.
// Taken from "SAP HANA SQL Command Network Protocol Reference" version 1.2 chapter 3.9.2.2
//
// Wire format:
//	Field            Data Type        Description
//	FIELDCOUNT       I2               Number of fields within this request.
//	LENGTHINDICATOR  B1               Length of the METHODNAME field.
//	METHODNAME       B[DATALENGTH]    Method name "LDAP".
//	LENGTHINDICATOR  B1-2             Length of the SERVERCHALLENGE field.
//	SERVERCHALLENGE  B[DATALENGTH]    Server challenge subparameters (see ServerChallenge).
//
// Missing fields are set elsewhere (e.g., when deserializing).
type InitialResponse struct {
	ServerChallenge ServerChallenge
}

// LDAP server challenge data.
// Taken from "SAP HANA SQL Command Network Protocol Reference" version 1.2 chapter 3.9.2.2
// The spec does not document the server challenge in detail.
//
// Wire format:
//	Field              Data Type        Description
//	FIELDCOUNT         I2               Number of fields within this request.
//	LENGTHINDICATOR    B1               Length of the CLIENTNONCE field.
//	CLIENTNONCE        B[DATALENGTH]    Client nonce that was sent in the initial request.
//	LENGTHINDICATOR    B1               Length of the SERVERNONCE field.
//	SERVERNONCE        B[DATALENGTH]    Server nonce.
//	LENGTHINDICATOR    B1-2             Length of the SERVERPUBLICKEY field.
//	SERVERPUBLICKEY    B[DATALENGTH]    Server public key.
//	LENGTHINDICATOR    B1               Length of the CAPABILITYRESULT field.
//	CAPABILITYRESULT   B[DATALENGTH]    Capability chosen by the server from the client request.
//
// The structure is derived from https://github.com/SAP/node-hdb/blob/master/lib/protocol/auth/LDAP.js
type ServerChallenge struct {
	ClientNonce      [64]byte
	ServerNonce      [64]byte
	ServerPublicKey  *rsa.PublicKey
	CapabilityResult [8]byte
}

func NewServerChallenge(clientNonce, serverNonce, serverPublicKeyPEM, capabilityResult []byte) (*ServerChallenge, error) {
	if len(clientNonce) != 64 {
		return nil, fmt.Errorf("invalid client nonce size %d - expected 64", len(clientNonce))
	}
	if len(serverNonce) != 64 {
		return nil, fmt.Errorf("invalid server nonce size %d - expected 64", len(serverNonce))
	}
	if len(capabilityResult) == 0 {
		return nil, fmt.Errorf("empty server capabilities")
	}
	// This magic number is called "default capabilities" in node-hdb.
	if capabilityResult[0] != 0x01 {
		return nil, fmt.Errorf("unknown server capabilities 0x%08x", capabilityResult[:])
	}
	if len(serverPublicKeyPEM) == 0 {
		return nil, fmt.Errorf("server did not provide RSA public key")
	}

	serverPublicKey, err := parseRSAPublicKey(serverPublicKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to parse server public key: %w", err)
	}

	var sc ServerChallenge
	copy(sc.ClientNonce[:], clientNonce)
	copy(sc.ServerNonce[:], serverNonce)
	sc.ServerPublicKey = serverPublicKey
	copy(sc.CapabilityResult[:], capabilityResult)

	return &sc, nil
}

func parseRSAPublicKey(data []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("invalid PEM data")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not an RSA public key")
	}

	return rsaPub, nil
}
