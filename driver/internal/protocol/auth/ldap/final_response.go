// SPDX-FileCopyrightText: 2014-2024 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package ldap

import "fmt"

// Final LDAP authentication reply.
// Taken from "SAP HANA SQL Command Network Protocol Reference" version 1.2 chapter 3.9.2.2
//
// Wire format:
//	Field            Data Type        Description
//	FIELDCOUNT       I2               Number of fields within this request.
//	LENGTHINDICATOR  B1               Length of the METHODNAME field.
//	METHODNAME       B[DATALENGTH]    Method name "LDAP".
//	LENGTHINDICATOR  B1               Length of the SERVERPROOF field.
//	SERVERPROOF      B[1]             Authentication result: SUCCESS or FAIL.
//
type FinalResponse struct {
	MethodName  string
	ServerProof []byte
}

func NewFinalResponse(methodName string, serverProof []byte) (*FinalResponse, error) {
	if len(serverProof) > 0 {
		return nil, fmt.Errorf("server proof failed: %v", serverProof)
	}
	return &FinalResponse{
		MethodName: methodName,
		ServerProof: serverProof[:],
	}, nil
}
