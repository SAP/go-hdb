// SPDX-FileCopyrightText: 2014-2024 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package ldap

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // SHA-1 is safe in OAEP (requires preimage resistance, not collision resistance)
)

// Final LDAP authentication request.
// Taken from "SAP HANA SQL Command Network Protocol Reference" version 1.2 chapter 3.9.2.2
//
// Wire format:
//	Field            Data Type        Description
//	FIELDCOUNT       I2               Number of fields within this request.
//	LENGTHINDICATOR  B1               Length of the USERNAME field.
//	USERNAME         B[DATALENGTH]    Username.
//	LENGTHINDICATOR  B1               Length of the METHODNAME field.
//	METHODNAME       B[DATALENGTH]    Method name "LDAP".
//	LENGTHINDICATOR  B1-2             Length of the CLIENTPROOF field.
//	CLIENTPROOF      B[DATALENGTH]    Client proof (see ClientProof).
//
// Missing fields are set elsewhere (e.g., when serializing).
type FinalRequest struct {
	ClientProof ClientProof
}

// LDAP client proof data.
// Taken from "SAP HANA SQL Command Network Protocol Reference" version 1.2 chapter 3.9.2.2
// The spec does not document the client proof in detail.
//
// Wire format:
//	Field                 Data Type        Description
//	FIELDCOUNT            I2               Number of fields within this request.
//	LENGTHINDICATOR       B1-2             Length of the ENCRYPTEDSESSIONKEY field.
//	ENCRYPTEDSESSIONKEY   B[DATALENGTH]    RSAEncrypt(publicKey, SESSIONKEY + SERVERNONCE).
//	LENGTHINDICATOR       B1-2             Length of the ENCRYPTEDPASSWORD field.
//	ENCRYPTEDPASSWORD     B[DATALENGTH]    AES256Encrypt(SESSIONKEY, PASSWORD + SERVERNONCE).
type ClientProof struct {
	EncryptedSessionKey []byte
	EncryptedPassword   []byte
}

const sessionKeySize = 32 // AES-256 key size

func NewClientProof(password string, sc *ServerChallenge) (*ClientProof, error) {
	// Generate random session key
	sessionKey := make([]byte, sessionKeySize)
	rand.Read(sessionKey) //nolint:errcheck

	encryptedSessionKey, err := encryptSessionKey(sessionKey, sc)
	if err != nil {
		return nil, err
	}

	encryptedPassword, err := encryptPassword(password, sessionKey, sc)
	if err != nil {
		return nil, err
	}

	return &ClientProof{
		EncryptedSessionKey: encryptedSessionKey,
		EncryptedPassword:   encryptedPassword,
	}, nil
}

// Implementation details are based on
// https://github.com/SAP/node-hdb/blob/master/lib/protocol/auth/LDAP.js
func encryptSessionKey(sessionKey []byte, sc *ServerChallenge) ([]byte, error) {
	plaintext := make([]byte, len(sessionKey)+len(sc.ServerNonce))
	copy(plaintext[:len(sessionKey)], sessionKey)
	copy(plaintext[len(sessionKey):], sc.ServerNonce[:])

	ciphertext, err := rsa.EncryptOAEP(
		sha1.New(), //nolint:gosec
		rand.Reader,
		sc.ServerPublicKey,
		plaintext,
		nil, // no label
	)
	if err != nil {
		return nil, err
	}

	return ciphertext, nil
}

// Implementation details are based on
// https://github.com/SAP/node-hdb/blob/master/lib/protocol/auth/LDAP.js
func encryptPassword(password string, sessionKey []byte, sc *ServerChallenge) ([]byte, error) {
	passwordBytes := []byte(password)

	plaintext := make([]byte, len(passwordBytes)+1+len(sc.ServerNonce))
	copy(plaintext, passwordBytes)
	plaintext[len(passwordBytes)] = 0x00 // Magic separator byte
	copy(plaintext[len(passwordBytes)+1:], sc.ServerNonce[:])

	plaintext = pkcs7Pad(plaintext, aes.BlockSize)

	block, err := aes.NewCipher(sessionKey)
	if err != nil {
		return nil, err
	}

	iv := sc.ServerNonce[:aes.BlockSize]

	ciphertext := make([]byte, len(plaintext))
	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext, plaintext)

	return ciphertext, nil
}

// pkcs7Pad pads data to a multiple of blockSize using PKCS7 padding.
func pkcs7Pad(data []byte, blockSize int) []byte {
	padding := blockSize - (len(data) % blockSize)
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(data, padtext...)
}
