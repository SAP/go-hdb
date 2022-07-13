// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

// TODO: to delete
/*
https://cryptography.fandom.com/wiki/Blind_signature
https://www.sohamkamani.com/golang/rsa-encryption/
https://knowledge.digicert.com/solution/SO16297.html
https://github.com/zakjan/cert-chain-resolver/blob/c6b0b792af9a/certUtil/chain.go#L20
*/

const (
	x509ServerNonceSize = 64
)

// authX509 implements mnClientCert.
type authX509 struct {
	cert, key   []byte
	serverNonce []byte
	logonName   string
}

func newAuthX509(cert, key []byte) authMethod {
	return &authX509{cert: cert, key: key}
}

func (a *authX509) String() string {
	return fmt.Sprintf("method %s cert %v key %v", a.methodName(), a.cert, a.key)
}

func (a *authX509) methodName() string { return mnX509 }

func (a *authX509) prepareInitReq(prms *authPrms) {
	prms.addString(a.methodName())
	prms.addEmpty()
}

func (a *authX509) initRepDecode(d *authDecoder) error {
	var err error
	if a.serverNonce, err = d.bytes(); err != nil {
		return err
	}
	if len(a.serverNonce) != x509ServerNonceSize {
		return fmt.Errorf("invalid server nonce size %d - expected %d", len(a.serverNonce), x509ServerNonceSize)
	}
	return nil
}

func (a *authX509) prepareFinalReq(prms *authPrms) error {
	prms.addString(a.methodName())

	subPrms := prms.addPrms()

	certPEMBlocks, err := decodeClientCert(a.cert)
	if err != nil {
		return err
	}

	numBlocks := len(certPEMBlocks)

	message := bytes.NewBuffer(certPEMBlocks[0].Bytes)

	subPrms.addBytes(certPEMBlocks[0].Bytes)

	if numBlocks == 1 {
		subPrms.addEmpty()
	} else {
		chainPrms := subPrms.addPrms()
		for _, block := range certPEMBlocks[1:] {
			message.Write(block.Bytes)
			chainPrms.addBytes(block.Bytes)
		}
	}

	message.Write(a.serverNonce)

	certKeyBlock, err := decodeClientKey(a.key)
	if err != nil {
		return err
	}

	signature, err := signRSA(certKeyBlock, message)
	if err != nil {
		return err
	}
	subPrms.addBytes(signature)
	return nil
}

func (a *authX509) finalRepDecode(d *authDecoder) error {
	if err := d.numPrm(2); err != nil {
		return err
	}
	methodName, err := d.string()
	if err != nil {
		return err
	}
	if methodName != a.methodName() {
		return fmt.Errorf("invalid method name %s - expected %s", methodName, a.methodName())
	}
	d.subSize()
	if err := d.numPrm(1); err != nil {
		return err
	}
	a.logonName, err = d.cesu8String()
	return err
}

func decodePEM(data []byte) ([]*pem.Block, error) {
	var blocks []*pem.Block
	block, rest := pem.Decode(data)
	for block != nil {
		blocks = append(blocks, block)
		block, rest = pem.Decode(rest)
	}
	return blocks, nil
}

func decodeClientCert(data []byte) ([]*pem.Block, error) {
	blocks, err := decodePEM(data)
	if err != nil {
		return nil, err
	}
	switch {
	case blocks == nil:
		return nil, errors.New("invalid client cert")
	case len(blocks) < 1:
		return nil, fmt.Errorf("invalid number of blocks in cert file %d - expected min 1", len(blocks))
	}
	return blocks, nil
}

const (
	numClientKeyBlocks = 1
	pemTypePrivateKey  = "PRIVATE KEY"
)

func decodeClientKey(data []byte) (*pem.Block, error) {
	blocks, err := decodePEM(data)
	if err != nil {
		return nil, err
	}
	switch {
	case blocks == nil:
		return nil, fmt.Errorf("invalid client key")
	case len(blocks) != numClientKeyBlocks:
		return nil, fmt.Errorf("invalid number of blocks in key file %d - expected %d", len(blocks), numClientKeyBlocks)
	}
	block := blocks[0]
	if block.Type != pemTypePrivateKey {
		return nil, fmt.Errorf("invalid PEM type %s - expected %s", block.Type, pemTypePrivateKey)
	}
	return block, nil
}

func signRSA(certKeyBlock *pem.Block, message *bytes.Buffer) ([]byte, error) {
	rsaPrivateKey, err := x509.ParsePKCS1PrivateKey(certKeyBlock.Bytes)
	if err != nil {
		return nil, err
	}
	// see example https://pkg.go.dev/crypto/rsa#SignPKCS1v15
	rng := rand.Reader
	hashed := sha256.Sum256(message.Bytes())

	return rsa.SignPKCS1v15(rng, rsaPrivateKey, crypto.SHA256, hashed[:])
}
