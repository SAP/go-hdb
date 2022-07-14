// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
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
	prms.addEmpty() // empty username
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

	signature, err := sign(certKeyBlock, message)
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

// encryptedBlock tells whether a private key is
// encrypted by examining its Proc-Type header
// for a mention of ENCRYPTED
// according to RFC 1421 Section 4.6.1.1.
func encryptedBlock(block *pem.Block) bool {
	return strings.Contains(block.Headers["Proc-Type"], "ENCRYPTED")
}

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
	if encryptedBlock(block) {
		return nil, errors.New("client key is password encrypted")
	}
	return block, nil
}

func getSigner(certKeyBlock *pem.Block) (crypto.Signer, error) {
	switch certKeyBlock.Type {
	case "RSA PRIVATE KEY":
		return x509.ParsePKCS1PrivateKey(certKeyBlock.Bytes)
	case "PRIVATE KEY":
		key, err := x509.ParsePKCS8PrivateKey(certKeyBlock.Bytes)
		if err != nil {
			return nil, err
		}
		signer, ok := key.(crypto.Signer)
		if !ok {
			return nil, errors.New("internal error: parsed PKCS8 private key is not a crypto.Signer")
		}
		return signer, nil
	case "EC PRIVATE KEY":
		return x509.ParseECPrivateKey(certKeyBlock.Bytes)
	default:
		return nil, fmt.Errorf("unsupported key type %q", certKeyBlock.Type)
	}
}

func sign(certKeyBlock *pem.Block, message *bytes.Buffer) ([]byte, error) {
	signer, err := getSigner(certKeyBlock)
	if err != nil {
		return nil, err
	}

	hashed := sha256.Sum256(message.Bytes())
	return signer.Sign(rand.Reader, hashed[:], crypto.SHA256)
}
