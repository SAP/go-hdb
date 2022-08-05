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

const (
	x509ServerNonceSize = 64
)

// authX509 implements X509 authentication.
type authX509 struct {
	cert, key   []byte
	serverNonce []byte
	logonName   string
}

// newAuthX509 creates a new authX509 instance.
func newAuthX509(cert, key []byte) *authX509 { return &authX509{cert: cert, key: key} }

func (a *authX509) String() string {
	return fmt.Sprintf("method type %s cert %v key %v", a.typ(), a.cert, a.key)
}

// SetCertKey implements the AuthCertKeySetter interface.
func (a *authX509) SetCertKey(cert, key []byte) { a.cert = cert; a.key = key }

func (a *authX509) typ() string { return amtX509 }

func (a *authX509) order() byte { return amoX509 }

func (a *authX509) prepareInitReq(prms *authPrms) {
	prms.addString(a.typ())
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
	prms.addString(a.typ())

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
	mt, err := d.string()
	if err != nil {
		return err
	}
	if err := checkAuthMethodType(mt, a.typ()); err != nil {
		return err
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
	case len(blocks) != 1:
		return nil, fmt.Errorf("invalid number of blocks in key file %d - expected 1", len(blocks))
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
