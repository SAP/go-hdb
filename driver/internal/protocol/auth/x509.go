// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package auth

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

// X509 implements X509 authentication.
type X509 struct {
	cert, key   []byte
	serverNonce []byte
	logonName   string
}

// NewX509 creates a new authX509 instance.
func NewX509(cert, key []byte) *X509 { return &X509{cert: cert, key: key} }

func (a *X509) String() string {
	return fmt.Sprintf("method type %s cert %v key %v", a.Typ(), a.cert, a.key)
}

// SetCertKey implements the AuthCertKeySetter interface.
func (a *X509) SetCertKey(cert, key []byte) { a.cert = cert; a.key = key }

// Typ implements the CookieGetter interface.
func (a *X509) Typ() string { return MtX509 }

// Order implements the CookieGetter interface.
func (a *X509) Order() byte { return MoX509 }

// PrepareInitReq implements the Method interface.
func (a *X509) PrepareInitReq(prms *Prms) {
	prms.addString(a.Typ())
	prms.addEmpty()
}

// InitRepDecode implements the Method interface.
func (a *X509) InitRepDecode(d *Decoder) error {
	a.serverNonce = d.bytes()
	if len(a.serverNonce) != x509ServerNonceSize {
		return fmt.Errorf("invalid server nonce size %d - expected %d", len(a.serverNonce), x509ServerNonceSize)
	}
	return nil
}

// PrepareFinalReq implements the Method interface.
func (a *X509) PrepareFinalReq(prms *Prms) error {
	prms.addEmpty() // empty username
	prms.addString(a.Typ())

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

// FinalRepDecode implements the Method interface.
func (a *X509) FinalRepDecode(d *Decoder) error {
	if err := d.NumPrm(2); err != nil {
		return err
	}
	mt := d.String()
	if err := checkAuthMethodType(mt, a.Typ()); err != nil {
		return err
	}
	d.subSize()
	if err := d.NumPrm(1); err != nil {
		return err
	}
	var err error
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
		return nil, errors.New("invalid client certificate")
	case len(blocks) < 1:
		return nil, fmt.Errorf("invalid number of blocks in certificate file %d - expected min 1", len(blocks))
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
