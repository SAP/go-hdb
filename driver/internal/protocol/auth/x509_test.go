package auth

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver/internal/protocol/x509"
)

func testX509Validate(t *testing.T) {

	// to test certificates one need to copy the .pem files into this directory
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	extPem := ".pem"

	for _, entry := range entries {
		if entry.Type().IsRegular() && filepath.Ext(entry.Name()) == extPem {
			t.Logf("check file %s", entry.Name())

			data, err := os.ReadFile(entry.Name())
			if err != nil {
				t.Fatal(err)
			}

			certKey, err := x509.NewCertKey(data, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := certKey.Validate(time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func testSignRsa(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pubKey := privKey.Public()
	message := bytes.NewBuffer([]byte("test"))

	data, hash, err := getDataToSign(pubKey, message)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 32 {
		t.Fatalf("unexpected data length %d - expected 32", len(data))
	}
	if hash != crypto.SHA256 {
		t.Fatalf("unexpected hash %s - expected SHA256", hash)
	}
}

func testSignEcdsaP256(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pubKey := privKey.Public()
	message := bytes.NewBuffer([]byte("test"))

	data, hash, err := getDataToSign(pubKey, message)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 32 {
		t.Fatalf("unexpected data length %d - expected 32", len(data))
	}
	if hash != crypto.SHA256 {
		t.Fatalf("unexpected hash %s - expected SHA256", hash)
	}
}

func testSignEcdsaP384(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pubKey := privKey.Public()
	message := bytes.NewBuffer([]byte("test"))

	data, hash, err := getDataToSign(pubKey, message)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 48 {
		t.Fatalf("unexpected data length %d - expected 32", len(data))
	}
	if hash != crypto.SHA384 {
		t.Fatalf("unexpected hash %s - expected SHA384", hash)
	}
}

func testSignEcdsaP521(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pubKey := privKey.Public()
	message := bytes.NewBuffer([]byte("test"))

	data, hash, err := getDataToSign(pubKey, message)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 64 {
		t.Fatalf("unexpected data length %d - expected 32", len(data))
	}
	if hash != crypto.SHA512 {
		t.Fatalf("unexpected hash %s - expected SHA512", hash)
	}
}

func testSignEd25519(t *testing.T) {
	pubKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	message := bytes.NewBuffer([]byte("test"))

	data, hash, err := getDataToSign(pubKey, message)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 4 {
		t.Fatalf("unexpected data length %d - expected 4", len(data))
	}
	if hash != 0 {
		t.Fatalf("unexpected hash %s - expected 0", hash)
	}
}

func TestX509(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testX509Verify", testX509Validate},
		{"testSignRsa", testSignRsa},
		{"testSignEcdsaP256", testSignEcdsaP256},
		{"testSignEcdsaP384", testSignEcdsaP384},
		{"testSignEcdsaP521", testSignEcdsaP521},
		{"testSignEd25519", testSignEd25519},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
