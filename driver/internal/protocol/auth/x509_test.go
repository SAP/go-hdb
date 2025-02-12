package auth

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testX509Validate(t *testing.T) {
	// to test certificates one need to copy the .pem files into this directory.
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
			certBlocks, err := decodeClientCert(data)
			if err != nil {
				t.Fatal(err)
			}
			certs, err := parseCerts(certBlocks)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateCerts(certs, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestX509(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testX509Verify", testX509Validate},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
