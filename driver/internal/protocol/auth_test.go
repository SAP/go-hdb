// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
)

func TestBasicAuth(t *testing.T) {
	testData := []struct {
		method          string
		salt            []byte
		serverChallenge []byte
		rounds          int
		clientChallenge []byte
		password        []byte
		clientProof     []byte
		serverProof     []byte
	}{
		{
			method:          mnSCRAMSHA256,
			salt:            []byte{214, 199, 255, 118, 92, 174, 94, 190, 197, 225, 57, 154, 157, 109, 119, 245},
			serverChallenge: []byte{224, 22, 242, 18, 237, 99, 6, 28, 162, 248, 96, 7, 115, 152, 134, 65, 141, 65, 168, 126, 168, 86, 87, 72, 16, 119, 12, 91, 227, 123, 51, 194, 203, 168, 56, 133, 70, 236, 230, 214, 89, 167, 130, 123, 132, 178, 211, 186},
			clientChallenge: []byte{219, 141, 27, 200, 255, 90, 182, 125, 133, 151, 127, 36, 26, 106, 213, 31, 57, 89, 50, 201, 237, 11, 158, 110, 8, 13, 2, 71, 9, 235, 213, 27, 64, 43, 181, 181, 147, 140, 10, 63, 156, 133, 133, 165, 171, 67, 187, 250, 41, 145, 176, 164, 137, 54, 72, 42, 47, 112, 252, 77, 102, 152, 220, 223},
			password:        []byte{65, 100, 109, 105, 110, 49, 50, 51, 52},
			clientProof:     []byte{23, 243, 209, 70, 117, 54, 25, 92, 21, 173, 194, 108, 63, 25, 188, 185, 230, 61, 124, 190, 73, 80, 225, 126, 191, 119, 32, 112, 231, 72, 184, 199},
		},
		{
			method:          mnSCRAMPBKDF2SHA256,
			salt:            []byte{51, 178, 213, 213, 92, 82, 194, 40, 80, 120, 197, 91, 166, 67, 23, 63},
			serverChallenge: []byte{32, 91, 165, 18, 158, 77, 134, 69, 128, 157, 69, 209, 47, 33, 171, 164, 56, 172, 229, 0, 153, 3, 65, 29, 239, 210, 186, 134, 81, 32, 29, 137, 239, 167, 39, 1, 171, 117, 85, 138, 109, 38, 42, 77, 43, 42, 82, 70},
			rounds:          15000,
			clientChallenge: []byte{137, 156, 182, 60, 158, 138, 93, 103, 80, 202, 54, 191, 210, 78, 142, 207, 210, 176, 157, 129, 128, 19, 135, 0, 127, 26, 58, 197, 188, 216, 121, 26, 120, 196, 34, 138, 5, 8, 58, 32, 36, 240, 199, 126, 164, 112, 64, 35, 46, 102, 255, 249, 126, 250, 24, 103, 198, 152, 33, 75, 6, 179, 187, 230},
			password:        []byte{84, 111, 111, 114, 49, 50, 51, 52},
			clientProof:     []byte{253, 181, 101, 0, 214, 222, 25, 99, 98, 253, 141, 106, 38, 255, 16, 153, 34, 74, 211, 70, 21, 91, 71, 223, 170, 36, 249, 124, 1, 135, 176, 37},
			serverProof:     []byte{228, 2, 183, 82, 29, 218, 234, 242, 40, 50, 142, 158, 142, 153, 185, 189, 130, 51, 176, 155, 23, 179, 58, 19, 126, 144, 139, 229, 116, 3, 242, 197},
		},
	}

	for _, r := range testData {
		var key []byte
		switch r.method {
		case mnSCRAMSHA256:
			key = scramsha256Key(r.password, r.salt)
		case mnSCRAMPBKDF2SHA256:
			key = scrampbkdf2sha256Key(r.password, r.salt, r.rounds)
		default:
			t.Fatalf("unknown authentication method %s", r.method)
		}
		clientProof := clientProof(key, r.salt, r.serverChallenge, r.clientChallenge)
		for i, v := range clientProof {
			if v != r.clientProof[i] {
				t.Fatalf("diff index % d - got %v - expected %v", i, clientProof, r.clientProof)
			}
		}
	}
}

func authEncodeStep(auth *auth, t *testing.T) []byte {
	part, err := auth.next()
	if err != nil {
		t.Fatal(err)
	}

	buf := bytes.Buffer{}
	enc := encoding.NewEncoder(&buf, cesu8.DefaultEncoder)

	err = part.encode(enc)
	if err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

func authDecodeStep(auth *auth, data []byte, t *testing.T) {
	part, err := auth.next()
	if err != nil {
		t.Fatal(err)
	}

	dec := encoding.NewDecoder(bytes.NewBuffer(data), cesu8.DefaultDecoder)

	println("decode")
	if err := part.decode(dec, nil); err != nil {
		println("decode error")
		t.Fatal(err)
	}
	println("end decode")
}

func TestJWTAuth(t *testing.T) {
	auth := newAuth(&SessionConfig{Token: "dummy token"})

	successful := t.Run("step0", func(t *testing.T) {
		actual := authEncodeStep(auth, t)
		expected := []byte("\x03\x00\x00\x03JWT\x0Bdummy token")

		if !bytes.Equal(expected, actual) {
			t.Fatalf("expected %q, got %q", string(expected), string(actual))
		}
	})

	if successful {
		successful = t.Run("step1", func(t *testing.T) {
			authDecodeStep(auth, []byte("\x02\x00\x03JWT\x07USER123"), t)

			authJWT := auth.methods.method.(*authJWT)
			if authJWT.username != "USER123" {
				t.Fatalf("expected USER123, got %s", authJWT.username)
			}
		})
	}

	if successful {
		successful = t.Run("step2", func(t *testing.T) {
			actual := authEncodeStep(auth, t)
			expected := []byte("\x03\x00\x07USER123\x03JWT\x00")

			if !bytes.Equal(expected, actual) {
				t.Fatalf("expected %q, got %q", string(expected), string(actual))
			}
		})
	}

	if successful {
		successful = t.Run("step3", func(t *testing.T) {
			authDecodeStep(auth, []byte("\x02\x00\x03JWT\x205be8f43e064e0589ce07ba9de6fce107"), t)

			const expectedCookie = "5be8f43e064e0589ce07ba9de6fce107"

			authJWT := auth.methods.method.(*authJWT)
			if string(authJWT.cookie) != expectedCookie {
				t.Fatalf("expected %q, got %q", expectedCookie, string(authJWT.cookie))
			}
		})
	}
}

func TestX509Auth(t *testing.T) {

	clientCert := []byte(`
-----BEGIN CERTIFICATE-----
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
YA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
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
cHaYvcseHIbqvoyt2iCKgD6yBS18
-----END CERTIFICATE-----
`)

	// random private PKCS8 key for testing purpose only
	clientKey := []byte(`
-----BEGIN PRIVATE KEY-----
MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAqPfgaTEWEP3S9w0t
gsicURfo+nLW09/0KfOPinhYZ4ouzU+3xC4pSlEp8Ut9FgL0AgqNslNaK34Kq+NZ
jO9DAQIDAQABAkAgkuLEHLaqkWhLgNKagSajeobLS3rPT0Agm0f7k55FXVt743hw
Ngkp98bMNrzy9AQ1mJGbQZGrpr4c8ZAx3aRNAiEAoxK/MgGeeLui385KJ7ZOYktj
hLBNAB69fKwTZFsUNh0CIQEJQRpFCcydunv2bENcN/oBTRw39E8GNv2pIcNxZkcb
NQIgbYSzn3Py6AasNj6nEtCfB+i1p3F35TK/87DlPSrmAgkCIQDJLhFoj1gbwRbH
/bDRPrtlRUDDx44wHoEhSDRdy77eiQIgE6z/k6I+ChN1LLttwX0galITxmAYrOBh
BVl433tgTTQ=
-----END PRIVATE KEY-----
`)

	auth := newAuth(&SessionConfig{ClientCert: clientCert, ClientKey: clientKey})

	serverNonce := make([]byte, x509ServerNonceSize)
	if _, err := rand.Read(serverNonce); err != nil {
		t.Fatal(err)
	}

	successful := t.Run("step0", func(t *testing.T) {
		actual := authEncodeStep(auth, t)
		expected := []byte("\x03\x00\x00\x04X509\x00")

		if !bytes.Equal(expected, actual) {
			t.Fatalf("expected %q, got %q", string(expected), string(actual))
		}
	})

	if successful {
		successful = t.Run("step1", func(t *testing.T) {
			data := bytes.NewBuffer([]byte("\x02\x00\x04X509\x40"))
			data.Write(serverNonce)
			authDecodeStep(auth, data.Bytes(), t)

			authX509 := auth.methods.method.(*authX509)
			if !bytes.Equal(authX509.serverNonce, serverNonce) {
				t.Fatalf("expected %v, got %v", serverNonce, authX509.serverNonce)
			}
		})
	}

	if successful {
		successful = t.Run("step2", func(t *testing.T) {
			actual := authEncodeStep(auth, t)

			t.Skip() //TODO

			expected := []byte("\x03\x00\x07USER123\x03JWT\x00")
			if !bytes.Equal(expected, actual) {
				t.Fatalf("expected %q, got %q", string(expected), string(actual))
			}
		})
	}

	if successful {
		successful = t.Run("step3", func(t *testing.T) {

			t.Skip() //TODO

			authDecodeStep(auth, []byte("\x02\x00\x04X509\x205be8f43e064e0589ce07ba9de6fce107"), t)

			const expectedCookie = "5be8f43e064e0589ce07ba9de6fce107"

			authJWT := auth.methods.method.(*authJWT)
			if string(authJWT.cookie) != expectedCookie {
				t.Fatalf("expected %q, got %q", expectedCookie, string(authJWT.cookie))
			}
		})
	}
}
