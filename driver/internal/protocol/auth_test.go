package protocol

import (
	"bytes"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/protocol/auth"
	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
)

func authEncodeStep(part partWriter, t *testing.T) []byte {
	buf := bytes.Buffer{}
	enc := encoding.NewEncoder(&buf, cesu8.DefaultEncoder)

	if err := part.encode(enc); err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

func authDecodeStep(part partReader, data []byte, t *testing.T) {
	dec := encoding.NewDecoder(bytes.NewBuffer(data), cesu8.DefaultDecoder)

	if err := part.decode(dec, nil); err != nil {
		t.Fatal(err)
	}
}

func testJWTAuth(t *testing.T) {
	a := NewAuth("")
	a.AddJWT("dummy token")

	successful := t.Run("init request", func(t *testing.T) {
		initRequest, err := a.InitRequest()
		if err != nil {
			t.Fatal(err)
		}

		actual := authEncodeStep(initRequest, t)
		expected := []byte("\x03\x00\x00\x03JWT\x0Bdummy token")

		if !bytes.Equal(expected, actual) {
			t.Fatalf("expected %q, got %q", string(expected), string(actual))
		}
	})

	if successful {
		successful = t.Run("init reply", func(t *testing.T) {
			initReply, err := a.InitReply()
			if err != nil {
				t.Fatal(err)
			}

			authDecodeStep(initReply, []byte("\x02\x00\x03JWT\x07USER123"), t)

			authJWT := a.Method().(*auth.JWT)

			logonname, _ := authJWT.Cookie()
			if logonname != "USER123" {
				t.Fatalf("expected USER123, got %s", logonname)
			}
		})
	}

	if successful {
		successful = t.Run("final request", func(t *testing.T) {
			finalRequest, err := a.FinalRequest()
			if err != nil {
				t.Fatal(err)
			}

			actual := authEncodeStep(finalRequest, t)
			expected := []byte("\x03\x00\x07USER123\x03JWT\x00")

			if !bytes.Equal(expected, actual) {
				t.Fatalf("expected %q, got %q", string(expected), string(actual))
			}
		})
	}

	if successful {
		t.Run("final reply", func(t *testing.T) {
			finalReply, err := a.FinalReply()
			if err != nil {
				t.Fatal(err)
			}

			authDecodeStep(finalReply, []byte("\x02\x00\x03JWT\x205be8f43e064e0589ce07ba9de6fce107"), t)

			const expectedCookie = "5be8f43e064e0589ce07ba9de6fce107"

			authJWT := a.Method().(*auth.JWT)
			_, cookie := authJWT.Cookie()
			if string(cookie) != expectedCookie {
				t.Fatalf("expected %q, got %q", expectedCookie, string(cookie))
			}
		})
	}
}

func TestAuth(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testJWTAuth", testJWTAuth},
	}

	for _, test := range tests {
		func(name string, fct func(t *testing.T)) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				fct(t)
			})
		}(test.name, test.fct)
	}
}
