package protocol

import (
	"bytes"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
)

func TestJWTAuthentication(t *testing.T) {
	stepper := newAuthJWT("dummy token")

	encStep := func(t *testing.T) []byte {
		auth, err := stepper.next()
		if err != nil {
			t.Error(err)
		}

		buf := bytes.Buffer{}
		enc := encoding.NewEncoder(&buf, cesu8.DefaultEncoder)

		err = auth.encode(enc)
		if err != nil {
			t.Error(err)
		}

		return buf.Bytes()
	}

	decStep := func(t *testing.T, data []byte) partReadWriter {
		auth, err := stepper.next()
		if err != nil {
			t.Error(err)
		}

		dec := encoding.NewDecoder(bytes.NewBuffer(data), cesu8.DefaultDecoder)

		if err := auth.decode(dec, nil); err != nil {
			t.Error(err)
		}

		return auth
	}

	t.Run("step0", func(t *testing.T) {
		actual := encStep(t)
		expected := []byte("\x03\x00\x00\x03JWT\x0Bdummy token")

		if !bytes.Equal(expected, actual) {
			t.Errorf("expected %q, got %q", string(expected), string(actual))
		}
	})

	t.Run("step1", func(t *testing.T) {
		auth := decStep(t, []byte("\x02\x00\x03JWT\x07USER123"))

		initRep := auth.(*authInitRepJWT)
		if initRep.username != "USER123" {
			t.Errorf("expected USER123, got %s", initRep.username)
		}
	})

	t.Run("step2", func(t *testing.T) {
		actual := encStep(t)
		expected := []byte("\x03\x00\x07USER123\x03JWT\x00")

		if !bytes.Equal(expected, actual) {
			t.Errorf("expected %q, got %q", string(expected), string(actual))
		}
	})

	t.Run("step3", func(t *testing.T) {
		auth := decStep(t, []byte("\x02\x00\x03JWT\x205be8f43e064e0589ce07ba9de6fce107"))

		const expectedCookie = "5be8f43e064e0589ce07ba9de6fce107"

		finalRep := auth.(*authFinalRepJWT)
		if string(finalRep.cookie) != expectedCookie {
			t.Errorf("expected %q, got %q", expectedCookie, string(finalRep.cookie))
		}
	})
}
