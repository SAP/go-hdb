package protocol

import (
	"bytes"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/protocol/auth"
	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
)

func testJWTAuth(t *testing.T) {

	authEncodeStep := func(part PartEncoder) []byte {
		enc := encoding.Encoder(make([]byte, 0))

		if err := part.encode(&enc, cesu8.DefaultEncoder()); err != nil {
			t.Fatal(err)
		}

		return enc
	}

	authDecodeStep := func(part PartDecoder, data []byte) {
		dec := encoding.Decoder(data)
		attrs := &ReaderAttrs{tr: cesu8.DefaultDecoder()}

		if err := part.decode(&dec, nil, attrs); err != nil {
			t.Fatal(err)
		}
	}

	a := NewAuthHnd("")
	a.AddJWT("dummy token")

	initRequest, err := a.InitRequest()
	if err != nil {
		t.Fatal(err)
	}

	actual := authEncodeStep(initRequest)
	expected := []byte("\x03\x00\x00\x03JWT\x0Bdummy token")

	if !bytes.Equal(expected, actual) {
		t.Fatalf("expected %q, got %q", string(expected), string(actual))
	}

	initReply, err := a.InitReply()
	if err != nil {
		t.Fatal(err)
	}

	authDecodeStep(initReply, []byte("\x02\x00\x03JWT\x07USER123"))

	authJWT := a.Selected().(*auth.JWT)

	logonname, _ := authJWT.Cookie()
	if logonname != "USER123" {
		t.Fatalf("expected USER123, got %s", logonname)
	}

	finalRequest, err := a.FinalRequest()
	if err != nil {
		t.Fatal(err)
	}

	actual = authEncodeStep(finalRequest)
	expected = []byte("\x03\x00\x07USER123\x03JWT\x00")

	if !bytes.Equal(expected, actual) {
		t.Fatalf("expected %q, got %q", string(expected), string(actual))
	}

	finalReply, err := a.FinalReply()
	if err != nil {
		t.Fatal(err)
	}

	authDecodeStep(finalReply, []byte("\x02\x00\x03JWT\x205be8f43e064e0589ce07ba9de6fce107"))

	const expectedCookie = "5be8f43e064e0589ce07ba9de6fce107"

	authJWT = a.Selected().(*auth.JWT)
	_, cookie := authJWT.Cookie()
	if string(cookie) != expectedCookie {
		t.Fatalf("expected %q, got %q", expectedCookie, string(cookie))
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
