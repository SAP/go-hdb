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
		enc := encoding.NewEncoder(make([]byte, 0), cesu8.DefaultEncoder())

		if err := part.encode(enc); err != nil {
			t.Fatal(err)
		}

		return enc.Buffer()
	}

	authDecodeStep := func(part PartDecoder, data []byte) {
		dec := encoding.NewDecoder(data, cesu8.DefaultDecoder())
		attrs := &ReaderAttrs{}

		if err := part.decode(dec, nil, attrs); err != nil {
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

func testAuthRequestDecode(t *testing.T) {

	authEncodeStep := func(part PartEncoder) []byte {
		enc := encoding.NewEncoder(make([]byte, 0), cesu8.DefaultEncoder())

		if err := part.encode(enc); err != nil {
			t.Fatal(err)
		}

		return enc.Buffer()
	}

	authDecodeStep := func(part PartDecoder, data []byte) {
		dec := encoding.NewDecoder(data, cesu8.DefaultDecoder())
		attrs := &ReaderAttrs{}

		if err := part.decode(dec, nil, attrs); err != nil {
			t.Fatal(err)
		}
	}

	a := NewAuthHnd("SYSTEM")
	a.AddJWT("dummy token")

	initRequest, err := a.InitRequest()
	if err != nil {
		t.Fatal(err)
	}

	req := new(AuthInitRequest)
	authDecodeStep(req, authEncodeStep(initRequest))
	if l := req.Logonname(); l != "SYSTEM" {
		t.Fatalf("expected logon name SYSTEM, got %q", l)
	}
	// the wire-interpreted and the client-prepared request render identically.
	if s := req.String(); s != initRequest.String() {
		t.Fatalf("init request trace mismatch: got %q, expected %q", s, initRequest.String())
	}

	initReply, err := a.InitReply()
	if err != nil {
		t.Fatal(err)
	}
	authDecodeStep(initReply, []byte("\x02\x00\x03JWT\x07USER123"))

	finalRequest, err := a.FinalRequest()
	if err != nil {
		t.Fatal(err)
	}

	fr := new(AuthFinalRequest)
	authDecodeStep(fr, authEncodeStep(finalRequest))
	if s := fr.String(); s != "method type JWT logonname USER123 token xxxxx cookie xxxxx" {
		t.Fatalf("unexpected final request parameters %s", s)
	}

	// the wire-interpreted replies (sniffer side, no handler) decode in order
	// and render identically to the client-negotiated ones.
	finalReply, err := a.FinalReply()
	if err != nil {
		t.Fatal(err)
	}
	finalReplyData := []byte("\x02\x00\x03JWT\x205be8f43e064e0589ce07ba9de6fce107")
	authDecodeStep(finalReply, finalReplyData)

	initRep := new(AuthInitReply)
	authDecodeStep(initRep, []byte("\x02\x00\x03JWT\x07USER123"))
	if initRep.Method == nil {
		t.Fatal("init reply wire interpretation failed")
	}
	if s := initRep.String(); s != initReply.String() {
		t.Fatalf("init reply trace mismatch: got %q, expected %q", s, initReply.String())
	}

	finalRep := &AuthFinalReply{Method: initRep.Method}
	authDecodeStep(finalRep, finalReplyData)
	if s := finalRep.String(); s != finalReply.String() {
		t.Fatalf("final reply trace mismatch: got %q, expected %q", s, finalReply.String())
	}
}

func TestAuth(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testJWTAuth", testJWTAuth},
		{"testAuthRequestDecode", testAuthRequestDecode},
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
