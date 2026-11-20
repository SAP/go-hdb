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

type testGSSInitiator struct {
	spn   string
	reply []byte
}

func (*testGSSInitiator) Principal() string { return "probe@EXAMPLE.TEST" }

func (i *testGSSInitiator) InitSecContext(spn string) ([]byte, func([]byte) error, error) {
	i.spn = spn
	return []byte("AP-REQ"), func(reply []byte) error {
		i.reply = reply
		return nil
	}, nil
}

// testGSSAuth checks the GSS messages against those a HANA 2.0 SPS 08 server
// exchanged with a client that signed in by Kerberos.
func testGSSAuth(t *testing.T) {

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

	const krb5 = "\x141.2.840.113554.1.2.2"

	initiator := &testGSSInitiator{}
	a := NewAuthHnd("")
	a.AddGSS(initiator)

	initRequest, err := a.InitRequest()
	if err != nil {
		t.Fatal(err)
	}
	actual := authEncodeStep(initRequest)
	expected := []byte("\x03\x00\x00\x03GSSC\x04\x00" + krb5 + "\x01\x01\x161.2.840.113554.1.2.2.1\x12probe@EXAMPLE.TEST")
	if !bytes.Equal(expected, actual) {
		t.Fatalf("expected %q, got %q", string(expected), string(actual))
	}

	initReply, err := a.InitReply()
	if err != nil {
		t.Fatal(err)
	}
	authDecodeStep(initReply, []byte("\x02\x00\x03GSSR\x05\x00"+krb5+"\x01\x02\x161.2.840.113554.1.2.2.1\x18hdb/hxehost@EXAMPLE.TEST\x08GSSPROBE"))

	continueRequest := a.ContinueRequest()
	if continueRequest == nil {
		t.Fatal("GSS takes no second request")
	}
	actual = authEncodeStep(continueRequest)
	expected = []byte("\x03\x00\x00\x03GSS\x20\x03\x00" + krb5 + "\x01\x03\x06AP-REQ")
	if !bytes.Equal(expected, actual) {
		t.Fatalf("expected %q, got %q", string(expected), string(actual))
	}
	if initiator.spn != "hdb/hxehost@EXAMPLE.TEST" {
		t.Fatalf("expected the server's service principal, got %q", initiator.spn)
	}

	authDecodeStep(a.ContinueReply(continueRequest), []byte("\x02\x00\x03GSS\x20\x03\x00"+krb5+"\x01\x06\x06AP-REP"))
	if string(initiator.reply) != "AP-REP" {
		t.Fatalf("expected the server's token, got %q", initiator.reply)
	}

	finalRequest, err := a.FinalRequest()
	if err != nil {
		t.Fatal(err)
	}
	actual = authEncodeStep(finalRequest)
	expected = []byte("\x03\x00\x08GSSPROBE\x03GSS\x19\x02\x00" + krb5 + "\x01\x05")
	if !bytes.Equal(expected, actual) {
		t.Fatalf("expected %q, got %q", string(expected), string(actual))
	}

	finalReply, err := a.FinalReply()
	if err != nil {
		t.Fatal(err)
	}
	const expectedCookie = "de37045b8f98c0bc93e7d6e7b6b5752c"
	authDecodeStep(finalReply, []byte("\x02\x00\x03GSS\x3a\x03\x00"+krb5+"\x01\x07\x20"+expectedCookie))
	logonname, cookie := a.Selected().(*auth.GSS).Cookie()
	if logonname != "GSSPROBE" || string(cookie) != expectedCookie {
		t.Fatalf("expected GSSPROBE and %q, got %s and %q", expectedCookie, logonname, string(cookie))
	}

	// a sniffer reads the requests without the client's initiator.
	req := new(AuthInitRequest)
	authDecodeStep(req, authEncodeStep(initRequest))
	if s := req.String(); s != "method type GSS principal probe@EXAMPLE.TEST spn  logonname  cookie xxxxx" {
		t.Fatalf("unexpected init request %s", s)
	}
	fr := new(AuthFinalRequest)
	authDecodeStep(fr, authEncodeStep(finalRequest))
	if s := fr.String(); s != "method type GSS principal  spn  logonname GSSPROBE cookie xxxxx" {
		t.Fatalf("unexpected final request %s", s)
	}
}

func TestAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"JWT auth round-trip", testJWTAuth},
		{"auth request decode", testAuthRequestDecode},
		{"GSS auth round-trip", testGSSAuth},
	}

	for _, test := range tests {
		func(name string, fn func(t *testing.T)) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				fn(t)
			})
		}(test.name, test.fn)
	}
}
