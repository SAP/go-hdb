package auth

import (
	"errors"
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/internal/trace"
)

// GSS object identifiers, sent as dotted text.
const (
	gssKerberosOID      = "1.2.840.113554.1.2.2"   // Kerberos V5 mechanism (RFC 4121)
	gssPrincipalNameOID = "1.2.840.113554.1.2.2.1" // Kerberos principal name type
)

// GSS request and reply types.
const (
	gssInitReq  byte = 1 // client principal
	gssInitRep  byte = 2 // service principal and logonname
	gssTokenReq byte = 3 // initial context token
	gssFinalReq byte = 5
	gssTokenRep byte = 6 // server token, if any
	gssFinalRep byte = 7 // session cookie
)

// GSSInitiator provides the Kerberos GSS-API context of the client.
type GSSInitiator interface {
	// Principal returns the client principal name (user@REALM).
	Principal() string
	// InitSecContext returns the initial context token (RFC 4121) for the
	// service principal name the server sends (hdb/host@REALM), and the
	// check of the server token: an AP-REP if mutual authentication was
	// requested, empty otherwise.
	InitSecContext(spn string) (token []byte, accept func(reply []byte) error, err error)
}

// GSS implements Kerberos authentication via GSS-API. After the initial
// reply names the service principal the client sends its initial context
// token in a second authentication request (see Continuer).
type GSS struct {
	initiator GSSInitiator
	principal string
	spn       string
	logonname string
	token     []byte
	accept    func(reply []byte) error
	_cookie   []byte
}

// NewGSS creates a new GSS instance.
func NewGSS(initiator GSSInitiator) *GSS { return &GSS{initiator: initiator} }

func (a *GSS) String() string {
	return fmt.Sprintf("method type %s principal %s spn %s logonname %s cookie %s",
		a.Typ(), trace.Cut(a.principal), trace.Cut(a.spn), trace.Cut(a.logonname), trace.Redacted(a._cookie))
}

// Cookie implements the CookieGetter interface.
func (a *GSS) Cookie() (string, []byte) { return a.logonname, a._cookie }

// Typ implements the Method interface.
func (a *GSS) Typ() string { return MtGSS }

// Order implements the Method interface.
func (a *GSS) Order() byte { return MoGSS }

// AuthLoginName implements the Method interface.
func (a *GSS) AuthLoginName() string { return a.logonname }

// EncodeInitReq implements the Method interface.
func (a *GSS) EncodeInitReq(prms *Prms) error {
	a.principal = a.initiator.Principal()
	if a.principal == "" {
		return errors.New("empty kerberos principal")
	}
	subPrms := prms.addPrms()
	subPrms.AddString(gssKerberosOID)
	subPrms.addBytes([]byte{gssInitReq})
	subPrms.AddString(gssPrincipalNameOID)
	subPrms.AddString(a.principal)
	return nil
}

// DecodeInitReq implements the Method interface. A sniffer decodes the
// token request as an initial request as well.
func (a *GSS) DecodeInitReq(dec *encoding.Decoder) error {
	_, b := dec.LIBytes()
	sub := encoding.NewDecoder(b, dec.Transformer())
	numPrm := int(sub.Int16())
	if numPrm < 2 {
		return fmt.Errorf("invalid number of gss parameters %d", numPrm)
	}
	if err := gssCheckOID(lIString(sub)); err != nil {
		return err
	}
	switch typ := lIType(sub); {
	case typ == gssInitReq && numPrm == 4:
		lIString(sub) // name type
		a.principal = lIString(sub)
	case typ == gssTokenReq && numPrm == 3:
		_, a.token = sub.LIBytes()
	default:
		return fmt.Errorf("invalid gss request type %d with %d parameters", typ, numPrm)
	}
	return nil
}

// DecodeInitReply implements the Method interface. A sniffer decodes the
// token reply as an initial reply as well.
func (a *GSS) DecodeInitReply(dec *encoding.Decoder) error {
	sub, numPrm, typ, err := gssReply(dec)
	if err != nil {
		return err
	}
	switch {
	case typ == gssInitRep && numPrm == 5:
		sub.AuthString() // name type
		a.spn = sub.AuthString()
		a.logonname, err = sub.AuthCesu8String()
		return err
	case typ == gssTokenRep && (numPrm == 2 || numPrm == 3):
		if numPrm == 3 {
			a.token = sub.AuthBytes()
		}
		return nil
	}
	return fmt.Errorf("invalid gss reply type %d with %d parameters", typ, numPrm)
}

// EncodeContinueReq implements the Continuer interface.
func (a *GSS) EncodeContinueReq(prms *Prms) error {
	if a.spn == "" {
		return errors.New("server sent no kerberos service principal")
	}
	token, accept, err := a.initiator.InitSecContext(a.spn)
	if err != nil {
		return err
	}
	a.accept = accept
	subPrms := prms.addPrms()
	subPrms.AddString(gssKerberosOID)
	subPrms.addBytes([]byte{gssTokenReq})
	subPrms.addBytes(token)
	return nil
}

// DecodeContinueReply implements the Continuer interface.
func (a *GSS) DecodeContinueReply(dec *encoding.Decoder) error {
	sub, numPrm, typ, err := gssReply(dec)
	if err != nil {
		return err
	}
	if typ != gssTokenRep || (numPrm != 2 && numPrm != 3) {
		return fmt.Errorf("invalid gss reply type %d with %d parameters - expected type %d", typ, numPrm, gssTokenRep)
	}
	a.token = nil
	if numPrm == 3 {
		a.token = sub.AuthBytes()
	}
	return a.accept(a.token)
}

// EncodeFinalReq implements the Method interface.
func (a *GSS) EncodeFinalReq(prms *Prms) error {
	subPrms := prms.addPrms()
	subPrms.AddString(gssKerberosOID)
	subPrms.addBytes([]byte{gssFinalReq})
	return nil
}

// DecodeFinalReq implements the Method interface.
func (a *GSS) DecodeFinalReq(dec *encoding.Decoder, logonname string) error {
	a.logonname = logonname
	_, b := dec.LIBytes()
	sub := encoding.NewDecoder(b, dec.Transformer())
	if err := DecodeAndCheckNumPrm(sub, 2); err != nil {
		return err
	}
	if err := gssCheckOID(lIString(sub)); err != nil {
		return err
	}
	if typ := lIType(sub); typ != gssFinalReq {
		return fmt.Errorf("invalid gss request type %d - expected %d", typ, gssFinalReq)
	}
	return nil
}

// DecodeFinalReply implements the Method interface.
func (a *GSS) DecodeFinalReply(dec *encoding.Decoder) error {
	if err := DecodeAndCheckNumPrm(dec, 2); err != nil {
		return err
	}
	mt := dec.AuthString()
	if err := checkAuthMethodType(mt, a.Typ()); err != nil {
		return err
	}
	sub, numPrm, typ, err := gssReply(dec)
	if err != nil {
		return err
	}
	if typ != gssFinalRep || numPrm != 3 {
		return fmt.Errorf("invalid gss reply type %d with %d parameters - expected type %d", typ, numPrm, gssFinalRep)
	}
	a._cookie = sub.AuthBytes()
	return nil
}

// gssReply decodes the head of a gss reply detail: its parameter count,
// mechanism and type.
func gssReply(dec *encoding.Decoder) (*encoding.Decoder, int, byte, error) {
	sub := encoding.NewDecoder(dec.AuthBytes(), dec.Transformer())
	numPrm := int(sub.Int16())
	if numPrm < 2 {
		return nil, 0, 0, fmt.Errorf("invalid number of gss parameters %d", numPrm)
	}
	if err := gssCheckOID(sub.AuthString()); err != nil {
		return nil, 0, 0, err
	}
	typ := sub.AuthBytes()
	if len(typ) != 1 {
		return nil, 0, 0, fmt.Errorf("invalid gss type size %d", len(typ))
	}
	return sub, numPrm, typ[0], nil
}

func gssCheckOID(oid string) error {
	if oid != gssKerberosOID {
		return fmt.Errorf("invalid gss mechanism %s - expected %s", oid, gssKerberosOID)
	}
	return nil
}

func lIString(dec *encoding.Decoder) string {
	_, b := dec.LIBytes()
	return string(b)
}

func lIType(dec *encoding.Decoder) byte {
	_, b := dec.LIBytes()
	if len(b) != 1 {
		return 0
	}
	return b[0]
}
