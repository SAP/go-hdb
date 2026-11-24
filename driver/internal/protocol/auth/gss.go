package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/SAP/go-hdb/driver/gss"
	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"github.com/SAP/go-hdb/driver/internal/trace"
)

// GSS-API object identifiers carried in the authentication parameters as
// dotted-decimal text. The mechanism OID is the Kerberos V5 identifier defined
// by RFC 4121 (RFC 1964 section 1); the name OID is its principal name type.
const (
	oidKerberosV5        = "1.2.840.113554.1.2.2"   // Kerberos V5 GSS-API mechanism
	oidKerberosPrincipal = "1.2.840.113554.1.2.2.1" // Kerberos principal name type
)

// Message discriminators. The GSS handshake runs in three phases - an initial
// exchange that names the service, a context-token exchange, and a final
// exchange that completes the login - each with a request (client) and reply
// (server) parameter vector. The leading byte of each vector identifies which
// one it is; the values below are the HANA wire encoding observed for each.
const (
	phaseInitReq  byte = 1 // request: client principal name
	phaseInitRep  byte = 2 // reply: service principal name and logon name
	phaseTokenReq byte = 3 // request: client context token
	phaseTokenRep byte = 6 // reply: server context token (may be absent)
	phaseFinalReq byte = 5 // request: completes the login
	phaseFinalRep byte = 7 // reply: optional session cookie
)

// GSS implements Kerberos authentication via GSS-API. After the initial reply
// names the service principal, the client exchanges context tokens with the
// server (see TokenExchanger) until the context is established, then sends the
// final request. The in-flight tokens live on the exchanged parts, not here.
type GSS struct {
	initiator  gss.Initiator
	targetName string
	logonname  string
	_cookie    []byte
}

// NewGSS creates a new GSS instance.
func NewGSS(initiator gss.Initiator) *GSS { return &GSS{initiator: initiator} }

func (a *GSS) String() string {
	return fmt.Sprintf("method type %s target %s logonname %s cookie %s",
		a.Typ(), trace.Cut(a.targetName), trace.Cut(a.logonname), trace.Redacted(a._cookie))
}

// Cookie implements the CookieGetter interface.
func (a *GSS) Cookie() (string, []byte) { return a.logonname, a._cookie }

// Typ implements the Method interface.
func (a *GSS) Typ() string { return MtGSS }

// Order implements the Method interface.
func (a *GSS) Order() byte { return MoGSS }

// AuthLoginName implements the Method interface.
func (a *GSS) AuthLoginName() string { return a.logonname }

// NextToken implements the TokenExchanger interface: it runs one
// InitSecContext round. serverToken is nil on the first call and the server's
// reply thereafter; the returned clientToken is sent to the server unless
// established is true.
func (a *GSS) NextToken(ctx context.Context, serverToken []byte) ([]byte, bool, error) {
	if a.targetName == "" {
		return nil, false, errors.New("server sent no kerberos service principal")
	}
	return a.initiator.InitSecContext(ctx, a.targetName, serverToken)
}

// EncodeInitReq implements the Method interface.
func (a *GSS) EncodeInitReq(prms *Prms) error {
	srcName := a.initiator.SrcName()
	if srcName == "" {
		return errors.New("empty kerberos principal")
	}
	subPrms := prms.addPrms()
	subPrms.AddString(oidKerberosV5)
	subPrms.addBytes([]byte{phaseInitReq})
	subPrms.AddString(oidKerberosPrincipal)
	subPrms.AddString(srcName)
	return nil
}

// DecodeInitReq implements the Method interface (used by the sniffer to decode
// the client's init request).
func (a *GSS) DecodeInitReq(dec *encoding.Decoder) error {
	_, b := dec.LIBytes()
	sub := encoding.NewDecoder(b, dec.Transformer())
	numPrm := int(sub.Int16())
	if numPrm < 2 {
		return fmt.Errorf("invalid number of gss parameters %d", numPrm)
	}
	_, oid := sub.LIString()
	if err := gssCheckOID(oid); err != nil {
		return err
	}
	_, typB := sub.LIBytes()
	if len(typB) != 1 {
		return fmt.Errorf("invalid gss type size %d", len(typB))
	}
	switch typ := typB[0]; {
	case typ == phaseInitReq && numPrm == 4:
		sub.LIBytes() // name type, not retained
		// principal, not retained: the sniffer only logs it via String.
	default:
		return fmt.Errorf("invalid gss request type %d with %d parameters", typ, numPrm)
	}
	return nil
}

// DecodeInitReply implements the Method interface.
func (a *GSS) DecodeInitReply(dec *encoding.Decoder) error {
	sub, numPrm, typ, err := gssReadHead(dec)
	if err != nil {
		return err
	}
	if typ != phaseInitRep || numPrm != 5 {
		return fmt.Errorf("invalid gss reply type %d with %d parameters - expected type %d", typ, numPrm, phaseInitRep)
	}
	sub.AuthString() // name type
	a.targetName = sub.AuthString()
	a.logonname, err = sub.AuthCesu8String()
	return err
}

// EncodeToken serializes a client context token into a token request detail.
// It is the GSS token wire framing; the generic token request part calls it.
func EncodeToken(prms *Prms, clientToken []byte) error {
	subPrms := prms.addPrms()
	subPrms.AddString(oidKerberosV5)
	subPrms.addBytes([]byte{phaseTokenReq})
	subPrms.addBytes(clientToken)
	return nil
}

// DecodeTokenReq decodes a client context token from a token request detail
// (client-to-server length rules). Used by the sniffer.
func DecodeTokenReq(dec *encoding.Decoder) ([]byte, error) {
	_, b := dec.LIBytes()
	sub := encoding.NewDecoder(b, dec.Transformer())
	numPrm := int(sub.Int16())
	if numPrm != 3 {
		return nil, fmt.Errorf("invalid number of gss parameters %d - expected 3", numPrm)
	}
	_, oid := sub.LIString()
	if err := gssCheckOID(oid); err != nil {
		return nil, err
	}
	_, typB := sub.LIBytes()
	if len(typB) != 1 {
		return nil, fmt.Errorf("invalid gss type size %d", len(typB))
	}
	if typ := typB[0]; typ != phaseTokenReq {
		return nil, fmt.Errorf("invalid gss request type %d - expected %d", typ, phaseTokenReq)
	}
	_, token := sub.LIBytes()
	return token, nil
}

// DecodeTokenRep decodes a server context token from a token reply detail
// (server-to-client length rules). The server token is optional (absent when
// the server has nothing to send).
func DecodeTokenRep(dec *encoding.Decoder) ([]byte, error) {
	sub, numPrm, typ, err := gssReadHead(dec)
	if err != nil {
		return nil, err
	}
	if typ != phaseTokenRep || (numPrm != 2 && numPrm != 3) {
		return nil, fmt.Errorf("invalid gss reply type %d with %d parameters - expected type %d", typ, numPrm, phaseTokenRep)
	}
	if numPrm == 3 {
		return sub.AuthBytes(), nil
	}
	return nil, nil
}

// EncodeFinalReq implements the Method interface.
func (a *GSS) EncodeFinalReq(prms *Prms) error {
	subPrms := prms.addPrms()
	subPrms.AddString(oidKerberosV5)
	subPrms.addBytes([]byte{phaseFinalReq})
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
	_, oid := sub.LIString()
	if err := gssCheckOID(oid); err != nil {
		return err
	}
	_, typB := sub.LIBytes()
	if len(typB) != 1 {
		return fmt.Errorf("invalid gss type size %d", len(typB))
	}
	if typ := typB[0]; typ != phaseFinalReq {
		return fmt.Errorf("invalid gss request type %d - expected %d", typ, phaseFinalReq)
	}
	return nil
}

// DecodeFinalReply implements the Method interface. The session cookie is
// optional; its absence is tolerated.
func (a *GSS) DecodeFinalReply(dec *encoding.Decoder) error {
	if err := DecodeAndCheckNumPrm(dec, 2); err != nil {
		return err
	}
	mt := dec.AuthString()
	if err := checkAuthMethodType(mt, a.Typ()); err != nil {
		return err
	}
	sub, numPrm, typ, err := gssReadHead(dec)
	if err != nil {
		return err
	}
	if typ != phaseFinalRep || (numPrm != 2 && numPrm != 3) {
		return fmt.Errorf("invalid gss reply type %d with %d parameters - expected type %d", typ, numPrm, phaseFinalRep)
	}
	if numPrm == 3 {
		a._cookie = sub.AuthBytes()
	}
	return nil
}

// gssReadHead reads the framing common to every reply detail - parameter
// count, mechanism OID and the one-byte phase discriminator - and returns a
// decoder positioned at the remaining parameters.
func gssReadHead(dec *encoding.Decoder) (sub *encoding.Decoder, numPrm int, phase byte, err error) {
	sub = encoding.NewDecoder(dec.AuthBytes(), dec.Transformer())
	if numPrm = int(sub.Int16()); numPrm < 2 {
		return nil, 0, 0, fmt.Errorf("gss: parameter count %d below minimum 2", numPrm)
	}
	if err = gssCheckOID(sub.AuthString()); err != nil {
		return nil, 0, 0, err
	}
	b := sub.AuthBytes()
	if len(b) != 1 {
		return nil, 0, 0, fmt.Errorf("gss: discriminator width %d, want 1 byte", len(b))
	}
	return sub, numPrm, b[0], nil
}

func gssCheckOID(oid string) error {
	if oid != oidKerberosV5 {
		return fmt.Errorf("invalid gss mechanism %s - expected %s", oid, oidKerberosV5)
	}
	return nil
}
