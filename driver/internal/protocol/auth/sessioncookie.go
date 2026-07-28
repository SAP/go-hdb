package auth

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"golang.org/x/text/transform"
)

// SessionCookie implements session cookie authentication.
type SessionCookie struct {
	cookie    []byte
	logonname string
	clientID  string
}

// NewSessionCookie creates a new authSessionCookie instance.
func NewSessionCookie(cookie []byte, logonname, clientID string) *SessionCookie {
	return &SessionCookie{cookie: cookie, logonname: logonname, clientID: clientID}
}

func (a *SessionCookie) String() string {
	return fmt.Sprintf("method type %s cookie %v", a.Typ(), a.cookie)
}

// Typ implements the Method interface.
func (a *SessionCookie) Typ() string { return MtSessionCookie }

// Order implements the Method interface.
func (a *SessionCookie) Order() byte { return MoSessionCookie }

// PrepareInitReq implements the Method interface.
func (a *SessionCookie) PrepareInitReq(prms *Prms) error {
	prms.addString(a.Typ())
	prms.addBytes(append(a.cookie, a.clientID...)) // cookie + clientID !!!
	return nil
}

// InitRepDecode implements the Method interface.
func (a *SessionCookie) InitRepDecode(_ *encoding.Decoder) error {
	return nil
}

// PrepareFinalReq implements the Method interface.
func (a *SessionCookie) PrepareFinalReq(prms *Prms) error {
	prms.AddCESU8String(a.logonname)
	prms.addString(a.Typ())
	prms.addEmpty() // empty parameter
	return nil
}

// FinalRepDecode implements the Method interface.
func (a *SessionCookie) FinalRepDecode(dec *encoding.Decoder, _ transform.Transformer) error {
	if err := DecodeAndCheckNumPrm(dec, 2); err != nil {
		return err
	}
	mt := dec.AuthString()
	if err := checkAuthMethodType(mt, a.Typ()); err != nil {
		return err
	}
	dec.AuthBytes() // second parameter seems to be empty
	return nil
}
