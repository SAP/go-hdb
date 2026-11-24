//go:build gssapi && linux

package gss

// Reference Kerberos Initiator backed by the system libgssapi_krb5 (MIT
// Kerberos). Build-tagged so neither cgo nor libgssapi are dependencies of the
// default build path or of go-hdb consumers.
//
// developer-machine prerequisites:
// .Linux: apt install libkrb5-dev (or distro equivalent)
//
// It uses raw Kerberos V5 (not SPNEGO) and requests mutual authentication,
// integrity, confidentiality and replay detection. It is a known-correct
// reference, not a hardened production implementation; provide your own
// Initiator for production.

// #cgo pkg-config: krb5-gssapi
// #include <stdlib.h>
// #include <string.h>
// #include <gssapi/gssapi.h>
// #include <gssapi/gssapi_krb5.h>
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"unsafe"
)

// requested context flags: mutual authentication, integrity, confidentiality
// and replay detection.
const krb5ReqFlags = C.GSS_C_MUTUAL_FLAG | C.GSS_C_INTEG_FLAG | C.GSS_C_CONF_FLAG | C.GSS_C_REPLAY_FLAG

// krb5Initiator is one connection's GSS security context.
type krb5Initiator struct {
	contextHandle C.gss_ctx_id_t
}

// NewInitiator returns a Kerberos Initiator using the default credential from
// the current credential cache (populated by kinit or a keytab).
func NewInitiator() (Initiator, error) {
	return &krb5Initiator{contextHandle: C.GSS_C_NO_CONTEXT}, nil
}

// SrcName returns the client principal from the default credential.
func (a *krb5Initiator) SrcName() string {
	var minor C.OM_uint32
	var cred C.gss_cred_id_t = C.GSS_C_NO_CREDENTIAL
	var name C.gss_name_t
	major := C.gss_inquire_cred(&minor, cred, &name, nil, nil, nil)
	if major != C.GSS_S_COMPLETE {
		return ""
	}
	defer C.gss_release_name(&minor, &name)

	var buf C.gss_buffer_desc
	major = C.gss_display_name(&minor, name, &buf, nil)
	if major != C.GSS_S_COMPLETE {
		return ""
	}
	defer C.gss_release_buffer(&minor, &buf)
	return C.GoStringN((*C.char)(buf.value), C.int(buf.length))
}

// InitSecContext advances the security context, mirroring gss_init_sec_context.
func (a *krb5Initiator) InitSecContext(_ context.Context, targetName string, inputToken []byte) ([]byte, bool, error) {
	var minor C.OM_uint32

	// import the service principal name (a Kerberos principal, e.g. hdb/host@REALM).
	cName := C.CString(targetName)
	defer C.free(unsafe.Pointer(cName))
	nameBuf := C.gss_buffer_desc{length: C.size_t(len(targetName)), value: unsafe.Pointer(cName)}
	var target C.gss_name_t
	if major := C.gss_import_name(&minor, &nameBuf, C.GSS_KRB5_NT_PRINCIPAL_NAME, &target); major != C.GSS_S_COMPLETE {
		return nil, false, gssError("gss_import_name", major, minor)
	}
	defer C.gss_release_name(&minor, &target)

	var inBuf C.gss_buffer_desc
	if len(inputToken) > 0 {
		pin := runtime.Pinner{}
		pin.Pin(&inputToken[0])
		defer pin.Unpin()
		inBuf.length = C.size_t(len(inputToken))
		inBuf.value = unsafe.Pointer(&inputToken[0])
	}

	var outBuf C.gss_buffer_desc
	var retFlags C.OM_uint32
	major := C.gss_init_sec_context(
		&minor,
		C.GSS_C_NO_CREDENTIAL, // default credential from the cache
		&a.contextHandle,
		target,
		C.gss_mech_krb5, // Kerberos V5 mechanism OID (MIT: extern const gss_OID)
		krb5ReqFlags,
		0, // default lifetime
		C.GSS_C_NO_CHANNEL_BINDINGS,
		&inBuf,
		nil, // no actual mech type
		&outBuf,
		&retFlags,
		nil, // no time_rec
	)
	if major != C.GSS_S_COMPLETE && major != C.GSS_S_CONTINUE_NEEDED {
		return nil, false, gssError("gss_init_sec_context", major, minor)
	}
	defer C.gss_release_buffer(&minor, &outBuf)

	outputToken := C.GoBytes(outBuf.value, C.int(outBuf.length))
	established := major == C.GSS_S_COMPLETE
	return outputToken, established, nil
}

func gssError(call string, major, minor C.OM_uint32) error {
	return fmt.Errorf("gss: %s failed (major 0x%08x, minor 0x%08x)", call, uint32(major), uint32(minor))
}
