// Package gss defines the interface for Kerberos authentication via GSS-API.
// See the package README for rationale, the mapping to the native GSS API,
// and implementation notes.
package gss

import "context"

// Initiator drives one connection's GSS-API context establishment. The driver
// never calls the GSS API itself; the application wraps the native API
// (gss_init_sec_context on Linux/macOS, InitializeSecurityContext on Windows
// SSPI) behind this interface. The driver obtains a fresh Initiator per
// connection from ConnectorConfig.GSS, so each connection owns its own GSS
// security context and no locking is required.
type Initiator interface {
	// SrcName returns the initiator (client) name - the Kerberos client
	// principal (user@REALM) in practice. The GSS term is used here because
	// the interface is mechanism-neutral; a Windows/SSPI implementor reads it
	// as the client credential's name.
	SrcName() string
	// InitSecContext advances the security context, mirroring
	// gss_init_sec_context. targetName is the service name the server
	// announced; inputToken is the server's latest token, nil on the first
	// call; the returned outputToken is sent to the server. established
	// reports that the context is complete (GSS_S_COMPLETE vs
	// GSS_S_CONTINUE_NEEDED); the driver calls InitSecContext in a loop until
	// then.
	InitSecContext(ctx context.Context, targetName string, inputToken []byte) (outputToken []byte, established bool, err error)
}
