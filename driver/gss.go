package driver

// GSSInitiator provides the client's Kerberos security context for GSS
// authentication (see ConnectorConfig.GSS).
type GSSInitiator interface {
	// Principal returns the client principal name (user@REALM).
	Principal() string
	// InitSecContext returns the GSS-API initial context token (RFC 4121),
	// a Kerberos AP-REQ, for the service principal name the server sends
	// (hdb/host@REALM), and the check of the server token: an AP-REP if
	// mutual authentication was requested, empty otherwise. Connections
	// may call it concurrently.
	InitSecContext(spn string) (token []byte, accept func(reply []byte) error, err error)
}
