package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"time"
)

type command int

type authMethod int

const (
	version5 = 0x05
	// Address types
	addrTypeIPv4 = 0x01
	addrTypeFQDN = 0x03
	addrTypeIPv6 = 0x04
	// Command types
	cmdConnect command = 0x01
	// Authentication methods
	authNotRequired    authMethod = 0x00
	authBasic          authMethod = 0x02
	authJWT            authMethod = 0x80
	authNoneAcceptable authMethod = 0xff
	// Authentication method versions
	authBasicVersion = 0x01
	authJWTVersion   = 0x01
	authReplySuccess = 0x00
)

// A Dialer opens connections to a target server via a SOCKS5 proxy.
type Dialer struct {
	*Config
	authMethods []authMethod
}

// NewDialer creates a Dialer pointing to the SOCKS5 server specified
// in config.
func NewDialer(config *Config) *Dialer {
	d := &Dialer{Config: config}
	d.authMethods = []authMethod{authNotRequired}
	if config.JWTToken != "" {
		d.authMethods = append(d.authMethods, authJWT)
	}
	if config.User != "" {
		d.authMethods = append(d.authMethods, authBasic)
	}
	return d
}

// DialContext establishes a connection to the server at addr via the
// proxy server configured in d.
func (d *Dialer) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", d.Address)
	if err != nil {
		return nil, err
	}
	err = d.connect(ctx, conn, addr)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// connect performs the SOCKS5 initial handshake
func (d *Dialer) connect(ctx context.Context, conn net.Conn, addr string) error {
	host, port, err := splitHostPort(addr)
	if err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok && !deadline.IsZero() {
		conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}

	errCh := make(chan error, 1)
	done := make(chan struct{})
	defer func() {
		close(done)
	}()
	go func() {
		select {
		case <-ctx.Done():
			conn.SetDeadline(time.Unix(1, 0))
			errCh <- ctx.Err()
		case <-done:
			errCh <- nil
		}
	}()
	// Version/method selection
	// +----+----------+----------+
	// |VER | NMETHODS | METHODS  |
	// +----+----------+----------+
	// | 1  |    1     | 1 to 255 |
	// +----+----------+----------+
	b := make([]byte, 0, 6+len(host))
	// Set version 5
	b = append(b, version5)
	// Set authentication methods
	b = append(b, byte(len(d.authMethods)))
	for _, m := range d.authMethods {
		b = append(b, byte(m))
	}
	// Send payload
	if _, err = conn.Write(b); err != nil {
		return err
	}
	// Read response
	if _, err = io.ReadFull(conn, b[:2]); err != nil {
		return err
	}
	// Response:
	// +----+--------+
	// |VER | METHOD |
	// +----+--------+
	// | 1  |   1    |
	// +----+--------+
	// Check version
	if b[0] != version5 {
		return fmt.Errorf("unexpected SOCKS version %d; expected %d", b[0], version5)
	}
	// Authenticate
	if err = d.authenticate(ctx, conn, authMethod(b[1])); err != nil {
		return fmt.Errorf("authentication failed: %s", err)
	}

	// Request
	// +----+-----+-------+------+----------+----------+
	// |VER | CMD |  RSV  | ATYP | DST.ADDR | DST.PORT |
	// +----+-----+-------+------+----------+----------+
	// | 1  |  1  | X'00' |  1   | Variable |    2     |
	// +----+-----+-------+------+----------+----------+
	b = b[:0]
	b = append(b, version5, byte(cmdConnect), 0) // VER CMD RSV
	// Set target address
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			b = append(b, addrTypeIPv4) // ATYP
			b = append(b, ip4...)       // DST.ADDR
		} else if ip6 := ip.To16(); ip6 != nil {
			b = append(b, addrTypeIPv6) // ATYP
			b = append(b, ip6...)       // DST.ADDR
		} else {
			return fmt.Errorf("unknown address type for %s; expected hostname, IPv4 or IPv6", host)
		}
	} else {
		if len(host) > 255 {
			return errors.New("hostname cannot exceed 255 bytes")
		}
		b = append(b, addrTypeFQDN)    // ATYP
		b = append(b, byte(len(host))) // (ADDRLEN)
		b = append(b, host...)         // DST.ADDR
	}
	b = append(b, (byte(port >> 8)), byte(port)) // DST.PORT
	// Send payload
	if _, err = conn.Write(b); err != nil {
		return err
	}
	// Read response
	if _, err = io.ReadFull(conn, b[:4]); err != nil {
		return err
	}
	// Response:
	// +----+-----+-------+------+----------+----------+
	// |VER | REP |  RSV  | ATYP | BND.ADDR | BND.PORT |
	// +----+-----+-------+------+----------+----------+
	// | 1  |  1  | 0x00  |  1   | Variable |    2     |
	// +----+-----+-------+------+----------+----------+
	// Check version
	if b[0] != version5 {
		return fmt.Errorf("unexpected SOCKS version %d; expected %d", b[0], version5)
	}
	// Check reply code
	if r := reply(b[1]); r != replySucceeded {
		return errors.New("unexpected reply " + r.String())
	}
	// Check reserved octet
	if b[2] != 0 {
		return fmt.Errorf("unexpected value %d in reserved field; value should be zero", b[2])
	}
	// Check address type and skip the address itself
	switch b[3] {
	case addrTypeFQDN:
		if _, err := io.ReadFull(conn, b[:1]); err != nil {
			return err
		}
		io.CopyN(ioutil.Discard, conn, int64(b[0]))
	case addrTypeIPv4:
		io.CopyN(ioutil.Discard, conn, 4)
	case addrTypeIPv6:
		io.CopyN(ioutil.Discard, conn, 16)
	default:
		return fmt.Errorf("unknown address type 0x%X; expected 0x01 (IPv4), 0x03 (domain name) or 0x04 (IPv6)", b[3])
	}
	// Skip BND.PORT
	io.CopyN(ioutil.Discard, conn, 2)
	return nil
}

// authenticate calls the authentication sub-negotiation specific to method
func (d *Dialer) authenticate(ctx context.Context, conn net.Conn, method authMethod) error {
	switch method {
	case authNotRequired:
		return nil
	case authBasic:
		return d.authenticateBasic(ctx, conn)
	case authJWT:
		return d.authenticateJWT(ctx, conn)
	}
	return fmt.Errorf("unsupported authentication method %d", method)
}

// authenticateBasic performs the username/password authentication sub-negotiation
func (d *Dialer) authenticateBasic(ctx context.Context, conn net.Conn) error {
	if len(d.User) == 0 {
		return errors.New("username cannot be empty")
	}
	if len(d.User) > 255 {
		return errors.New("username cannot exceed 255 bytes")
	}
	if len(d.Password) == 0 {
		return errors.New("password cannot be empty")
	}
	if len(d.Password) > 255 {
		return errors.New("password cannod exceed 255 bytes")
	}
	// Request:
	// +----+------+----------+------+----------+
	// |VER | ULEN |  UNAME   | PLEN |  PASSWD  |
	// +----+------+----------+------+----------+
	// | 1  |  1   | 1 to 255 |  1   | 1 to 255 |
	// +----+------+----------+------+----------+
	b := []byte{authBasicVersion}
	b = append(b, byte(len(d.User)))
	b = append(b, d.User...)
	b = append(b, byte(len(d.Password)))
	b = append(b, d.Password...)
	if _, err := conn.Write(b); err != nil {
		return err
	}
	if _, err := io.ReadFull(conn, b[:2]); err != nil {
		return err
	}
	// Response:
	// +----+--------+
	// |VER | STATUS |
	// +----+--------+
	// | 1  |   1    |
	// +----+--------+
	if b[0] != authBasicVersion {
		return fmt.Errorf("invalid username/password authentication version %d; expected %d", b[0], authBasicVersion)
	}
	if b[1] != authReplySuccess {
		return fmt.Errorf("username/password authentication failed with error code %d", b[1])
	}
	return nil
}

// authenticateJWT performs the custom sub-negotiation for authentication with
// a JWT token: https://bit.ly/37KJb3q
func (d *Dialer) authenticateJWT(ctx context.Context, conn net.Conn) error {
	if len(d.JWTToken) == 0 {
		return errors.New("JWT token cannot be empty")
	}
	if len(d.LocationID) > 255 {
		return errors.New("location ID cannot exceed 255 bytes")
	}
	// Request:
	// +----+------+----------+------+-----------+
	// |VER | TLEN |  TOKEN   | LLEN |  LOC_ID   |
	// +----+------+----------+------+-----------+
	// | 1  |  4   | Variable |  1   | Variable  |
	// +----+------+----------+------+-----------+
	b := &bytes.Buffer{}
	b.Grow(1 + 4 + len(d.JWTToken) + 1 + len(d.LocationID))
	b.WriteByte(authJWTVersion)
	binary.Write(b, binary.BigEndian, int32(len(d.JWTToken)))
	b.WriteString(d.JWTToken)
	b.WriteByte(byte(len(d.LocationID)))
	if d.LocationID != "" {
		b.WriteString(d.LocationID)
	}
	if _, err := conn.Write(b.Bytes()); err != nil {
		return err
	}
	r := make([]byte, 2)
	if _, err := io.ReadFull(conn, r); err != nil {
		return err
	}
	if r[0] != authJWTVersion {
		return fmt.Errorf("invalid JWT authentication version %d; expected %d", r[0], authJWTVersion)
	}
	if r[1] != authReplySuccess {
		return fmt.Errorf("JWT authentication failed with error code %d", r[1])
	}
	return nil
}

func splitHostPort(addr string) (string, int, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, err
	}
	if portNum < 1 || portNum > 0xFFFF {
		return "", 0, fmt.Errorf("port number %s out of range", port)
	}
	return host, portNum, nil
}
