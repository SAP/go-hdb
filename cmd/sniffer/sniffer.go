package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/SAP/go-hdb/driver"
)

func main() {
	addr, dbAddr := cli()

	log.Printf("listening on %s (database address %s)", addr.String(), dbAddr.String())

	l, err := net.Listen(addr.Network(), addr.String()) //nolint: noctx
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("accept error: %s", err)
			continue
		}

		go handler(conn, dbAddr)
	}
}

func handler(conn net.Conn, dbAddr net.Addr) {
	defer conn.Close()

	// A structurally corrupt protocol frame panics the shared reader by design
	// (see driver/internal/protocol: negative or out-of-range declared lengths
	// fail fast). Contain it to this connection so one bad client cannot crash
	// the proxy and drop every other proxied session.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("sniffer panic recovered - close connection - local address %s - remote address %s: %v",
				conn.LocalAddr().String(),
				conn.RemoteAddr().String(),
				r,
			)
		}
	}()

	dbConn, err := net.Dial(dbAddr.Network(), dbAddr.String()) //nolint: noctx
	if err != nil {
		log.Printf("hdb connection error: %s", err)
		return
	}

	defer dbConn.Close()

	err = driver.NewSniffer(conn, dbConn).Run()
	switch {
	case errors.Is(err, io.EOF):
		log.Printf("client connection closed - local address %s - remote address %s",
			conn.LocalAddr().String(),
			conn.RemoteAddr().String(),
		)
	default:
		log.Printf("sniffer protocol error: %s - close connection - local address %s - remote address %s",
			err,
			conn.LocalAddr().String(),
			conn.RemoteAddr().String(),
		)
	}
}

const (
	defaultAddr   = "localhost:50000"
	defaultDBAddr = "localhost:39013"
)

type addrValue struct {
	addr string
}

func (v *addrValue) String() string  { return v.addr }
func (v *addrValue) Network() string { return "tcp" }
func (v *addrValue) Set(s string) error {
	if _, _, err := net.SplitHostPort(s); err != nil {
		return err
	}
	v.addr = s
	return nil
}

func cli() (addr, dbAddr net.Addr) {
	const usageText = `
%[1]s is a HANA network protocol analyzer. It lets you see what happens
at the protocol level when connecting a client to the database server.
%[1]s is an early alpha version, supporting mainly go-hdb-based clients.

Using with other clients might
- completely fail or
- provide incomplete output

%[1]s is a development and diagnostic tool. Trace redaction is honored for
structured protocol parts, but SQL command text is not redacted and may
expose secrets contained in statements.

Usage of %[1]s:
`
	a := &addrValue{addr: defaultAddr}
	dba := &addrValue{addr: defaultDBAddr}

	args := flag.NewFlagSet("", flag.ExitOnError)
	args.Usage = func() {
		fmt.Fprintf(args.Output(), usageText, os.Args[0])
		args.PrintDefaults()
	}
	args.Var(a, "s", "<host:port>: Sniffer address to accept connections. (required)")
	args.Var(dba, "db", "<host:port>: Database address to connect to. (required)")

	args.Parse(os.Args[1:]) //nolint:errcheck

	return a, dba
}
