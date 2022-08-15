// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
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

	l, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go handler(conn, dbAddr)
	}
}

func handler(conn net.Conn, dbAddr net.Addr) {
	dbConn, err := net.Dial(dbAddr.Network(), dbAddr.String())
	if err != nil {
		log.Printf("hdb connection error: %s", err)
		return
	}

	defer dbConn.Close()

	err = driver.NewSniffer(conn, dbConn).Run()
	switch err {
	case nil:
		return
	case io.EOF:
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
%[1]s is a Hana Network Protocol analyzer. It lets you see whats happening
on protocol level connecting a client to the database server.
%[1]s is an early alpha-version, supporting mainly go-hdb based clients.

Using with other clients might
- completely fail or
- provide incomplete output

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

	args.Parse(os.Args[1:])

	return a, dba
}
