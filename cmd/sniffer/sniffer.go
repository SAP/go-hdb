/*
Copyright 2020 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	p "github.com/SAP/go-hdb/internal/protocol"
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

	err = p.NewSniffer(conn, dbConn).Do()
	if err == nil {
		return
	}

	switch err {
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

func cli() (net.Addr, net.Addr) {
	const usageText = `
%[1]s is a Hana Network Protocol analyzer. It lets you see whats happening
on protocol level connecting a client to the database server.
%[1]s is an early alpha-version, supporting mainly go-hdb based clients.

Using with other clients might
- completely fail or
- provide incomplete output

Usage of %[1]s:
`
	addr := &addrValue{addr: defaultAddr}
	dbAddr := &addrValue{addr: defaultDBAddr}

	args := flag.NewFlagSet("", flag.ExitOnError)
	args.Usage = func() {
		fmt.Fprintf(args.Output(), usageText, os.Args[0])
		args.PrintDefaults()
	}
	args.Var(addr, "s", "<host:port>: Sniffer address to accept connections. (required)")
	args.Var(dbAddr, "db", "<host:port>: Database address to connect to. (required)")

	args.Parse(os.Args[1:])

	return addr, dbAddr
}
