// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"net"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// Sniff instatiates and runs a protocol sniffer.
func Sniff(conn net.Conn, dbConn net.Conn) error { return p.NewSniffer(conn, dbConn).Do() }
