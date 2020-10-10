// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"crypto/tls"
	"time"

	"github.com/SAP/go-hdb/driver/dial"
	"github.com/SAP/go-hdb/internal/container/vermap"
)

// SessionConfig represents the session relevant driver connector options.
type SessionConfig struct {
	DriverVersion, DriverName string
	ApplicationName           string

	Host, Username, Password string
	Locale                   string

	BufferSize, FetchSize, BulkSize, LobChunkSize int

	Dialer       dial.Dialer
	Timeout      time.Duration
	TCPKeepAlive time.Duration

	Dfv              int
	SessionVariables *vermap.VerMap
	TLSConfig        *tls.Config
	Legacy           bool
}
