// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"github.com/SAP/go-hdb/driver/internal/container/vermap"
	"golang.org/x/text/transform"
)

// SessionConfig represents the session relevant driver connector options.
type SessionConfig struct {
	DriverVersion, DriverName           string
	ApplicationName, Username, Password string
	SessionVariables                    *vermap.VerMap
	Locale                              string
	FetchSize, LobChunkSize             int
	Dfv                                 int
	Legacy                              bool
	CESU8Decoder                        func() transform.Transformer
	CESU8Encoder                        func() transform.Transformer
}
