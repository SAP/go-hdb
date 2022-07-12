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
	DriverVersion, DriverName     string
	Username, Password            string
	ClientCertFile, ClientKeyFile string
	Token                         string // jwt
	ApplicationName               string
	SessionVariables              *vermap.VerMap
	Locale                        string
	FetchSize, LobChunkSize       int
	Dfv                           int
	Legacy                        bool
	CESU8Decoder                  func() transform.Transformer
	CESU8Encoder                  func() transform.Transformer
}

// Clone returns a (shallow) copy (clone) of a SessionConfig.
func (c *SessionConfig) Clone() *SessionConfig {
	return &SessionConfig{
		DriverVersion:    c.DriverVersion,
		DriverName:       c.DriverName,
		Username:         c.Username,
		Password:         c.Password,
		ClientCertFile:   c.ClientCertFile,
		ClientKeyFile:    c.ClientKeyFile,
		Token:            c.Token,
		ApplicationName:  c.ApplicationName,
		SessionVariables: c.SessionVariables, //TODO real clone
		Locale:           c.Locale,
		FetchSize:        c.FetchSize,
		LobChunkSize:     c.LobChunkSize,
		Dfv:              c.Dfv,
		Legacy:           c.Legacy,
		CESU8Decoder:     c.CESU8Decoder,
		CESU8Encoder:     c.CESU8Encoder,
	}
}
