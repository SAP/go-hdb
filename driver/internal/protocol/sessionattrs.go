// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"os"
	"sync"

	"github.com/SAP/go-hdb/driver/unicode/cesu8"
	"golang.org/x/text/transform"
)

var defaultApplicationName, _ = os.Executable()

const (
	defaultFetchSize    = 128       // Default value fetchSize.
	defaultLobChunkSize = 8192      // Default value lobChunkSize.
	defaultDfv          = DfvLevel8 // Default data version format level.
	defaultLegacy       = false     // Default value legacy.
)

const (
	minFetchSize    = 1       // Minimal fetchSize value.
	minLobChunkSize = 128     // Minimal lobChunkSize
	maxLobChunkSize = 1 << 14 // Maximal lobChunkSize (TODO check)
)

// SessionAttrs represents the session relevant driver connector attributes.
type SessionAttrs struct {
	mu               sync.RWMutex
	applicationName  string
	sessionVariables map[string]string
	locale           string
	fetchSize        int
	lobChunkSize     int
	dfv              int
	legacy           bool
	cesu8Decoder     func() transform.Transformer
	cesu8Encoder     func() transform.Transformer
}

// NewSessionAttrs returns a new SessionAttrs instance.
func NewSessionAttrs() *SessionAttrs {
	return &SessionAttrs{
		applicationName: defaultApplicationName,
		fetchSize:       defaultFetchSize,
		lobChunkSize:    defaultLobChunkSize,
		dfv:             defaultDfv,
		legacy:          defaultLegacy,
		cesu8Decoder:    cesu8.DefaultDecoder,
		cesu8Encoder:    cesu8.DefaultEncoder,
	}
}

// Clone returns a clone of a SessionAttrs.
func (a *SessionAttrs) Clone() *SessionAttrs {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return &SessionAttrs{
		applicationName:  a.applicationName,
		sessionVariables: cloneSessionVariables(a.sessionVariables),
		locale:           a.locale,
		fetchSize:        a.fetchSize,
		lobChunkSize:     a.lobChunkSize,
		dfv:              a.dfv,
		legacy:           a.legacy,
		cesu8Decoder:     a.cesu8Decoder,
		cesu8Encoder:     a.cesu8Encoder,
	}
}

// ApplicationName returns the application name attribute.
func (a *SessionAttrs) ApplicationName() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.applicationName
}

// SetApplicationName sets the application name attribute.
func (a *SessionAttrs) SetApplicationName(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.applicationName = name
}

// SessionVariables returns the session variables attribute.
func (a *SessionAttrs) SessionVariables() map[string]string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return cloneSessionVariables(a.sessionVariables)
}

// SetSessionVariables sets the session varibles attribute.
func (a *SessionAttrs) SetSessionVariables(sessionVariables map[string]string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sessionVariables = cloneSessionVariables(sessionVariables)
}

// Locale returns the locale attribute.
func (a *SessionAttrs) Locale() string { a.mu.RLock(); defer a.mu.RUnlock(); return a.locale }

// SetLocale sets the locale attribute.
func (a *SessionAttrs) SetLocale(locale string) { a.mu.Lock(); defer a.mu.Unlock(); a.locale = locale }

// FetchSize returns the fetch size attribute.
func (a *SessionAttrs) FetchSize() int { a.mu.RLock(); defer a.mu.RUnlock(); return a.fetchSize }

func (a *SessionAttrs) setFetchSize(fetchSize int) {
	if fetchSize < minFetchSize {
		fetchSize = minFetchSize
	}
	a.fetchSize = fetchSize
}

// SetFetchSize sets the fetch size attribute.
func (a *SessionAttrs) SetFetchSize(fetchSize int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.setFetchSize(fetchSize)
}

// LobChunkSize returns the lob chunk size attribute.
func (a *SessionAttrs) LobChunkSize() int { a.mu.RLock(); defer a.mu.RUnlock(); return a.lobChunkSize }

// SetLobChunkSize sets the lob chunk size attribute.
func (a *SessionAttrs) SetLobChunkSize(lobChunkSize int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case lobChunkSize < minLobChunkSize:
		lobChunkSize = minLobChunkSize
	case lobChunkSize > maxLobChunkSize:
		lobChunkSize = maxLobChunkSize
	}
	a.lobChunkSize = lobChunkSize
}

// Dfv returns the client data format version attribute.
func (a *SessionAttrs) Dfv() int { a.mu.RLock(); defer a.mu.RUnlock(); return a.dfv }

// SetDfv sets the client data format version attribute.
func (a *SessionAttrs) SetDfv(dfv int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !IsSupportedDfv(dfv) {
		dfv = defaultDfv
	}
	a.dfv = dfv
}

// Legacy returns the legacy attribute.
func (a *SessionAttrs) Legacy() bool { a.mu.RLock(); defer a.mu.RUnlock(); return a.legacy }

// SetLegacy sets the connector legacy attribute.
func (a *SessionAttrs) SetLegacy(b bool) { a.mu.Lock(); defer a.mu.Unlock(); a.legacy = b }

// CESU8Decoder returns the CESU-8 decoder of the connector.
func (a *SessionAttrs) CESU8Decoder() func() transform.Transformer {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.cesu8Decoder
}

// SetCESU8Decoder sets the CESU-8 decoder of the connector.
func (a *SessionAttrs) SetCESU8Decoder(cesu8Decoder func() transform.Transformer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cesu8Decoder = cesu8Decoder
}

// CESU8Encoder returns the CESU-8 encoder of the connector.
func (a *SessionAttrs) CESU8Encoder() func() transform.Transformer {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.cesu8Encoder
}

// SetCESU8Encoder sets the CESU-8 encoder of the connector.
func (a *SessionAttrs) SetCESU8Encoder(cesu8Encoder func() transform.Transformer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cesu8Encoder = cesu8Encoder
}
