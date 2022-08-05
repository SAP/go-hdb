// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/SAP/go-hdb/driver/dial"
	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// conn attributes default values.
const (
	defaultBufferSize   = 16276             // default value bufferSize.
	defaultBulkSize     = 10000             // default value bulkSize.
	defaultTimeout      = 300 * time.Second // default value connection timeout (300 seconds = 5 minutes).
	defaultTCPKeepAlive = 15 * time.Second  // default TCP keep-alive value (copied from net.dial.go)
)

// minimal / maximal values.
const (
	minTimeout  = 0 * time.Second // minimal timeout value.
	minBulkSize = 1               // minimal bulkSize value.
	maxBulkSize = p.MaxNumArg     // maximum bulk size.
)

// connAttrs is holding connection relevant attributes.
type connAttrs struct {
	mu             sync.RWMutex
	_host          string
	_timeout       time.Duration
	_pingInterval  time.Duration
	_bufferSize    int
	_bulkSize      int
	_tcpKeepAlive  time.Duration // see net.Dialer
	_tlsConfig     *tls.Config
	_defaultSchema string
	_dialer        dial.Dialer
}

func newConnAttrs() *connAttrs {
	return &connAttrs{
		_bufferSize:   defaultBufferSize,
		_bulkSize:     defaultBulkSize,
		_timeout:      defaultTimeout,
		_tcpKeepAlive: defaultTCPKeepAlive,
		_dialer:       dial.DefaultDialer,
	}
}

func (a *connAttrs) host() string           { a.mu.RLock(); defer a.mu.RUnlock(); return a._host }
func (a *connAttrs) timeout() time.Duration { a.mu.RLock(); defer a.mu.RUnlock(); return a._timeout }
func (a *connAttrs) _setTimeout(timeout time.Duration) {
	if timeout < minTimeout {
		timeout = minTimeout
	}
	a._timeout = timeout
}
func (a *connAttrs) setTimeout(timeout time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._setTimeout(timeout)
}
func (a *connAttrs) pingInterval() time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a._pingInterval
}
func (a *connAttrs) setPingInterval(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._pingInterval = d
}
func (a *connAttrs) bufferSize() int { a.mu.RLock(); defer a.mu.RUnlock(); return a._bufferSize }
func (a *connAttrs) setBufferSize(bufferSize int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._bufferSize = bufferSize
}
func (a *connAttrs) bulkSize() int { a.mu.RLock(); defer a.mu.RUnlock(); return a._bulkSize }
func (a *connAttrs) _setBulkSize(bulkSize int) {
	switch {
	case bulkSize < minBulkSize:
		bulkSize = minBulkSize
	case bulkSize > maxBulkSize:
		bulkSize = maxBulkSize
	}
	a._bulkSize = bulkSize
}
func (a *connAttrs) setBulkSize(bulkSize int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._setBulkSize(bulkSize)
}
func (a *connAttrs) tcpKeepAlive() time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a._tcpKeepAlive
}
func (a *connAttrs) setTCPKeepAlive(tcpKeepAlive time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._tcpKeepAlive = tcpKeepAlive
}
func (a *connAttrs) tlsConfig() *tls.Config {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a._tlsConfig.Clone()
}
func (a *connAttrs) setTLSConfig(tlsConfig *tls.Config) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a._tlsConfig = tlsConfig.Clone()
}
func (a *connAttrs) _setTLS(serverName string, insecureSkipVerify bool, rootCAFiles []string) error {
	a._tlsConfig = &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkipVerify,
	}
	var certPool *x509.CertPool
	for _, fn := range rootCAFiles {
		rootPEM, err := os.ReadFile(fn)
		if err != nil {
			return err
		}
		if certPool == nil {
			certPool = x509.NewCertPool()
		}
		if ok := certPool.AppendCertsFromPEM(rootPEM); !ok {
			return fmt.Errorf("failed to parse root certificate - filename: %s", fn)
		}
	}
	if certPool != nil {
		a._tlsConfig.RootCAs = certPool
	}
	return nil
}
func (a *connAttrs) setTLS(serverName string, insecureSkipVerify bool, rootCAFiles []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a._setTLS(serverName, insecureSkipVerify, rootCAFiles)
}
func (a *connAttrs) defaultSchema() string          { return a._defaultSchema }
func (a *connAttrs) setDefaultSchema(schema string) { a._defaultSchema = schema }
func (a *connAttrs) dialer() dial.Dialer            { a.mu.RLock(); defer a.mu.RUnlock(); return a._dialer }
func (a *connAttrs) setDialer(dialer dial.Dialer) {
	if dialer == nil {
		dialer = dial.DefaultDialer
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a._dialer = dialer
}
