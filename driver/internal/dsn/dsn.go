// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Package dsn implements dsn (data source name) handling for go-hdb.
package dsn

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// DSN parameters. For parameter client locale see http://help.sap.com/hana/SAP_HANA_SQL_Command_Network_Protocol_Reference_en.pdf.
const (
	DSNDefaultSchema = "defaultSchema" // Database default schema.
	DSNLocale        = "locale"        // Client locale as described in the protocol reference.
	DSNTimeout       = "timeout"       // Driver side connection timeout in seconds.
	DSNFetchSize     = "fetchSize"     // Maximum number of fetched records from database by database/sql/driver/Rows.Next().
	DSNPingInterval  = "pingInterval"  // Connection ping interval in seconds.
)

/*
DSN TLS parameters.
For more information please see https://golang.org/pkg/crypto/tls/#Config.
For more flexibility in TLS configuration please see driver.Connector.
*/
const (
	DSNTLSRootCAFile         = "TLSRootCAFile"         // Path,- filename to root certificate(s).
	DSNTLSServerName         = "TLSServerName"         // ServerName to verify the hostname.
	DSNTLSInsecureSkipVerify = "TLSInsecureSkipVerify" // Controls whether a client verifies the server's certificate chain and host name.
)

// TLSPrms is holding the TLS parameters of a DSN structure.
type TLSPrms struct {
	ServerName         string
	InsecureSkipVerify bool
	RootCAFiles        []string
}

const urlSchema = "hdb" // mirrored from driver/DriverName

/*
A DSN represents a parsed DSN string. A DSN string is an URL string with the following format

	"hdb://<username>:<password>@<host address>:<port number>"

and optional query parameters (see DSN query parameters and DSN query default values).

Example:
	"hdb://myuser:mypassword@localhost:30015?timeout=60"

Examples TLS connection:
	"hdb://myuser:mypassword@localhost:39013?TLSRootCAFile=trust.pem"
	"hdb://myuser:mypassword@localhost:39013?TLSRootCAFile=trust.pem&TLSServerName=hostname"
	"hdb://myuser:mypassword@localhost:39013?TLSInsecureSkipVerify"
*/
type DSN struct {
	Host               string
	Username, Password string
	DefaultSchema      string
	FetchSize          int
	Timeout            time.Duration
	Locale             string
	PingInterval       time.Duration
	TLS                *TLSPrms
}

// ParseError is the error returned in case DSN is invalid.
type ParseError struct {
	s   string
	err error
}

func (e ParseError) Error() string {
	if err := errors.Unwrap(e.err); err != nil {
		return err.Error()
	}
	return e.s
}

// Unwrap returns the nested error.
func (e ParseError) Unwrap() error { return e.err }

//
func parameterNotSupportedError(k string) error {
	return &ParseError{s: fmt.Sprintf("parameter %s is not supported", k)}
}
func invalidNumberOfParametersError(k string, act, exp int) error {
	return &ParseError{s: fmt.Sprintf("invalid number of parameters for %s %d - expected %d", k, act, exp)}
}
func invalidNumberOfParametersRangeError(k string, act, min, max int) error {
	return &ParseError{s: fmt.Sprintf("invalid number of parameters for %s %d - expected %d - %d", k, act, min, max)}
}
func invalidNumberOfParametersMinError(k string, act, min int) error {
	return &ParseError{s: fmt.Sprintf("invalid number of parameters for %s %d - expected at least %d", k, act, min)}
}
func parseError(k, v string) error {
	return &ParseError{s: fmt.Sprintf("failed to parse %s: %s", k, v)}
}

// Parse parses a DSN string into a DSN structure.
func Parse(s string) (*DSN, error) {
	if s == "" {
		return nil, &ParseError{s: "invalid parameter - DSN is empty"}
	}

	u, err := url.Parse(s)
	if err != nil {
		return nil, &ParseError{err: err}
	}

	dsn := &DSN{Host: u.Host}
	if u.User != nil {
		dsn.Username = u.User.Username()
		password, _ := u.User.Password()
		dsn.Password = password
	}

	for k, v := range u.Query() {
		switch k {

		default:
			return nil, parameterNotSupportedError(k)

		case DSNDefaultSchema:
			if len(v) != 1 {
				return nil, invalidNumberOfParametersError(k, len(v), 1)
			}
			dsn.DefaultSchema = v[0]

		case DSNLocale:
			if len(v) != 1 {
				return nil, invalidNumberOfParametersError(k, len(v), 1)
			}
			dsn.Locale = v[0]

		case DSNTimeout:
			if len(v) != 1 {
				return nil, invalidNumberOfParametersError(k, len(v), 1)
			}
			t, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, parseError(k, v[0])
			}
			dsn.Timeout = time.Duration(t) * time.Second

		case DSNFetchSize:
			if len(v) != 1 {
				return nil, invalidNumberOfParametersError(k, len(v), 1)
			}
			fetchSize, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, parseError(k, v[0])
			}
			dsn.FetchSize = fetchSize

		case DSNPingInterval:
			if len(v) != 1 {
				return nil, invalidNumberOfParametersError(k, len(v), 1)
			}
			t, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, parseError(k, v[0])
			}
			dsn.PingInterval = time.Duration(t) * time.Second

		case DSNTLSServerName:
			if len(v) != 1 {
				return nil, invalidNumberOfParametersError(k, len(v), 1)
			}
			if dsn.TLS == nil {
				dsn.TLS = &TLSPrms{}
			}
			dsn.TLS.ServerName = v[0]

		case DSNTLSInsecureSkipVerify:
			if len(v) > 1 {
				return nil, invalidNumberOfParametersRangeError(k, len(v), 0, 1)
			}
			b := true
			if len(v) > 0 && v[0] != "" {
				b, err = strconv.ParseBool(v[0])
				if err != nil {
					return nil, parseError(k, v[0])
				}
			}
			if dsn.TLS == nil {
				dsn.TLS = &TLSPrms{}
			}
			dsn.TLS.InsecureSkipVerify = b

		case DSNTLSRootCAFile:
			if len(v) == 0 {
				return nil, invalidNumberOfParametersMinError(k, len(v), 1)
			}
			if dsn.TLS == nil {
				dsn.TLS = &TLSPrms{}
			}
			dsn.TLS.RootCAFiles = v
		}
	}
	return dsn, nil
}

// String reassembles the DSN into a valid DSN string.
func (dsn *DSN) String() string {
	values := url.Values{}
	if dsn.DefaultSchema != "" {
		values.Set(DSNDefaultSchema, dsn.DefaultSchema)
	}
	if dsn.Locale != "" {
		values.Set(DSNLocale, dsn.Locale)
	}
	if dsn.Timeout != 0 {
		values.Set(DSNTimeout, fmt.Sprintf("%d", dsn.Timeout/time.Second))
	}
	if dsn.FetchSize != 0 {
		values.Set(DSNFetchSize, fmt.Sprintf("%d", dsn.FetchSize))
	}
	if dsn.PingInterval != 0 {
		values.Set(DSNPingInterval, fmt.Sprintf("%d", dsn.PingInterval/time.Second))
	}
	if dsn.TLS != nil {
		if dsn.TLS.ServerName != "" {
			values.Set(DSNTLSServerName, dsn.TLS.ServerName)
		}
		values.Set(DSNTLSInsecureSkipVerify, strconv.FormatBool(dsn.TLS.InsecureSkipVerify))
		for _, fn := range dsn.TLS.RootCAFiles {
			values.Add(DSNTLSRootCAFile, fn)
		}
	}
	u := &url.URL{
		Scheme:   urlSchema,
		Host:     dsn.Host,
		RawQuery: values.Encode(),
	}
	switch {
	case dsn.Username != "" && dsn.Password != "":
		u.User = url.UserPassword(dsn.Username, dsn.Password)
	case dsn.Username != "":
		u.User = url.User(dsn.Username)
	}
	return u.String()
}
