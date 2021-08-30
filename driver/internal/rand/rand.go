// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Package rand implements random value functions.
package rand

import (
	"crypto/rand"
)

const (
	// alpa numeric character set
	csAlphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// AlphanumReader is a global, shared instance of a random generator of alpha-numeric characters.
var AlphanumReader = new(alphanumReader)

type alphanumReader struct{}

func (r *alphanumReader) Read(p []byte) (n int, err error) {
	if n, err = rand.Read(p); err != nil {
		return n, err
	}
	size := byte(len(csAlphanum)) // len character sets <= max(byte)
	for i, b := range p {
		p[i] = csAlphanum[b%size]
	}
	return n, nil
}

// AlphanumString returns a random string of alphanumeric characters and panics if crypto random reader returns an error.
func AlphanumString(n int) string {
	b := make([]byte, n)
	if _, err := AlphanumReader.Read(b); err != nil {
		panic(err.Error()) // rand should never fail
	}
	return string(b)
}
