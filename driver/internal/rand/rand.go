// Package rand implements function for randomized content.
package rand

import (
	"crypto/rand"
)

const (
	// alphanumeric character set.
	csAlphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// AlphanumReader is a global shared instance of an alphanumeric character random generator.
var AlphanumReader = alphanum{}

type alphanum struct{}

func (r *alphanum) Read(p []byte) (n int, err error) {
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
		panic(err) // rand should never fail
	}
	return string(b)
}
