//go:build go1.24

package auth

import "crypto/rand"

func clientChallenge() []byte {
	r := make([]byte, clientChallengeSize)
	// does not return err starting with go1.24
	rand.Read(r) //nolint: errcheck
	return r
}
