//go:build !go1.24

package auth

import "crypto/rand"

func clientChallenge() []byte {
	r := make([]byte, clientChallengeSize)
	if _, err := rand.Read(r); err != nil {
		panic(err)
	}
	return r
}
