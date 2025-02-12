//go:build go1.24

package auth

import (
	"crypto/pbkdf2"
	"crypto/sha256"
)

func scrampbkdf2sha256Key(password string, salt []byte, rounds int) ([]byte, error) {
	b, err := pbkdf2.Key(sha256.New, password, salt, rounds, clientProofSize)
	if err != nil {
		return nil, err
	}
	return _sha256(b), nil
}
