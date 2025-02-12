//go:build !go1.24

package auth

import (
	"crypto/sha256"

	"golang.org/x/crypto/pbkdf2"
)

func scrampbkdf2sha256Key(password string, salt []byte, rounds int) ([]byte, error) {
	return _sha256(pbkdf2.Key([]byte(password), salt, rounds, clientProofSize, sha256.New)), nil
}
