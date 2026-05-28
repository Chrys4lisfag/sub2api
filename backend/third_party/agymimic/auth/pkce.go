package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
)

// PKCEPair holds an RFC 7636 verifier + S256-derived challenge.
type PKCEPair struct {
	Verifier  string
	Challenge string
}

// NewPKCE generates a fresh PKCE pair (43-128 char verifier, S256 challenge).
func NewPKCE() (PKCEPair, error) {
	raw := make([]byte, 64)
	if _, err := rand.Read(raw); err != nil {
		return PKCEPair{}, err
	}
	verifier := base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	return PKCEPair{Verifier: verifier, Challenge: challenge}, nil
}
