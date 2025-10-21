package amps_kerberos

import (
	"encoding/base64"
	"fmt"
)

// AuthBase provides common functionality for both GSSAPI and SSPI implementations
type AuthBase struct {
	spn string
}

// encodeToken encodes a binary token to base64
func (auth *AuthBase) encodeToken(token []byte) string {
	return base64.StdEncoding.EncodeToString(token)
}

// decodeToken decodes a base64 token to binary
func (auth *AuthBase) decodeToken(token string) ([]byte, error) {
	if token == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("error decoding token: %v", err)
	}
	return decoded, nil
}
