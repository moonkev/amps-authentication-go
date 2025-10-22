//go:build windows
// +build windows

package amps_kerberos

import (
	"fmt"

	"github.com/alexbrainman/sspi"
	"github.com/alexbrainman/sspi/negotiate"
	"github.com/moonkev/amps_kerberos/amps"
)

type AMPSKerberosSSPIAuthenticator struct {
	AuthBase
	context *negotiate.ClientContext
	cred    *sspi.Credentials
}

// NewAuthenticator creates a new Kerberos authenticator using Windows SSPI
func NewAuthenticator(spn string, _ string, _ string) (amps.Authenticator, error) {
	return &AMPSKerberosSSPIAuthenticator{
		AuthBase: AuthBase{spn: spn},
	}, nil
}

// Authenticate implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Authenticate(username string, password string) (string, error) {
	cred, err := negotiate.AcquireCurrentUserCredentials()
	if err != nil {
		return "", fmt.Errorf("failed to acquire credentials: %v", err)
	}
	auth.cred = cred

	context, initialToken, err := negotiate.NewClientContext(cred, auth.spn)
	if err != nil {
		return "", fmt.Errorf("failed to initialize security context: %v", err)
	}
	auth.context = context

	return auth.encodeToken(initialToken), nil
}

// Retry implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Retry(username string, password string) (string, error) {
	if auth.context == nil {
		return "", fmt.Errorf("no active security context")
	}

	inToken, err := auth.decodeToken(password)
	if err != nil {
		return "", err
	}

	authCompleted, outToken, err := auth.context.Update(inToken)
	if err != nil {
		return "", fmt.Errorf("authentication step failed: %v", err)
	}

	if authCompleted {
		return "", nil // Authentication complete
	}

	return auth.encodeToken(outToken), nil
}

// Completed implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Completed(username string, password string, reason string) {
	if auth.context != nil {
		auth.context.Release()
		auth.context = nil
	}
	if auth.cred != nil {
		auth.cred.Release()
		auth.cred = nil
	}
}
