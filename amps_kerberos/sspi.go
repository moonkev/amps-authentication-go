// +build windows

package amps_kerberos

import (
	"fmt"

	"github.com/alexbrainman/sspi"
	"github.com/alexbrainman/sspi/negotiate"
	"github.com/moonkev/amps_kerberos/amps"
)

// AMPSKerberosSSPIAuthenticator provides Kerberos authentication for AMPS using Windows SSPI
type AMPSKerberosSSPIAuthenticator struct {
	AuthBase
	creds    *sspi.Credentials
	context  *negotiate.ClientContext
	targetPN string
}

// NewAuthenticator creates a new Kerberos authenticator using Windows SSPI
func NewAuthenticator(spn string, _ string, _ string) (amps.Authenticator, error) {
	return &AMPSKerberosSSPIAuthenticator{
		AuthBase: AuthBase{spn: spn},
		targetPN: spn,
	}, nil
}

// Authenticate implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Authenticate(username string, password string) (string, error) {
	creds, err := negotiate.AcquireCred()
	if err != nil {
		return "", fmt.Errorf("failed to acquire credentials: %v", err)
	}
	auth.creds = creds

	ctx, err := negotiate.NewClientContext(auth.creds, auth.targetPN)
	if err != nil {
		auth.creds.Release()
		return "", fmt.Errorf("failed to create context: %v", err)
	}
	auth.context = ctx

	token, err := auth.context.Next(nil)
	if err != nil {
		auth.cleanup()
		return "", fmt.Errorf("failed to get initial token: %v", err)
	}

	return auth.encodeToken(token), nil
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

	token, err := auth.context.Next(inToken)
	if err != nil {
		return "", fmt.Errorf("authentication step failed: %v", err)
	}

	if len(token) == 0 {
		return "", nil // Authentication complete
	}

	return auth.encodeToken(token), nil
}

// Completed implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Completed(username string, password string, reason string) {
	auth.cleanup()
}

func (auth *AMPSKerberosSSPIAuthenticator) cleanup() {
	if auth.context != nil {
		auth.context.Release()
		auth.context = nil
	}
	if auth.creds != nil {
		auth.creds.Release()
		auth.creds = nil
	}
}
	if err != nil {
		return "", fmt.Errorf("error initializing security context: %v", err)
	}

	return auth.encodeToken(output), nil
}

// Retry implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Retry(username string, password string) (string, error) {
	if auth.context == nil {
		return "", fmt.Errorf("authentication context not initialized")
	}

	inToken, err := auth.decodeToken(password)
	if err != nil {
		return "", err
	}

	_, authDone, output, err := auth.context.Update(inToken)
	if err != nil {
		return "", fmt.Errorf("error in authentication step: %v", err)
	}

	// If authDone is true, authentication is complete
	if authDone {
		return "", nil
	}

	return auth.encodeToken(output), nil
}

// Completed implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Completed(username string, password string, reason string) {
	if auth.context != nil {
		auth.context.Release()
		auth.context = nil
	}
	if auth.credentials != nil {
		auth.credentials.Release()
		auth.credentials = nil
	}
}
