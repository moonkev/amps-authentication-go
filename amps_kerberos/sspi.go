//go:build windows

package amps_kerberos

import (
	"fmt"

	"github.com/alexbrainman/sspi/negotiate"
	"github.com/moonkev/amps_kerberos/amps"
)

// AMPSKerberosSSPIAuthenticator provides Kerberos authentication for AMPS using Windows SSPI
type AMPSKerberosSSPIAuthenticator struct {
	AuthBase
	context *negotiate.ClientContext
}

// NewAuthenticator creates a new Kerberos authenticator using Windows SSPI
// This is the preferred authenticator on Windows systems.
// Parameters:
// - spn: Service Principal Name for the AMPS server (e.g., "AMPS/hostname")
func NewAuthenticator(spn string, _ string, _ string) (amps.Authenticator, error) {
	return &AMPSKerberosSSPIAuthenticator{
		AuthBase: AuthBase{
			spn: spn,
		},
	}, nil
}

// Authenticate implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Authenticate(username string, password string) (string, error) {
	cred := negotiate.NewClientCredentials()
	var err error
	auth.context, err = cred.NewContext()
	if err != nil {
		return "", fmt.Errorf("error creating SSPI context: %v", err)
	}

	output, _, err := auth.context.InitializeSecurityContext(auth.spn, nil, 0)
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

	output, _, err := auth.context.InitializeSecurityContext(auth.spn, inToken, 0)
	if err != nil {
		return "", fmt.Errorf("error in authentication step: %v", err)
	}

	return auth.encodeToken(output), nil
}

// Completed implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Completed(username string, password string, reason string) {
	if auth.context != nil {
		auth.context.Release()
		auth.context = nil
	}
}
