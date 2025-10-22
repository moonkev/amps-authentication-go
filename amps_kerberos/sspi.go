//go:build windows
// +build windows

package amps_kerberos

import (
	"fmt"
	"syscall"

	"github.com/alexbrainman/sspi"
	"github.com/moonkev/amps_kerberos/amps"
)

type AMPSKerberosSSPIAuthenticator struct {
	AuthBase
	cred    *sspi.Credentials
	context *sspi.Context
}

// NewAuthenticator creates a new Kerberos authenticator using Windows SSPI
func NewAuthenticator(spn string, _ string, _ string) (amps.Authenticator, error) {
	return &AMPSKerberosSSPIAuthenticator{
		AuthBase: AuthBase{spn: spn},
	}, nil
}

// Authenticate implements the AMPS Authenticator interface
func (auth *AMPSKerberosSSPIAuthenticator) Authenticate(username string, password string) (string, error) {
	// Get credentials
	cred, err := sspi.AcquireCredentials(
		"",                        // No principal name
		"Negotiate",               // Security package
		sspi.SECPKG_CRED_OUTBOUND, // Credentials for client
		nil)                       // No auth data needed
	if err != nil {
		return "", fmt.Errorf("failed to acquire credentials: %v", err)
	}
	auth.cred = cred

	// Create target name
	targetName, err := syscall.UTF16PtrFromString(auth.spn)
	if err != nil {
		return "", fmt.Errorf("failed to convert SPN: %v", err)
	}

	// Initialize security context
	ctx := sspi.NewClientContext(cred, sspi.ISC_REQ_MUTUAL_AUTH)
	auth.context = ctx

	// Create input buffer (empty for initial request)
	inBuf := make([]sspi.SecBuffer, 1)
	inBuf[0].Set(sspi.SECBUFFER_TOKEN, nil)
	inBufDesc := sspi.NewSecBufferDesc(inBuf)

	// Create output buffer
	outBuf := make([]sspi.SecBuffer, 1)
	outBuf[0].Set(sspi.SECBUFFER_TOKEN, nil)
	outBufDesc := sspi.NewSecBufferDesc(outBuf)

	err = ctx.Update(targetName, inBufDesc, outBufDesc)
	if err != nil {
		return "", fmt.Errorf("failed to initialize security context: %v", err)
	}

	token := outBuf[0].Bytes()
	if len(token) == 0 {
		return "", nil
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

	// Create input buffer with token
	inBuf := make([]sspi.SecBuffer, 1)
	inBuf[0].Set(sspi.SECBUFFER_TOKEN, inToken)
	inBufDesc := sspi.NewSecBufferDesc(inBuf)

	// Create output buffer
	outBuf := make([]sspi.SecBuffer, 1)
	outBuf[0].Set(sspi.SECBUFFER_TOKEN, nil)
	outBufDesc := sspi.NewSecBufferDesc(outBuf)

	// Continue security context
	targetName, err := syscall.UTF16PtrFromString(auth.spn)
	if err != nil {
		return "", fmt.Errorf("failed to convert SPN: %v", err)
	}

	err = auth.context.Update(targetName, inBufDesc, outBufDesc)
	if err != nil {
		return "", fmt.Errorf("authentication step failed: %v", err)
	}

	token := outBuf[0].Bytes()
	if len(token) == 0 {
		return "", nil // Authentication complete
	}

	return auth.encodeToken(token), nil
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
