//go:build !windows

package amps_kerberos

import (
	"fmt"

	"github.com/60East/amps-go-client/amps"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

// AMPSKerberosGSSAPIAuthenticator provides Kerberos GSSAPI authentication for AMPS
type AMPSKerberosGSSAPIAuthenticator struct {
	AuthBase
	krb5Client  *client.Client
	krb5Context gssapi.ContextHandle
}

// NewAuthenticator creates a new Kerberos authenticator using GSSAPI
// This is the preferred authenticator on Linux systems.
// Parameters:
// - spn: Service Principal Name for the AMPS server (e.g., "AMPS/hostname")
// - krb5ConfigPath: Path to the Kerberos configuration file (usually /etc/krb5.conf)
// - ccachePath: Optional path to credentials cache file. If empty, will use the default cache.
func NewAuthenticator(spn string, krb5ConfigPath string, ccachePath string) (amps.Authenticator, error) {
	cfg, err := config.Load(krb5ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("error loading Kerberos config: %v", err)
	}

	var cl *client.Client
	if ccachePath != "" {
		ccache, err := credentials.LoadCCache(ccachePath)
		if err != nil {
			return nil, fmt.Errorf("error loading credentials cache: %v", err)
		}
		cl, err = client.NewFromCCache(ccache, cfg)
		if err != nil {
			return nil, fmt.Errorf("error creating Kerberos client from ccache: %v", err)
		}
	} else {
		cl = client.NewWithConfig(cfg)
	}

	return &AMPSKerberosGSSAPIAuthenticator{
		AuthBase: AuthBase{
			spn: spn,
		},
		krb5Client: cl,
	}, nil
}

// Authenticate implements the AMPS Authenticator interface
func (auth *AMPSKerberosGSSAPIAuthenticator) Authenticate(username string, password string) (string, error) {
	// Initialize SPNEGO
	spnegoCl := spnego.NewClient(auth.krb5Client, auth.spn)
	var err error
	auth.krb5Context, err = spnegoCl.NewContext()
	if err != nil {
		return "", fmt.Errorf("error creating SPNEGO context: %v", err)
	}

	// Get initial token
	token, err := auth.krb5Context.MakeAuthN()
	if err != nil {
		return "", fmt.Errorf("error creating authentication token: %v", err)
	}

	return auth.encodeToken(token), nil
}

// Retry implements the AMPS Authenticator interface
func (auth *AMPSKerberosGSSAPIAuthenticator) Retry(username string, password string) (string, error) {
	if auth.krb5Context == nil {
		return "", fmt.Errorf("authentication context not initialized")
	}

	inToken, err := auth.decodeToken(password)
	if err != nil {
		return "", err
	}

	outToken, err := auth.krb5Context.Step(inToken)
	if err != nil {
		return "", fmt.Errorf("error in authentication step: %v", err)
	}

	return auth.encodeToken(outToken), nil
}

// Completed implements the AMPS Authenticator interface
func (auth *AMPSKerberosGSSAPIAuthenticator) Completed(username string, password string, reason string) {
	if auth.krb5Context != nil {
		auth.krb5Context.Release()
		auth.krb5Context = nil
	}
}
