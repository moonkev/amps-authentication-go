package amps_kerberos

import (
	"encoding/base64"
	"fmt"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

// AMPSKerberosAuthenticator provides Kerberos GSSAPI authentication for AMPS
type AMPSKerberosAuthenticator struct {
	spn         string
	krb5Client  *client.Client
	krb5Context gssapi.ContextHandle
}

// NewAMPSKerberosAuthenticator creates a new Kerberos authenticator
// It requires:
// - spn: Service Principal Name for the AMPS server (e.g., "AMPS/hostname")
// - krb5ConfigPath: Path to the Kerberos configuration file (usually /etc/krb5.conf)
// - ccachePath: Optional path to credentials cache file. If empty, will use the default cache.
func NewAMPSKerberosAuthenticator(spn string, krb5ConfigPath string, ccachePath string) (*AMPSKerberosAuthenticator, error) {
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

	return &AMPSKerberosAuthenticator{
		spn:        spn,
		krb5Client: cl,
	}, nil
}

// Authenticate implements the AMPS Authenticator interface
func (auth *AMPSKerberosAuthenticator) Authenticate(username string, password string) (string, error) {
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

	return base64.StdEncoding.EncodeToString(token), nil
}

// Retry implements the AMPS Authenticator interface
func (auth *AMPSKerberosAuthenticator) Retry(username string, password string) (string, error) {
	if auth.krb5Context == nil {
		return "", fmt.Errorf("authentication context not initialized")
	}

	var inToken []byte
	if password != "" {
		var err error
		inToken, err = base64.StdEncoding.DecodeString(password)
		if err != nil {
			return "", fmt.Errorf("error decoding input token: %v", err)
		}
	}

	outToken, err := auth.krb5Context.Step(inToken)
	if err != nil {
		return "", fmt.Errorf("error in authentication step: %v", err)
	}

	return base64.StdEncoding.EncodeToString(outToken), nil
}

// Completed implements the AMPS Authenticator interface
func (auth *AMPSKerberosAuthenticator) Completed(username string, password string, reason string) {
	if auth.krb5Context != nil {
		auth.krb5Context.Release()
		auth.krb5Context = nil
	}
}
