//go:build !windows

package amps_kerberos

import (
	"encoding/base64"
	"fmt"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/moonkev/amps_kerberos/amps"
)

// AMPSKerberosGSSAPIAuthenticator provides Kerberos GSSAPI authentication for AMPS
type AMPSKerberosGSSAPIAuthenticator struct {
	AuthBase
	krb5Client *client.Client
	ticket     messages.Ticket
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
		cl = client.NewWithPassword("", "", "", cfg)
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
	err := auth.krb5Client.Login()
	if err != nil {
		return "", fmt.Errorf("error logging in: %v", err)
	}

	tkt, _, err := auth.krb5Client.GetServiceTicket(auth.spn)
	if err != nil {
		return "", fmt.Errorf("error getting service ticket: %v", err)
	}

	auth.ticket = tkt

	// For now, just return the ticket bytes
	ticketBytes, err := auth.ticket.Marshal()
	if err != nil {
		return "", fmt.Errorf("error marshalling ticket: %v", err)
	}

	return base64.StdEncoding.EncodeToString(ticketBytes), nil
}

// Retry implements the AMPS Authenticator interface
func (auth *AMPSKerberosGSSAPIAuthenticator) Retry(username string, password string) (string, error) {
	// For now, just acknowledge the retry
	return "", nil
}

// Completed implements the AMPS Authenticator interface
func (auth *AMPSKerberosGSSAPIAuthenticator) Completed(username string, password string, reason string) {
	// No cleanup needed for GSSAPI
}
