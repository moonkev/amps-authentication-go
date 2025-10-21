# AMPS Go Client Kerberos Authentication

This package provides Kerberos authentication for the AMPS Go client using GSSAPI.

## Dependencies

- AMPS Go Client
- `github.com/jcmturner/gokrb5/v8` for Kerberos functionality

## Installation

```bash
go get github.com/moonkev/amps-authentication-go
```

## Usage

Here's a simple example of how to use the Kerberos authenticator:

```go
package main

import (
    "fmt"
    "os"

    "github.com/60East/amps-go-client/amps"
    "github.com/60East/amps-authentication-go/kerberos"
)

func main() {
    username := "username"
    hostname := "hostname"
    ampsSPN := "AMPS/" + hostname
    ampsURI := fmt.Sprintf("tcp://%s@%s:10304/amps/json", username, hostname)

    // Create the Kerberos authenticator
    auth, err := kerberos.NewAMPSKerberosAuthenticator(
        ampsSPN,
        "/etc/krb5.conf",
        "", // empty string means use default credentials cache
    )
    if err != nil {
        fmt.Printf("Error creating authenticator: %v\n", err)
        os.Exit(1)
    }

    // Create and connect the AMPS client
    client := amps.NewClient("KerberosExampleClient")
    err = client.Connect(ampsURI)
    if err != nil {
        fmt.Printf("Error connecting to AMPS: %v\n", err)
        os.Exit(1)
    }

    // Attempt to log on with the authenticator
    err = client.Logon(auth, 5000)
    if err != nil {
        fmt.Printf("Error logging on: %v\n", err)
        os.Exit(1)
    }

    fmt.Println("Successfully authenticated with Kerberos!")

    // Close the connection when done
    client.Close()
}
```

## Configuration

The authenticator requires:
1. A valid Kerberos configuration file (typically `/etc/krb5.conf`)
2. Valid Kerberos credentials in the default credential cache or a specified keytab file
3. The AMPS server's Service Principal Name (SPN) in the format "AMPS/hostname"

The authenticator supports two ways of obtaining Kerberos credentials:
1. Using the default credentials cache (created by `kinit`)
2. Using a keytab file (specify the path in the `ccachePath` parameter)

## Notes

- This implementation uses GSSAPI, which is supported on both Linux and Windows
- The authenticator automatically handles the SPNEGO token exchange with the AMPS server
- Make sure your Kerberos environment is properly configured before using this authenticator