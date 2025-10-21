package main

import (
	"fmt"
	"os"

	"github.com/60East/amps-go-client/amps"
	"github.com/60East/amps_kerberos/amps_kerberos"
)

func main() {
	username := "username"
	hostname := "hostname"
	ampsSPN := "AMPS/" + hostname
	ampsURI := fmt.Sprintf("tcp://%s@%s:10304/amps/json", username, hostname)

	// Create the Kerberos authenticator
	// You can specify a custom krb5.conf path and credentials cache path if needed
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
