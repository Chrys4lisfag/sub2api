// demo_identity prints the derived identity for several Google emails so you
// can see (a) same email → byte-identical IDs, (b) different emails → totally
// uncorrelated IDs, (c) refresh-token-only fallback when there's no email.
package main

import (
	"fmt"

	"github.com/koval/agymimic/auth"
)

func main() {
	cases := []*auth.Tokens{
		{Email: "4i3cake@gmail.com"},
		{Email: "4i3cake@gmail.com"}, // again — must match #1
		{Email: "alice@gmail.com"},
		{Email: "bob@gmail.com"},
		{RefreshToken: "1//rt-no-email-known"},
	}
	for i, t := range cases {
		auth.EnsureIdentity(t)
		label := t.Email
		if label == "" {
			label = "(rt fallback)"
		}
		fmt.Printf("#%d  %-30s  install=%s\n           instance=%s\n           conn   =%s\n",
			i+1, label, t.InstallationID, t.InstanceLabel, t.ConnectionID)
	}
}
