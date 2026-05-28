package auth

import "encoding/base64"

func rawURLEncode(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
