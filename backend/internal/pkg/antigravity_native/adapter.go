// Package antigravity_native is a thin adapter that exposes the
// github.com/koval/agymimic SDK as sub2api's "Antigravity Native" backend.
//
// It coexists with internal/pkg/antigravity (the legacy in-tree client).
// Both target the same upstream (daily-cloudcode-pa.sandbox.googleapis.com)
// but the native variant routes through agymimic's self-contained Go client,
// which owns its own:
//   - PKCE OAuth flow
//   - per-account deterministic identity (installation_id, connection_id,
//     fake DESKTOP-XXXXXXX\<user> instance label) derived from the email
//     so the same Google account always advertises the same machine
//   - Unleash organic-traffic mimic loop
//
// Use NewProxyHTTPClient to build an http.Client whose Transport honors a
// sub2api proxy URL string ("http://user:pass@host:port" or "socks5://..."),
// then feed that client into both OAuth helpers (auth.*WithClient) and the
// API client (api.WithHTTPClient).
package antigravity_native

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/koval/agymimic/api"
	agyauth "github.com/koval/agymimic/auth"
)

// NewProxyHTTPClient returns an *http.Client whose Transport routes every
// outbound request through proxyURL. proxyURL == "" → http.DefaultClient.
//
// Supports http/https/socks5 schemes. Auth in URL ("user:pass@host") is
// forwarded as a Proxy-Authorization header by Go's stdlib transport for
// http/https; socks5 auth requires the URL form too.
func NewProxyHTTPClient(proxyURL string, timeout time.Duration) (*http.Client, error) {
	if proxyURL == "" {
		// Match agymimic's api.New() default transport tuning (HTTP/2, idle pool).
		return &http.Client{
			Transport: defaultTransport(),
			Timeout:   timeout,
		}, nil
	}
	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("parse proxy url: %w", err)
	}
	tr := defaultTransport()
	tr.Proxy = http.ProxyURL(u)
	return &http.Client{Transport: tr, Timeout: timeout}, nil
}

func defaultTransport() *http.Transport {
	return &http.Transport{
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          16,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
}

// NewAPIClient builds an agymimic api.Client for one account using its
// stored tokens and an optional proxy URL.
//
// The returned client refreshes tokens on its own and keeps its own
// per-account identity (already deterministic — derived from t.Email by
// auth.EnsureIdentity at login time). Just hand it requests; it handles
// the rest of the wire protocol.
func NewAPIClient(t *agyauth.Tokens, proxyURL string, timeout time.Duration) (*api.Client, error) {
	httpc, err := NewProxyHTTPClient(proxyURL, timeout)
	if err != nil {
		return nil, err
	}
	return api.New(t, api.WithHTTPClient(httpc)), nil
}
