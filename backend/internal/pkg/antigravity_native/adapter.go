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
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyutil"
	"github.com/koval/agymimic/api"
	agyauth "github.com/koval/agymimic/auth"
)

// NewProxyHTTPClient returns an *http.Client whose Transport routes every
// outbound request through proxyURL. proxyURL == "" → direct.
//
// Supports http/https/socks5/socks5h schemes. http/https proxies use
// CONNECT tunneling via Transport.Proxy. SOCKS5 uses x/net/proxy's
// dialer via Transport.DialContext (shared proxyutil package — same
// implementation the legacy gateway and tlsfingerprint paths use).
//
// Auth in URL ("user:pass@host:port") is forwarded automatically:
//   - http/https: Proxy-Authorization header added by Go stdlib
//   - socks5h:    SOCKS5 USERNAME/PASSWORD subnegotiation
func NewProxyHTTPClient(proxyURL string, timeout time.Duration) (*http.Client, error) {
	tr := defaultTransport()
	if proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("parse proxy url: %w", err)
		}
		if err := proxyutil.ConfigureTransportProxy(tr, u); err != nil {
			return nil, fmt.Errorf("configure proxy: %w", err)
		}
	}
	return &http.Client{Transport: tr, Timeout: timeout}, nil
}

// defaultTransport returns the canonical agymimic-backed Transport used by
// every native account, with or without a proxy. Tuning notes:
//
//   - ForceAttemptHTTP2 = false — match real agy.exe (HTTP/1.1 chunked to
//     cloudcode-pa, verified via Frida capture May 2026).
//
//   - MaxIdleConnsPerHost = 16 — Go's stdlib default is 2, which silently
//     forces all but two concurrent requests to pay TCP+TLS handshake.
//     With 16 we cover the typical worst case of concurrent omp turns or
//     multiple users sharing an account without re-dialing.
//
//   - IdleConnTimeout = 5 min — covers the long pauses between turns in an
//     agentic conversation. Default 90 s often expires mid-conversation
//     when the user is thinking / running a tool that takes a minute+.
//
//   - DialContext.KeepAlive = 30 s — keeps the kernel-side TCP socket
//     warm across NAT/firewall idle timers so the pooled connection
//     isn't silently disconnected by intermediaries.
//
// Mirrors the tuning we apply directly in agymimic/api/client.go::New().
// Both call sites must stay aligned — the native gateway uses BOTH the
// agymimic-built client (no proxy or direct) AND this adapter (per-account
// proxy), and they should behave identically once on the wire.
func defaultTransport() *http.Transport {
	return &http.Transport{
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       5 * time.Minute,
		ResponseHeaderTimeout: 60 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
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
