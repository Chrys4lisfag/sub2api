package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// warp-panel is a Flask app on the same box that spawns / manages
// wgcf+WireGuard+gost containers. Each spawned container exposes
// a SOCKS5 endpoint on 1080/tcp and an HTTP proxy on 1081/tcp mapped
// onto host ports picked from a PORT_MIN..PORT_MAX range.
//
// Panel base URL + Basic Auth creds are stored in sub2api's `settings`
// table (SettingKeyWarpPanel{URL,User,Pass}) so nothing sensitive
// lives in git. Values are read on every panel call; operators can
// rotate creds live via a `settings` UPDATE, no restart needed.
const (
	// Wait budget for the spawned container to become health=healthy.
	// wgcf boot + WireGuard handshake typically completes in ~5-15s.
	warpSpawnPollInterval = 500 * time.Millisecond
	warpSpawnHealthy      = 30 * time.Second
)

// warpPanelCreds is the runtime warp-panel connection tuple resolved
// from the DB at call time.
type warpPanelCreds struct {
	baseURL string
	user    string
	pass    string
}

// warpListResponse mirrors warp-panel's GET /api/list JSON shape.
type warpListResponse struct {
	Items     []warpListItem `json:"items"`
	NextSocks int            `json:"next_socks"`
	NextHTTP  int            `json:"next_http"`
	Host      string         `json:"host"`
}

type warpListItem struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Health   string `json:"health"`
	Socks    string `json:"socks"`
	HTTP     string `json:"http"`
	Mode     string `json:"mode"`
	AuthUser string `json:"auth_user"`
	AuthPass string `json:"auth_pass"`
	Created  string `json:"created"`
}

// CreateWarpProxyInput describes the user's ask from the "Warp" tab.
// Everything except the credentials has a sensible default.
type CreateWarpProxyInput struct {
	Name     string // sub2api proxy row name (blank → auto-derived from warp container name)
	Protocol string // "socks5h" | "socks5" | "http" (defaults to socks5h)
	Username string // auth for the spawned proxy
	Password string // auth for the spawned proxy
}

// CreateWarpProxy provisions a fresh warp container via the panel then
// stores a sub2api Proxy row pointing at its SOCKS5 endpoint.
//
// Blocks until the container reports health=healthy or the timeout
// expires. On timeout, the row is still created (subsequent quality
// checks will surface the real state).
func (s *adminServiceImpl) CreateWarpProxy(ctx context.Context, input *CreateWarpProxyInput) (*Proxy, error) {
	if input == nil {
		return nil, fmt.Errorf("warp proxy: nil input")
	}
	protocol := strings.ToLower(strings.TrimSpace(input.Protocol))
	if protocol == "" {
		protocol = "socks5h"
	}
	switch protocol {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, fmt.Errorf("warp proxy: unsupported protocol %q", protocol)
	}
	user := strings.TrimSpace(input.Username)
	pass := strings.TrimSpace(input.Password)
	// Panel rejects one-of-two creds. If either is blank, treat as
	// unauthenticated proxy (both empty).
	if user == "" || pass == "" {
		user = ""
		pass = ""
	}
	// Panel rejects whitespace / @ / / — validate here too so the
	// caller sees a useful error rather than a raw 400 from the panel.
	for _, v := range []string{user, pass} {
		for _, c := range v {
			if c == ' ' || c == '\t' || c == '\n' || c == '@' || c == '/' {
				return nil, fmt.Errorf("warp proxy: credential contains invalid character %q", c)
			}
		}
	}

	creds, err := s.warpPanelCreds(ctx)
	if err != nil {
		return nil, err
	}

	// 1. Discover next free ports + host.
	list, err := warpPanelList(ctx, creds)
	if err != nil {
		return nil, fmt.Errorf("warp panel list: %w", err)
	}
	socks := list.NextSocks
	httpPort := list.NextHTTP
	host := list.Host
	if socks == 0 || httpPort == 0 || host == "" {
		return nil, fmt.Errorf("warp panel: no free ports (next_socks=%d next_http=%d host=%q)", socks, httpPort, host)
	}

	// 2. Spawn the container.
	if err := warpPanelCreate(ctx, creds, socks, httpPort, user, pass); err != nil {
		return nil, fmt.Errorf("warp panel create: %w", err)
	}

	// 3. Poll until healthy (or budget elapses).
	warpWaitHealthy(ctx, creds, socks)

	// 4. Persist a sub2api Proxy row.
	name := strings.TrimSpace(input.Name)
	if name == "" {
		name = fmt.Sprintf("warp-%s-%d", strings.ReplaceAll(host, ".", "-"), socks)
	}
	createInput := &CreateProxyInput{
		Name:     name,
		Protocol: protocol,
		Host:     host,
		Port:     socks,
		Username: user,
		Password: pass,
	}
	return s.CreateProxy(ctx, createInput)
}

// warpPanelCreds resolves the panel URL + Basic Auth creds from the
// `settings` table. Returns an actionable error when any key is
// missing so the operator knows exactly what to insert.
func (s *adminServiceImpl) warpPanelCreds(ctx context.Context) (warpPanelCreds, error) {
	if s.settingService == nil {
		return warpPanelCreds{}, fmt.Errorf("warp panel: setting service unavailable")
	}
	baseURL, user, pass := s.settingService.GetWarpPanelConfig(ctx)
	baseURL = strings.TrimRight(baseURL, "/")
	var missing []string
	if baseURL == "" {
		missing = append(missing, SettingKeyWarpPanelURL)
	}
	if user == "" {
		missing = append(missing, SettingKeyWarpPanelUser)
	}
	if pass == "" {
		missing = append(missing, SettingKeyWarpPanelPass)
	}
	if len(missing) > 0 {
		return warpPanelCreds{}, fmt.Errorf("warp panel not configured: missing DB settings [%s] — INSERT/UPDATE the sub2api `settings` table on the deploy server", strings.Join(missing, ", "))
	}
	return warpPanelCreds{baseURL: baseURL, user: user, pass: pass}, nil
}

// warpPanelList calls GET /api/list on the warp panel and parses.
func warpPanelList(ctx context.Context, creds warpPanelCreds) (*warpListResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, creds.baseURL+"/api/list", nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(creds.user, creds.pass)
	resp, err := warpHTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("warp panel /api/list: HTTP %d: %s", resp.StatusCode, string(body))
	}
	var out warpListResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("warp panel /api/list: decode: %w", err)
	}
	return &out, nil
}

// warpPanelCreate POSTs form-encoded values to /create matching the
// panel's expected fields exactly (socks, http, mode, proxy_user,
// proxy_pass). The panel redirects on success (Flask flash pattern);
// we treat 200 OR 302 as success.
func warpPanelCreate(ctx context.Context, creds warpPanelCreds, socks, httpPort int, proxyUser, proxyPass string) error {
	form := url.Values{}
	form.Set("socks", fmt.Sprintf("%d", socks))
	form.Set("http", fmt.Sprintf("%d", httpPort))
	form.Set("mode", "dual")
	form.Set("proxy_user", proxyUser)
	form.Set("proxy_pass", proxyPass)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, creds.baseURL+"/create", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(creds.user, creds.pass)

	// Don't auto-follow redirect — the panel emits a 302 → / which
	// would return the whole HTML page for no reason.
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: warpHTTPTransport(),
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusFound && resp.StatusCode != http.StatusSeeOther {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("warp panel /create: HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// warpWaitHealthy polls /api/list until an item with host_port==socks
// reports health=healthy. Best-effort — returns silently on timeout.
func warpWaitHealthy(ctx context.Context, creds warpPanelCreds, socks int) {
	deadline := time.Now().Add(warpSpawnHealthy)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		list, err := warpPanelList(ctx, creds)
		if err == nil {
			for _, it := range list.Items {
				if it.Socks == fmt.Sprintf("%d", socks) && it.Health == "healthy" {
					return
				}
			}
		}
		time.Sleep(warpSpawnPollInterval)
	}
}

// warpHTTPClient — short-timeout client for /api/list. /create uses
// a distinct client with no-follow-redirects so we don't consume the
// panel's flash-message HTML page.
func warpHTTPClient() *http.Client {
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: warpHTTPTransport(),
	}
}

func warpHTTPTransport() http.RoundTripper {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: 8 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          4,
		IdleConnTimeout:       30 * time.Second,
	}
}
