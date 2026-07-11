package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// browser2webfront is a standalone stealth-browser streaming service on the
// same box (mirrors the warp-panel deployment pattern). It runs ONE non-headless
// cloakbrowser session on a virtual display and streams video+input over VNC;
// this client drives its JSON control plane so the admin can perform an
// interactive OAuth login in the streamed browser.
//
// Base URL + Basic Auth creds live in sub2api's `settings` table
// (SettingKeyBrowserLogin{URL,User,Pass}) so nothing sensitive lives in git.
// Values are read on every call; operators can rotate creds live via a
// `settings` UPDATE, no restart needed.

// browserLoginTimeout bounds a single control call. Session start launches a
// browser (+ optional geoip DB fetch), so allow generous headroom; the fast
// calls (navigate/result/stop) finish well within it.
const browserLoginTimeout = 120 * time.Second

// browserLoginCreds is the runtime browser2webfront connection tuple resolved
// from the DB at call time.
type browserLoginCreds struct {
	baseURL string
	user    string
	pass    string
}

// BrowserLoginService talks to the browser2webfront control API.
type BrowserLoginService struct {
	settingService *SettingService
	proxyRepo      ProxyRepository
}

func NewBrowserLoginService(settingService *SettingService, proxyRepo ProxyRepository) *BrowserLoginService {
	return &BrowserLoginService{settingService: settingService, proxyRepo: proxyRepo}
}

// BrowserLoginStartInput describes the admin's ask when opening the modal.
type BrowserLoginStartInput struct {
	ProxyID   *int64 // account proxy — the streamed browser egresses through it
	ProfileID string // per-account persistent profile; empty → server mints a uuid
	StartURL  string // initial page; empty → https://www.google.com
}

// BrowserLoginSession mirrors browser2webfront's POST /session response.
type BrowserLoginSession struct {
	SessionID string `json:"session_id"`
	VNCToken  string `json:"vnc_token"`
	VNCPath   string `json:"vnc_path"`
	ProfileID string `json:"profile_id"`
}

// BrowserLoginResult mirrors browser2webfront's GET /session/result response.
type BrowserLoginResult struct {
	CallbackURL string `json:"callback_url"`
	Code        string `json:"code"`
	CurrentURL  string `json:"current_url"`
}

// StartSession opens the single browser session, egressing through the account's
// proxy (if any). Returns the VNC token the frontend needs to attach noVNC.
func (s *BrowserLoginService) StartSession(ctx context.Context, input *BrowserLoginStartInput) (*BrowserLoginSession, error) {
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return nil, err
	}

	var proxyURL string
	if input.ProxyID != nil {
		if p, err := s.proxyRepo.GetByID(ctx, *input.ProxyID); err == nil && p != nil {
			proxyURL = p.URL()
		}
	}

	startURL := input.StartURL
	if startURL == "" {
		startURL = "https://www.google.com"
	}

	payload := map[string]any{
		"profile_id": input.ProfileID,
		"start_url":  startURL,
	}
	if proxyURL != "" {
		payload["proxy"] = proxyURL
	}

	var out BrowserLoginSession
	if err := s.doJSON(ctx, creds, http.MethodPost, "/session", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Navigate drives the streamed browser's active tab to url (e.g. the OAuth
// consent URL) so the "Open OAuth link" button loads it inside the stream.
func (s *BrowserLoginService) Navigate(ctx context.Context, url string) error {
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return err
	}
	return s.doJSON(ctx, creds, http.MethodPost, "/session/navigate", map[string]any{"url": url}, nil)
}

// Result returns the latest captured OAuth callback code + the current URL.
func (s *BrowserLoginService) Result(ctx context.Context) (*BrowserLoginResult, error) {
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return nil, err
	}
	var out BrowserLoginResult
	if err := s.doJSON(ctx, creds, http.MethodGet, "/session/result", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StopSession tears the browser session down (the profile dir is kept for reuse).
func (s *BrowserLoginService) StopSession(ctx context.Context) error {
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return err
	}
	return s.doJSON(ctx, creds, http.MethodDelete, "/session", nil, nil)
}

// browserLoginCreds resolves the base URL + Basic Auth creds from the `settings`
// table. Returns an actionable error naming every missing key.
func (s *BrowserLoginService) browserLoginCreds(ctx context.Context) (browserLoginCreds, error) {
	if s.settingService == nil {
		return browserLoginCreds{}, fmt.Errorf("browser login: setting service unavailable")
	}
	baseURL, user, pass := s.settingService.GetBrowserLoginConfig(ctx)
	baseURL = strings.TrimRight(baseURL, "/")
	var missing []string
	if baseURL == "" {
		missing = append(missing, SettingKeyBrowserLoginURL)
	}
	if user == "" {
		missing = append(missing, SettingKeyBrowserLoginUser)
	}
	if pass == "" {
		missing = append(missing, SettingKeyBrowserLoginPass)
	}
	if len(missing) > 0 {
		return browserLoginCreds{}, fmt.Errorf("browser login not configured: missing DB settings [%s] — INSERT/UPDATE the sub2api `settings` table on the deploy server", strings.Join(missing, ", "))
	}
	return browserLoginCreds{baseURL: baseURL, user: user, pass: pass}, nil
}

// doJSON issues a Basic-Auth'd JSON request and, on a non-2xx response,
// propagates the upstream status + body verbatim so a 409 "session busy" or a
// launch error is legible to the admin.
func (s *BrowserLoginService) doJSON(ctx context.Context, creds browserLoginCreds, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, creds.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(creds.user, creds.pass)

	resp, err := browserLoginHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("browser login %s %s: HTTP %d: %s", method, path, resp.StatusCode, string(respBody))
	}
	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("browser login %s %s: decode: %w", method, path, err)
		}
	}
	return nil
}

func browserLoginHTTPClient() *http.Client {
	return &http.Client{
		Timeout: browserLoginTimeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			MaxIdleConns:          4,
			IdleConnTimeout:       30 * time.Second,
		},
	}
}
