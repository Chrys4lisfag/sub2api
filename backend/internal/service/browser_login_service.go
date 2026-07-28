package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
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
	accountRepo    AccountRepository
}

func NewBrowserLoginService(settingService *SettingService, proxyRepo ProxyRepository, accountRepo AccountRepository) *BrowserLoginService {
	return &BrowserLoginService{settingService: settingService, proxyRepo: proxyRepo, accountRepo: accountRepo}
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
	VNCURL    string `json:"vnc_url"`
}

// BrowserLoginResult mirrors browser2webfront's GET /session/result response.
type BrowserLoginResult struct {
	CallbackURL string `json:"callback_url"`
	Code        string `json:"code"`
	CurrentURL  string `json:"current_url"`
}

// BrowserLoginNavigateResult preserves non-fatal page.goto diagnostics.
type BrowserLoginNavigateResult struct {
	OK      bool   `json:"ok"`
	Warning string `json:"warning,omitempty"`
}

// GoogleAutologinInput contains private credentials used by browser2webfront.
// Values are intentionally never included in errors or logs.
type GoogleAutologinInput struct {
	AccountID           *int64
	Login               string
	Password            string
	TwoFactorImportCode string
}

// GoogleAutologinStatus is the only upstream state exposed to admin callers.
// Unknown upstream fields are discarded during JSON decoding.
type GoogleAutologinStatus struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// StartSession opens the single browser session, egressing through the account's
// proxy (if any). Returns the VNC token the frontend needs to attach noVNC.
func (s *BrowserLoginService) StartSession(ctx context.Context, input *BrowserLoginStartInput) (*BrowserLoginSession, error) {
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return nil, err
	}
	vncURL := s.settingService.GetBrowserLoginVNCURL(ctx)
	parsedVNCURL, parseErr := neturl.Parse(vncURL)
	if parseErr != nil || parsedVNCURL.Host == "" || (parsedVNCURL.Scheme != "http" && parsedVNCURL.Scheme != "https") {
		return nil, fmt.Errorf("browser login VNC URL is not configured or invalid")
	}

	var proxyURL string
	if input.ProxyID != nil {
		p, err := s.proxyRepo.GetByID(ctx, *input.ProxyID)
		if err != nil {
			return nil, fmt.Errorf("load browser proxy %d: %w", *input.ProxyID, err)
		}
		if p == nil || strings.TrimSpace(p.URL()) == "" {
			return nil, fmt.Errorf("browser proxy %d not found or invalid", *input.ProxyID)
		}
		proxyURL = p.URL()
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
	if err := s.doJSON(ctx, creds, http.MethodPost, "/session", "", payload, &out); err != nil {
		return nil, err
	}
	out.VNCURL = vncURL
	return &out, nil
}

// Navigate drives the streamed browser's active tab to url (e.g. the OAuth
// consent URL) so the "Open OAuth link" button loads it inside the stream.
func (s *BrowserLoginService) Navigate(ctx context.Context, sessionID, url string) (*BrowserLoginNavigateResult, error) {
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return nil, err
	}
	var out BrowserLoginNavigateResult
	if err := s.doJSON(ctx, creds, http.MethodPost, "/session/navigate", sessionID, map[string]any{"url": url}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Result returns the latest captured OAuth callback code + the current URL.
func (s *BrowserLoginService) Result(ctx context.Context, sessionID string) (*BrowserLoginResult, error) {
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return nil, err
	}
	var out BrowserLoginResult
	if err := s.doJSON(ctx, creds, http.MethodGet, "/session/result", sessionID, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RunGoogleAutologin persists reusable account credentials, then starts the
// private browser2webfront automation run.
func (s *BrowserLoginService) RunGoogleAutologin(ctx context.Context, sessionID string, input *GoogleAutologinInput) (*GoogleAutologinStatus, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, fmt.Errorf("browser session header is required")
	}
	if input == nil || strings.TrimSpace(input.Login) == "" || strings.TrimSpace(input.Password) == "" {
		return nil, fmt.Errorf("login and password are required")
	}
	if s.settingService == nil {
		return nil, fmt.Errorf("HeroSMS API key is not configured")
	}
	heroSMSAPIKey := strings.TrimSpace(s.settingService.GetHeroSMSAPIKey(ctx))
	if heroSMSAPIKey == "" {
		return nil, fmt.Errorf("HeroSMS API key is not configured")
	}

	if input.AccountID != nil {
		if *input.AccountID <= 0 {
			return nil, fmt.Errorf("account_id must be positive")
		}
		if s.accountRepo == nil {
			return nil, fmt.Errorf("account persistence is unavailable")
		}
		account, err := s.accountRepo.GetByID(ctx, *input.AccountID)
		if err != nil {
			return nil, fmt.Errorf("load account: %w", err)
		}
		if account == nil {
			return nil, fmt.Errorf("account not found")
		}
		credentials := shallowCopyMap(account.Credentials)
		if credentials == nil {
			credentials = make(map[string]any)
		}
		credentials["google_login"] = input.Login
		credentials["google_password"] = input.Password
		credentials["google_2fa_import_code"] = input.TwoFactorImportCode
		if err := persistAccountCredentials(ctx, s.accountRepo, account, credentials); err != nil {
			return nil, fmt.Errorf("save account credentials: %w", err)
		}
	}

	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return nil, err
	}
	payload := map[string]any{
		"login":                  input.Login,
		"password":               input.Password,
		"two_factor_import_code": input.TwoFactorImportCode,
		"herosms_api_key":        heroSMSAPIKey,
	}
	var out GoogleAutologinStatus
	if err := s.doJSON(ctx, creds, http.MethodPost, "/session/google-autologin", sessionID, payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GoogleAutologinStatus returns sanitized automation state only.
func (s *BrowserLoginService) GoogleAutologinStatus(ctx context.Context, sessionID string) (*GoogleAutologinStatus, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, fmt.Errorf("browser session header is required")
	}
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return nil, err
	}
	var out GoogleAutologinStatus
	if err := s.doJSON(ctx, creds, http.MethodGet, "/session/google-autologin", sessionID, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StopSession tears the browser session down (the profile dir is kept for reuse).
func (s *BrowserLoginService) StopSession(ctx context.Context, sessionID string) error {
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return err
	}
	return s.doJSON(ctx, creds, http.MethodDelete, "/session", sessionID, nil, nil)
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
func (s *BrowserLoginService) doJSON(ctx context.Context, creds browserLoginCreds, method, path, sessionID string, body any, out any) error {
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
	if sessionID != "" {
		req.Header.Set("X-Browser-Session-ID", sessionID)
	}

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
