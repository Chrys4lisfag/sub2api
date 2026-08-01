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
// calls (navigate/result/stop) finish well within it. Error bodies/messages are
// separately bounded before crossing the admin API trust boundary.
const (
	browserLoginTimeout           = 120 * time.Second
	browserLoginErrorBodyLimit    = 8 * 1024
	browserLoginErrorMessageLimit = 512
)

// browserLoginCreds is the runtime browser2webfront connection tuple resolved
// from the DB at call time.
type browserLoginCreds struct {
	baseURL string
	user    string
	pass    string
}

// BrowserLoginUpstreamError preserves only upstream HTTP status plus a bounded,
// sanitized message. Raw browser2webfront bodies never cross this boundary.
type BrowserLoginUpstreamError struct {
	StatusCode int
	Message    string
}

func (e *BrowserLoginUpstreamError) Error() string {
	if e == nil {
		return "browser login upstream request failed"
	}
	return e.Message
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

const browserProfileExtraKey = "browser_profile_id"

// BrowserLoginStartInput describes the admin's ask when opening the modal.
type BrowserLoginStartInput struct {
	AccountID *int64 // existing account whose persistent browser profile owns the session
	ProxyID   *int64 // account proxy — the streamed browser egresses through it
	ProfileID string // create flow only; existing accounts resolve this from persisted account state
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

func accountBrowserProfileID(accountID int64) string {
	return fmt.Sprintf("account-%d", accountID)
}

func browserProfileIDFromAccount(account *Account) string {
	if account == nil || account.Extra == nil {
		return ""
	}
	value, _ := account.Extra[browserProfileExtraKey].(string)
	return strings.TrimSpace(value)
}

// resolveBrowserProfileID binds existing-account sessions to repository state.
// A missing or shared legacy profile is replaced with a deterministic,
// account-specific ID and persisted before browser2webfront is called.
func (s *BrowserLoginService) resolveBrowserProfileID(ctx context.Context, input *BrowserLoginStartInput) (string, error) {
	requested := strings.TrimSpace(input.ProfileID)
	if input.AccountID == nil {
		return requested, nil
	}
	accountID := *input.AccountID
	if accountID <= 0 {
		return "", fmt.Errorf("account_id must be positive")
	}
	if s.accountRepo == nil {
		return "", fmt.Errorf("account profile persistence is unavailable")
	}
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return "", fmt.Errorf("load browser account: %w", err)
	}
	if account == nil {
		return "", fmt.Errorf("browser account not found")
	}

	stored := browserProfileIDFromAccount(account)
	if stored != "" {
		matches, findErr := s.accountRepo.FindByExtraField(ctx, browserProfileExtraKey, stored)
		if findErr != nil {
			return "", fmt.Errorf("check browser profile ownership: %w", findErr)
		}
		shared := false
		currentIncluded := false
		for i := range matches {
			if matches[i].ID == accountID {
				currentIncluded = true
			} else {
				shared = true
			}
		}
		if !shared {
			return stored, nil
		}

		// Split every owner in one pass. Migrating only the account being opened
		// would leave the final owner attached to the old shared cookie jar.
		if !currentIncluded {
			matches = append(matches, *account)
		}
		for i := range matches {
			derived := accountBrowserProfileID(matches[i].ID)
			owners, ownershipErr := s.accountRepo.FindByExtraField(ctx, browserProfileExtraKey, derived)
			if ownershipErr != nil {
				return "", fmt.Errorf("check migrated browser profile ownership: %w", ownershipErr)
			}
			for j := range owners {
				if owners[j].ID != matches[i].ID {
					return "", fmt.Errorf("derived browser profile is already owned by another account")
				}
			}
		}
		for i := range matches {
			derived := accountBrowserProfileID(matches[i].ID)
			if updateErr := s.accountRepo.UpdateExtra(ctx, matches[i].ID, map[string]any{browserProfileExtraKey: derived}); updateErr != nil {
				return "", fmt.Errorf("split shared account browser profile: %w", updateErr)
			}
		}
		return accountBrowserProfileID(accountID), nil
	}

	profileID := accountBrowserProfileID(accountID)
	matches, err := s.accountRepo.FindByExtraField(ctx, browserProfileExtraKey, profileID)
	if err != nil {
		return "", fmt.Errorf("check derived browser profile ownership: %w", err)
	}
	for i := range matches {
		if matches[i].ID != accountID {
			return "", fmt.Errorf("derived browser profile is already owned by another account")
		}
	}
	if err := s.accountRepo.UpdateExtra(ctx, accountID, map[string]any{browserProfileExtraKey: profileID}); err != nil {
		return "", fmt.Errorf("save account browser profile: %w", err)
	}
	return profileID, nil
}

// StartSession opens the single browser session, egressing through the account's
// proxy (if any). Returns the VNC token the frontend needs to attach noVNC.
func (s *BrowserLoginService) StartSession(ctx context.Context, input *BrowserLoginStartInput) (*BrowserLoginSession, error) {
	if input == nil {
		return nil, fmt.Errorf("browser session input is required")
	}
	profileID, err := s.resolveBrowserProfileID(ctx, input)
	if err != nil {
		return nil, err
	}
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
		"profile_id": profileID,
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

// CancelGoogleAutologin stops only the active automation run. The streamed
// browser session remains available for manual work or another activation run.
func (s *BrowserLoginService) CancelGoogleAutologin(ctx context.Context, sessionID string) (*GoogleAutologinStatus, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, fmt.Errorf("browser session header is required")
	}
	creds, err := s.browserLoginCreds(ctx)
	if err != nil {
		return nil, err
	}
	var out GoogleAutologinStatus
	if err := s.doJSON(ctx, creds, http.MethodDelete, "/session/google-autologin", sessionID, nil, &out); err != nil {
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

// doJSON issues a Basic-Auth'd JSON request. Non-2xx responses retain their
// upstream status but expose only an allow-listed, bounded, credential-redacted
// message; raw browser2webfront bodies never reach admin callers.
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
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, browserLoginErrorBodyLimit))
		return &BrowserLoginUpstreamError{
			StatusCode: resp.StatusCode,
			Message: browserLoginSafeErrorMessage(
				resp.StatusCode,
				respBody,
				body,
				creds,
				sessionID,
			),
		}
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("browser login %s %s: read response: %w", method, path, err)
	}
	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("browser login %s %s: decode: %w", method, path, err)
		}
	}
	return nil
}

func browserLoginSafeErrorMessage(statusCode int, raw []byte, requestBody any, creds browserLoginCreds, sessionID string) string {
	message := ""
	var payload map[string]any
	if json.Unmarshal(raw, &payload) == nil {
		for _, key := range []string{"detail", "error", "message"} {
			if text := browserLoginErrorText(payload[key]); text != "" {
				message = text
				break
			}
		}
	}
	if message == "" {
		message = fmt.Sprintf("browser login upstream returned HTTP %d", statusCode)
	}

	secrets := []string{creds.user, creds.pass, sessionID}
	if request, ok := requestBody.(map[string]any); ok {
		for _, key := range []string{
			"login",
			"password",
			"two_factor_import_code",
			"herosms_api_key",
			"proxy",
		} {
			if value, ok := request[key].(string); ok {
				secrets = append(secrets, value, strings.TrimSpace(value))
			}
		}
	}
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	message = strings.Join(strings.Fields(message), " ")
	runes := []rune(message)
	if len(runes) > browserLoginErrorMessageLimit {
		message = string(runes[:browserLoginErrorMessageLimit-1]) + "…"
	}
	if message == "" {
		return fmt.Sprintf("browser login upstream returned HTTP %d", statusCode)
	}
	return message
}

func browserLoginErrorText(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		for _, key := range []string{"detail", "error", "message"} {
			if text := browserLoginErrorText(typed[key]); text != "" {
				return text
			}
		}
	}
	return ""
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
