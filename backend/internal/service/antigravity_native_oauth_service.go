// AntigravityNativeOAuthService is the OAuth flow service for the
// PlatformAntigravityNative platform. It mirrors AntigravityOAuthService's
// public surface (same input/output struct shapes so handlers can swap with
// minimal churn) but delegates all token work to github.com/koval/agymimic.
//
// Key behavioral differences vs. the legacy service:
//   - PKCE/state/session helpers come from agymimic (auth.NewPKCE,
//     auth.EncodeState) so the generated state is decoded by the same code
//     path the SDK uses internally
//   - per-account identity (installation_id, fake DESKTOP-XXX\<user>
//     instance label, connection_id) is auto-derived from the Google email
//     by auth.EnsureIdentity — same email always → same IDs
//   - all outbound HTTP runs through the proxy assigned to the account
//     (or to the OAuth session at auth-URL generation time)
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	agynative "github.com/Wei-Shaw/sub2api/internal/pkg/antigravity_native"
	agyauth "github.com/koval/agymimic/auth"
)

// AntigravityNativeOAuthService is the OAuth gatekeeper for the
// antigravity_native platform. Constructed once at wire-time.
type AntigravityNativeOAuthService struct {
	// We reuse the legacy SessionStore implementation — same in-memory
	// state machine, no need for a second one.
	sessionStore *antigravity.SessionStore
	proxyRepo    ProxyRepository
}

func NewAntigravityNativeOAuthService(proxyRepo ProxyRepository) *AntigravityNativeOAuthService {
	return &AntigravityNativeOAuthService{
		sessionStore: antigravity.NewSessionStore(),
		proxyRepo:    proxyRepo,
	}
}

// ────────────────────────────────────────────────────────────────────────────
// auth URL
// ────────────────────────────────────────────────────────────────────────────

// AntigravityNativeAuthURLResult mirrors AntigravityAuthURLResult so the
// admin handler can share its DTO shape.
type AntigravityNativeAuthURLResult struct {
	AuthURL   string `json:"auth_url"`
	SessionID string `json:"session_id"`
	State     string `json:"state"`
}

// GenerateAuthURL kicks off a fresh OAuth flow.
//   - generates PKCE pair (43-byte verifier, S256 challenge)
//   - encodes state = base64url(JSON({verifier, projectId:""}))
//   - resolves proxy URL from proxyID (if any) so we use it during the
//     subsequent ExchangeCode call
//   - stores the session (verifier + proxy URL) in-memory until callback
func (s *AntigravityNativeOAuthService) GenerateAuthURL(ctx context.Context, proxyID *int64) (*AntigravityNativeAuthURLResult, error) {
	pkce, err := agyauth.NewPKCE()
	if err != nil {
		return nil, fmt.Errorf("native: pkce: %w", err)
	}
	sessionID, err := nativeRandomHex(16)
	if err != nil {
		return nil, fmt.Errorf("native: session id: %w", err)
	}

	var proxyURL string
	if proxyID != nil {
		if p, err := s.proxyRepo.GetByID(ctx, *proxyID); err == nil && p != nil {
			proxyURL = p.URL()
		}
	}

	// We reuse antigravity.OAuthSession's struct for cleanup compatibility.
	session := &antigravity.OAuthSession{
		State:        pkce.Verifier, // dummy — we encode the real state below
		CodeVerifier: pkce.Verifier,
		ProxyURL:     proxyURL,
		CreatedAt:    time.Now(),
	}
	s.sessionStore.Set(sessionID, session)

	state := agyauth.EncodeState(pkce.Verifier, "")
	authURL := agyauth.BuildAuthURL(state, pkce.Challenge)

	return &AntigravityNativeAuthURLResult{
		AuthURL:   authURL,
		SessionID: sessionID,
		State:     state,
	}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// code exchange
// ────────────────────────────────────────────────────────────────────────────

// AntigravityNativeExchangeCodeInput mirrors AntigravityExchangeCodeInput.
type AntigravityNativeExchangeCodeInput struct {
	SessionID string
	State     string
	Code      string
	ProxyID   *int64
}

// AntigravityNativeTokenInfo mirrors AntigravityTokenInfo plus a few
// native-only fields (installation_id, instance_label, connection_id) that
// the gateway service uses for the Unleash mimic loop.
type AntigravityNativeTokenInfo struct {
	AccessToken    string `json:"access_token"`
	RefreshToken   string `json:"refresh_token"`
	ExpiresIn      int64  `json:"expires_in"`
	ExpiresAt      int64  `json:"expires_at"`
	TokenType      string `json:"token_type"`
	Email          string `json:"email,omitempty"`
	ProjectID      string `json:"project_id,omitempty"`
	TierID         string `json:"tier_id,omitempty"`
	InstallationID string `json:"installation_id,omitempty"`
	InstanceLabel  string `json:"instance_label,omitempty"`
	ConnectionID   string `json:"connection_id,omitempty"`
}

// ExchangeCode trades the authorization code for tokens + project ID, then
// fills the deterministic identity fields. Same effect as logging in via
// `agycli login` then `agycli probe` — but routed through the admin's
// chosen proxy.
func (s *AntigravityNativeOAuthService) ExchangeCode(ctx context.Context, input *AntigravityNativeExchangeCodeInput) (*AntigravityNativeTokenInfo, error) {
	session, ok := s.sessionStore.Get(input.SessionID)
	if !ok {
		return nil, fmt.Errorf("native oauth: session not found or expired")
	}
	defer s.sessionStore.Delete(input.SessionID)

	// state arg sanity check (we encoded the real one in GenerateAuthURL)
	verifier, _, err := agyauth.DecodeState(input.State)
	if err != nil {
		return nil, fmt.Errorf("native oauth: decode state: %w", err)
	}
	if verifier != session.CodeVerifier {
		return nil, fmt.Errorf("native oauth: verifier mismatch (csrf?)")
	}

	httpc, err := agynative.NewProxyHTTPClient(session.ProxyURL, 30*time.Second)
	if err != nil {
		return nil, err
	}

	tokens, err := agyauth.ExchangeCodeWithClient(ctx, input.Code, verifier, httpc)
	if err != nil {
		// Pass agymimic's verbatim error through — it now contains the
		// raw Google error body (e.g. {"error":"invalid_grant",...}) or
		// the actual transport failure (proxy refused, DNS, TLS). The
		// handler layer prepends a "Token 交换失败: " prefix only when
		// the operator's UI needs human framing; the underlying message
		// is now actionable.
		return nil, err
	}

	return tokensToInfo(tokens), nil
}

// ────────────────────────────────────────────────────────────────────────────
// refresh
// ────────────────────────────────────────────────────────────────────────────

// ValidateRefreshToken validates an existing refresh_token and returns a full
// fresh token info (for admin-imported tokens). Equivalent to the existing
// legacy service method but routed through agymimic.
func (s *AntigravityNativeOAuthService) ValidateRefreshToken(ctx context.Context, refreshToken string, proxyID *int64) (*AntigravityNativeTokenInfo, error) {
	if refreshToken == "" {
		return nil, fmt.Errorf("native oauth: empty refresh token")
	}
	var proxyURL string
	if proxyID != nil {
		if p, err := s.proxyRepo.GetByID(ctx, *proxyID); err == nil && p != nil {
			proxyURL = p.URL()
		}
	}
	httpc, err := agynative.NewProxyHTTPClient(proxyURL, 30*time.Second)
	if err != nil {
		return nil, err
	}
	t := &agyauth.Tokens{RefreshToken: refreshToken}
	if err := agyauth.RefreshWithClient(ctx, t, httpc); err != nil {
		return nil, fmt.Errorf("native oauth: refresh: %w", err)
	}
	pid, tier, _ := agyauth.DiscoverProjectWithClient(ctx, t.AccessToken, "", httpc)
	t.ProjectID = pid
	t.TierID = tier
	agyauth.EnsureIdentity(t)
	return tokensToInfo(t), nil
}

// RefreshAccountToken refreshes tokens for a stored account. Called by the
// scheduler/gateway when ExpiresAt is in the past.
func (s *AntigravityNativeOAuthService) RefreshAccountToken(ctx context.Context, account *Account) (*AntigravityNativeTokenInfo, error) {
	if account == nil {
		return nil, fmt.Errorf("native oauth: nil account")
	}
	rt, _ := account.Credentials["refresh_token"].(string)
	if rt == "" {
		return nil, fmt.Errorf("native oauth: account has no refresh_token")
	}
	var proxyURL string
	if account.ProxyID != nil {
		if p, err := s.proxyRepo.GetByID(ctx, *account.ProxyID); err == nil && p != nil {
			proxyURL = p.URL()
		}
	}
	httpc, err := agynative.NewProxyHTTPClient(proxyURL, 30*time.Second)
	if err != nil {
		return nil, err
	}
	t := &agyauth.Tokens{
		RefreshToken:   rt,
		AccessToken:    stringField(account.Credentials, "access_token"),
		Email:          stringField(account.Credentials, "email"),
		ProjectID:      stringField(account.Credentials, "project_id"),
		TierID:         stringField(account.Credentials, "tier_id"),
		InstallationID: stringField(account.Credentials, "installation_id"),
		InstanceLabel:  stringField(account.Credentials, "instance_label"),
		ConnectionID:   stringField(account.Credentials, "connection_id"),
	}
	if err := agyauth.RefreshWithClient(ctx, t, httpc); err != nil {
		return nil, fmt.Errorf("native oauth: refresh: %w", err)
	}
	if t.ProjectID == "" {
		if pid, tier, err := agyauth.DiscoverProjectWithClient(ctx, t.AccessToken, "", httpc); err == nil {
			t.ProjectID = pid
			t.TierID = tier
		}
	}
	agyauth.EnsureIdentity(t)
	return tokensToInfo(t), nil
}

// ────────────────────────────────────────────────────────────────────────────
// credential I/O
// ────────────────────────────────────────────────────────────────────────────

// BuildAccountCredentials produces the credentials map persisted in the
// account's `credentials` JSONB column. Stable schema:
//
//	{
//	  "access_token":  "ya29.…",
//	  "refresh_token": "1//…",
//	  "expires_at":    1700000000,
//	  "email":         "…",
//	  "project_id":    "resonant-path-…",
//	  "tier_id":       "FREE_TIER",
//	  "installation_id": "<uuid>",
//	  "instance_label":  "DESKTOP-…\\<user>-DESKTOP-…",
//	  "connection_id":   "<uuid>"
//	}
func (s *AntigravityNativeOAuthService) BuildAccountCredentials(t *AntigravityNativeTokenInfo) map[string]any {
	return map[string]any{
		"access_token":    t.AccessToken,
		"refresh_token":   t.RefreshToken,
		"expires_at":      t.ExpiresAt,
		"token_type":      t.TokenType,
		"email":           t.Email,
		"project_id":      t.ProjectID,
		"tier_id":         t.TierID,
		"installation_id": t.InstallationID,
		"instance_label":  t.InstanceLabel,
		"connection_id":   t.ConnectionID,
	}
}

// TokensFromCredentials rehydrates an agyauth.Tokens from the JSONB map.
// Returns the rehydrated tokens; missing identity fields are filled in via
// EnsureIdentity so legacy/imported credentials still get a stable identity.
func TokensFromCredentials(creds map[string]any) *agyauth.Tokens {
	t := &agyauth.Tokens{
		AccessToken:    stringField(creds, "access_token"),
		RefreshToken:   stringField(creds, "refresh_token"),
		Email:          stringField(creds, "email"),
		ProjectID:      stringField(creds, "project_id"),
		TierID:         stringField(creds, "tier_id"),
		InstallationID: stringField(creds, "installation_id"),
		InstanceLabel:  stringField(creds, "instance_label"),
		ConnectionID:   stringField(creds, "connection_id"),
	}
	if expAt := int64Field(creds, "expires_at"); expAt > 0 {
		t.ExpiresAt = time.Unix(expAt, 0)
	}
	agyauth.EnsureIdentity(t)
	return t
}

// Stop releases the in-memory session store.
func (s *AntigravityNativeOAuthService) Stop() { s.sessionStore.Stop() }

// ────────────────────────────────────────────────────────────────────────────
// helpers
// ────────────────────────────────────────────────────────────────────────────

func tokensToInfo(t *agyauth.Tokens) *AntigravityNativeTokenInfo {
	now := time.Now()
	expIn := int64(0)
	expAt := int64(0)
	if !t.ExpiresAt.IsZero() {
		if d := t.ExpiresAt.Sub(now); d > 0 {
			expIn = int64(d.Seconds())
		}
		expAt = t.ExpiresAt.Unix()
	}
	return &AntigravityNativeTokenInfo{
		AccessToken:    t.AccessToken,
		RefreshToken:   t.RefreshToken,
		ExpiresIn:      expIn,
		ExpiresAt:      expAt,
		TokenType:      "Bearer",
		Email:          t.Email,
		ProjectID:      t.ProjectID,
		TierID:         t.TierID,
		InstallationID: t.InstallationID,
		InstanceLabel:  t.InstanceLabel,
		ConnectionID:   t.ConnectionID,
	}
}

func stringField(m map[string]any, k string) string {
	if m == nil {
		return ""
	}
	v, _ := m[k].(string)
	return v
}

func int64Field(m map[string]any, k string) int64 {
	if m == nil {
		return 0
	}
	switch v := m[k].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	return 0
}

func nativeRandomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
