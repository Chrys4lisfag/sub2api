package auth

// HTTP-client-aware variants of OAuth and project-discovery functions.
//
// All network-touching helpers in this package historically used
// http.DefaultClient, which is fine for the local CLI but makes per-account
// proxy routing impossible from an embedding application (sub2api, etc.).
//
// The *WithClient variants below accept an explicit *http.Client. Pass an
// http.Client whose Transport uses your proxy and we'll make every request
// (token exchange, refresh, userInfo, loadCodeAssist, onboardUser) go
// through it. The original funcs are preserved as zero-argument wrappers
// calling these with http.DefaultClient.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	I "github.com/koval/agymimic/internal"
)

// ExchangeCodeWithClient is ExchangeCode but uses the supplied *http.Client
// (e.g. proxy-aware) for every outbound request.
func ExchangeCodeWithClient(ctx context.Context, code, verifier string, httpc *http.Client) (*Tokens, error) {
	if httpc == nil {
		httpc = http.DefaultClient
	}
	form := url.Values{}
	form.Set("client_id", I.OAuthClientID)
	form.Set("client_secret", I.OAuthClientSecret)
	form.Set("code", code)
	form.Set("grant_type", "authorization_code")
	form.Set("redirect_uri", I.OAuthRedirectURI)
	form.Set("code_verifier", verifier)

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, I.GoogleTokenEndpoint, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
	req.Header.Set("Accept", "*/*")

	resp, err := httpc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("token exchange: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("token exchange: %d: %s", resp.StatusCode, string(body))
	}
	var tr struct {
		AccessToken  string `json:"access_token"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &tr); err != nil {
		return nil, fmt.Errorf("token decode: %w", err)
	}
	tokens := &Tokens{
		AccessToken:  tr.AccessToken,
		RefreshToken: tr.RefreshToken,
		ExpiresAt:    time.Now().Add(time.Duration(tr.ExpiresIn-30) * time.Second),
	}
	if email, _ := fetchEmailWithClient(ctx, tr.AccessToken, httpc); email != "" {
		tokens.Email = email
	}
	pid, tier, _ := DiscoverProjectWithClient(ctx, tr.AccessToken, "", httpc)
	if pid == "" {
		pid = I.FallbackProjectID
	}
	tokens.ProjectID = pid
	tokens.TierID = tier

	EnsureIdentity(tokens)
	return tokens, nil
}

// RefreshWithClient is Refresh but uses the supplied *http.Client.
func RefreshWithClient(ctx context.Context, t *Tokens, httpc *http.Client) error {
	if httpc == nil {
		httpc = http.DefaultClient
	}
	form := url.Values{}
	form.Set("client_id", I.OAuthClientID)
	form.Set("client_secret", I.OAuthClientSecret)
	form.Set("refresh_token", t.RefreshToken)
	form.Set("grant_type", "refresh_token")

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, I.GoogleTokenEndpoint, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")

	resp, err := httpc.Do(req)
	if err != nil {
		return fmt.Errorf("refresh: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("refresh: %d: %s", resp.StatusCode, string(body))
	}
	var tr struct {
		AccessToken  string `json:"access_token"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &tr); err != nil {
		return fmt.Errorf("refresh decode: %w", err)
	}
	t.AccessToken = tr.AccessToken
	t.ExpiresAt = time.Now().Add(time.Duration(tr.ExpiresIn-30) * time.Second)
	if tr.RefreshToken != "" {
		t.RefreshToken = tr.RefreshToken
	}
	return nil
}

// DiscoverProjectWithClient is DiscoverProject but proxy-aware.
func DiscoverProjectWithClient(ctx context.Context, accessToken, preferProject string, httpc *http.Client) (string, string, error) {
	if httpc == nil {
		httpc = http.DefaultClient
	}
	for _, base := range I.LoadEndpoints {
		payload, err := loadCodeAssistWithClient(ctx, base, accessToken, preferProject, httpc)
		if err != nil {
			continue
		}
		if pid := extractProjectID(payload); pid != "" {
			tier := ""
			if t, ok := payload["currentTier"].(map[string]any); ok {
				if id, _ := t["id"].(string); id != "" {
					tier = id
				}
			}
			return pid, tier, nil
		}
		tierID := defaultTier(payload)
		if pid, err := onboardUserWithClient(ctx, base, accessToken, tierID, preferProject, httpc); err == nil && pid != "" {
			return pid, tierID, nil
		}
	}
	return "", "", errors.New("loadCodeAssist + onboardUser both failed across all endpoints")
}

func loadCodeAssistWithClient(ctx context.Context, base, accessToken, preferProject string, httpc *http.Client) (map[string]any, error) {
	body := map[string]any{"metadata": metadataMap(preferProject)}
	buf, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+I.PathLoadCodeAssist, bytes.NewReader(buf))
	I.SetLoadCodeAssistHeaders(req, accessToken, "")
	resp, err := httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("loadCodeAssist %s: %d: %s", base, resp.StatusCode, string(raw))
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func onboardUserWithClient(ctx context.Context, base, accessToken, tierID, preferProject string, httpc *http.Client) (string, error) {
	body := map[string]any{
		"tierId":   tierID,
		"metadata": metadataMap(preferProject),
	}
	buf, _ := json.Marshal(body)
	for attempt := 0; attempt < 10; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+I.PathOnboardUser, bytes.NewReader(buf))
		I.SetLoadCodeAssistHeaders(req, accessToken, "")
		resp, err := httpc.Do(req)
		if err != nil {
			return "", err
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return "", fmt.Errorf("onboardUser: %d: %s", resp.StatusCode, string(raw))
		}
		var out struct {
			Done     bool `json:"done"`
			Response struct {
				CloudAICompanionProject struct {
					ID string `json:"id"`
				} `json:"cloudaicompanionProject"`
			} `json:"response"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return "", err
		}
		if out.Done {
			if id := out.Response.CloudAICompanionProject.ID; id != "" {
				return id, nil
			}
			if preferProject != "" {
				return preferProject, nil
			}
			return "", errors.New("onboardUser done with no project")
		}
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "", errors.New("onboardUser: gave up after 10 polls")
}

func fetchEmailWithClient(ctx context.Context, accessToken string, httpc *http.Client) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, I.GoogleUserInfoV1, nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("User-Agent", I.LoadCodeAssistUA(""))
	resp, err := httpc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var u struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
		return "", err
	}
	return u.Email, nil
}

// BuildAuthURL builds the Google OAuth URL for the Antigravity client.
// state is base64url(JSON({verifier, projectId})); use EncodeState to get one.
// Embedding apps that already own their callback server should call this
// instead of StartAuthorize (which spawns its own local listener).
func BuildAuthURL(state, codeChallenge string) string {
	u, _ := url.Parse(I.GoogleAuthEndpoint)
	q := u.Query()
	q.Set("client_id", I.OAuthClientID)
	q.Set("response_type", "code")
	q.Set("redirect_uri", I.OAuthRedirectURI)
	q.Set("scope", strings.Join(I.OAuthScopes, " "))
	q.Set("code_challenge", codeChallenge)
	q.Set("code_challenge_method", "S256")
	q.Set("state", state)
	q.Set("access_type", "offline")
	q.Set("prompt", "consent")
	u.RawQuery = q.Encode()
	return u.String()
}

// EncodeState packs (verifier, projectId) into the base64url JSON form we
// send as `state` and read back via DecodeState.
func EncodeState(verifier, projectID string) string {
	st := authState{Verifier: verifier, ProjectID: projectID}
	b, _ := json.Marshal(st)
	// matches existing behavior in StartAuthorize (RawURLEncoding)
	return base64Marshal(b)
}

func base64Marshal(b []byte) string {
	// indirection so we share the same encoder as DecodeState
	return rawURLEncode(b)
}
