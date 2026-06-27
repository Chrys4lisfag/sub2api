// Package auth implements the Antigravity OAuth 2.0 + PKCE flow.
//
// The flow byte-for-byte mimics agy.exe:
//  1. generate PKCE pair
//  2. encode state = base64url(json({verifier, projectId}))
//  3. open https://accounts.google.com/o/oauth2/v2/auth with our client_id
//     + S256 challenge + scopes + access_type=offline + prompt=consent
//  4. local callback server on :51121 catches code
//  5. POST oauth2.googleapis.com/token with code + verifier + client_secret
//  6. POST cloudcode-pa.googleapis.com/v1internal:loadCodeAssist
//     to discover/provision the cloudaicompanionProject
package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	I "github.com/koval/agymimic/internal"
)

// Tokens holds everything we need to persist for one Antigravity account.
//
// MACHINE-IDENTITY FIELDS (relevant for multi-account servers / sub2api):
//   - InstallationID:  stable per-account UUID. agy keeps one in
//     ~/.gemini/antigravity-cli/installation_id; we keep one per account
//     so two accounts on the same server look like two different installs.
//   - InstanceLabel:   the "<hostname>\<user>-<hostname>" string agy sends
//     in `unleash-instanceid` headers and the Unleash body. Default leaks
//     real hostname+user; override per account with a fake value.
//
// NEITHER FIELD is sent to cloudcode-pa.googleapis.com — that endpoint
// only sees OAuth bearer + project_id. They only matter if you also run
// the metrics.Client (Unleash) leg for organic-traffic mimicry.
type Tokens struct {
	AccessToken    string    `json:"access_token"`
	RefreshToken   string    `json:"refresh_token"`
	ExpiresAt      time.Time `json:"expires_at"`
	Email          string    `json:"email,omitempty"`
	ProjectID      string    `json:"project_id"`               // cloudaicompanion project
	TierID         string    `json:"tier_id,omitempty"`        // FREE | PAID
	InstallationID string    `json:"installation_id"`          // stable per-account UUID
	InstanceLabel  string    `json:"instance_label,omitempty"` // fake Unleash instance string (set on first login)
	ConnectionID   string    `json:"connection_id,omitempty"`  // persisted Unleash connection UUID
}

type authState struct {
	Verifier  string `json:"verifier"`
	ProjectID string `json:"projectId,omitempty"`
}

// Authorize starts the local callback server and returns the URL the user
// should open in a browser. ProjectID may be "" to let the server pick one.
//
// On success, the caller must invoke ExchangeCode() with the redirect
// `code` and `state` query params.
type Authorize struct {
	URL      string
	Verifier string
	// listener is held so we can serve the callback later
	listener net.Listener
	server   *http.Server
	codeCh   chan callbackResult
}

type callbackResult struct {
	code  string
	state string
	err   string
}

// StartAuthorize spawns a local HTTP listener on port 51121 (or the next
// free port if 51121 is in use) and returns the URL plus a Done channel
// that fires when the OAuth callback arrives.
func StartAuthorize(projectID string) (*Authorize, error) {
	pkce, err := NewPKCE()
	if err != nil {
		return nil, fmt.Errorf("pkce: %w", err)
	}
	st := authState{Verifier: pkce.Verifier, ProjectID: projectID}
	stateJSON, _ := json.Marshal(st)
	stateB64 := base64.RawURLEncoding.EncodeToString(stateJSON)

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", I.OAuthRedirectPort))
	if err != nil {
		// fall back to any free port
		listener, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("listen: %w", err)
		}
	}
	port := listener.Addr().(*net.TCPAddr).Port
	redirect := fmt.Sprintf("http://localhost:%d/oauth-callback", port)

	u, _ := url.Parse(I.GoogleAuthEndpoint)
	q := u.Query()
	q.Set("client_id", I.OAuthClientID)
	q.Set("response_type", "code")
	q.Set("redirect_uri", redirect)
	q.Set("scope", strings.Join(I.OAuthScopes, " "))
	q.Set("code_challenge", pkce.Challenge)
	q.Set("code_challenge_method", "S256")
	q.Set("state", stateB64)
	q.Set("access_type", "offline")
	q.Set("prompt", "consent")
	u.RawQuery = q.Encode()

	codeCh := make(chan callbackResult, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth-callback", func(w http.ResponseWriter, r *http.Request) {
		v := r.URL.Query()
		res := callbackResult{
			code:  v.Get("code"),
			state: v.Get("state"),
			err:   v.Get("error"),
		}
		select {
		case codeCh <- res:
		default:
		}
		if res.err == "" {
			fmt.Fprintln(w, "<h1>Antigravity login complete.</h1><p>You can close this window.</p>")
		} else {
			fmt.Fprintf(w, "<h1>Login failed</h1><pre>%s</pre>", res.err)
		}
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(listener)

	return &Authorize{
		URL:      u.String(),
		Verifier: pkce.Verifier,
		listener: listener,
		server:   srv,
		codeCh:   codeCh,
	}, nil
}

// Wait blocks until the OAuth callback arrives or ctx is cancelled.
// Returns the authorization `code` from Google.
func (a *Authorize) Wait(ctx context.Context) (string, error) {
	defer a.listener.Close()
	select {
	case res := <-a.codeCh:
		if res.err != "" {
			return "", fmt.Errorf("oauth: %s", res.err)
		}
		if res.code == "" {
			return "", fmt.Errorf("oauth: empty code")
		}
		return res.code, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// ExchangeCode exchanges an authorization code for refresh + access tokens,
// then resolves a cloudaicompanionProject and returns a ready-to-use Tokens.
func ExchangeCode(ctx context.Context, code string, verifier string) (*Tokens, error) {
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, errors.New(strings.TrimSpace(string(body)))
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

	// best-effort email
	if email, _ := fetchEmail(ctx, tr.AccessToken); email != "" {
		tokens.Email = email
	}

	// resolve cloudaicompanionProject
	projectID, tierID, _ := DiscoverProject(ctx, tr.AccessToken, "")
	if projectID == "" {
		projectID = I.FallbackProjectID
	}
	tokens.ProjectID = projectID
	tokens.TierID = tierID

	// Deterministic identity bound to the Google account (email-seeded).
	// Re-login → same InstallationID/InstanceLabel/ConnectionID; different
	// accounts → unrelated values.
	EnsureIdentity(tokens)

	return tokens, nil
}

// Refresh exchanges a refresh_token for a new access_token (and possibly a new
// refresh_token). Updates and returns t.
func Refresh(ctx context.Context, t *Tokens) error {
	form := url.Values{}
	form.Set("client_id", I.OAuthClientID)
	form.Set("client_secret", I.OAuthClientSecret)
	form.Set("refresh_token", t.RefreshToken)
	form.Set("grant_type", "refresh_token")

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, I.GoogleTokenEndpoint, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")

	resp, err := http.DefaultClient.Do(req)
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

// DecodeState decodes the state parameter sent in the OAuth redirect into
// (verifier, projectID).
func DecodeState(state string) (verifier, projectID string, err error) {
	pad := state
	if r := len(pad) % 4; r != 0 {
		pad += strings.Repeat("=", 4-r)
	}
	raw, decErr := base64.RawURLEncoding.DecodeString(state)
	if decErr != nil {
		raw, decErr = base64.URLEncoding.DecodeString(pad)
	}
	if decErr != nil {
		return "", "", decErr
	}
	var st authState
	if err := json.Unmarshal(raw, &st); err != nil {
		return "", "", err
	}
	return st.Verifier, st.ProjectID, nil
}

func fetchEmail(ctx context.Context, accessToken string) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, I.GoogleUserInfoV1, nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("User-Agent", I.LoadCodeAssistUA(""))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var u struct{ Email string `json:"email"` }
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
		return "", err
	}
	return u.Email, nil
}
