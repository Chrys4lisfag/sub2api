// Package api is the Antigravity wire client. One Client instance wraps one
// OAuth credential set (Tokens). Use it to call /v1internal:generateContent
// or /v1internal:streamGenerateContent against daily-cloudcode-pa.
package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/koval/agymimic/auth"
	I "github.com/koval/agymimic/internal"
	"github.com/koval/agymimic/types"
)

// Client is an Antigravity API client bound to one OAuth credential.
type Client struct {
	tokens    *auth.Tokens
	tokensMu  sync.RWMutex
	endpoint  string         // primary endpoint; defaults to daily
	httpc     *http.Client
	version   string         // antigravity version string for UA
}

// Option configures a Client.
type Option func(*Client)

// WithEndpoint overrides the primary endpoint (default: daily.sandbox).
func WithEndpoint(url string) Option { return func(c *Client) { c.endpoint = url } }

// WithHTTPClient lets the caller supply a custom transport (e.g. with proxy).
// The supplied client should NOT auto-decode (set DisableCompression=false is fine).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpc = h } }

// WithVersion overrides the User-Agent's antigravity version string.
func WithVersion(v string) Option { return func(c *Client) { c.version = v } }

// New returns a ready Client. Refresh() the tokens before any call if expired.
func New(tokens *auth.Tokens, opts ...Option) *Client {
	c := &Client{
		tokens:   tokens,
		endpoint: I.EndpointDailySandbox,
		httpc: &http.Client{
			Transport: &http.Transport{
				// Real agy.exe uses HTTP/1.1 with Transfer-Encoding: chunked to
				// cloudcode-pa (verified via Frida capture May 2026). Earlier
				// http2debug=2 logs that suggested HTTP/2 were ONLY for the
				// Unleash + oauth endpoints; cloudcode-pa traffic bypassed Go
				// stdlib http2 entirely. Match the real wire.
				ForceAttemptHTTP2:     false,
				MaxIdleConns:          16,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 60 * time.Second,
			},
			Timeout: 0, // streaming — never bound the whole call
		},
		version: "", // empty = use LatestAntigravityVersion() each request
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// ensureFresh refreshes the access token if it's within 30s of expiry.
func (c *Client) ensureFresh(ctx context.Context) error {
	c.tokensMu.RLock()
	t := c.tokens
	c.tokensMu.RUnlock()
	if time.Now().Before(t.ExpiresAt.Add(-30 * time.Second)) {
		return nil
	}
	c.tokensMu.Lock()
	defer c.tokensMu.Unlock()
	if time.Now().Before(c.tokens.ExpiresAt.Add(-30 * time.Second)) {
		return nil // raced
	}
	return auth.Refresh(ctx, c.tokens)
}

// buildRequest fills the agy.exe-style envelope around the inner request body.
func (c *Client) buildRequest(model string, req types.GenerateInner) types.Request {
	c.tokensMu.RLock()
	project := c.tokens.ProjectID
	c.tokensMu.RUnlock()
	return types.Request{
		Project:   project,
		Model:     model,
		Request:   req,
		UserAgent: "antigravity",
		RequestID: "agent-" + uuid.NewString(),
	}
}

// Generate sends one /v1internal:generateContent and returns the full
// (non-streaming) response.
func (c *Client) Generate(ctx context.Context, model string, req types.GenerateInner) (*types.Response, error) {
	if err := c.ensureFresh(ctx); err != nil {
		return nil, fmt.Errorf("refresh: %w", err)
	}
	body, _ := json.Marshal(c.buildRequest(model, req))
	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+I.PathGenerateContent, bytes.NewReader(body))
	c.tokensMu.RLock()
	I.SetAntigravityHeaders(httpReq, c.tokens.AccessToken, c.version)
	c.tokensMu.RUnlock()

	resp, err := c.httpc.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, parseAPIError(resp.StatusCode, raw)
	}
	var out types.Response
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &out, nil
}

// StreamEvent is one SSE chunk delivered by Stream().
type StreamEvent struct {
	Resp *types.Response // nil only if Err != nil
	Err  error
}

// Stream sends /v1internal:streamGenerateContent?alt=sse and returns a channel
// of decoded chunks. Channel closes when the stream ends or ctx is cancelled.
func (c *Client) Stream(ctx context.Context, model string, req types.GenerateInner) (<-chan StreamEvent, error) {
	if err := c.ensureFresh(ctx); err != nil {
		return nil, fmt.Errorf("refresh: %w", err)
	}
	body, _ := json.Marshal(c.buildRequest(model, req))
	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+I.PathStreamGenerate, bytes.NewReader(body))
	c.tokensMu.RLock()
	I.SetAntigravityHeaders(httpReq, c.tokens.AccessToken, c.version)
	c.tokensMu.RUnlock()
	httpReq.Header.Set("Accept", "text/event-stream")

	// DEBUG
	if os.Getenv("AGYMIMIC_DEBUG") != "" {
		fmt.Fprintln(os.Stderr, "[DBG] URL:", httpReq.URL.String())
		for k, v := range httpReq.Header {
			fmt.Fprintf(os.Stderr, "[DBG] H %s: %s\n", k, v)
		}
		fmt.Fprintln(os.Stderr, "[DBG] body:", string(body))
	}

	resp, err := c.httpc.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("post: %w", err)
	}
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, parseAPIError(resp.StatusCode, raw)
	}

	out := make(chan StreamEvent, 16)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 256*1024), 4*1024*1024)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "[DONE]" {
				return
			}
			var ev types.Response
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				out <- StreamEvent{Err: fmt.Errorf("sse decode: %w (raw=%q)", err, data)}
				continue
			}
			out <- StreamEvent{Resp: &ev}
		}
		if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
			out <- StreamEvent{Err: err}
		}
	}()
	return out, nil
}

// CountTokens posts to /v1internal:countTokens and returns the prompt-token
// estimate from the backend.
func (c *Client) CountTokens(ctx context.Context, model string, req types.GenerateInner) (int, error) {
	if err := c.ensureFresh(ctx); err != nil {
		return 0, err
	}
	// :countTokens body shape is {"request": {contents:[...], ...}} only.
	// model param goes via URL ?model= if needed; backend infers it otherwise.
	// `model` arg currently ignored — we keep it in the signature for future
	// per-model token-count routing.
	_ = model
	body, _ := json.Marshal(map[string]any{"request": req})
	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+I.PathCountTokens, bytes.NewReader(body))
	c.tokensMu.RLock()
	I.SetAntigravityHeaders(httpReq, c.tokens.AccessToken, c.version)
	c.tokensMu.RUnlock()
	resp, err := c.httpc.Do(httpReq)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return 0, parseAPIError(resp.StatusCode, raw)
	}
	var out struct {
		TotalTokens int `json:"totalTokens"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return 0, err
	}
	return out.TotalTokens, nil
}

// Tokens returns the current Tokens snapshot (safe to read but not mutate
// — use auth.Refresh).
func (c *Client) Tokens() auth.Tokens {
	c.tokensMu.RLock()
	defer c.tokensMu.RUnlock()
	return *c.tokens
}

// RawRequest POSTs `body` raw to <endpoint><path> with the full Antigravity
// header set (Authorization, User-Agent, X-Goog-Api-Client, Client-Metadata,
// Content-Type: application/json), refreshing the access token first if it's
// expired. Caller owns the response body — including SSE parsing.
//
// Use this when you're forwarding an upstream request (sub2api etc.) and
// the body shape is already prepared. For new code prefer Generate/Stream
// which build the envelope for you.
//
// path should start with "/" — e.g. "/v1internal:streamGenerateContent?alt=sse".
func (c *Client) RawRequest(ctx context.Context, path string, body []byte) (*http.Response, error) {
	if err := c.ensureFresh(ctx); err != nil {
		return nil, fmt.Errorf("refresh: %w", err)
	}
	url := c.endpoint + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	c.tokensMu.RLock()
	I.SetAntigravityHeaders(req, c.tokens.AccessToken, c.version)
	c.tokensMu.RUnlock()
	if strings.Contains(path, "alt=sse") {
		req.Header.Set("Accept", "text/event-stream")
	}
	return c.httpc.Do(req)
}

// Endpoint returns the configured cloudcode-pa base URL (no trailing slash).
func (c *Client) Endpoint() string { return c.endpoint }

// ProjectID returns the cloudaicompanion project ID bound to this Client's
// tokens. Useful when building request bodies that need to include `project`.
func (c *Client) ProjectID() string {
	c.tokensMu.RLock()
	defer c.tokensMu.RUnlock()
	return c.tokens.ProjectID
}

func parseAPIError(code int, raw []byte) error {
	var e types.APIError
	if json.Unmarshal(raw, &e) == nil && e.Error.Message != "" {
		return fmt.Errorf("antigravity api %d %s: %s", code, e.Error.Status, e.Error.Message)
	}
	return fmt.Errorf("antigravity api %d: %s", code, string(raw))
}
