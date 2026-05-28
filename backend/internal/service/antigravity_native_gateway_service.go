// AntigravityNativeGatewayService routes inference traffic for
// PlatformAntigravityNative accounts through github.com/koval/agymimic.
//
// V1 scope:
//   - ForwardGemini: full passthrough of v1internal:streamGenerateContent
//     and v1internal:generateContent through agymimic's API client.
//     Honors per-account proxy (account.ProxyID).
//   - Forward (Claude): NOT implemented in V1. Returns 400 with a clear
//     message asking the client to route via Gemini format. The legacy
//     antigravity backend (PlatformAntigravity) still handles Claude.
//
// Token refresh, deterministic identity, and the per-account Unleash mimic
// loop are all owned by agymimic. The service-level smart-retry / rate-limit
// logic of the legacy backend is intentionally NOT replicated here — V1
// surfaces upstream errors verbatim so the account scheduler can decide.
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	agynative "github.com/Wei-Shaw/sub2api/internal/pkg/antigravity_native"
	"github.com/koval/agymimic/api"
	"github.com/koval/agymimic/metrics"
)

// AntigravityNativeGatewayService is the platform=antigravity_native gateway.
type AntigravityNativeGatewayService struct {
	// accountRepo enumerates native accounts at startup so the per-account
	// Unleash metrics loop (organic-traffic mimicry) can spin up one mimic
	// daemon per credential set. Also used by Invalidate callers that need
	// to load a fresh credential snapshot.
	accountRepo  AccountRepository
	proxyRepo    ProxyRepository
	oauthService *AntigravityNativeOAuthService

	// Per-account agymimic Client cache — keyed by account ID. Recreated on
	// proxy/credential change (handled by callers via Invalidate).
	clientCacheMu sync.RWMutex
	clientCache   map[int64]*nativeCacheEntry

	// Per-account Unleash metrics Client cache — one organic-traffic mimic
	// loop per native credential. Populated lazily on first use; lifecycle
	// driven by Stop() at shutdown and Invalidate() on credential change.
	metricsCacheMu sync.Mutex
	metricsCache   map[int64]*nativeMetricsEntry
}

type nativeCacheEntry struct {
	client    *api.Client
	proxyURL  string // empty = no proxy
	updatedAt time.Time
}

// nativeMetricsEntry tracks one per-account Unleash mimic-loop client.
// agy.exe POSTs /api/client/register at start, then /metrics every 60s.
// Backend uses these signals to mark accounts as "active organic users";
// running mimic per-account makes the native fleet look like a population
// of real agy installations rather than a single chat-only client.
type nativeMetricsEntry struct {
	client     *metrics.Client
	cancel     context.CancelFunc
	instanceID string
	startedAt  time.Time
}

func NewAntigravityNativeGatewayService(
	accountRepo AccountRepository,
	proxyRepo ProxyRepository,
	oauthService *AntigravityNativeOAuthService,
) *AntigravityNativeGatewayService {
	return &AntigravityNativeGatewayService{
		accountRepo:  accountRepo,
		proxyRepo:    proxyRepo,
		oauthService: oauthService,
		clientCache:  map[int64]*nativeCacheEntry{},
		metricsCache: map[int64]*nativeMetricsEntry{},
	}
}

// Invalidate drops cached client for the given account ID. Call after
// credentials/proxy change.
// Invalidate drops cached client + metrics loop for the given account ID.
// Call after credentials/proxy change so the next request rebuilds both
// with the fresh values.
func (s *AntigravityNativeGatewayService) Invalidate(accountID int64) {
	s.clientCacheMu.Lock()
	delete(s.clientCache, accountID)
	s.clientCacheMu.Unlock()

	s.metricsCacheMu.Lock()
	if m, ok := s.metricsCache[accountID]; ok {
		if m.cancel != nil {
			m.cancel()
		}
		delete(s.metricsCache, accountID)
	}
	s.metricsCacheMu.Unlock()
}

// Stop tears down every per-account metrics mimic loop. Call at process
// shutdown so background HTTP requests don't outlive the parent context.
func (s *AntigravityNativeGatewayService) Stop() {
	s.metricsCacheMu.Lock()
	for id, m := range s.metricsCache {
		if m.cancel != nil {
			m.cancel()
		}
		delete(s.metricsCache, id)
	}
	s.metricsCacheMu.Unlock()
}

// ensureMetricsLoop spins up (or returns existing) the Unleash mimic-loop
// goroutine for one account. Idempotent: re-entry returns the cached
// client unless Invalidate cleared it. Failures are non-fatal — V1 keeps
// serving inference even if the organic-traffic mimic can't bind.
func (s *AntigravityNativeGatewayService) ensureMetricsLoop(ctx context.Context, account *Account, proxyURL string) {
	if account == nil {
		return
	}
	s.metricsCacheMu.Lock()
	if _, ok := s.metricsCache[account.ID]; ok {
		s.metricsCacheMu.Unlock()
		return
	}
	tokens := TokensFromCredentials(account.Credentials)
	if tokens == nil || tokens.InstallationID == "" || tokens.InstanceLabel == "" || tokens.ConnectionID == "" {
		// Identity fields not yet populated (e.g. just-imported legacy creds).
		// Skip; next refresh will fill them in and trigger another attempt.
		s.metricsCacheMu.Unlock()
		return
	}
	httpc, err := agynative.NewProxyHTTPClient(proxyURL, 30*time.Second)
	if err != nil {
		s.metricsCacheMu.Unlock()
		return
	}
	client := metrics.New(metrics.Options{
		HTTPClient:   httpc,
		InstanceID:   tokens.InstanceLabel,
		ConnectionID: tokens.ConnectionID,
	})
	loopCtx, cancel := context.WithCancel(context.Background())
	s.metricsCache[account.ID] = &nativeMetricsEntry{
		client:     client,
		cancel:     cancel,
		instanceID: tokens.InstanceLabel,
		startedAt:  time.Now().UTC(),
	}
	s.metricsCacheMu.Unlock()
	go func() {
		// Start runs the register + 60s features/metrics loop until ctx is done.
		client.Start(loopCtx)
	}()
}

// IsModelSupported asks the legacy wire-model resolver whether a model name
// is plausibly routable. Cheap pre-check used by the scheduler.
func (s *AntigravityNativeGatewayService) IsModelSupported(requestedModel string) bool {
	return strings.HasPrefix(requestedModel, "gemini-") ||
		strings.HasPrefix(requestedModel, "claude-") ||
		strings.HasPrefix(requestedModel, "gpt-")
}

// resolveProxyURL returns the proxy URL string for an account, or "" if none.
func (s *AntigravityNativeGatewayService) resolveProxyURL(ctx context.Context, account *Account) string {
	if account == nil || account.ProxyID == nil {
		return ""
	}
	p, err := s.proxyRepo.GetByID(ctx, *account.ProxyID)
	if err != nil || p == nil {
		return ""
	}
	return p.URL()
}

// getClient returns a cached or freshly-built agymimic Client for an account.
// Rebuilds if the proxy URL or credentials changed since last build.
func (s *AntigravityNativeGatewayService) getClient(ctx context.Context, account *Account) (*api.Client, error) {
	if account == nil {
		return nil, fmt.Errorf("native: nil account")
	}
	proxyURL := s.resolveProxyURL(ctx, account)

	s.clientCacheMu.RLock()
	entry, ok := s.clientCache[account.ID]
	s.clientCacheMu.RUnlock()
	if ok && entry.proxyURL == proxyURL {
		return entry.client, nil
	}

	tokens := TokensFromCredentials(account.Credentials)
	if tokens.AccessToken == "" && tokens.RefreshToken == "" {
		return nil, fmt.Errorf("native: account %d has no credentials", account.ID)
	}
	cli, err := agynative.NewAPIClient(tokens, proxyURL, 0)
	if err != nil {
		return nil, fmt.Errorf("native: build client: %w", err)
	}

	s.clientCacheMu.Lock()
	s.clientCache[account.ID] = &nativeCacheEntry{
		client:    cli,
		proxyURL:  proxyURL,
		updatedAt: time.Now(),
	}
	s.clientCacheMu.Unlock()
	return cli, nil
}

// ForwardGemini handles Gemini-format requests (the native happy path).
//
// Signature mirrors AntigravityGatewayService.ForwardGemini so the dispatch
// layer can swap services purely by checking account.Platform.
func (s *AntigravityNativeGatewayService) ForwardGemini(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	originalModel string,
	action string,
	stream bool,
	body []byte,
	isStickySession bool,
) (*ForwardResult, error) {
	if account == nil {
		return nil, fmt.Errorf("native gemini: nil account")
	}
	if account.Platform != domain.PlatformAntigravityNative {
		return nil, fmt.Errorf("native gemini: wrong platform %q", account.Platform)
	}

	cli, err := s.getClient(ctx, account)
	if err != nil {
		return nil, err
	}

	// Best-effort: ensure the per-account Unleash organic-traffic mimic
	// loop is running. Idempotent — subsequent calls are no-ops.
	s.ensureMetricsLoop(ctx, account, s.resolveProxyURL(ctx, account))

	// Wire-level model name (e.g. gemini-3-pro-high → wire alias).
	wireModel := antigravity.AntigravityWireModel(originalModel)
	if wireModel == "" {
		wireModel = originalModel
	}

	// Wrap the incoming body in the v1internal envelope. body is the
	// Gemini-format inner request ({contents, generationConfig, …}).
	envelope, err := wrapNativeV1Internal(cli.ProjectID(), wireModel, body)
	if err != nil {
		return nil, fmt.Errorf("native gemini: envelope: %w", err)
	}

	path := "/v1internal:" + chooseGeminiAction(action, stream)
	if stream {
		path += "?alt=sse"
	}

	startTime := time.Now()
	resp, err := cli.RawRequest(ctx, path, envelope)
	if err != nil {
		return nil, fmt.Errorf("native gemini: upstream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, &UpstreamFailoverError{
			StatusCode:      resp.StatusCode,
			ResponseBody:    raw,
			ResponseHeaders: resp.Header,
		}
	}

	if stream {
		return s.streamGeminiToClient(c, resp, startTime, originalModel, wireModel)
	}
	return s.passNonStreamingGemini(c, resp, startTime, originalModel, wireModel)
}

// Forward handles Claude-format requests. Native does not implement the
// Claude/Anthropic protocol in V1 (agymimic talks Gemini-format to
// daily-cloudcode-pa). Returns an UpstreamFailoverError so the gateway
// handler's failover loop hops to the next eligible account in the same
// group rather than retrying this one — keeps multi-platform groups
// (where admins mix native + legacy accounts) working transparently.
func (s *AntigravityNativeGatewayService) Forward(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	isStickySession bool,
) (*ForwardResult, error) {
	msg := `{"type":"error","error":{"type":"invalid_request_error","message":"native antigravity accounts do not support the Anthropic /v1/messages protocol — use the Gemini /v1beta endpoints, or route this request to a legacy antigravity account"}}`
	return nil, &UpstreamFailoverError{
		StatusCode:             http.StatusBadRequest,
		ResponseBody:           []byte(msg),
		ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
		ForceCacheBilling:      false,
		RetryableOnSameAccount: false,
	}
}

// ────────────────────────────────────────────────────────────────────────────
// helpers
// ────────────────────────────────────────────────────────────────────────────

// wrapNativeV1Internal wraps a Gemini-format `request` body in the agy v1
// envelope: {project, model, request:{...}, userAgent, requestId}.
func wrapNativeV1Internal(projectID, model string, geminiBody []byte) ([]byte, error) {
	// First, decide whether the incoming body is already a v1internal envelope
	// (idempotent passthrough) or a bare Gemini request body to be wrapped.
	if len(geminiBody) > 0 && bytes.Contains(geminiBody, []byte(`"userAgent":"antigravity"`)) {
		return geminiBody, nil
	}

	var inner map[string]any
	if len(geminiBody) > 0 {
		if err := json.Unmarshal(geminiBody, &inner); err != nil {
			return nil, fmt.Errorf("decode body: %w", err)
		}
	}
	// If the caller already gave us {"request": {...}} double-wrap, unwrap once.
	if r, ok := inner["request"].(map[string]any); ok && len(inner) == 1 {
		inner = r
	}

	envelope := map[string]any{
		"model":       model,
		"request":     inner,
		"userAgent":   "antigravity",
		"requestId":   "agent-" + uuid.NewString(),
	}
	if projectID != "" {
		envelope["project"] = projectID
	}
	return json.Marshal(envelope)
}

func chooseGeminiAction(action string, stream bool) string {
	if action != "" {
		return action
	}
	if stream {
		return "streamGenerateContent"
	}
	return "generateContent"
}

// streamGeminiToClient pipes the upstream SSE body to the gin client and
// extracts usage metadata from the final chunk.
func (s *AntigravityNativeGatewayService) streamGeminiToClient(
	c *gin.Context,
	resp *http.Response,
	startTime time.Time,
	originalModel, wireModel string,
) (*ForwardResult, error) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Writer.WriteHeader(http.StatusOK)
	flusher, _ := c.Writer.(http.Flusher)

	result := &ForwardResult{
		Model:         originalModel,
		UpstreamModel: wireModel,
		Stream:        true,
	}

	buf := make([]byte, 0, 8192)
	chunkBuf := make([]byte, 8192)
	first := true
	var firstTokenMs int
	for {
		n, readErr := resp.Body.Read(chunkBuf)
		if n > 0 {
			if first {
				first = false
				firstTokenMs = int(time.Since(startTime).Milliseconds())
				result.FirstTokenMs = &firstTokenMs
			}
			buf = append(buf, chunkBuf[:n]...)
			// flush by-line so SSE framing stays intact
			for {
				idx := bytes.IndexByte(buf, '\n')
				if idx < 0 {
					break
				}
				line := buf[:idx+1]
				buf = buf[idx+1:]
				if _, wErr := c.Writer.Write(line); wErr != nil {
					result.ClientDisconnect = true
					_, _ = io.Copy(io.Discard, resp.Body)
					return s.finalizeResult(result, startTime), nil
				}
				if flusher != nil {
					flusher.Flush()
				}
				if u := extractGeminiUsageFromSSELine(line); u != nil {
					result.Usage.InputTokens = u.PromptTokens
					result.Usage.OutputTokens = u.CandidateTokens
					if u.ModelVersion != "" {
						result.UpstreamModel = u.ModelVersion
					}
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				if len(buf) > 0 {
					_, _ = c.Writer.Write(buf)
					if flusher != nil {
						flusher.Flush()
					}
				}
				break
			}
			return nil, readErr
		}
	}
	return s.finalizeResult(result, startTime), nil
}

func (s *AntigravityNativeGatewayService) passNonStreamingGemini(
	c *gin.Context,
	resp *http.Response,
	startTime time.Time,
	originalModel, wireModel string,
) (*ForwardResult, error) {
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	c.Header("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)
	_, _ = c.Writer.Write(raw)

	result := &ForwardResult{
		Model:         originalModel,
		UpstreamModel: wireModel,
	}
	if u := extractGeminiUsageFromResponse(raw); u != nil {
		result.Usage.InputTokens = u.PromptTokens
		result.Usage.OutputTokens = u.CandidateTokens
		if u.ModelVersion != "" {
			result.UpstreamModel = u.ModelVersion
		}
	}
	return s.finalizeResult(result, startTime), nil
}

func (s *AntigravityNativeGatewayService) finalizeResult(r *ForwardResult, startTime time.Time) *ForwardResult {
	r.Duration = time.Since(startTime)
	if r.Usage.OutputTokens == 0 && r.Usage.InputTokens == 0 {
		// best-effort — gateway middleware will fall back to its own counters
	}
	return r
}

// ────────────────────────────────────────────────────────────────────────────
// SSE usage extraction
// ────────────────────────────────────────────────────────────────────────────

type geminiUsageView struct {
	PromptTokens    int
	CandidateTokens int
	TotalTokens     int
	ThinkingTokens  int
	ModelVersion    string
}

func extractGeminiUsageFromSSELine(line []byte) *geminiUsageView {
	const prefix = "data:"
	t := bytes.TrimSpace(line)
	if !bytes.HasPrefix(t, []byte(prefix)) {
		return nil
	}
	payload := bytes.TrimSpace(t[len(prefix):])
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return nil
	}
	return extractGeminiUsageFromResponse(payload)
}

func extractGeminiUsageFromResponse(body []byte) *geminiUsageView {
	var env struct {
		Response struct {
			UsageMetadata struct {
				PromptTokenCount     int `json:"promptTokenCount"`
				CandidatesTokenCount int `json:"candidatesTokenCount"`
				TotalTokenCount      int `json:"totalTokenCount"`
				ThoughtsTokenCount   int `json:"thoughtsTokenCount"`
			} `json:"usageMetadata"`
			ModelVersion string `json:"modelVersion"`
		} `json:"response"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil
	}
	if env.Response.UsageMetadata.PromptTokenCount == 0 &&
		env.Response.UsageMetadata.CandidatesTokenCount == 0 {
		return nil
	}
	return &geminiUsageView{
		PromptTokens:    env.Response.UsageMetadata.PromptTokenCount,
		CandidateTokens: env.Response.UsageMetadata.CandidatesTokenCount,
		TotalTokens:     env.Response.UsageMetadata.TotalTokenCount,
		ThinkingTokens:  env.Response.UsageMetadata.ThoughtsTokenCount,
		ModelVersion:    env.Response.ModelVersion,
	}
}
