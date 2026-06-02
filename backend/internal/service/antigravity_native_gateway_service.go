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
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/koval/agymimic/fingerprint"
	"github.com/koval/agymimic/metrics"
	"github.com/koval/agymimic/types"
)

// AntigravityNativeGatewayService is the platform=antigravity_native gateway.
type AntigravityNativeGatewayService struct {
	// accountRepo enumerates native accounts at startup so the per-account
	// Unleash metrics loop (organic-traffic mimicry) can spin up one mimic
	// daemon per credential set. Also used by Invalidate callers that need
	// to load a fresh credential snapshot.
	accountRepo    AccountRepository
	proxyRepo      ProxyRepository
	oauthService   *AntigravityNativeOAuthService
	settingService *SettingService

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
	settingService *SettingService,
) *AntigravityNativeGatewayService {
	return &AntigravityNativeGatewayService{
		accountRepo:    accountRepo,
		proxyRepo:      proxyRepo,
		oauthService:   oauthService,
		settingService: settingService,
		clientCache:    map[int64]*nativeCacheEntry{},
		metricsCache:   map[int64]*nativeMetricsEntry{},
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

// isListToolsEmulationEnabled returns true when the agy_list_tools
// transparent MCP discovery roundtrip is enabled for this request.
// Resolution order:
//
//  1. Per-account credential `list_tools_emulation` (bool or string
//     "true"/"false"/"on"/"off") — explicit override
//  2. Global setting SettingKeyAntigravityNativeListToolsEmulation
//  3. Default off
//
// When settingService is nil (tests / partial wire), only per-account
// explicit credentials apply.
func (s *AntigravityNativeGatewayService) isListToolsEmulationEnabled(ctx context.Context, account *Account) bool {
	if account != nil {
		if v, ok := account.Credentials["list_tools_emulation"].(bool); ok {
			return v
		}
		if str, ok := account.Credentials["list_tools_emulation"].(string); ok {
			switch strings.ToLower(strings.TrimSpace(str)) {
			case "true", "on", "1", "yes":
				return true
			case "false", "off", "0", "no":
				return false
			}
		}
	}
	if s.settingService == nil {
		return false
	}
	return s.settingService.IsAntigravityNativeListToolsEmulationEnabled(ctx)
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

	// Wire-level model name. ResolveWireFromBody honors the caller's
	// thinkingConfig.thinkingLevel when the public name is the suffix-less
	// `gemini-3.5-flash` base — so omp can ship a single model entry whose
	// slider picks the real backend tier (extra-low / mid / high).
	wireModel := antigravity.ResolveWireFromBody(originalModel, body)
	if wireModel == "" {
		wireModel = originalModel
	}

	// Run the tool-list preprocessing pipeline (schema normalize +
	// optional call_mcp_tool aggregator). Mutates `body` JSON in place
	// before envelope wrap. Aggregator is enabled per account via the
	// `tool_aggregator` credential flag (defaults to true — main fix
	// for the omp 200+ tools empty-args failure mode).
	useAggregator := accountToolAggregatorEnabled(account)
	aggregatorName := accountMcpAggregatorName(account)
	body, toolReport, err := preprocessNativeBody(body, useAggregator, aggregatorName)
	if err != nil {
		return nil, fmt.Errorf("native gemini: tool preprocess: %w", err)
	}

	// agy_list_tools transparent discovery loop. Runs only when:
	//   - the global setting (or per-account override) enables it
	//   - the aggregator is on (otherwise there's no catalog to expose)
	//   - the tools list contains at least one mcp__* tool
	//
	// Each loop iteration POSTs a non-streaming upstream request,
	// inspects the response for a `functionCall{name: "agy_list_tools"}`,
	// synthesizes a `functionResponse` with the MCP catalog (filtered by
	// optional `server` arg), appends the assistant.call + user.response
	// pair to the body's contents[], and re-issues. Loop terminates when
	// the model emits real output (text or `call_mcp_tool`) or the
	// budget is exhausted. The final response is then written to the
	// client as one SSE event (for streaming clients) or one JSON body
	// (for non-streaming). Clients never observe the discovery turns.
	if s.isListToolsEmulationEnabled(ctx, account) && toolReport.AggregatorOn && len(toolReport.McpTools) > 0 {
		startTime := time.Now()
		finalResp, iters, loopErr := s.resolveAgyListToolsLoop(ctx, cli, wireModel, body, toolReport)
		if loopErr == nil {
			slog.InfoContext(ctx, "native: agy_list_tools loop completed",
				slog.Int64("account_id", account.ID),
				slog.Int("iterations", iters))
			// Run the response through the existing back-translator so
			// `call_mcp_tool` is rewritten to `mcp__server_tool` before
			// the client sees it.
			finalResp = rewriteAggregatedFunctionCalls(finalResp, toolReport)
			return s.flushBufferedNativeResponse(ctx, c, account.ID, finalResp, startTime, originalModel, wireModel, toolReport, stream)
		}
		// On loop error: if it's an upstream-failover error, propagate;
		// otherwise log + fall through to normal streaming path.
		if _, isFailover := loopErr.(*UpstreamFailoverError); isFailover {
			return nil, loopErr
		}
		slog.WarnContext(ctx, "native: agy_list_tools loop failed, falling back to normal streaming",
			slog.Int64("account_id", account.ID),
			slog.String("err", loopErr.Error()))
	}

	// Wrap the now-normalized body in the v1internal envelope.
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
		logNativeUpstreamError(ctx, account.ID, originalModel, wireModel, action, stream, resp.StatusCode, resp.Header, raw)

		// If upstream rejected our advertised Antigravity version, kick
		// the fingerprint refresher out of its 6 h slumber so the NEXT
		// request uses the fresh manifest. The current request still
		// surfaces the error to the caller — we don't auto-retry here
		// because envelope/headers were already serialized with the old
		// version. The gateway handler's failover loop (or the client's
		// next request) picks up the refreshed value.
		if isAntigravityVersionRejection(resp.StatusCode, raw) {
			slog.WarnContext(ctx, "native: upstream rejected version, forcing fingerprint refresh",
				slog.Int64("account_id", account.ID),
				slog.Int("status", resp.StatusCode))
			fingerprint.ForceRefresh()
		}

		return nil, &UpstreamFailoverError{
			StatusCode:             resp.StatusCode,
			ResponseBody:           raw,
			ResponseHeaders:        resp.Header,
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: isAntigravityVersionRejection(resp.StatusCode, raw),
		}
	}

	if stream {
		return s.streamGeminiToClient(ctx, c, account.ID, resp, startTime, originalModel, wireModel, toolReport)
	}
	return s.passNonStreamingGemini(ctx, c, account.ID, resp, startTime, originalModel, wireModel, toolReport)
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

// wrapNativeV1Internal wraps a Gemini-format inner request body in the
// envelope agy.exe sends to /v1internal:streamGenerateContent. Verified
// via Frida capture of crypto/tls.Conn.Write (May 2026):
//
//	{
//	  "project":     "<project_id>",
//	  "requestId":   "checkpoint/<uuid>",   // NOT "agent-<uuid>"
//	  "model":       "<wire_model>",        // envelope-level
//	  "userAgent":   "antigravity",
//	  "requestType": "checkpoint",          // agy always sends this
//	  "request": {
//	    "contents":       [...],
//	    "systemInstruction": {...},
//	    "tools":           [...],
//	    "toolConfig":      {"functionCallingConfig":{"mode":"NONE"}},
//	    "generationConfig":{...},
//	    "sessionId":       "<int64-as-string-or-int>"
//	  }
//	}
//
// Returns the marshaled envelope ready for the HTTP body.
func wrapNativeV1Internal(projectID, model string, geminiBody []byte) ([]byte, error) {
	// Idempotent passthrough: caller may have pre-built the full envelope.
	if len(geminiBody) > 0 && bytes.Contains(geminiBody, []byte(`"userAgent":"antigravity"`)) {
		return geminiBody, nil
	}

	var inner map[string]any
	if len(geminiBody) > 0 {
		if err := json.Unmarshal(geminiBody, &inner); err != nil {
			return nil, fmt.Errorf("decode body: %w", err)
		}
	}
	// Caller-handed double-wrap: {"request": {...}} → unwrap once.
	if r, ok := inner["request"].(map[string]any); ok && len(inner) == 1 {
		inner = r
	}

	// Inject sessionId on the inner request if the caller didn't supply one.
	// agy sends a deterministic 64-bit int. We synthesize a session per
	// envelope build; the upstream backend uses sessionId for trajectory
	// correlation but does not validate it for cryptographic continuity.
	if _, present := inner["sessionId"]; !present {
		inner["sessionId"] = newSessionID()
	}

	// Inject generationConfig defaults agy.exe always sends. Caller-provided
	// values win — we only fill gaps.
	applyAgyDefaultsToInnerRequest(inner, model)

	envelope := map[string]any{
		"requestId":   "checkpoint/" + uuid.NewString(),
		"model":       model,
		"userAgent":   "antigravity",
		"requestType": "checkpoint",
		"request":     inner,
	}
	if projectID != "" {
		envelope["project"] = projectID
	}
	return json.Marshal(envelope)
}

// newSessionID synthesizes a session correlator in the same shape real
// agy emits — a base-10 int64 (sometimes negative; sign-bit set by
// agy's hashing scheme). Per-envelope uniqueness is sufficient.
func newSessionID() string {
	// Take the low 63 bits of a fresh UUID as a positive int64, then flip
	// the sign half the time so the distribution matches real agy traffic
	// (we observed negative values like -3750763034362895579 in captures).
	u := uuid.New()
	hi := uint64(u[8])<<56 | uint64(u[9])<<48 | uint64(u[10])<<40 | uint64(u[11])<<32 |
		uint64(u[12])<<24 | uint64(u[13])<<16 | uint64(u[14])<<8 | uint64(u[15])
	signed := int64(hi)
	return fmt.Sprintf("%d", signed)
}

// applyAgyDefaultsToInnerRequest fills in generationConfig + toolConfig
// defaults that real agy.exe always sends. Values present in `inner` are
// preserved — we only patch gaps so callers can override per-request.
//
// Verified vs Frida capture (model=gemini-3.5-flash-low, "Medium" tier):
//
//	"toolConfig":      {"functionCallingConfig": {"mode": "NONE"}},
//	"generationConfig": {
//	  "maxOutputTokens": 16384,
//	  "thinkingConfig":  {"includeThoughts": true, "thinkingBudget": 4000}
//	}
//
// thinkingBudget scales with model tier per the fetchAvailableModels probe:
//
//	gemini-3-flash             → -1 (dynamic)
//	gemini-3.5-flash-extra-low → 1000   (Low)
//	gemini-3.5-flash-low       → 4000   (Medium)
//	gemini-3-flash-agent       → 10000  (High)
//	gemini-3.1-pro-low         → 1001
//	gemini-pro-agent           → 10001  (3.1 Pro High)
//	claude-*                   → 1024
//	gpt-oss-120b-medium        → 8192
func applyAgyDefaultsToInnerRequest(inner map[string]any, wireModel string) {
	if inner == nil {
		return
	}

	// toolConfig.functionCallingConfig.mode: agy sends "NONE" to let the
	// model decide if a tool call is needed. Caller can override to AUTO/ANY.
	if _, present := inner["toolConfig"]; !present {
		inner["toolConfig"] = map[string]any{
			"functionCallingConfig": map[string]any{
				"mode": "NONE",
			},
		}
	}

	// generationConfig defaults — fill in thinkingConfig per tier.
	gc, _ := inner["generationConfig"].(map[string]any)
	if gc == nil {
		gc = map[string]any{}
		inner["generationConfig"] = gc
	}
	if _, present := gc["maxOutputTokens"]; !present {
		gc["maxOutputTokens"] = 16384
	}
	tc, _ := gc["thinkingConfig"].(map[string]any)
	if tc == nil {
		tc = map[string]any{}
		gc["thinkingConfig"] = tc
	}
	if _, present := tc["includeThoughts"]; !present {
		tc["includeThoughts"] = true
	}
	if _, present := tc["thinkingBudget"]; !present {
		tc["thinkingBudget"] = thinkingBudgetForModel(wireModel)
	}
}

// thinkingBudgetForModel returns the budget agy uses for each known wire
// model. Values match the fetchAvailableModels probe of daily-cloudcode-pa
// (May 2026). Unknown models fall back to -1 (dynamic).
func thinkingBudgetForModel(wire string) int {
	switch strings.ToLower(strings.TrimSpace(wire)) {
	case "gemini-3-flash":
		return -1
	case "gemini-3.5-flash-extra-low":
		return 1000
	case "gemini-3.5-flash-low":
		return 4000
	case "gemini-3-flash-agent":
		return 10000
	case "gemini-3.1-pro-low":
		return 1001
	case "gemini-pro-agent":
		return 10001
	case "gemini-3.1-flash-lite", "gemini-3.1-flash-image", "gemini-3.1-flash-image-preview":
		return -1
	case "claude-sonnet-4-6", "claude-opus-4-6-thinking":
		return 1024
	case "gpt-oss-120b-medium":
		return 8192
	}
	return -1
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
	ctx context.Context,
	c *gin.Context,
	accountID int64,
	resp *http.Response,
	startTime time.Time,
	originalModel, wireModel string,
	toolReport toolPrepReport,
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
	var lastChunkPayload []byte // last unwrapped JSON payload, used for end-of-stream anomaly inspection
	// Stream-wide signals — must be accumulated across all chunks because the
	// final chunk almost always has empty `text` + a thoughtSignature + STOP.
	// Inspecting that chunk alone would (and did) raise a false-positive
	// `stop_without_content` even on perfectly-fine completions.
	streamSawText := false
	streamSawFunctionCall := false
	streamEmptyArgsFn := ""
	for {
		n, readErr := resp.Body.Read(chunkBuf)
		if n > 0 {
			if first {
				first = false
				firstTokenMs = int(time.Since(startTime).Milliseconds())
				result.FirstTokenMs = &firstTokenMs
			}
			buf = append(buf, chunkBuf[:n]...)
			// flush by-line so SSE framing stays intact.
			// Each SSE line carries the agymimic envelope `{response: {candidates,
			// usageMetadata, ...}}`. Standard Gemini SDKs (@google/genai etc.)
			// expect the candidates payload at the top level — strip the
			// `response` wrapper before forwarding so client parsers don't see
			// an empty `candidates` array and report 0 tokens / no content.
			for {
				idx := bytes.IndexByte(buf, '\n')
				if idx < 0 {
					break
				}
				line := buf[:idx+1]
				buf = buf[idx+1:]
				out := unwrapAgyResponseEnvelopeLine(line)
				// Back-translate call_mcp_tool function calls in this SSE
				// chunk so omp sees the original mcp__<server>_<tool>
				// function name + the inner Arguments object. No-op when
				// aggregator is off or no MCP tools were hidden.
				out = rewriteSSELineFunctionCalls(out, toolReport)

				// If we haven't sent ANY bytes yet AND this chunk carries
				// a version-rejection payload, kick the refresher and
				// return a clean failover error. After the first byte is
				// flushed we're committed (streaming SSE can't undo) —
				// just trigger refresh so the next request lands clean.
				if isVersionRejectionPayload(out) {
					slog.WarnContext(ctx, "native: version-rejection text in SSE chunk, forcing fingerprint refresh",
						slog.Int64("account_id", accountID),
						slog.Bool("any_bytes_sent", !first))
					fingerprint.ForceRefresh()
					if first {
						// Drain remaining body so the connection can be reused.
						_, _ = io.Copy(io.Discard, resp.Body)
						return nil, &UpstreamFailoverError{
							StatusCode:             http.StatusBadRequest,
							ResponseBody:           out,
							ResponseHeaders:        resp.Header,
							PassthroughVerbatim:    false,
							RetryableOnSameAccount: true,
						}
					}
				}

				if _, wErr := c.Writer.Write(out); wErr != nil {
					result.ClientDisconnect = true
					_, _ = io.Copy(io.Discard, resp.Body)
					return s.finalizeResult(result, startTime), nil
				}
				if flusher != nil {
					flusher.Flush()
				}
				if payload := extractDataPayload(out); len(payload) > 0 {
					lastChunkPayload = append(lastChunkPayload[:0], payload...)
					sawText, sawFn, emptyArgsFn := inspectStreamChunk(payload)
					if sawText {
						streamSawText = true
					}
					if sawFn {
						streamSawFunctionCall = true
					}
					if emptyArgsFn != "" && streamEmptyArgsFn == "" {
						streamEmptyArgsFn = emptyArgsFn
					}
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
					tail := unwrapAgyResponseEnvelopeLine(buf)
					_, _ = c.Writer.Write(tail)
					if flusher != nil {
						flusher.Flush()
					}
					if payload := extractDataPayload(tail); len(payload) > 0 {
						lastChunkPayload = append(lastChunkPayload[:0], payload...)
						sawText, sawFn, emptyArgsFn := inspectStreamChunk(payload)
						if sawText {
							streamSawText = true
						}
						if sawFn {
							streamSawFunctionCall = true
						}
						if emptyArgsFn != "" && streamEmptyArgsFn == "" {
							streamEmptyArgsFn = emptyArgsFn
						}
					}
				}
				break
			}
			return nil, readErr
		}
	}
	// Anomaly classification — stream-wide so we don't fire on benign final
	// chunks that only carry a thoughtSignature + finishReason=STOP.
	if streamEmptyArgsFn != "" {
		logNativeRequestAnomaly(ctx, accountID, originalModel, wireModel, true, lastChunkPayload,
			"empty_function_args", map[string]string{"function": streamEmptyArgsFn, "reason": "args missing/empty across stream"})
	} else if !streamSawText && !streamSawFunctionCall && len(lastChunkPayload) > 0 {
		logNativeRequestAnomaly(ctx, accountID, originalModel, wireModel, true, lastChunkPayload,
			"stop_without_content", map[string]string{"reason": "no text and no function call seen across stream"})
	}
	return s.finalizeResult(result, startTime), nil
}

// inspectStreamChunk parses a single SSE data payload and reports whether
// it carries text, a function call, and (if it carries a function call)
// whether the args are missing/empty. Returns ("",) when the payload is
// not valid JSON. The empty-args check fires on a per-chunk basis because
// once a function call is emitted streaming concludes immediately — there's
// no "later chunk" to redeem an args={} call.
func inspectStreamChunk(payload []byte) (sawText bool, sawFunctionCall bool, emptyArgsFn string) {
	type part struct {
		Text         *string                `json:"text,omitempty"`
		FunctionCall map[string]interface{} `json:"functionCall,omitempty"`
	}
	type candidate struct {
		Content struct {
			Parts []part `json:"parts"`
		} `json:"content"`
	}
	var env struct {
		Candidates []candidate `json:"candidates"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return false, false, ""
	}
	for _, c := range env.Candidates {
		for _, p := range c.Content.Parts {
			if p.Text != nil && *p.Text != "" {
				sawText = true
			}
			if p.FunctionCall != nil {
				sawFunctionCall = true
				name, _ := p.FunctionCall["name"].(string)
				args, hasArgs := p.FunctionCall["args"]
				if !hasArgs || args == nil {
					emptyArgsFn = name
					continue
				}
				if m, ok := args.(map[string]interface{}); ok && len(m) == 0 {
					emptyArgsFn = name
				}
			}
		}
	}
	return sawText, sawFunctionCall, emptyArgsFn
}

// extractDataPayload returns the JSON object embedded in an SSE `data:` line,
// or nil if the line is not a data event. The returned slice aliases the
// input — callers MUST copy it before holding past the next loop iteration.
func extractDataPayload(line []byte) []byte {
	trimmed := bytes.TrimSpace(line)
	const prefix = "data:"
	if !bytes.HasPrefix(trimmed, []byte(prefix)) {
		return nil
	}
	payload := bytes.TrimSpace(trimmed[len(prefix):])
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return nil
	}
	return payload
}

func (s *AntigravityNativeGatewayService) passNonStreamingGemini(
	ctx context.Context,
	c *gin.Context,
	accountID int64,
	resp *http.Response,
	startTime time.Time,
	originalModel, wireModel string,
	toolReport toolPrepReport,
) (*ForwardResult, error) {
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// Standard Gemini SDKs read `candidates` / `usageMetadata` at the top
	// level. The agymimic upstream wraps them inside a `response` field;
	// unwrap before forwarding so non-streaming clients also see the
	// canonical shape.
	out := unwrapAgyResponseEnvelopeBody(raw)
	// Back-translate call_mcp_tool function calls if the aggregator was
	// used for this request — keeps omp's tool dispatch transparent.
	out = rewriteAggregatedFunctionCalls(out, toolReport)

	// Some upstream errors arrive as 200 OK with an `error` field or with
	// the rejection text smuggled into candidates[].content.parts[].text.
	// If the body LOOKS LIKE a version-rejection, kick the refresher and
	// return a failover error so the gateway retries on the next account
	// (or on the next request once the manifest refresh lands).
	if isVersionRejectionPayload(out) {
		slog.WarnContext(ctx, "native: version-rejection text in 200 body, forcing fingerprint refresh",
			slog.Int64("account_id", accountID))
		fingerprint.ForceRefresh()
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusBadRequest,
			ResponseBody:           out,
			ResponseHeaders:        resp.Header,
			PassthroughVerbatim:    false,
			RetryableOnSameAccount: true,
		}
	}

	c.Header("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)
	_, _ = c.Writer.Write(out)

	// Non-stream is the natural place to inspect for semantic anomalies —
	// the full body is in hand. Streamed requests are checked at the SSE
	// loop's end (best-effort, on the final chunk only). 200 with empty
	// function-call args is the case that prompted this logging; see the
	// inspectGeminiResponseForAnomalies docstring for the full set.
	if anomaly, details := inspectGeminiResponseForAnomalies(out); anomaly != "" {
		logNativeRequestAnomaly(ctx, accountID, originalModel, wireModel, false, out, anomaly, details)
	}

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

// flushBufferedNativeResponse writes a buffered non-streaming Gemini
// response to the client. Used by the agy_list_tools loop after it has
// collected the final upstream response (all discovery roundtrips
// resolved). The body has already been unwrap-envelope'd + back-
// translated by the caller — we just need to flush it in the format
// the client requested.
//
// Streaming client (stream=true): emit one SSE event containing the
// full response, then close. Most major SDKs (@google/genai etc.)
// handle single-event streams correctly.
//
// Non-streaming client (stream=false): write the body as a normal JSON
// response.
func (s *AntigravityNativeGatewayService) flushBufferedNativeResponse(
	ctx context.Context,
	c *gin.Context,
	accountID int64,
	body []byte,
	startTime time.Time,
	originalModel, wireModel string,
	toolReport toolPrepReport,
	stream bool,
) (*ForwardResult, error) {
	// Unwrap agymimic's {response: {...}} envelope so client sees canonical Gemini shape.
	out := unwrapAgyResponseEnvelopeBody(body)

	// Same anomaly check + version-rejection guard as passNonStreamingGemini.
	if isVersionRejectionPayload(out) {
		slog.WarnContext(ctx, "native: version-rejection in agy_list_tools final response",
			slog.Int64("account_id", accountID))
		fingerprint.ForceRefresh()
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusBadRequest,
			ResponseBody:           out,
			ResponseHeaders:        http.Header{},
			PassthroughVerbatim:    false,
			RetryableOnSameAccount: true,
		}
	}

	if stream {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Writer.WriteHeader(http.StatusOK)
		_, _ = c.Writer.Write(agyListToolsSSEEvent(out))
		if flusher, ok := c.Writer.(http.Flusher); ok {
			flusher.Flush()
		}
	} else {
		c.Header("Content-Type", "application/json")
		c.Writer.WriteHeader(http.StatusOK)
		_, _ = c.Writer.Write(out)
	}

	if anomaly, details := inspectGeminiResponseForAnomalies(out); anomaly != "" {
		logNativeRequestAnomaly(ctx, accountID, originalModel, wireModel, stream, out, anomaly, details)
	}

	result := &ForwardResult{
		Model:         originalModel,
		UpstreamModel: wireModel,
	}
	if u := extractGeminiUsageFromResponse(body); u != nil {
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

// unwrapAgyResponseEnvelopeBody strips the outer `{"response": {...}}`
// wrapper agymimic / cloudcode-pa returns and emits the inner object as
// canonical Gemini API JSON. Returns the input verbatim when:
//   - it is not valid JSON
//   - the parsed object has no `response` field
//   - the `response` field is not an object
//
// The non-streaming wrapper preserves any extra top-level fields by
// returning the inner object directly (the spec only allows the standard
// candidate / usageMetadata / modelVersion / promptFeedback keys at the
// top of a generateContent response).
func unwrapAgyResponseEnvelopeBody(body []byte) []byte {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return body
	}
	var env struct {
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(trimmed, &env); err != nil || len(env.Response) == 0 {
		return body
	}
	// Sanity check: the unwrapped value should also be a JSON object.
	inner := bytes.TrimSpace(env.Response)
	if len(inner) == 0 || inner[0] != '{' {
		return body
	}
	return inner
}

// unwrapAgyResponseEnvelopeLine unwraps a single SSE line of the form
//
//	data: {"response": {...}}
//
// into
//
//	data: {...}
//
// preserving the `data: ` prefix and trailing newline(s). Non-data lines
// (comments, empty keep-alives) are returned verbatim. Lines whose JSON
// payload is not enveloped fall through to the body unwrapper, which
// returns them unchanged.
func unwrapAgyResponseEnvelopeLine(line []byte) []byte {
	const prefix = "data:"
	// Detach trailing CR/LF so we can reattach after rewriting.
	tail := line
	cr := 0
	for len(tail) > 0 && (tail[len(tail)-1] == '\n' || tail[len(tail)-1] == '\r') {
		cr++
		tail = tail[:len(tail)-1]
	}
	if !bytes.HasPrefix(bytes.TrimSpace(tail), []byte(prefix)) {
		return line
	}
	// Find the colon and unwrap the JSON payload.
	idx := bytes.IndexByte(tail, ':')
	if idx < 0 {
		return line
	}
	head := tail[:idx+1]
	payload := tail[idx+1:]
	unwrapped := unwrapAgyResponseEnvelopeBody(bytes.TrimSpace(payload))
	// Preserve the single space after `data:` that real Gemini APIs emit.
	out := make([]byte, 0, len(head)+1+len(unwrapped)+cr)
	out = append(out, head...)
	out = append(out, ' ')
	out = append(out, unwrapped...)
	for i := 0; i < cr; i++ {
		out = append(out, line[len(line)-cr+i])
	}
	return out
}

// ────────────────────────────────────────────────────────────────────────────
// Observability
// ────────────────────────────────────────────────────────────────────────────

// logNativeUpstreamError records a structured warn log for any non-2xx
// response from the daily cloudcode-pa endpoint. The full body and a
// curated header set are captured so admins can correlate per-account
// 429s, schema-related 400s, etc. The body is truncated to 4 KiB to keep
// the docker JSON log driver from blowing up on huge upstream payloads.
func logNativeUpstreamError(
	ctx context.Context,
	accountID int64,
	originalModel, wireModel, action string,
	stream bool,
	status int,
	headers http.Header,
	body []byte,
) {
	const maxBody = 4 * 1024
	preview := body
	truncated := false
	if len(preview) > maxBody {
		preview = preview[:maxBody]
		truncated = true
	}
	retryAfter := ""
	wwwAuth := ""
	if headers != nil {
		retryAfter = headers.Get("Retry-After")
		wwwAuth = headers.Get("WWW-Authenticate")
	}
	slog.WarnContext(ctx, "antigravity-native upstream error",
		slog.Int64("account_id", accountID),
		slog.String("model", originalModel),
		slog.String("wire_model", wireModel),
		slog.String("action", action),
		slog.Bool("stream", stream),
		slog.Int("status", status),
		slog.String("retry_after", retryAfter),
		slog.String("www_authenticate", wwwAuth),
		slog.Bool("body_truncated", truncated),
		slog.String("body", string(preview)),
	)
}

// logNativeRequestAnomaly records a warn log when an HTTP 200 response
// from cloudcode-pa carries semantically broken content — e.g. the model
// emitted a `functionCall` with empty `args`, a STOP without any text or
// function call, or a response with no candidates at all. These don't
// surface as errors (status is 200, body is valid JSON) but client SDKs
// throw schema-validation failures downstream. Logging them inline lets
// us correlate user reports with the exact model + prompt that caused
// the issue.
//
// Pass the inner Gemini response (post-unwrap from the agymimic envelope).
func logNativeRequestAnomaly(
	ctx context.Context,
	accountID int64,
	originalModel, wireModel string,
	stream bool,
	candidatesJSON []byte,
	anomaly string,
	details map[string]string,
) {
	attrs := []any{
		slog.Int64("account_id", accountID),
		slog.String("model", originalModel),
		slog.String("wire_model", wireModel),
		slog.Bool("stream", stream),
		slog.String("anomaly", anomaly),
	}
	for k, v := range details {
		attrs = append(attrs, slog.String(k, v))
	}
	if len(candidatesJSON) > 0 {
		const maxBody = 2 * 1024
		preview := candidatesJSON
		if len(preview) > maxBody {
			preview = preview[:maxBody]
		}
		attrs = append(attrs, slog.String("candidates_preview", string(preview)))
	}
	slog.WarnContext(ctx, "antigravity-native response anomaly", attrs...)
}

// inspectGeminiResponseForAnomalies parses an upstream response body
// (already unwrapped from the agymimic envelope) and returns the first
// anomaly it finds, or "" when the response looks clean. Anomalies:
//
//   - "no_candidates"         — top-level `candidates` is missing/empty
//   - "empty_function_args"   — a `functionCall` part has `args: {}` /
//     missing args; matches the bug pattern that triggered this logging
//   - "stop_without_content"  — finishReason=STOP but no text and no
//     function call (the symptom omp was hitting before the response
//     envelope fix; kept as a guard against regressions)
//
// Returns "" + empty detail map when the response is well-formed.
func inspectGeminiResponseForAnomalies(body []byte) (string, map[string]string) {
	if len(body) == 0 {
		return "", nil
	}
	type part struct {
		Text         *string                `json:"text,omitempty"`
		FunctionCall map[string]interface{} `json:"functionCall,omitempty"`
	}
	type candidate struct {
		Content struct {
			Parts []part `json:"parts"`
		} `json:"content"`
		FinishReason string `json:"finishReason"`
	}
	var env struct {
		Candidates []candidate `json:"candidates"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return "", nil
	}
	if len(env.Candidates) == 0 {
		return "no_candidates", nil
	}
	c := env.Candidates[0]
	if len(c.Content.Parts) == 0 {
		return "no_candidates", nil
	}
	sawText := false
	for _, p := range c.Content.Parts {
		if p.Text != nil && *p.Text != "" {
			sawText = true
		}
		if p.FunctionCall != nil {
			name, _ := p.FunctionCall["name"].(string)
			args, hasArgs := p.FunctionCall["args"]
			if !hasArgs || args == nil {
				return "empty_function_args", map[string]string{"function": name, "reason": "args missing"}
			}
			if m, ok := args.(map[string]interface{}); ok && len(m) == 0 {
				return "empty_function_args", map[string]string{"function": name, "reason": "args is empty object"}
			}
			return "", nil // function call with args is fine
		}
	}
	if !sawText && (c.FinishReason == "STOP" || c.FinishReason == "stop") {
		return "stop_without_content", map[string]string{"finish_reason": c.FinishReason}
	}
	return "", nil
}

// ────────────────────────────────────────────────────────────────────────────
// Quota fetch — every request goes through agymimic's api.Client so the
// wire fingerprint, identity, and token refresh match real agy.exe. Mirrors
// the legacy *antigravity.Client.FetchAvailableModels + LoadCodeAssist pair
// but uses agynative.NewAPIClient under the hood.
// ────────────────────────────────────────────────────────────────────────────

// FetchQuota performs the two cloudcode-pa roundtrips a dashboard usage
// refresh needs:
//   - /v1internal:fetchAvailableModels  → model list + per-model quota
//   - /v1internal:loadCodeAssist        → tier + AI Credits balance
//
// Both calls are issued via the cached agymimic api.Client (so the bearer
// token is auto-refreshed by agymimic before each request). On a successful
// refresh inside ensureFresh() the new tokens are persisted back to the DB
// so subsequent dashboard polls don't trigger another redundant refresh.
//
// Returns parsed legacy types so the downstream UsageInfo builder in
// AntigravityQuotaFetcher can stay identical between legacy and native.
func (s *AntigravityNativeGatewayService) FetchQuota(
	ctx context.Context,
	account *Account,
) (*antigravity.FetchAvailableModelsResponse, *antigravity.LoadCodeAssistResponse, error) {
	if account == nil {
		return nil, nil, fmt.Errorf("native quota: nil account")
	}
	if account.Platform != domain.PlatformAntigravityNative {
		return nil, nil, fmt.Errorf("native quota: wrong platform %q", account.Platform)
	}

	cli, err := s.getClient(ctx, account)
	if err != nil {
		return nil, nil, err
	}

	// 1) fetchAvailableModels — body is literally {} per agy.exe wire capture.
	//    cli.RawRequest calls ensureFresh() internally, refreshing the
	//    access_token via agyauth.RefreshWithClient if it's within 30 s of
	//    expiry, then attaches the agy-style headers.
	modelsResp, err := s.nativeRawJSON(ctx, cli, "/v1internal:fetchAvailableModels", []byte("{}"))
	if err != nil {
		return nil, nil, err
	}
	var models antigravity.FetchAvailableModelsResponse
	if err := json.Unmarshal(modelsResp, &models); err != nil {
		return nil, nil, fmt.Errorf("native quota: parse fetchAvailableModels: %w", err)
	}

	// 2) loadCodeAssist — body matches agy.exe's wire (just ideType plus an
	//    optional duetProject hint). Failure here is non-fatal: tier display
	//    and AI Credits balance go missing but the model list still renders.
	loadBody := map[string]any{
		"metadata": map[string]any{"ideType": "ANTIGRAVITY"},
	}
	if pid := strings.TrimSpace(cli.ProjectID()); pid != "" {
		loadBody["metadata"].(map[string]any)["duetProject"] = pid
	}
	loadBodyBytes, _ := json.Marshal(loadBody)
	loadRaw, err := s.nativeRawJSON(ctx, cli, "/v1internal:loadCodeAssist", loadBodyBytes)
	if err != nil {
		slog.WarnContext(ctx, "native quota: loadCodeAssist failed (non-fatal)",
			"account_id", account.ID, "error", err)
		s.maybePersistRefreshedTokens(ctx, account, cli)
		return &models, nil, nil
	}
	var load antigravity.LoadCodeAssistResponse
	if err := json.Unmarshal(loadRaw, &load); err != nil {
		slog.WarnContext(ctx, "native quota: parse loadCodeAssist (non-fatal)",
			"account_id", account.ID, "error", err)
		s.maybePersistRefreshedTokens(ctx, account, cli)
		return &models, nil, nil
	}

	s.maybePersistRefreshedTokens(ctx, account, cli)
	return &models, &load, nil
}

// nativeRawJSON wraps cli.RawRequest with the bookkeeping every native
// cloudcode-pa call needs:
//   - drain and close the response body once
//   - decode HTTP 403 into *antigravity.ForbiddenError so the upstream
//     forbidden-classification path in AntigravityQuotaFetcher fires
//   - turn non-200 into the same error shape the legacy client uses
//     ("fetchAvailableModels 失败 (HTTP <n>): <body>") so existing
//     buildAntigravityDegradedUsage heuristics still classify it
func (s *AntigravityNativeGatewayService) nativeRawJSON(
	ctx context.Context,
	cli *api.Client,
	path string,
	body []byte,
) ([]byte, error) {
	resp, err := cli.RawRequest(ctx, path, body)
	if err != nil {
		return nil, fmt.Errorf("native quota: %s: %w", path, err)
	}
	defer resp.Body.Close()

	// agymimic pins `Accept-Encoding: gzip` on every cloudcode-pa request to
	// match real agy.exe's wire fingerprint. When the client sets that
	// header explicitly, net/http intentionally does NOT auto-decode — the
	// caller is expected to handle Content-Encoding itself.
	//
	// Without this, the response body still has the gzip magic bytes
	// (\x1f\x8b…) and json.Unmarshal blows up with
	// "invalid character '\x1f' looking for beginning of value".
	var bodyReader io.Reader = resp.Body
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gr, gzErr := gzip.NewReader(resp.Body)
		if gzErr != nil {
			return nil, fmt.Errorf("native quota: gzip reader %s: %w", path, gzErr)
		}
		defer func() { _ = gr.Close() }()
		bodyReader = gr
	}
	raw, readErr := io.ReadAll(bodyReader)
	if readErr != nil {
		return nil, fmt.Errorf("native quota: read %s: %w", path, readErr)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return raw, nil
	case http.StatusForbidden:
		return nil, &antigravity.ForbiddenError{StatusCode: resp.StatusCode, Body: string(raw)}
	default:
		// Keep error string compatible with buildAntigravityDegradedUsage
		// (which checks for "HTTP 401" / "HTTP 429" substrings).
		return nil, fmt.Errorf("fetchAvailableModels 失败 (HTTP %d): %s", resp.StatusCode, string(raw))
	}
}

// maybePersistRefreshedTokens compares the agymimic client's current in-memory
// tokens against the account's stored credentials. If agymimic's
// ensureFresh() refreshed the access_token during this FetchQuota call, the
// new pair is written back to the DB so the next dashboard poll (or the
// background TokenRefreshService) sees the fresh values and doesn't trigger
// a redundant refresh.
//
// Best-effort: a persistence failure is logged but does not affect the
// quota result we return to the caller.
func (s *AntigravityNativeGatewayService) maybePersistRefreshedTokens(
	ctx context.Context,
	account *Account,
	cli *api.Client,
) {
	if cli == nil || account == nil || s.accountRepo == nil {
		return
	}
	fresh := cli.Tokens()
	if fresh.AccessToken == "" {
		return
	}
	stored, _ := account.Credentials["access_token"].(string)
	if fresh.AccessToken == stored {
		return // no refresh happened
	}
	updated := map[string]any{
		"access_token":    fresh.AccessToken,
		"refresh_token":   fresh.RefreshToken,
		"token_type":      "Bearer",
		"email":           fresh.Email,
		"project_id":      fresh.ProjectID,
		"tier_id":         fresh.TierID,
		"installation_id": fresh.InstallationID,
		"instance_label":  fresh.InstanceLabel,
		"connection_id":   fresh.ConnectionID,
	}
	if !fresh.ExpiresAt.IsZero() {
		updated["expires_at"] = fresh.ExpiresAt.Unix()
	}
	merged := MergeCredentials(account.Credentials, updated)
	if err := persistAccountCredentials(ctx, s.accountRepo, account, merged); err != nil {
		slog.WarnContext(ctx, "native quota: persist refreshed tokens failed",
			"account_id", account.ID, "error", err)
		return
	}
	// (cached client still has fresh tokens in memory; persistence above is
	// purely for the next process start / dashboard poll.)
}

// isAntigravityVersionRejection identifies an upstream response that the
// daily-cloudcode-pa endpoint emits when the User-Agent's Antigravity
// version is below the current accepted minimum. Body is the verbatim
// upstream response (truncated by the caller in some paths — we only
// scan the first 4 KiB).
//
// Patterns observed in the wild:
//
//	HTTP 400 FAILED_PRECONDITION
//	  "This version of Antigravity is no longer supported. Please update..."
//	HTTP 426 Upgrade Required
//	  "Antigravity version <X.Y.Z> is no longer supported"
//
// We match case-insensitively on the canonical phrase + a small set of
// equivalent variants Google has used historically. Returns true when
// any pattern is detected.
func isAntigravityVersionRejection(status int, body []byte) bool {
	// Status filter — version errors land on 400 (FAILED_PRECONDITION)
	// or 426 (Upgrade Required). 4xx with the keyword in body, anything
	// other than 5xx, is fair game.
	if status < 400 || status >= 500 {
		return false
	}
	if len(body) == 0 {
		return false
	}
	scan := body
	if len(scan) > 4096 {
		scan = scan[:4096]
	}
	low := bytes.ToLower(scan)
	keywords := [][]byte{
		[]byte("no longer supported"),
		[]byte("please update"),
		[]byte("client version is too old"),
		[]byte("upgrade required"),
		[]byte("antigravity version"),
		[]byte("version of antigravity"),
	}
	hits := 0
	for _, k := range keywords {
		if bytes.Contains(low, k) {
			hits++
			if hits >= 1 && bytes.Contains(low, []byte("antigravity")) {
				return true
			}
		}
	}
	return false
}

// isVersionRejectionPayload scans a 200 OK response body for telltale
// signs that upstream actually rejected the request because the client
// version is too old. cloudcode-pa occasionally returns 200 with the
// error payload either:
//
//   - at top level: `{"error":{"message":"This version of Antigravity is no longer supported"}}`
//   - inside candidates[].content.parts[].text smuggled as model output
//
// We treat either pattern as a hard failure and trigger force-refresh +
// failover so the user doesn't see the rejection text as the model's reply.
func isVersionRejectionPayload(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	scan := body
	if len(scan) > 16*1024 {
		scan = scan[:16*1024]
	}
	low := bytes.ToLower(scan)
	// Must mention antigravity AND a "outdated client" phrase.
	if !bytes.Contains(low, []byte("antigravity")) {
		return false
	}
	patterns := [][]byte{
		[]byte("no longer supported"),
		[]byte("please update"),
		[]byte("client version is too old"),
		[]byte("upgrade required"),
	}
	for _, p := range patterns {
		if bytes.Contains(low, p) {
			return true
		}
	}
	return false
}

// FetchUpstreamModels returns the list of model IDs the upstream
// (cloudcode-pa) advertises for this native account. Wraps agymimic's
// api.Client.FetchAvailableModels which talks the v1internal:
// fetchAvailableModels RPC. Reuses the per-account client cache built
// during streamGenerateContent — so first sync after a native account
// is created may pay the cache-build cost (~200 ms), subsequent syncs
// are warm.
//
// Used by the admin "Sync upstream supported models" button in the
// account edit modal. The legacy antigravity path (LegacyOAuthService)
// can't refresh native tokens — this method is the native-specific
// equivalent.
func (s *AntigravityNativeGatewayService) FetchUpstreamModels(ctx context.Context, account *Account) ([]string, error) {
	if account == nil {
		return nil, fmt.Errorf("native fetch models: nil account")
	}
	if account.Platform != domain.PlatformAntigravityNative {
		return nil, fmt.Errorf("native fetch models: wrong platform %q", account.Platform)
	}
	cli, err := s.getClient(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("native fetch models: client: %w", err)
	}
	models, err := cli.FetchAvailableModels(ctx)
	if err != nil {
		return nil, fmt.Errorf("native fetch models: upstream: %w", err)
	}
	out := make([]string, 0, len(models))
	for _, m := range models {
		id := strings.TrimSpace(m.ID)
		if id != "" {
			out = append(out, id)
		}
	}
	return out, nil
}

// ────────────────────────────────────────────────────────────────────────────
// TestConnection — admin "Test Account Connection" probe for native accounts.
// Issues a single non-streaming /v1internal:generateContent through agymimic
// so the dialog can show actual model output instead of a black box.
// ────────────────────────────────────────────────────────────────────────────

// TestConnection sends a minimal probe (single "." user turn + identity-patch
// systemInstruction, maxOutputTokens=16) and returns the first candidate's
// text. Routes entirely through agymimic so the wire fingerprint matches
// agy.exe and the access_token is auto-refreshed.
//
// Returns a TestConnectionResult with .Text == the model output and
// .MappedModel == the wire model name the request actually used (after
// public→wire translation).
func (s *AntigravityNativeGatewayService) TestConnection(
	ctx context.Context,
	account *Account,
	modelID string,
	prompt string,
) (*TestConnectionResult, error) {
	if account == nil {
		return nil, fmt.Errorf("native test: nil account")
	}
	if account.Platform != domain.PlatformAntigravityNative {
		return nil, fmt.Errorf("native test: wrong platform %q", account.Platform)
	}

	cli, err := s.getClient(ctx, account)
	if err != nil {
		return nil, err
	}

	// Use the caller's prompt when provided so the dialog's "Prompt: 'hi'"
	// label matches what the model actually sees. Fall back to "ping" if
	// the UI sent an empty string.
	userPrompt := strings.TrimSpace(prompt)
	if userPrompt == "" {
		userPrompt = "ping"
	}
	probeReq := types.GenerateInner{
		Contents: []types.Content{
			{
				Role:  "user",
				Parts: []types.Part{{Text: userPrompt}},
			},
		},
		SystemInstruction: &types.SystemInstruction{
			Parts: []types.Part{{Text: antigravity.GetDefaultIdentityPatch()}},
		},
		// Enough headroom for a short reply ("Hello!" etc) but still cheap.
		// maxOutputTokens=16 with "." as the prompt frequently returned an
		// empty candidate (the model couldn't fit anything past the
		// thoughtSignature prelude).
		GenerationConfig: &types.GenerationConfig{
			MaxOutputTokens: 256,
		},
	}

	wireModel := antigravity.AntigravityWireModel(modelID)
	if wireModel == "" {
		wireModel = modelID
	}

	resp, err := cli.Generate(ctx, wireModel, probeReq)
	if err != nil {
		return nil, fmt.Errorf("native test: generate: %w", err)
	}
	s.maybePersistRefreshedTokens(ctx, account, cli)

	text := extractTextFromAgyResponse(resp)
	// Log the response shape diagnostics. If text is empty we still want
	// to know whether the upstream returned candidates / parts / a finish
	// reason that explains it (MAX_TOKENS, SAFETY, etc).
	slog.InfoContext(ctx, "native test connection probe",
		"account_id", account.ID,
		"public_model", modelID,
		"wire_model", wireModel,
		"text_len", len(text),
		"candidates", responseCandidateCount(resp),
		"finish_reason", responseFirstFinishReason(resp),
		"parts_summary", responsePartsSummary(resp),
	)
	return &TestConnectionResult{
		Text:        text,
		MappedModel: wireModel,
	}, nil
}

// extractTextFromAgyResponse returns the concatenated text of every Part
// in the first candidate. Thought parts are included only when no other
// text is found, so a candidate that contains nothing but a thought
// signature still surfaces something readable rather than an empty box.
func extractTextFromAgyResponse(resp *types.Response) string {
	if resp == nil || len(resp.Response.Candidates) == 0 {
		return ""
	}
	parts := resp.Response.Candidates[0].Content.Parts
	var realText strings.Builder
	var thoughtText strings.Builder
	for _, p := range parts {
		if p.Text == "" {
			continue
		}
		if p.Thought {
			thoughtText.WriteString(p.Text)
			continue
		}
		realText.WriteString(p.Text)
	}
	if realText.Len() > 0 {
		return realText.String()
	}
	return thoughtText.String()
}

// responseCandidateCount returns len(candidates) for log diagnostics.
func responseCandidateCount(resp *types.Response) int {
	if resp == nil {
		return 0
	}
	return len(resp.Response.Candidates)
}

// responseFirstFinishReason returns Candidates[0].FinishReason ("" if
// no candidates). Surfaces MAX_TOKENS / SAFETY / OTHER for diagnostics.
func responseFirstFinishReason(resp *types.Response) string {
	if resp == nil || len(resp.Response.Candidates) == 0 {
		return ""
	}
	return resp.Response.Candidates[0].FinishReason
}

// responsePartsSummary returns a compact "n=text,thought,fn,inline,empty"
// breakdown of Candidates[0].Content.Parts for log diagnostics.
func responsePartsSummary(resp *types.Response) string {
	if resp == nil || len(resp.Response.Candidates) == 0 {
		return "no_candidates"
	}
	var text, thought, fn, inline, empty int
	for _, p := range resp.Response.Candidates[0].Content.Parts {
		switch {
		case p.Thought && p.Text != "":
			thought++
		case p.Text != "":
			text++
		case p.FunctionCall != nil:
			fn++
		case p.InlineData != nil:
			inline++
		default:
			empty++
		}
	}
	return fmt.Sprintf("text=%d thought=%d fn=%d inline=%d empty=%d",
		text, thought, fn, inline, empty)
}
