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
	"crypto/sha256"
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
	"github.com/Wei-Shaw/sub2api/internal/pkg/diagnosticcapture"
	"github.com/koval/agymimic/api"
	"github.com/koval/agymimic/fingerprint"
	"github.com/koval/agymimic/metrics"
	"github.com/koval/agymimic/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
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
	chatHistoryLog *ChatHistoryLogService

	// usageCache is the shared antigravity USAGE WINDOWS cache
	// (populated by AccountUsageService.getAntigravityUsage every
	// dashboard render). Native gateway consults it BEFORE hitting
	// the upstream to proactively skip accounts whose requested-model
	// family is already at 100 % utilization. On real 429s, native
	// updates the same cache so the dashboard + subsequent selections
	// see the exhaustion — one source of truth, no parallel field.
	usageCache *UsageCache

	// Per-account agymimic Client cache — keyed by account ID. Recreated on
	// proxy/credential change (handled by callers via Invalidate).
	clientCacheMu sync.RWMutex
	clientCache   map[int64]*nativeCacheEntry

	// Per-account Unleash metrics Client cache — one organic-traffic mimic
	// loop per native credential. Populated lazily on first use; lifecycle
	// driven by Stop() at shutdown and Invalidate() on credential change.
	metricsCacheMu sync.Mutex
	metricsCache   map[int64]*nativeMetricsEntry

	usageCacheMu sync.Mutex
}

type nativeCacheEntry struct {
	client                *api.Client
	proxyURL              string // empty = no proxy
	credentialFingerprint [sha256.Size]byte
	updatedAt             time.Time
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
	chatHistoryLog *ChatHistoryLogService,
	usageCache *UsageCache,
) *AntigravityNativeGatewayService {
	return &AntigravityNativeGatewayService{
		accountRepo:    accountRepo,
		proxyRepo:      proxyRepo,
		oauthService:   oauthService,
		settingService: settingService,
		chatHistoryLog: chatHistoryLog,
		usageCache:     usageCache,
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

// resolveMcpDiscoveryMode returns the GLOBAL MCP discovery mode for
// this request. Per the project decision, discovery mode is global-only
// (no per-account override). Resolution: cached setting → default "both".
func (s *AntigravityNativeGatewayService) resolveMcpDiscoveryMode(ctx context.Context) string {
	if s.settingService == nil {
		return McpDiscoveryModeBoth
	}
	return s.settingService.GetAntigravityNativeMcpDiscoveryMode(ctx)
}

// resolveToolCallMode returns the GLOBAL tool-call mode for this
// request ("single_name" default / "agy_mimic"). Global-only setting.
// Resolution: cached setting → default "single_name".
func (s *AntigravityNativeGatewayService) resolveToolCallMode(ctx context.Context) string {
	if s.settingService == nil {
		return ToolCallModeSingleName
	}
	return s.settingService.GetAntigravityNativeToolCallMode(ctx)
}

// modeDeclaresListTool reports whether the given discovery mode causes
// agy_list_tools to be declared upstream + loop-detected.
func modeDeclaresListTool(mode string) bool {
	return mode == McpDiscoveryModeListTool || mode == McpDiscoveryModeBoth
}

// modeInjectsCatalog reports whether the given discovery mode causes a
// full MCP catalog to be injected into systemInstruction.
func modeInjectsCatalog(mode string) bool {
	return mode == McpDiscoveryModePrompt || mode == McpDiscoveryModeBoth
}

// resolveMcpAggregatorName returns the effective MCP aggregator function
// name for this request. Resolution order:
//
//  1. Per-account credential `mcp_aggregator_name` (if set + valid)
//  2. Global setting SettingKeyAntigravityNativeMcpAggregatorName
//     (cached, 60s TTL, validated)
//  3. Built-in default "call_mcp_tool" (agy parity)
//
// Invalid per-account values fall through to global; invalid global
// values fall through to default. This keeps the gateway robust against
// typos at either layer.
func (s *AntigravityNativeGatewayService) resolveMcpAggregatorName(ctx context.Context, account *Account) string {
	// Per-account override has highest priority.
	if account != nil {
		raw, _ := account.Credentials["mcp_aggregator_name"].(string)
		raw = strings.TrimSpace(raw)
		if raw != "" && isValidMcpAggregatorName(raw) {
			return raw
		}
	}
	// Global default via cached setting.
	if s.settingService != nil {
		if name := s.settingService.GetAntigravityNativeMcpAggregatorName(ctx); name != "" {
			return name
		}
	}
	return defaultMcpAggregatorName
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

func nativeCredentialFingerprint(credentials map[string]any) ([sha256.Size]byte, error) {
	raw, err := json.Marshal(credentials)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(raw), nil
}

// getClient returns a cached or freshly-built agymimic Client for an account.
// Rebuilds if the proxy URL or credentials changed since last build.
func (s *AntigravityNativeGatewayService) getClient(ctx context.Context, account *Account) (*api.Client, error) {
	if account == nil {
		return nil, fmt.Errorf("native: nil account")
	}
	proxyURL := s.resolveProxyURL(ctx, account)
	credentialFingerprint, err := nativeCredentialFingerprint(account.Credentials)
	if err != nil {
		return nil, fmt.Errorf("native: fingerprint credentials: %w", err)
	}

	s.clientCacheMu.RLock()
	entry, ok := s.clientCache[account.ID]
	s.clientCacheMu.RUnlock()
	if ok && entry.proxyURL == proxyURL && entry.credentialFingerprint == credentialFingerprint {
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
		client:                cli,
		proxyURL:              proxyURL,
		credentialFingerprint: credentialFingerprint,
		updatedAt:             time.Now(),
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

	// Proactive re-auth check — skip accounts the dashboard's USAGE
	// WINDOWS already flags as "Re-auth Required" (needs_reauth
	// populated by the periodic quota fetch on HTTP 401 / invalid_grant).
	// pauseAccountForReauth persists account.Status='error' so the
	// selector's IsSchedulable() skips this account on subsequent
	// SelectAccountForModel calls. Failover on this request rotates
	// via the UpstreamFailoverError{401} below.
	if s.nativeIsReauthRequired(account.ID) {
		s.pauseAccountForReauth(ctx, account, "USAGE WINDOWS snapshot flagged needs_reauth", nil)
		body := []byte(`{"error":{"code":401,"message":"Antigravity native account needs re-authentication.","status":"UNAUTHENTICATED"}}`)
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusUnauthorized,
			ResponseBody:           body,
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: false,
		}
	}

	// Proactive USAGE WINDOWS check — read the same in-memory cache
	// the dashboard renders. If ANY family-mate of the requested
	// model is at 100 % with reset time still in the future, DO NOT
	// call upstream: mark the account rate-limited (SetRateLimited
	// touches account.RateLimitedAt so the next SelectAccountForModel
	// naturally skips this account), then return
	// UpstreamFailoverError{429} so the current-request failover loop
	// rotates to a healthy account.
	//
	// The 429 body is Google's canonical RESOURCE_EXHAUSTED shape —
	// PassthroughVerbatim=false + the wrapper below only surfaces if
	// the entire failover loop is exhausted (all accounts rate
	// limited); in that terminal case the loop's per-platform error
	// mapping produces the client-facing text, not this body.
	if s.nativeIsFamilyExhausted(account.ID, originalModel) {
		resetAt := s.nativeFamilyResetForModel(account.ID, originalModel)
		if !resetAt.IsZero() && s.accountRepo != nil {
			if err := s.accountRepo.SetRateLimited(ctx, account.ID, resetAt); err != nil {
				slog.WarnContext(ctx, "native: SetRateLimited failed",
					slog.Int64("account_id", account.ID),
					slog.String("error", err.Error()))
			} else {
				slog.InfoContext(ctx, "native: proactive family exhaustion → SetRateLimited",
					slog.Int64("account_id", account.ID),
					slog.String("model", originalModel),
					slog.String("family", nativeFamilyForModel(originalModel)),
					slog.Time("reset_at", resetAt))
			}
		}
		body := []byte(`{"error":{"code":429,"message":"Individual quota reached. Please upgrade your subscription to increase your limits.","status":"RESOURCE_EXHAUSTED"}}`)
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusTooManyRequests,
			ResponseBody:           body,
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: false,
		}
	}

	cli, err := s.getClient(ctx, account)
	if err != nil {
		// Pre-2026-06-18: returned the raw error here. The gateway
		// handler's failover loop only engages on *UpstreamFailoverError,
		// so a raw error caused the handler's "else" branch (no response
		// written) → gin returned 200 + Content-Length: 0 to the client.
		// Hindsight's google.genai SDK then materialised this as an
		// all-empty GenerateContentResponse, indistinguishable from a
		// safety block. Wrap as failover so either (a) we hop to another
		// account in the same group, or (b) the exhaustion path writes
		// a real error body. classifyNativeUpstreamErr decides the
		// shape — OAuth refresh failures get 401 + verbatim body so
		// admins see "invalid_grant"; everything else gets a 502.
		uErr := classifyNativeUpstreamErr(err, "getClient")
		if uErr.StatusCode == http.StatusUnauthorized {
			// invalid_grant → OAuth token dead. Persist so subsequent
			// selections skip this account until an operator re-runs
			// the OAuth flow.
			s.pauseAccountForReauth(ctx, account, err.Error(), uErr.ResponseBody)
		}
		return nil, uErr
	}

	// Best-effort: ensure the per-account Unleash organic-traffic mimic
	// loop is running. Idempotent — subsequent calls are no-ops.
	s.ensureMetricsLoop(ctx, account, s.resolveProxyURL(ctx, account))

	// Wire-level model name. ResolveWireFromBody honors slider thinkingLevel
	// for virtual suffixless Gemini aliases, while exact suffixed IDs stay
	// pinned to their requested tier.
	wireModel := antigravity.ResolveWireFromBody(originalModel, body)
	if wireModel == "" {
		wireModel = originalModel
	}
	if antigravity.IsNumericBudgetVirtualAlias(originalModel) {
		// The public base name is virtual. Convert its UI-level directive to
		// the exact numeric tier shape observed from real AGY before any
		// preprocessing or v1internal wrapping.
		body = antigravity.NormalizeNumericBudgetTierBody(body, wireModel)
	}

	// Run the tool-list preprocessing pipeline (schema normalize +
	// optional call_mcp_tool aggregator + agy_list_tools decl injection
	// + MCP catalog injection). Behavior is driven by the global
	// discovery mode setting:
	//
	//   "prompt"    — full catalog in systemInstruction, NO agy_list_tools decl
	//   "list_tool" — minimal hint catalog, agy_list_tools decl present
	//   "both"      — full catalog + agy_list_tools decl (recommended)
	//
	// The agy_list_tools decl is added upfront (NOT inside the loop) so
	// the model can see it on the very first turn and choose to call it
	// for discovery — fixing the chicken-and-egg where prior versions
	// only declared it inside an already-running loop.
	// Tool AGGREGATION/INJECTION is Gemini-specific behavior: the MCP
	// catalog text, the call_mcp_tool aggregator declaration and the
	// agy_list_tools discovery tool are all sub2api inventions layered on
	// top of Gemini requests. Non-Gemini families served by the same
	// cloudcode-pa endpoint (Claude 4.6, GPT-OSS) must NOT receive them:
	// real agy.exe sends those models a plain request whose only tools are
	// the caller's own declarations. Disabling the aggregator here keeps
	// SDK-shape normalization and JSON-Schema conversion (both required
	// for upstream acceptance) while skipping every injection branch,
	// which is gated on useAggregator.
	useAggregator := accountToolAggregatorEnabled(account) && !nativeSkipsToolInjection(wireModel)
	aggregatorName := s.resolveMcpAggregatorName(ctx, account)
	discoveryMode := s.resolveMcpDiscoveryMode(ctx)
	toolCallMode := s.resolveToolCallMode(ctx)
	body, toolReport, err := preprocessNativeBody(body, useAggregator, aggregatorName, discoveryMode, toolCallMode)
	if err != nil {
		return nil, fmt.Errorf("native gemini: tool preprocess: %w", err)
	}

	// One-line per-request summary so any later failure can be
	// correlated by account_id + model. Keep this at INFO so it lands
	// in docker logs sub2api without bumping global log level.
	slog.InfoContext(ctx, "native: request preprocessed",
		slog.Int64("account_id", account.ID),
		slog.String("model", originalModel),
		slog.String("wire_model", wireModel),
		slog.Bool("stream", stream),
		slog.String("tool_call_mode", toolCallMode),
		slog.String("discovery_mode", discoveryMode),
		slog.String("aggregator_name", aggregatorName),
		slog.Bool("aggregator_on", toolReport.AggregatorOn),
		slog.Int("mcp_tools_count", len(toolReport.McpTools)),
		slog.Int("builtin_tools_count", len(toolReport.BuiltinTools)),
		slog.Int("schemas_normalized", toolReport.Normalized),
		// Sample of mcp tool names actually hidden behind the
		// aggregator (first 10). Lets `docker logs sub2api | grep
		// 'native: request preprocessed'` answer "WHICH mcp tools
		// got collapsed?" without re-running the request.
		slog.Any("mcp_names_sample", sampleMcpHandleNames(toolReport.McpTools, 10)),
		slog.Any("builtin_names_sample", sampleStrings(toolReport.BuiltinTools, 10)))

	// agy_list_tools transparent discovery loop. Fires ONLY in agy_mimic
	// mode where mcp__* tools are stripped from the upstream declarations
	// and the only way the model can reach an MCP server is through the
	// call_mcp_tool aggregator. In that world the discovery roundtrip
	// (catalog-as-functionResponse) is genuinely useful.
	//
	// In single_name mode the model sees every mcp__<server>_<tool> tool
	// directly in the declarations — discovery is redundant. Running the
	// loop anyway would force every request through a buffered non-
	// streaming upstream call (the loop uses /v1internal:generateContent)
	// + a single-event SSE flush, killing token-by-token streaming for
	// the client. We saw this break omp's "thinking stream" UX even on
	// requests where the model never called agy_list_tools.
	//
	// Conditions to fire: agy_mimic mode AND discovery_mode declares the
	// list-tool AND aggregator is on AND at least one mcp__* tool was
	// hidden behind the aggregator. The decl itself is already present
	// in the request body from preprocessing; the loop just intercepts
	// the model's call, synthesizes a functionResponse, and re-issues.
	if toolCallMode == ToolCallModeAgyMimic && modeDeclaresListTool(discoveryMode) && toolReport.AggregatorOn && len(toolReport.McpTools) > 0 {
		startTime := time.Now()
		finalResp, iters, loopErr := s.resolveAgyListToolsLoop(ctx, cli, wireModel, body, toolReport)
		if loopErr == nil {
			slog.InfoContext(ctx, "native: agy_list_tools loop completed",
				slog.Int64("account_id", account.ID),
				slog.String("mode", discoveryMode),
				slog.Int("iterations", iters))
			// Defense-in-depth: strip any lingering agy_list_tools call
			// from the final response before back-translation. The loop
			// already handles this in normal flow, but a budget-exhausted
			// final iteration could still contain it if the model ignored
			// the budget-exhausted hint.
			finalResp = stripAgyListToolsFromResponse(finalResp)
			finalResp = rewriteAggregatedFunctionCalls(finalResp, toolReport)
			return s.flushBufferedNativeResponse(ctx, c, account, body, finalResp, startTime, originalModel, wireModel, toolReport, stream, iters)
		}
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
		// Same rationale as the getClient wrap above. RawRequest's most
		// common failure mode in practice is the agymimic client's
		// internal token refresh returning `400 invalid_grant` — that
		// must surface as 401 to the client (and NOT silently 200-empty)
		// so the admin re-authenticates the account.
		uErr := classifyNativeUpstreamErr(err, "upstream")
		if uErr.StatusCode == http.StatusUnauthorized {
			s.pauseAccountForReauth(ctx, account, err.Error(), uErr.ResponseBody)
		}
		return nil, uErr
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		logNativeUpstreamError(ctx, account.ID, originalModel, wireModel, action, stream, resp.StatusCode, resp.Header, raw, envelope)

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

		// 403 PERMISSION_DENIED — pause account so the failover loop
		// stops cycling through dead accounts and the operator sees the
		// reason in the admin UI. ratelimit_service.handleAntigravity403
		// does this for the legacy backend; we mirror it here for native
		// (which doesn't go through the same rate-limit path because
		// agymimic owns the upstream call).
		if resp.StatusCode == http.StatusForbidden {
			upstreamMsg := extractAgyErrorMessage(raw)
			fbType := classifyForbiddenType(string(raw))
			switch fbType {
			case forbiddenTypeValidation:
				validationURL := extractValidationURL(string(raw))
				s.pauseAccountForValidation(ctx, account, upstreamMsg, raw, validationURL)
			case forbiddenTypeViolation:
				s.pauseAccountForViolation(ctx, account, upstreamMsg, raw)
			}
		}

		// 400 / 401 with re-auth signal → persist account.Status='error'
		// so future SelectAccountForModel calls skip this account. The
		// invalid_grant path in classifyNativeUpstreamErr handles SDK-
		// level auth failures; this branch handles upstream JSON bodies
		// that indicate the OAuth session is dead ("Re-auth Required" /
		// "reauth" / "invalid_grant"). Failover on this request still
		// rotates via the UpstreamFailoverError below.
		if (resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized) &&
			nativeBodyIndicatesReauth(raw) {
			s.pauseAccountForReauth(ctx, account, extractAgyErrorMessage(raw), raw)
		}

		// 429 → update the USAGE WINDOWS in-memory cache with the
		// failing model at 100 % + parsed reset time. Same cache the
		// dashboard reads AND our proactive check (nativeIsFamilyExhausted)
		// consults on the next request. No STATUS badges, no Extra
		// writes — cache IS the source of truth. Reset priority via
		// ParseGeminiRateLimitResetTime (structured retryDelay +
		// quotaResetDelay + "Please retry in Xs" + "Resets in Xh Ym Zs")
		// → ApplyCustom429Policy → default cooldown.
		if resp.StatusCode == http.StatusTooManyRequests {
			s.nativeMarkFamilyExhaustedInCache(ctx, account, originalModel, raw)
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
	return s.passNonStreamingGemini(ctx, c, account.ID, resp, startTime, originalModel, wireModel, action, envelope, toolReport)
}

// Forward (Anthropic /v1/messages) lives in antigravity_native_claude.go.

// ────────────────────────────────────────────────────────────────────────────
// helpers
// ────────────────────────────────────────────────────────────────────────────

// classifyNativeUpstreamErr wraps a non-HTTP error from the agymimic
// pipeline (getClient, RawRequest) as an *UpstreamFailoverError so the
// gateway handler's failover loop and exhaustion path can produce a
// real client response — never the silent 200/empty that the handler's
// "ForwardNative already wrote the response" branch produces for naked
// fmt.Errorf.
//
// Stage is "getClient" or "upstream" — included only in the error chain
// (.Error() output) for diagnostics; the wire response uses StatusCode
// + ResponseBody.
//
// Classification:
//
//   - "invalid_grant" anywhere in the error → 401 UNAUTHENTICATED with a
//     Google-format body. The account's OAuth refresh_token is dead and
//     no further request will succeed until an admin re-authenticates
//     it. RetryableOnSameAccount=false so the failover loop hops to the
//     next native account in the group instead of retrying this one.
//   - everything else → 502 Bad Gateway, RetryableOnSameAccount=true so
//     transient network / DNS / TLS hiccups get a retry on the same
//     account before being demoted.
//
// PassthroughVerbatim is on for both so the exhaustion path forwards
// the real diagnostic to the client (and to chat-history logging) rather
// than masking it with a generic error.
func classifyNativeUpstreamErr(err error, stage string) *UpstreamFailoverError {
	msg := err.Error()
	isInvalidGrant := strings.Contains(msg, "invalid_grant")
	status := http.StatusBadGateway
	body := fmt.Sprintf(
		`{"error":{"code":502,"message":"Antigravity native upstream %s failed: %s","status":"UNAVAILABLE"}}`,
		stage, jsonEscape(msg),
	)
	retryable := true
	if isInvalidGrant {
		status = http.StatusUnauthorized
		body = fmt.Sprintf(
			`{"error":{"code":401,"message":"Antigravity native OAuth refresh failed — account needs re-authentication: %s","status":"UNAUTHENTICATED"}}`,
			jsonEscape(msg),
		)
		retryable = false
	}
	return &UpstreamFailoverError{
		StatusCode:             status,
		ResponseBody:           []byte(body),
		ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
		PassthroughVerbatim:    true,
		RetryableOnSameAccount: retryable,
	}
}

// jsonEscape returns s with the four characters that break a JSON string
// literal (backslash, double-quote, CR, LF) escaped. Used only by
// classifyNativeUpstreamErr's fmt.Sprintf'd error body — proper json.Marshal
// would round-trip via a map[string]any which is overkill for a one-field
// error envelope.
func jsonEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\r", `\r`)
	return s
}

// wrapNativeV1Internal wraps a Gemini-format inner request body in the
// checkpoint-style envelope accepted by /v1internal:streamGenerateContent.
// Shape was verified against a May 2026 checkpoint capture; agent-mode
// captures use different requestType, tool mode, and token defaults:
//
//	{
//	  "project":     "<project_id>",
//	  "requestId":   "checkpoint/<uuid>",   // NOT "agent-<uuid>"
//	  "model":       "<wire_model>",        // envelope-level
//	  "userAgent":   "antigravity",
//	  "requestType": "checkpoint",          // this gateway profile
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
	// Idempotent passthrough for a caller-provided full envelope. Semantic
	// retries are the exception: keep the prebuilt envelope, but synchronize
	// its top-level model and thinking defaults with the handler's model.
	if len(geminiBody) > 0 && gjson.ValidBytes(geminiBody) &&
		strings.EqualFold(gjson.GetBytes(geminiBody, "userAgent").String(), "antigravity") {
		if gjson.GetBytes(geminiBody, "model").String() == model {
			return geminiBody, nil
		}
		out, err := sjson.SetBytes(geminiBody, "model", model)
		if err != nil {
			return nil, fmt.Errorf("set prebuilt envelope model: %w", err)
		}
		thinkingConfigPath := "request.generationConfig.thinkingConfig"
		if !gjson.GetBytes(out, thinkingConfigPath).Exists() &&
			gjson.GetBytes(out, "request.config.thinkingConfig").Exists() {
			thinkingConfigPath = "request.config.thinkingConfig"
		}
		if isGemini37FlashTier(model) {
			// A prebuilt model change is the semantic-retry path. Synchronize it
			// to the exact real-AGY numeric tier shape and remove stale levels.
			for _, base := range []string{"request.generationConfig.thinkingConfig", "request.config.thinkingConfig"} {
				for _, key := range gemini37ThinkingDirectiveKeys {
					path := base + "." + key
					if gjson.GetBytes(out, path).Exists() {
						out, err = sjson.DeleteBytes(out, path)
						if err != nil {
							return nil, fmt.Errorf("clear prebuilt envelope thinking directive: %w", err)
						}
					}
				}
			}
			out, err = sjson.SetBytes(out, thinkingConfigPath+".thinkingBudget", thinkingBudgetForModel(model))
			if err != nil {
				return nil, fmt.Errorf("set prebuilt envelope thinking budget: %w", err)
			}
			out, err = sjson.SetBytes(out, thinkingConfigPath+".includeThoughts", true)
			if err != nil {
				return nil, fmt.Errorf("set prebuilt envelope include thoughts: %w", err)
			}
		} else {
			out, err = sjson.SetBytes(out, thinkingConfigPath+".thinkingBudget", thinkingBudgetForModel(model))
			if err != nil {
				return nil, fmt.Errorf("set prebuilt envelope thinking budget: %w", err)
			}
		}
		return out, nil
	}

	inner := make(map[string]any)
	if len(geminiBody) > 0 {
		if !gjson.ValidBytes(geminiBody) {
			return nil, fmt.Errorf("decode body: invalid JSON")
		}
		decoder := json.NewDecoder(bytes.NewReader(geminiBody))
		decoder.UseNumber()
		if err := decoder.Decode(&inner); err != nil {
			return nil, fmt.Errorf("decode body: %w", err)
		}
	}
	if inner == nil {
		inner = make(map[string]any)
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

	// Inject defaults for this synthesized checkpoint profile. Caller-provided
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
// defaults used by this synthesized checkpoint profile. Values present in
// `inner` are preserved — we only patch gaps so callers can override.
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
//	gemini-3.7-flash-low/medium/high → 1000 / 4000 / -1
//	gemini-3.8-flash-low/medium/high → 1000 / 4000 / -1
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
	//
	// Claude is the exception: hash-bound agy.exe captures (2026-09-02) show
	// NO toolConfig at all on Claude requests, and the sanitizer used for
	// those captures preserves the `toolConfig` / `mode` keys, so the absence
	// is real rather than redaction. Match the capture and leave it unset;
	// the Gemini default stays as-is because it is already proven in
	// production.
	if _, present := inner["toolConfig"]; !present && !nativeIsClaudeModel(wireModel) {
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
		gc["maxOutputTokens"] = defaultNativeMaxOutputTokens(wireModel)
	} else {
		// Clamp to the per-model upper bound. cloudcode-pa enforces
		// hard caps that vary by wire model — exceeding them returns
		// upstream 400 INVALID_ARGUMENT with no field detail. Verified
		// 2026-06-14 via binary search against gemini-pro-agent:
		// maxOutputTokens<=65535 → 200, 65536 → 400. The omp
		// @google/genai SDK ships 65536 as its default which hits this
		// off-by-one at the 2^16 boundary.
		gc["maxOutputTokens"] = clampMaxOutputTokens(gc["maxOutputTokens"], wireModel)
	}
	tc, _ := gc["thinkingConfig"].(map[string]any)
	if tc == nil {
		tc = map[string]any{}
		gc["thinkingConfig"] = tc
	}
	if _, present := tc["includeThoughts"]; !present {
		tc["includeThoughts"] = true
	}
	if nativeIsClaudeModel(wireModel) {
		applyClaudeThinkingDefaults(gc, tc, wireModel)
		return
	}
	if isGemini37FlashTier(wireModel) {
		if hasGemini37ThinkingDirective(tc) {
			return
		}
		if config, ok := inner["config"].(map[string]any); ok {
			if configThinking, ok := config["thinkingConfig"].(map[string]any); ok && hasGemini37ThinkingDirective(configThinking) {
				return
			}
		}
		tc["thinkingBudget"] = thinkingBudgetForModel(wireModel)
		return
	}
	if _, present := tc["thinkingBudget"]; !present {
		tc["thinkingBudget"] = thinkingBudgetForModel(wireModel)
	}
}

// nativeClaudeMinThinkingBudget is the provider's hard floor on
// thinking.budget_tokens for Antigravity's Claude models. Probed live on
// 2026-09-04: budgets of 128 / 256 / 512 / 1023 all return HTTP 400
// "thinking.enabled.budget_tokens: Input should be greater than or equal to
// 1024", while 1024 succeeds. A budget therefore cannot be scaled down to fit
// a small maxOutputTokens — thinking is either >= 1024 or off.
const nativeClaudeMinThinkingBudget = 1024

// applyClaudeThinkingDefaults synthesizes the captured Claude thinking config
// while respecting the caller's output-token ceiling.
//
// Two upstream rules bound this:
//
//   - `max_tokens` must be GREATER THAN `thinking.budget_tokens` (live 400 on
//     2026-09-02 with maxOutputTokens=1024 and the captured budget of 1024).
//   - `thinking.budget_tokens` must be >= 1024 (live 400 on 2026-09-04, see
//     nativeClaudeMinThinkingBudget).
//
// Together they mean a caller whose maxOutputTokens is <= 1024 cannot have
// thinking at all. Real agy.exe never hits this because it always sends
// maxOutputTokens 64000.
//
// Policy, in order of preference:
//
//  1. An EXPLICIT caller thinking budget is never touched — the caller owns
//     that decision, and if it violates an upstream rule the provider's own
//     precise 400 is surfaced instead of being silently rewritten.
//  2. Otherwise the captured default is injected when it fits.
//  3. When it cannot fit, thinking is left OFF rather than silently raising
//     the caller's explicit output ceiling. This case is reported by
//     claudeThinkingSuppressed so callers get a WARN log and a response
//     header instead of an invisible capability change.
func applyClaudeThinkingDefaults(gc, tc map[string]any, wireModel string) {
	if _, present := tc["thinkingBudget"]; present {
		return
	}
	budget := thinkingBudgetForModel(wireModel)
	if claudeThinkingCannotFit(gc, budget) {
		tc["includeThoughts"] = false
		tc["thinkingBudget"] = 0
		return
	}
	tc["thinkingBudget"] = budget
}

// claudeThinkingCannotFit reports whether the default budget would violate the
// `max_tokens` > `budget_tokens` rule for the caller's ceiling.
func claudeThinkingCannotFit(gc map[string]any, budget int) bool {
	if budget <= 0 {
		return false
	}
	maxOut, ok := numericConfigValue(gc["maxOutputTokens"])
	if !ok {
		return false
	}
	return maxOut <= budget
}

// claudeThinkingSuppressed reports whether a Claude request will run WITHOUT
// thinking purely because the caller's maxOutputTokens cannot host the
// provider's minimum budget, and returns that ceiling for logging. It only
// fires when the caller did not ask for thinking themselves, so an explicit
// caller budget is never described as suppressed.
func claudeThinkingSuppressed(inner map[string]any, wireModel string) (int, bool) {
	if inner == nil || !nativeIsClaudeModel(wireModel) {
		return 0, false
	}
	gc, _ := inner["generationConfig"].(map[string]any)
	if gc == nil {
		return 0, false
	}
	if tc, _ := gc["thinkingConfig"].(map[string]any); tc != nil {
		if _, explicit := tc["thinkingBudget"]; explicit {
			return 0, false
		}
	}
	budget := thinkingBudgetForModel(wireModel)
	if !claudeThinkingCannotFit(gc, budget) {
		return 0, false
	}
	maxOut, _ := numericConfigValue(gc["maxOutputTokens"])
	return maxOut, true
}

// numericConfigValue reads a JSON-decoded number in any of the shapes the
// decoders in this package produce (json.Number via UseNumber, float64 from
// plain Unmarshal, or a plain int from Go callers).
func numericConfigValue(v any) (int, bool) {
	switch x := v.(type) {
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return 0, false
		}
		return int(n), true
	case float64:
		return int(x), true
	case int:
		return x, true
	case int64:
		return int(x), true
	}
	return 0, false
}

func isGemini37FlashTier(model string) bool {
	normalized := strings.ToLower(strings.TrimSpace(model))
	normalized = strings.TrimPrefix(normalized, "models/")
	switch normalized {
	case "gemini-3.7-flash-low", "gemini-3.7-flash-medium", "gemini-3.7-flash-high",
		"gemini-3.8-flash-low", "gemini-3.8-flash-medium", "gemini-3.8-flash-high":
		return true
	default:
		return false
	}
}

var gemini37ThinkingDirectiveKeys = []string{"thinkingLevel", "thinking_level", "thinkingBudget", "thinking_budget"}

func hasGemini37ThinkingDirective(thinkingConfig map[string]any) bool {
	for _, key := range gemini37ThinkingDirectiveKeys {
		if _, present := thinkingConfig[key]; present {
			return true
		}
	}
	return false
}

// thinkingBudgetForModel returns the budget agy uses for each known wire
// model. Gemini 3.7 values are bound to successful agy.exe 1.1.13 runtime
// captures from 2026-08-14 (SHA-256 d628...6eb2). Older values retain their
// existing evidence dates. Unknown models fall back to -1 (dynamic).
func thinkingBudgetForModel(wire string) int {
	switch strings.ToLower(strings.TrimSpace(wire)) {
	case "gemini-3-flash":
		return -1
	// 3.5 Flash tiers (verified 2026-05)
	case "gemini-3.5-flash-extra-low":
		return 1000
	case "gemini-3.5-flash-low":
		return 4000
	case "gemini-3-flash-agent":
		return 10000
	// 3.7 Flash tiers
	case "gemini-3.7-flash-low":
		return 1000
	case "gemini-3.7-flash-medium":
		return 4000
	case "gemini-3.7-flash-high":
		return -1
	// 3.8 Flash tiers (verified 2026-09-02 from real agy.exe 1.1.24,
	// SHA-256 7585871b...880c95: hash-bound stream captures plus the
	// provider's own fetchAvailableModels metadata)
	case "gemini-3.8-flash-low":
		return 1000
	case "gemini-3.8-flash-medium":
		return 4000
	case "gemini-3.8-flash-high":
		return -1
	// 3.6 Flash tiers (verified 2026-07-21 via fetchAvailableModels probe)
	case "gemini-3.6-flash-low":
		return 1000
	case "gemini-3.6-flash-medium":
		return 4000
	case "gemini-3.6-flash-high":
		return 10000
	case "gemini-3.6-flash-tiered", "gemini-3.6-flash":
		return -1
	// 3.1 Pro tiers
	case "gemini-3.1-pro-low":
		return 1001
	case "gemini-pro-agent":
		return 10001
	// Flash-lite + image variants (no dedicated thinking budget)
	case "gemini-3.1-flash-lite", "gemini-3.1-flash-image", "gemini-3.1-flash-image-preview":
		return -1
	// Claude 4.6 family — Google exposes as thinking with 1024 budget
	case "claude-sonnet-4-6", "claude-opus-4-6-thinking":
		return 1024
	case "gpt-oss-120b-medium":
		return 8192
	}
	return -1
}

// maxOutputTokensCapForModel returns the per-model upper bound on the
// outbound generationConfig.maxOutputTokens. cloudcode-pa enforces
// these strictly — exceeding returns 400 INVALID_ARGUMENT with no
// field detail in the streaming endpoint. Verified caps:
//
//	gemini-pro-agent (3.1 Pro High) — 65535 (off-by-one at 2^16)
//	gemini-3.1-pro-low              — 65535 (assumed same; clamp safe)
//	gemini-3-flash-agent (Flash High) — 65536 accepted (no clamp needed)
//	gemini-3.5-flash-low / extra-low  — 65536 accepted
//
// Conservative default: 65535 for any non-flash wire model.
func maxOutputTokensCapForModel(wire string) int {
	w := strings.ToLower(strings.TrimSpace(wire))
	// Claude on Antigravity: real agy.exe 1.1.24 emits exactly 64000 for
	// both claude-sonnet-4-6 and claude-opus-4-6-thinking (hash-bound
	// captures, 2026-09-02). Treat that as the ceiling rather than the
	// Gemini 16-bit boundary.
	if nativeIsClaudeModel(w) {
		return nativeClaudeMaxOutputTokens
	}
	if strings.Contains(w, "flash") {
		return 65536
	}
	// Pro tier + unknown future models — clamp to 16-bit-1 boundary.
	return 65535
}

// nativeClaudeMaxOutputTokens is the generationConfig.maxOutputTokens real
// agy.exe sends for Antigravity's Claude 4.6 models.
const nativeClaudeMaxOutputTokens = 64000

// defaultNativeMaxOutputTokens returns the value to synthesize when the
// caller supplied no maxOutputTokens. Gemini keeps the historical 16384
// checkpoint-profile default; Claude uses the captured agy.exe value.
func defaultNativeMaxOutputTokens(wireModel string) int {
	if nativeIsClaudeModel(wireModel) {
		return nativeClaudeMaxOutputTokens
	}
	return 16384
}

// clampMaxOutputTokens normalizes the user-supplied maxOutputTokens
// against the per-model cap. Accepts float64 (json.Unmarshal default),
// int, int64, or json.Number; preserves type-shape on return for
// downstream marshaling. Unknown / non-numeric inputs return as-is
// (defensive).
func clampMaxOutputTokens(v any, wireModel string) any {
	cap := maxOutputTokensCapForModel(wireModel)
	asInt := -1
	switch x := v.(type) {
	case float64:
		asInt = int(x)
	case int:
		asInt = x
	case int64:
		asInt = int(x)
	case json.Number:
		n, err := x.Int64()
		if err == nil {
			asInt = int(n)
		}
	}
	if asInt <= 0 || asInt <= cap {
		return v
	}
	// Preserve float64 vs int shape so json.Marshal output stays
	// stable across paths.
	if _, isFloat := v.(float64); isFloat {
		return float64(cap)
	}
	return cap
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
	// Headers are written lazily inside the loop on the first upstream
	// byte. Pre-committing them here would block the failover loop from
	// retrying when upstream returns 200 OK with an empty body — gin
	// auto-writes status 200 on the first c.Writer.Write so we don't
	// need an explicit WriteHeader call.
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
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
	// Thought parts are not client-visible answer content: a STOP response that
	// contains only thoughts must remain uncommitted so the handler can fail over.
	streamSawText := false
	streamSawFunctionCall := false
	streamSawFinishReason := false
	streamFinishReason := ""
	streamEmptyArgsFn := ""
	// Hold the thought-only prefix until usable answer text or a function call
	// arrives. This is the only safe window for account failover: once any SSE
	// byte reaches the client, retrying would splice two streams together.
	// Bound the buffer so unusually large reasoning traces cannot consume
	// unbounded memory; above the cap we preserve streaming and stop promising
	// semantic-empty failover for that response.
	const semanticPrecommitLimit = 1 << 20
	var semanticPrecommit bytes.Buffer
	// `first` tracks "any byte received from upstream" — drives the
	// firstTokenMs latency metric. `headersCommitted` tracks "any byte
	// written to the gin client" — the only semantic that matters for
	// pre-flush failover decisions.
	headersCommitted := false
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
						slog.Bool("any_bytes_sent", headersCommitted))
					fingerprint.ForceRefresh()
					// Drain the rest of the body whether or not bytes
					// were already sent — we can't unsend what's gone
					// but we MUST stop polluting the stream with the
					// rejection text. The user retrying on the same
					// account will get the refreshed fingerprint.
					_, _ = io.Copy(io.Discard, resp.Body)
					if !headersCommitted {
						return nil, &UpstreamFailoverError{
							StatusCode:             http.StatusBadRequest,
							ResponseBody:           out,
							ResponseHeaders:        resp.Header,
							PassthroughVerbatim:    false,
							RetryableOnSameAccount: true,
						}
					}
					// Bytes already flushed — abort the stream cleanly.
					// finalize what we have and let the client surface a
					// truncated response; retrying yields the refreshed
					// version. Better than writing the rejection text
					// into the client's transcript.
					// (We could add an UpstreamVersionRejection field to
					// ForwardResult later if accounting wants to flag
					// the request — for now the slog WARN above is the
					// audit trail.)
					return s.finalizeResult(result, startTime), nil
				}

				// 200-OK-with-429-inside-SSE guard. cloudcode-pa
				// occasionally returns HTTP 200 but with an error event
				// as the first SSE payload (e.g. quota check failed
				// after the response started). If we let the bytes
				// through, c.Writer.Write commits the 200 OK headers
				// and the failover loop can no longer rotate. Catch it
				// pre-flush and surface as UpstreamFailoverError so the
				// loop switches accounts. Symmetric to the version-
				// rejection guard above.
				//
				// HTTP-level 429 is caught earlier in ForwardGemini's
				// non-200 branch; this branch only fires for the rare
				// "200 with error body" anomaly.
				if isUpstreamRateLimitPayload(out) {
					slog.WarnContext(ctx, "native: rate-limit error inside 200-OK SSE stream",
						slog.Int64("account_id", accountID),
						slog.String("model", originalModel),
						slog.String("wire_model", wireModel),
						slog.Bool("any_bytes_sent", headersCommitted),
					)
					_, _ = io.Copy(io.Discard, resp.Body)
					// Same cache mark as the HTTP-429 branch — fetch
					// account by ID (SSE inner loop only has the ID).
					if s.accountRepo != nil {
						if acc, gerr := s.accountRepo.GetByID(ctx, accountID); gerr == nil && acc != nil {
							s.nativeMarkFamilyExhaustedInCache(ctx, acc, originalModel, out)
						}
					}
					if !headersCommitted {
						// No bytes written yet — failover can still
						// rotate. Mark as HTTP 429 + PassthroughVerbatim
						// so the loop's switch path treats it the same
						// as a real HTTP 429.
						return nil, &UpstreamFailoverError{
							StatusCode:             http.StatusTooManyRequests,
							ResponseBody:           out,
							ResponseHeaders:        resp.Header,
							PassthroughVerbatim:    true,
							RetryableOnSameAccount: false,
						}
					}
					// Bytes already on the wire — can't rotate. Let
					// the loop end naturally; the synthetic-terminator
					// path at function tail will emit a finishReason so
					// the client SDK closes cleanly instead of raising
					// "stream ended without finish reason".
					return s.finalizeResult(result, startTime), nil
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
					if finishReason := payloadFinishReason(payload); finishReason != "" {
						streamSawFinishReason = true
						streamFinishReason = finishReason
					}
				}
				if u := extractGeminiUsageFromSSELine(line); u != nil {
					result.Usage.InputTokens = u.PromptTokens
					result.Usage.OutputTokens = u.CandidateTokens
					if u.ModelVersion != "" {
						result.UpstreamModel = u.ModelVersion
					}
				}

				if !headersCommitted {
					_, _ = semanticPrecommit.Write(out)
					if !streamSawText && !streamSawFunctionCall && semanticPrecommit.Len() <= semanticPrecommitLimit {
						continue
					}
					if !streamSawText && !streamSawFunctionCall {
						slog.WarnContext(ctx, "native: semantic precommit buffer limit reached — preserving stream",
							slog.Int64("account_id", accountID),
							slog.String("model", originalModel),
							slog.String("wire_model", wireModel),
							slog.Int("buffered_bytes", semanticPrecommit.Len()))
					}
					out = append([]byte(nil), semanticPrecommit.Bytes()...)
					semanticPrecommit.Reset()
				}
				if _, wErr := c.Writer.Write(out); wErr != nil {
					result.ClientDisconnect = true
					_, _ = io.Copy(io.Discard, resp.Body)
					return s.finalizeResult(result, startTime), nil
				}
				// First successful Write — gin has now committed the
				// 200 OK headers. From this point on, failover is unsafe.
				headersCommitted = true
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				if len(buf) > 0 {
					tail := unwrapAgyResponseEnvelopeLine(buf)
					tail = rewriteSSELineFunctionCalls(tail, toolReport)
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
						if finishReason := payloadFinishReason(payload); finishReason != "" {
							streamSawFinishReason = true
							streamFinishReason = finishReason
						}
					}
					if u := extractGeminiUsageFromSSELine(buf); u != nil {
						result.Usage.InputTokens = u.PromptTokens
						result.Usage.OutputTokens = u.CandidateTokens
						if u.ModelVersion != "" {
							result.UpstreamModel = u.ModelVersion
						}
					}
					if !headersCommitted {
						_, _ = semanticPrecommit.Write(tail)
						if streamSawText || streamSawFunctionCall || semanticPrecommit.Len() > semanticPrecommitLimit {
							tail = append([]byte(nil), semanticPrecommit.Bytes()...)
							semanticPrecommit.Reset()
							_, _ = c.Writer.Write(tail)
							headersCommitted = true
						}
					} else {
						_, _ = c.Writer.Write(tail)
					}
					if headersCommitted && flusher != nil {
						flusher.Flush()
					}
				}
				if first {
					// Upstream returned 200 OK but the body was empty
					// (zero bytes received before EOF). Headers were
					// NOT yet committed because we deferred WriteHeader
					// until the first upstream byte, so the failover
					// loop is still free to retry on another account.
					slog.WarnContext(ctx, "native: upstream 200 with empty stream body — failing over",
						slog.Int64("account_id", accountID),
						slog.String("model", originalModel),
						slog.String("wire_model", wireModel),
					)
					recordNativeAccountQuality(ctx, accountID, originalModel, time.Since(startTime), true)
					return nil, &UpstreamFailoverError{
						StatusCode:             http.StatusBadGateway,
						ResponseBody:           []byte(`{"error":{"code":502,"message":"Antigravity native upstream returned 200 with empty stream body","status":"UNAVAILABLE"}}`),
						ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
						PassthroughVerbatim:    true,
						RetryableOnSameAccount: true,
						Kind:                   FailoverKindSemanticEmpty,
					}
				}
				break
			}
			// Non-EOF read error mid-stream. Headers + SSE chunks are
			// already on the wire — we cannot fail over here (client
			// state is committed). Returning an error sends the failover
			// loop on a doomed retry whose response can't replace bytes
			// already on the wire; the client ends up with a half-stream
			// missing finishReason and raises
			// "Google API stream ended without a finish reason".
			// Log + fall through so the synthetic terminator below fires
			// and the SDK closes cleanly.
			slog.WarnContext(ctx, "native: upstream stream read error — synthesizing terminator",
				slog.Int64("account_id", accountID),
				slog.String("model", originalModel),
				slog.String("wire_model", wireModel),
				slog.String("error", readErr.Error()),
				slog.Bool("any_bytes_sent", !first),
				slog.Bool("saw_finish_reason", streamSawFinishReason),
			)
			break
		}
	}
	// Upstream completed without client-visible answer text or a tool call.
	// STOP and truncated no-finish streams are retryable semantic empties.
	// Other terminal reasons (SAFETY, RECITATION, etc.) are meaningful Gemini
	// responses and must pass through unchanged.
	if !headersCommitted && isNativeSemanticEmptyCompletion(streamSawText, streamSawFunctionCall, streamFinishReason) {
		slog.WarnContext(ctx, "native: upstream STOP without usable content — failing over",
			slog.Int64("account_id", accountID),
			slog.String("model", originalModel),
			slog.String("wire_model", wireModel),
			slog.String("finish_reason", streamFinishReason),
			slog.Int("buffered_bytes", semanticPrecommit.Len()),
			slog.String("failover_kind", FailoverKindSemanticEmpty.String()))
		logNativeRequestAnomaly(ctx, accountID, originalModel, wireModel, true, lastChunkPayload,
			"stop_without_content", map[string]string{"reason": "no non-thought text and no function call seen across stream"})
		// Feed the semantic-empty EWMA with the observed elapsed time so
		// downstream schedulers can see BOTH the empty-rate and the
		// latency the account cost the client. Requested model
		// (`originalModel`) is the key — 3.6 / 3.1 remain distinct rows
		// from any wire-remapped variant.
		recordNativeAccountQuality(ctx, accountID, originalModel, time.Since(startTime), true)
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusBadGateway,
			ResponseBody:           []byte(`{"error":{"code":502,"message":"Antigravity native upstream returned STOP without usable content","status":"UNAVAILABLE"}}`),
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: false,
			Kind:                   FailoverKindSemanticEmpty,
			DiagnosticResponseBody: append([]byte(nil), semanticPrecommit.Bytes()...),
		}
	}
	if !headersCommitted && semanticPrecommit.Len() > 0 {
		_, _ = c.Writer.Write(semanticPrecommit.Bytes())
		headersCommitted = true
		semanticPrecommit.Reset()
		if flusher != nil {
			flusher.Flush()
		}
	}
	// Synthetic finishReason terminator. omp / @google/genai treat a
	// stream that ends without a candidates[].finishReason field as
	// truncated and raise "Google API stream ended without a finish
	// reason (connection dropped or response truncated)". Fires when:
	//   - upstream stream ends EOF without a STOP chunk (cloudcode-pa
	//     occasionally truncates under load), OR
	//   - a non-EOF read error aborted the loop above.
	// Either way, write one final synthetic event so the SDK closes.
	if !first && !streamSawFinishReason {
		slog.WarnContext(ctx, "native: stream ended without finishReason — synthesizing OTHER",
			slog.Int64("account_id", accountID),
			slog.String("model", originalModel),
			slog.String("wire_model", wireModel),
			slog.Bool("saw_text", streamSawText),
			slog.Bool("saw_function_call", streamSawFunctionCall),
		)
		// Lead with a blank line so the synthesized event is always
		// separated from whatever raw bytes preceded it (the upstream
		// stream may have ended mid-line, in which case our `data: {...}`
		// would otherwise concatenate to the previous incomplete event
		// and the SDK parser would treat them as one malformed payload).
		_, _ = c.Writer.Write([]byte("\n\n"))
		_, _ = c.Writer.Write(syntheticFinishReasonSSE("OTHER"))
		if flusher != nil {
			flusher.Flush()
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
	// End-of-stream quality sample. Semantic-empty here is the residual
	// "headers already committed" path: the failover branch at line 1260
	// couldn't fire because bytes went out to the client before we
	// realised the stream carried no usable answer. Recording it keeps
	// the tracker's view of the account honest (client saw an empty
	// response) without a second failover attempt. Requested model is
	// the EWMA key; wire model is metadata only.
	semanticEmpty := isNativeSemanticEmptyCompletion(streamSawText, streamSawFunctionCall, streamFinishReason)
	recordNativeAccountQuality(ctx, accountID, originalModel, time.Since(startTime), semanticEmpty)
	slog.DebugContext(ctx, "native.quality.stream_complete",
		slog.Int64("account_id", accountID),
		slog.String("model", originalModel),
		slog.String("wire_model", wireModel),
		slog.Bool("semantic_empty", semanticEmpty),
		slog.Int64("latency_ms", time.Since(startTime).Milliseconds()))
	return s.finalizeResult(result, startTime), nil
}

// inspectStreamChunk parses a single SSE data payload and reports whether
// it carries text, a function call, and (if it carries a function call)
// whether the args are missing/empty. Returns ("",) when the payload is
// not valid JSON. The empty-args check fires on a per-chunk basis because
// once a function call is emitted streaming concludes immediately — there's
// no "later chunk" to redeem an args={} call.
func isNativeSemanticEmptyCompletion(sawText, sawFunctionCall bool, finishReason string) bool {
	return !sawText && !sawFunctionCall &&
		(finishReason == "" || strings.EqualFold(finishReason, "STOP"))
}

func isNativeSemanticEmptyAnomaly(anomaly string) bool {
	return anomaly == "stop_without_content" || anomaly == "no_candidates"
}

func inspectStreamChunk(payload []byte) (sawText bool, sawFunctionCall bool, emptyArgsFn string) {
	type part struct {
		Text         *string        `json:"text,omitempty"`
		Thought      bool           `json:"thought,omitempty"`
		FunctionCall map[string]any `json:"functionCall,omitempty"`
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
			if p.Text != nil && *p.Text != "" && !p.Thought {
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
				if m, ok := args.(map[string]any); ok && len(m) == 0 {
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

// payloadFinishReason returns the first non-empty
// candidates[].finishReason value. Used by streamGeminiToClient both to
// classify semantic-empty STOP responses and to decide whether a synthetic
// terminator is needed.
//
// Malformed JSON and payloads without a finish reason return "". This keeps
// truncated streams eligible for pre-commit failover.
func payloadFinishReason(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	var env struct {
		Candidates []struct {
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return ""
	}
	for _, c := range env.Candidates {
		if finishReason := strings.TrimSpace(c.FinishReason); finishReason != "" {
			return finishReason
		}
	}
	return ""
}

// syntheticFinishReasonSSE returns a fully formed SSE event
//
//	data: {"candidates":[{"finishReason":"<reason>","content":{"role":"model","parts":[{"text":""}]}}]}\n\n
//
// suitable for terminating a stream that the upstream cut short.
// Includes the trailing blank line so the SDK's event parser fires
// the final-event callback immediately.
func syntheticFinishReasonSSE(reason string) []byte {
	if reason == "" {
		reason = "OTHER"
	}
	body := map[string]any{
		"candidates": []any{map[string]any{
			"finishReason": reason,
			"content": map[string]any{
				"role":  "model",
				"parts": []any{map[string]any{"text": ""}},
			},
		}},
	}
	js, err := json.Marshal(body)
	if err != nil {
		// Hand-rolled fallback so the function is unconditionally safe.
		return []byte("data: {\"candidates\":[{\"finishReason\":\"" + reason + "\",\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"\"}]}}]}\n\n")
	}
	out := make([]byte, 0, len(js)+10)
	out = append(out, "data: "...)
	out = append(out, js...)
	out = append(out, '\n', '\n')
	return out
}

func (s *AntigravityNativeGatewayService) passNonStreamingGemini(
	ctx context.Context,
	c *gin.Context,
	accountID int64,
	resp *http.Response,
	startTime time.Time,
	originalModel, wireModel, action string,
	outboundRequest []byte,
	toolReport toolPrepReport,
) (*ForwardResult, error) {
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusBadGateway,
			ResponseBody:           []byte(`{"error":{"code":502,"message":"Antigravity native upstream response body could not be read","status":"UNAVAILABLE"}}`),
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: true,
		}
	}
	// Standard Gemini SDKs read `candidates` / `usageMetadata` at the top
	// level. The agymimic upstream wraps them inside a `response` field;
	// unwrap before forwarding so non-streaming clients also see the
	// canonical shape.
	out := unwrapAgyResponseEnvelopeBody(raw)
	// Back-translate call_mcp_tool function calls if the aggregator was
	// used for this request — keeps omp's tool dispatch transparent.
	out = rewriteAggregatedFunctionCalls(out, toolReport)

	anomaly, details := inspectGeminiResponseForAnomalies(out)
	outcome := anomaly
	switch {
	case len(bytes.TrimSpace(out)) == 0:
		outcome = "empty_body"
	case strings.EqualFold(payloadFinishReason(out), "MALFORMED_FUNCTION_CALL"):
		outcome = "malformed_function_call"
	case isVersionRejectionPayload(out):
		outcome = "version_rejection"
	case outcome == "":
		outcome = "success"
	}
	captureAntigravityAB(ctx, c, diagnosticcapture.Record{
		Route:             "antigravity-native",
		Model:             originalModel,
		WireModel:         wireModel,
		Action:            action,
		Stream:            false,
		AccountID:         accountID,
		Outcome:           outcome,
		OutboundRequest:   outboundRequest,
		UpstreamResponse:  raw,
		ConvertedResponse: out,
	})

	// Defense-in-depth: cloudcode-pa has been observed returning 200 OK
	// with Content-Length: 0 in rare edge cases (e.g. upstream-side
	// transient internal error that doesn't materialise as a 5xx). Without
	// this guard we'd write an empty 200 to the client, and SDKs that
	// expect `{candidates: [...]}` (google.genai, omp's @google/genai)
	// would construct a default-zero response object instead of raising,
	// surfacing as "Gemini returned empty response" downstream. Treat
	// an empty body as an upstream failure and let the failover loop
	// hop to another account.
	if len(bytes.TrimSpace(out)) == 0 {
		slog.WarnContext(ctx, "native: upstream 200 with empty body",
			slog.Int64("account_id", accountID),
			slog.String("model", originalModel),
			slog.String("wire_model", wireModel),
			slog.Int("raw_bytes", len(raw)),
		)
		logNativeRequestAnomaly(ctx, accountID, originalModel, wireModel, false, raw, "empty_body", map[string]string{
			"raw_bytes": fmt.Sprintf("%d", len(raw)),
		})
		recordNativeAccountQuality(ctx, accountID, originalModel, time.Since(startTime), true)
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusBadGateway,
			ResponseBody:           []byte(`{"error":{"code":502,"message":"Antigravity native upstream returned 200 with empty body","status":"UNAVAILABLE"}}`),
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: true,
			Kind:                   FailoverKindSemanticEmpty,
		}
	}

	// Match the streaming guard: cloudcode-pa may embed quota exhaustion in
	// an HTTP-200 non-stream body. Surface 429 before any client write so the
	// handler can rotate accounts and update the shared usage cache.
	if isUpstreamRateLimitPayload(out) {
		slog.WarnContext(ctx, "native: rate-limit error inside 200-OK non-stream response",
			slog.Int64("account_id", accountID),
			slog.String("model", originalModel),
			slog.String("wire_model", wireModel))
		if s.accountRepo != nil {
			if acc, getErr := s.accountRepo.GetByID(ctx, accountID); getErr == nil && acc != nil {
				s.nativeMarkFamilyExhaustedInCache(ctx, acc, originalModel, out)
			}
		}
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusTooManyRequests,
			ResponseBody:           out,
			ResponseHeaders:        resp.Header,
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: false,
		}
	}

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

	if anomaly != "" {
		logNativeRequestAnomaly(ctx, accountID, originalModel, wireModel, false, out, anomaly, details)
	}
	if isNativeSemanticEmptyAnomaly(anomaly) {
		slog.WarnContext(ctx, "native: non-stream response without usable content — failing over",
			slog.Int64("account_id", accountID),
			slog.String("model", originalModel),
			slog.String("wire_model", wireModel),
			slog.String("failover_kind", FailoverKindSemanticEmpty.String()))
		recordNativeAccountQuality(ctx, accountID, originalModel, time.Since(startTime), true)
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusBadGateway,
			ResponseBody:           []byte(`{"error":{"code":502,"message":"Antigravity native upstream returned STOP without usable content","status":"UNAVAILABLE"}}`),
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: false,
			Kind:                   FailoverKindSemanticEmpty,
			DiagnosticResponseBody: append([]byte(nil), out...),
		}
	}

	c.Header("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)
	_, _ = c.Writer.Write(out)

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
	// Success sample: usable body reached client without the semantic-
	// empty failover branch firing. Requested model is the EWMA key —
	// wire model (post ResolveWireFromBody) is metadata only, so
	// gemini-3.6-flash and gemini-3.6-flash-high stay distinct rows.
	recordNativeAccountQuality(ctx, accountID, originalModel, time.Since(startTime), false)
	slog.DebugContext(ctx, "native.quality.non_stream_complete",
		slog.Int64("account_id", accountID),
		slog.String("model", originalModel),
		slog.String("wire_model", wireModel),
		slog.Int64("latency_ms", time.Since(startTime).Milliseconds()))
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
	account *Account,
	requestBody []byte,
	body []byte,
	startTime time.Time,
	originalModel, wireModel string,
	toolReport toolPrepReport,
	stream bool,
	agyListToolsIterations int,
) (*ForwardResult, error) {
	accountID := int64(0)
	if account != nil {
		accountID = account.ID
	}
	// Unwrap agymimic's {response: {...}} envelope so client sees canonical Gemini shape.
	out := unwrapAgyResponseEnvelopeBody(body)
	if len(bytes.TrimSpace(out)) == 0 {
		logNativeRequestAnomaly(ctx, accountID, originalModel, wireModel, stream, body, "empty_body", map[string]string{
			"raw_bytes": fmt.Sprintf("%d", len(body)),
		})
		recordNativeAccountQuality(ctx, accountID, originalModel, time.Since(startTime), true)
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusBadGateway,
			ResponseBody:           []byte(`{"error":{"code":502,"message":"Antigravity native upstream returned 200 with empty buffered body","status":"UNAVAILABLE"}}`),
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: true,
			Kind:                   FailoverKindSemanticEmpty,
		}
	}

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

	anomaly, details := inspectGeminiResponseForAnomalies(out)
	if anomaly != "" {
		logNativeRequestAnomaly(ctx, accountID, originalModel, wireModel, stream, out, anomaly, details)
	}
	if isNativeSemanticEmptyAnomaly(anomaly) {
		slog.WarnContext(ctx, "native: buffered response without usable content — failing over",
			slog.Int64("account_id", accountID),
			slog.String("model", originalModel),
			slog.String("wire_model", wireModel),
			slog.Bool("stream", stream),
			slog.String("failover_kind", FailoverKindSemanticEmpty.String()))
		recordNativeAccountQuality(ctx, accountID, originalModel, time.Since(startTime), true)
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusBadGateway,
			ResponseBody:           []byte(`{"error":{"code":502,"message":"Antigravity native upstream returned STOP without usable content","status":"UNAVAILABLE"}}`),
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: false,
			Kind:                   FailoverKindSemanticEmpty,
			DiagnosticResponseBody: append([]byte(nil), out...),
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
	s.maybeLogChatHistory(ctx, account, requestBody, body, originalModel, wireModel,
		toolReport.DiscoveryMode, toolReport.AggregatorName, stream, startTime,
		0, agyListToolsIterations, "")
	// Success sample for the agy_list_tools-buffered path. Same rules
	// as the direct-stream/non-stream success recorders: requested model
	// is the EWMA key; wire model rides in slog metadata only.
	recordNativeAccountQuality(ctx, accountID, originalModel, time.Since(startTime), false)
	slog.DebugContext(ctx, "native.quality.buffered_complete",
		slog.Int64("account_id", accountID),
		slog.String("model", originalModel),
		slog.String("wire_model", wireModel),
		slog.Bool("stream", stream),
		slog.Int("agy_list_tools_iterations", agyListToolsIterations),
		slog.Int64("latency_ms", time.Since(startTime).Milliseconds()))
	return s.finalizeResult(result, startTime), nil
}

// maybeLogChatHistory builds + enqueues a ChatHistoryEntry when logging
// is enabled globally AND for this account. All work is async-safe; on
// failure we drop the entry rather than block the request.
func (s *AntigravityNativeGatewayService) maybeLogChatHistory(
	ctx context.Context,
	account *Account,
	requestBody []byte,
	responseBody []byte,
	originalModel, wireModel, discoveryMode, aggregatorName string,
	stream bool,
	startTime time.Time,
	firstTokenMs int64,
	agyListToolsIterations int,
	errMsg string,
) {
	if s.chatHistoryLog == nil || !s.chatHistoryLog.IsEnabled() {
		return
	}
	if !AccountAllowsChatHistory(account) {
		return
	}
	var reqObj, respObj map[string]any
	if len(requestBody) > 0 {
		_ = json.Unmarshal(requestBody, &reqObj)
	}
	if len(responseBody) > 0 {
		_ = json.Unmarshal(unwrapAgyResponseEnvelopeBody(responseBody), &respObj)
	}
	var accountID int64
	if account != nil {
		accountID = account.ID
	}
	entry := ChatHistoryEntry{
		AccountID:              accountID,
		Platform:               "antigravity_native",
		Model:                  originalModel,
		WireModel:              wireModel,
		Stream:                 stream,
		DiscoveryMode:          discoveryMode,
		AggregatorName:         aggregatorName,
		Request:                reqObj,
		Response:               respObj,
		ToolCallsSeen:          extractToolCallNamesFromResponse(respObj),
		DurationMs:             time.Since(startTime).Milliseconds(),
		FirstTokenMs:           firstTokenMs,
		AgyListToolsIterations: agyListToolsIterations,
		Error:                  errMsg,
	}
	s.chatHistoryLog.Log(entry)
}

func (s *AntigravityNativeGatewayService) finalizeResult(r *ForwardResult, startTime time.Time) *ForwardResult {
	r.Duration = time.Since(startTime)
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
	outboundEnvelope []byte,
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
		slog.Int("outbound_envelope_size", len(outboundEnvelope)),
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
		return "no_candidates", nil
	}
	type part struct {
		Text         *string        `json:"text,omitempty"`
		Thought      bool           `json:"thought,omitempty"`
		FunctionCall map[string]any `json:"functionCall,omitempty"`
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

	var firstEmptyFunction map[string]string
	sawStop := false
	sawMeaningfulTerminal := false
	for _, candidate := range env.Candidates {
		finishReason := strings.TrimSpace(candidate.FinishReason)
		if strings.EqualFold(finishReason, "STOP") {
			sawStop = true
		} else if finishReason != "" {
			sawMeaningfulTerminal = true
		}
		for _, p := range candidate.Content.Parts {
			if p.Text != nil && *p.Text != "" && !p.Thought {
				return "", nil
			}
			if p.FunctionCall == nil {
				continue
			}
			name, _ := p.FunctionCall["name"].(string)
			args, hasArgs := p.FunctionCall["args"]
			if !hasArgs || args == nil {
				if firstEmptyFunction == nil {
					firstEmptyFunction = map[string]string{"function": name, "reason": "args missing"}
				}
				continue
			}
			if m, ok := args.(map[string]any); ok && len(m) == 0 {
				if firstEmptyFunction == nil {
					firstEmptyFunction = map[string]string{"function": name, "reason": "args is empty object"}
				}
				continue
			}
			return "", nil
		}
	}
	if firstEmptyFunction != nil {
		return "empty_function_args", firstEmptyFunction
	}
	if sawMeaningfulTerminal {
		return "", nil
	}
	if sawStop {
		return "stop_without_content", map[string]string{"finish_reason": "STOP"}
	}
	return "no_candidates", nil
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
		if md, ok := loadBody["metadata"].(map[string]any); ok {
			md["duetProject"] = pid
		}
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
	defer func() { _ = resp.Body.Close() }()

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

// isUpstreamRateLimitPayload returns true when an SSE payload (or any
// upstream body slice) carries a Google `code:429 / RESOURCE_EXHAUSTED /
// QUOTA_EXHAUSTED` error event. Symmetric to isVersionRejectionPayload:
// pre-write inspector for the case where cloudcode-pa returns
// `HTTP 200 OK` but the very first SSE event is actually an error JSON.
// Without this guard the bytes flow straight through `c.Writer.Write`,
// gin commits the 200 OK headers, and the failover loop can no longer
// rotate accounts — the client sees a 200 stream containing a 429
// error and the SDK surfaces it as `RESOURCE_EXHAUSTED`.
//
// HTTP-level 429 (status != 200 from upstream) is handled separately by
// ForwardGemini's `resp.StatusCode != http.StatusOK` branch and is NOT
// the case this helper exists for.
func isUpstreamRateLimitPayload(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	scan := body
	if len(scan) > 8*1024 {
		scan = scan[:8*1024]
	}
	// Must contain an `"error"` envelope AND one of the well-known
	// rate-limit markers. The `"code":429` numeric match is conservative
	// (json formatters preserve compact spacing for numeric fields).
	if !bytes.Contains(scan, []byte(`"error"`)) {
		return false
	}
	for _, p := range [][]byte{
		[]byte(`"code":429`),
		[]byte(`"code": 429`),
		[]byte(`"status":"RESOURCE_EXHAUSTED"`),
		[]byte(`"status": "RESOURCE_EXHAUSTED"`),
		[]byte(`"reason":"QUOTA_EXHAUSTED"`),
		[]byte(`"reason": "QUOTA_EXHAUSTED"`),
	} {
		if bytes.Contains(scan, p) {
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

	userPrompt := strings.TrimSpace(prompt)
	if userPrompt == "" {
		userPrompt = "ping"
	}

	wireModel := antigravity.AntigravityWireModel(modelID)
	if wireModel == "" {
		wireModel = modelID
	}

	// Build the Gemini inner request manually (instead of calling
	// cli.Generate) so we can inspect the raw response body when upstream
	// returns 403 — the validation URL the operator needs is buried in
	// the JSON body and agymimic's parseAPIError discards it. RawRequest
	// gives us status + body, we own the parse.
	innerBody, _ := json.Marshal(map[string]any{
		"contents": []any{map[string]any{
			"role":  "user",
			"parts": []any{map[string]any{"text": userPrompt}},
		}},
		"systemInstruction": map[string]any{
			"parts": []any{map[string]any{"text": antigravity.GetDefaultIdentityPatch()}},
		},
		"generationConfig": map[string]any{
			"maxOutputTokens": 256,
		},
	})
	envelope, wrapErr := wrapNativeV1Internal(cli.ProjectID(), wireModel, innerBody)
	if wrapErr != nil {
		return nil, fmt.Errorf("native test: envelope: %w", wrapErr)
	}

	resp, rawErr := cli.RawRequest(ctx, "/v1internal:generateContent", envelope)
	if rawErr != nil {
		return nil, fmt.Errorf("native test: request: %w", rawErr)
	}
	rawBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	s.maybePersistRefreshedTokens(ctx, account, cli)

	// Non-200 paths — surface the verification URL on validation_required
	// 403, pause the account, return a structured TestConnectionResult so
	// the admin dialog can render a clickable link.
	if resp.StatusCode != http.StatusOK {
		logNativeUpstreamError(ctx, account.ID, modelID, wireModel, "generateContent", false, resp.StatusCode, resp.Header, rawBody, envelope)
		upstreamMsg := extractAgyErrorMessage(rawBody)
		if resp.StatusCode == http.StatusForbidden {
			fbType := classifyForbiddenType(string(rawBody))
			result := &TestConnectionResult{
				MappedModel:  wireModel,
				ErrorMessage: upstreamMsg,
			}
			if fbType == forbiddenTypeValidation {
				result.NeedsVerify = true
				result.ValidationURL = extractValidationURL(string(rawBody))
				s.pauseAccountForValidation(ctx, account, upstreamMsg, rawBody, result.ValidationURL)
				return result, nil
			}
			if fbType == forbiddenTypeViolation {
				result.IsBanned = true
				s.pauseAccountForViolation(ctx, account, upstreamMsg, rawBody)
				return result, nil
			}
			// Generic 403 — surface the upstream message verbatim.
			return result, fmt.Errorf("native test: %s", upstreamMsg)
		}
		return nil, fmt.Errorf("native test: antigravity api %d: %s", resp.StatusCode, upstreamMsg)
	}

	// Happy path — parse the body the same way agymimic.Generate does so
	// the existing helpers (extractTextFromAgyResponse + the diagnostics
	// log fields) work unchanged.
	var parsed types.Response
	if err := json.Unmarshal(rawBody, &parsed); err != nil {
		return nil, fmt.Errorf("native test: decode: %w", err)
	}
	text := extractTextFromAgyResponse(&parsed)
	slog.InfoContext(ctx, "native test connection probe",
		"account_id", account.ID,
		"public_model", modelID,
		"wire_model", wireModel,
		"text_len", len(text),
		"candidates", responseCandidateCount(&parsed),
		"finish_reason", responseFirstFinishReason(&parsed),
		"parts_summary", responsePartsSummary(&parsed),
	)
	return &TestConnectionResult{
		Text:        text,
		MappedModel: wireModel,
	}, nil
}

func (s *AntigravityNativeGatewayService) setAccountErrorIfCurrent(ctx context.Context, account *Account, message string) (bool, error) {
	if account == nil || s.accountRepo == nil {
		return false, nil
	}
	if conditional, ok := s.accountRepo.(ConditionalAccountErrorRepository); ok {
		return conditional.SetErrorIfUnchanged(ctx, account.ID, account.UpdatedAt, message)
	}
	if err := s.accountRepo.SetError(ctx, account.ID, message); err != nil {
		return false, err
	}
	return true, nil
}

// pauseAccountForValidation sets the account to error status with a
// human-readable reason that includes the upstream validation URL when
// available. Called from both TestConnection and ForwardGemini's 403
// branch — keeps a single source of truth for "VALIDATION_REQUIRED →
// stop scheduling this account until an operator verifies it in
// Google's signin flow".
func (s *AntigravityNativeGatewayService) pauseAccountForValidation(
	ctx context.Context,
	account *Account,
	upstreamMsg string,
	rawBody []byte,
	validationURL string,
) {
	if account == nil || s.accountRepo == nil {
		return
	}
	msg := "Validation required (403): account needs Google verification"
	if upstreamMsg != "" {
		msg += " | upstream: " + upstreamMsg
	}
	if validationURL != "" {
		msg += " | validation_url: " + validationURL
	}
	updated, err := s.setAccountErrorIfCurrent(ctx, account, msg)
	if err != nil {
		slog.WarnContext(ctx, "native: SetError(validation_required) failed",
			slog.Int64("account_id", account.ID), slog.String("error", err.Error()))
		return
	}
	if !updated {
		slog.InfoContext(ctx, "native: ignored stale validation pause", slog.Int64("account_id", account.ID))
		return
	}
	slog.WarnContext(ctx, "native: account paused — validation required",
		slog.Int64("account_id", account.ID),
		slog.String("upstream_msg", upstreamMsg),
		slog.String("validation_url", validationURL),
		slog.Int("body_bytes", len(rawBody)),
	)
}

// pauseAccountForViolation mirrors pauseAccountForValidation for the
// TOS_VIOLATION 403 case. Same persistence path; different log/key.
func (s *AntigravityNativeGatewayService) pauseAccountForViolation(
	ctx context.Context,
	account *Account,
	upstreamMsg string,
	rawBody []byte,
) {
	if account == nil || s.accountRepo == nil {
		return
	}
	msg := "Account violation (403): terms of service violation"
	if upstreamMsg != "" {
		msg += " | upstream: " + upstreamMsg
	}
	updated, err := s.setAccountErrorIfCurrent(ctx, account, msg)
	if err != nil {
		slog.WarnContext(ctx, "native: SetError(violation) failed",
			slog.Int64("account_id", account.ID), slog.String("error", err.Error()))
		return
	}
	if !updated {
		slog.InfoContext(ctx, "native: ignored stale violation pause", slog.Int64("account_id", account.ID))
		return
	}
	slog.WarnContext(ctx, "native: account paused — TOS violation",
		slog.Int64("account_id", account.ID),
		slog.String("upstream_msg", upstreamMsg),
		slog.Int("body_bytes", len(rawBody)),
	)
}

// pauseAccountForReauth mirrors pauseAccountForValidation/Violation for
// the "account needs re-authentication" case — when the upstream's
// OAuth token has been revoked (invalid_grant on refresh) or the
// upstream returns a "Re-auth Required" error body directly. Same
// persistence path (SetError → account.Status becomes 'error' →
// selector's IsSchedulable returns false → next SelectAccountForModel
// skips it) as the 403 helpers.
//
// Recovery: operator re-runs the OAuth flow via the admin UI (which
// clears the error status). Until then, the account is out of
// rotation, matching the "Re-auth Required" badge shown in USAGE
// WINDOWS.
func (s *AntigravityNativeGatewayService) pauseAccountForReauth(
	ctx context.Context,
	account *Account,
	upstreamMsg string,
	rawBody []byte,
) {
	if account == nil || s.accountRepo == nil {
		return
	}
	msg := "Re-auth required: account OAuth token needs re-authentication"
	if upstreamMsg != "" {
		msg += " | upstream: " + upstreamMsg
	}
	updated, err := s.setAccountErrorIfCurrent(ctx, account, msg)
	if err != nil {
		slog.WarnContext(ctx, "native: SetError(reauth_required) failed",
			slog.Int64("account_id", account.ID), slog.String("error", err.Error()))
		return
	}
	if !updated {
		slog.InfoContext(ctx, "native: ignored stale reauth pause", slog.Int64("account_id", account.ID))
		return
	}
	slog.WarnContext(ctx, "native: account paused — re-auth required",
		slog.Int64("account_id", account.ID),
		slog.String("upstream_msg", upstreamMsg),
		slog.Int("body_bytes", len(rawBody)),
	)
}

// nativeIsReauthRequired reports whether the requested-account's
// USAGE WINDOWS cache flags it as needs_reauth (set by the periodic
// quota fetch on HTTP 401 / invalid_grant). Used by ForwardGemini's
// proactive check to skip accounts the dashboard already shows as
// "Re-auth Required" — the SAME cache the UI reads. Cache-cold
// returns false (never over-block).
func (s *AntigravityNativeGatewayService) nativeIsReauthRequired(accountID int64) bool {
	if s.usageCache == nil {
		return false
	}
	raw, ok := s.usageCache.antigravityCache.Load(accountID)
	if !ok {
		return false
	}
	entry, ok := raw.(*antigravityUsageCache)
	if !ok || entry == nil || entry.usageInfo == nil {
		return false
	}
	return entry.usageInfo.NeedsReauth
}

// nativeBodyIndicatesReauth reports whether an upstream error body
// carries the "Re-auth" / re-authentication signal (case-insensitive).
// Cloudcode-pa surfaces this in several shapes:
//   - HTTP 400 with error.message containing "Re-auth" or "reauth"
//   - HTTP 401 UNAUTHENTICATED with a message about token refresh
//   - agymimic-wrapped errors surfacing "invalid_grant" in the body
//
// Any hit → the caller should call pauseAccountForReauth so the
// selector skips this account until an operator re-runs the OAuth
// flow.
func nativeBodyIndicatesReauth(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	low := strings.ToLower(string(body))
	return strings.Contains(low, "re-auth") ||
		strings.Contains(low, "reauth") ||
		strings.Contains(low, "invalid_grant") ||
		strings.Contains(low, "re-authentication")
}

// extractAgyErrorMessage parses the upstream JSON body for
// `error.message`. Returns "" when the body doesn't parse or the field
// is missing — caller should fall back to the raw bytes.
func extractAgyErrorMessage(rawBody []byte) string {
	if len(rawBody) == 0 {
		return ""
	}
	var env struct {
		Error struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rawBody, &env); err != nil {
		return ""
	}
	if env.Error.Message == "" {
		return env.Error.Status
	}
	if env.Error.Status != "" {
		return env.Error.Status + ": " + env.Error.Message
	}
	return env.Error.Message
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
			_, _ = thoughtText.WriteString(p.Text)
			continue
		}
		_, _ = realText.WriteString(p.Text)
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

// nativeFamilyForModel classifies a native antigravity model into
// one of the two upstream quota families that agy.exe's own UI
// exposes: "gemini" (all gemini-* models including image variants)
// or "others" (claude-*, gpt-*, anything else). Empty string when
// the input is empty.
//
// Same classification logic drives both:
//   - proactive check: nativeIsFamilyExhausted reads the USAGE WINDOWS
//     cache and returns true if ANY family-mate is at 100 %
//   - reactive cache update: on 429, we mark the failing model at
//     100 % in the same cache so the next proactive check catches it
//     without needing to write anything to account.Extra
func nativeFamilyForModel(model string) string {
	m := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(model, "models/")))
	if m == "" {
		return ""
	}
	if strings.HasPrefix(m, "gemini-") {
		return "gemini"
	}
	return "others"
}

// nativeIsClaudeModel reports whether the wire model is one of the Claude
// models Antigravity serves through the same cloudcode-pa endpoint. Verified
// against real agy.exe 1.1.24/1.1.25 `models` output on 2026-09-02:
// claude-sonnet-4-6 and claude-opus-4-6-thinking.
func nativeIsClaudeModel(model string) bool {
	m := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(model, "models/")))
	return strings.HasPrefix(m, "claude-")
}

// nativeSkipsToolInjection reports whether sub2api's Gemini-only tool
// aggregation layer must be bypassed for this wire model. Real agy.exe sends
// non-Gemini models (Claude, GPT-OSS) requests whose tool declarations are
// exactly the caller's own, with no call_mcp_tool aggregator, no
// agy_list_tools discovery declaration and no injected MCP catalog text.
func nativeSkipsToolInjection(model string) bool {
	return nativeFamilyForModel(model) != "gemini"
}

// nativeModelInFamily reports whether the given upstream model name
// (as it appears in cache keys — normalized/lowercased) belongs to
// the given family. Used by nativeIsFamilyExhausted's aggregation.
func nativeModelInFamily(model, family string) bool {
	m := strings.ToLower(model)
	switch family {
	case "gemini":
		return strings.HasPrefix(m, "gemini-")
	case "others":
		return !strings.HasPrefix(m, "gemini-")
	}
	return false
}

// nativeIsFamilyExhausted reads the USAGE WINDOWS in-memory cache
// (populated by AccountUsageService.getAntigravityUsage on every
// dashboard render + by nativeMarkFamilyExhaustedInCache on every
// 429) and returns true if ANY member of the requested family shows
// utilization >= 100 with the upstream-reported reset time still in
// the future.
//
// Cache-cold (no cache entry yet) returns false — never over-block
// scheduling. The reactive 429 path warms the cache on first
// upstream error, so subsequent selections converge.
func (s *AntigravityNativeGatewayService) nativeIsFamilyExhausted(accountID int64, requestedModel string) bool {
	if s.usageCache == nil {
		return false
	}
	fam := nativeFamilyForModel(requestedModel)
	if fam == "" {
		return false
	}
	raw, ok := s.usageCache.antigravityCache.Load(accountID)
	if !ok {
		return false
	}
	entry, ok := raw.(*antigravityUsageCache)
	if !ok || entry == nil || entry.usageInfo == nil {
		return false
	}
	quota := entry.usageInfo.AntigravityQuota
	if len(quota) == 0 {
		return false
	}
	now := time.Now()
	for modelName, q := range quota {
		if q == nil || q.Utilization < 100 {
			continue
		}
		if !nativeModelInFamily(modelName, fam) {
			continue
		}
		// Reset already passed → stale entry, don't over-block.
		if q.ResetTime != "" {
			if reset, err := time.Parse(time.RFC3339, q.ResetTime); err == nil && !now.Before(reset) {
				continue
			}
		}
		return true
	}
	return false
}

// nativeMarkFamilyExhaustedInCache updates the USAGE WINDOWS cache
// so the failing model shows 100 % utilization with the parsed
// reset time. That's the same cache the dashboard renders and the
// selector's proactive check reads — one source of truth, no writes
// to account.Extra or model_rate_limits.
//
// Reset priority: ParseGeminiRateLimitResetTime (structured
// retryDelay + quotaResetDelay + "Please retry in Xs" +
// "Resets in Xh Ym Zs") → ApplyCustom429Policy per-account cooldown
// → antigravityDefaultRateLimitDuration (30 s). Mirrors gemini's
// handleGeminiUpstreamError order exactly.
func (s *AntigravityNativeGatewayService) nativeMarkFamilyExhaustedInCache(
	ctx context.Context,
	account *Account,
	originalModel string,
	body []byte,
) time.Time {
	if s.usageCache == nil || account == nil {
		return time.Time{}
	}
	modelKey := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(originalModel, "models/")))
	if modelKey == "" {
		return time.Time{}
	}

	var resetAt time.Time
	if ts := ParseGeminiRateLimitResetTime(body); ts != nil {
		resetAt = time.Unix(*ts, 0)
	} else if cd, used, _ := ApplyCustom429Policy(account); used {
		resetAt = time.Now().Add(cd)
	} else {
		resetAt = time.Now().Add(antigravityDefaultRateLimitDuration)
	}

	// 1) Update the USAGE WINDOWS in-memory cache so dashboard renders
	//    the exhaustion (family bar goes red) — same cache the proactive
	//    check reads.
	s.usageCacheMu.Lock()
	raw, _ := s.usageCache.antigravityCache.Load(account.ID)
	entry, _ := raw.(*antigravityUsageCache)
	now := time.Now()
	usageInfo := &UsageInfo{UpdatedAt: &now}
	timestamp := now
	if entry != nil {
		timestamp = entry.timestamp
		if entry.usageInfo != nil {
			copied := *entry.usageInfo
			usageInfo = &copied
		}
	}
	quota := make(map[string]*AntigravityModelQuota, len(usageInfo.AntigravityQuota)+1)
	for name, modelQuota := range usageInfo.AntigravityQuota {
		quota[name] = modelQuota
	}
	quota[modelKey] = &AntigravityModelQuota{
		Utilization: 100,
		ResetTime:   resetAt.UTC().Format(time.RFC3339),
	}
	usageInfo.AntigravityQuota = quota
	updated := &antigravityUsageCache{usageInfo: usageInfo, timestamp: timestamp}
	updated.timestamp = now
	s.usageCache.antigravityCache.Store(account.ID, updated)
	s.usageCacheMu.Unlock()

	// 2) Mark the account rate-limited at the DB level so
	//    Account.IsSchedulable() naturally skips it on the NEXT select.
	//    Mirrors what GeminiMessagesCompatService.handleGeminiUpstreamError
	//    does — this is the standard sub2api pattern for "429 → skip
	//    account until reset". No STATUS badges (this only touches
	//    account.RateLimitedAt / RateLimitResetAt, not model_rate_limits).
	if s.accountRepo != nil {
		if err := s.accountRepo.SetRateLimited(ctx, account.ID, resetAt); err != nil {
			slog.WarnContext(ctx, "native: SetRateLimited failed",
				slog.Int64("account_id", account.ID),
				slog.String("error", err.Error()))
		}
	}

	slog.InfoContext(ctx, "native: 429 → cache marked + SetRateLimited",
		slog.Int64("account_id", account.ID),
		slog.String("model", modelKey),
		slog.String("family", nativeFamilyForModel(modelKey)),
		slog.Time("reset_at", resetAt),
		slog.Duration("cooldown", time.Until(resetAt).Truncate(time.Second)))
	return resetAt
}

// nativeFamilyResetForModel returns the earliest future reset_time
// across every exhausted (utilization >= 100) family-mate of the
// requested model in the USAGE WINDOWS cache. Used by the proactive
// check to feed a SetRateLimited call so the selector's next pass
// skips the account naturally. Returns zero time when nothing usable
// is found — caller must guard.
func (s *AntigravityNativeGatewayService) nativeFamilyResetForModel(accountID int64, requestedModel string) time.Time {
	if s.usageCache == nil {
		return time.Time{}
	}
	fam := nativeFamilyForModel(requestedModel)
	if fam == "" {
		return time.Time{}
	}
	raw, ok := s.usageCache.antigravityCache.Load(accountID)
	if !ok {
		return time.Time{}
	}
	entry, ok := raw.(*antigravityUsageCache)
	if !ok || entry == nil || entry.usageInfo == nil {
		return time.Time{}
	}
	now := time.Now()
	var best time.Time
	for modelName, q := range entry.usageInfo.AntigravityQuota {
		if q == nil || q.Utilization < 100 {
			continue
		}
		if !nativeModelInFamily(modelName, fam) {
			continue
		}
		if q.ResetTime == "" {
			continue
		}
		reset, err := time.Parse(time.RFC3339, q.ResetTime)
		if err != nil || !now.Before(reset) {
			continue
		}
		if best.IsZero() || reset.Before(best) {
			best = reset
		}
	}
	return best
}
