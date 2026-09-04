// Anthropic (/v1/messages) protocol support for PlatformAntigravityNative.
//
// Antigravity serves Claude 4.6 through the SAME cloudcode-pa
// v1internal:streamGenerateContent endpoint it uses for Gemini — the model
// name is simply a Claude wire ID. Verified against real agy.exe (1.1.24,
// SHA-256 7585871b…880c95 and 1.1.25, SHA-256 dbc665f9…2b3c) with
// hash-bound sanitized captures on 2026-09-02:
//
//	payload.model                                     = claude-sonnet-4-6
//	                                                  | claude-opus-4-6-thinking
//	payload.request.generationConfig.maxOutputTokens  = 64000
//	payload.request.generationConfig.thinkingConfig   = {includeThoughts:true,
//	                                                     thinkingBudget:1024}
//	payload.request.toolConfig                        = ABSENT
//	response                                          = Gemini-shaped
//	                                                    candidates[].content.parts[]
//
// So the native Claude path is: Anthropic request -> (reuse the legacy
// pure translator) Gemini inner request -> native v1internal envelope ->
// native agymimic client -> Gemini SSE -> (reuse the legacy pure stream
// processor) Anthropic SSE.
//
// Two deliberate differences from the legacy antigravity Claude path:
//
//  1. The envelope is built by wrapNativeV1Internal, so Claude inherits the
//     native checkpoint profile, session id and per-model thinking/token
//     defaults instead of the legacy agent/VALIDATED profile.
//  2. sub2api's Gemini-only tool aggregation (call_mcp_tool, agy_list_tools,
//     injected MCP catalog text) is NOT applied — see nativeSkipsToolInjection.
//     Only the caller's own tool declarations reach the model, which is what
//     real agy.exe does for non-Gemini models.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/koval/agymimic/fingerprint"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Forward handles Anthropic-format (/v1/messages) requests for native
// accounts. Claude models are routed through the native v1internal pipeline;
// any other requested model is rejected with a clear Anthropic-shaped error
// pointing at the Gemini endpoints.
func (s *AntigravityNativeGatewayService) Forward(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	isStickySession bool,
) (*ForwardResult, error) {
	if account == nil {
		return nil, fmt.Errorf("native claude: nil account")
	}
	if account.Platform != domain.PlatformAntigravityNative {
		return nil, fmt.Errorf("native claude: wrong platform %q", account.Platform)
	}

	startTime := time.Now()

	var claudeReq antigravity.ClaudeRequest
	if err := json.Unmarshal(body, &claudeReq); err != nil {
		return nil, writeClaudeProtocolError(c, http.StatusBadRequest, "invalid_request_error", "Invalid request body")
	}
	originalModel := strings.TrimSpace(claudeReq.Model)
	if originalModel == "" {
		return nil, writeClaudeProtocolError(c, http.StatusBadRequest, "invalid_request_error", "Missing model")
	}

	// Whitelist / mapping is shared with the legacy antigravity platform.
	mappedModel := mapAntigravityModel(account, originalModel)
	if mappedModel == "" {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalFeatureGate)
		return nil, writeClaudeProtocolError(c, http.StatusForbidden, "permission_error",
			fmt.Sprintf("model %s not in whitelist", originalModel))
	}
	thinkingEnabled := claudeReq.Thinking != nil &&
		(claudeReq.Thinking.Type == "enabled" || claudeReq.Thinking.Type == "adaptive")
	mappedModel = applyThinkingModelSuffix(mappedModel, thinkingEnabled)

	// Non-Claude models over the Anthropic protocol stay unsupported: the
	// Gemini families are reachable (and already validated) through the
	// /antigravity-native/v1beta endpoints, and silently re-shaping a Gemini
	// request here would bypass the slider/wire-model contract.
	if !nativeIsClaudeModel(mappedModel) {
		msg := `{"type":"error","error":{"type":"invalid_request_error","message":"native antigravity accounts accept the Anthropic /v1/messages protocol for Claude models only — use the Gemini /v1beta endpoints for Gemini models, or route this request to a legacy antigravity account"}}`
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusBadRequest,
			ResponseBody:           []byte(msg),
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			RetryableOnSameAccount: false,
		}
	}

	// Same proactive guards as ForwardGemini so a dead or exhausted account
	// fails over instead of burning the request.
	if s.nativeIsReauthRequired(account.ID) {
		s.pauseAccountForReauth(ctx, account, "USAGE WINDOWS snapshot flagged needs_reauth", nil)
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusUnauthorized,
			ResponseBody:           []byte(`{"type":"error","error":{"type":"authentication_error","message":"Antigravity native account needs re-authentication."}}`),
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: false,
		}
	}
	if s.nativeIsFamilyExhausted(account.ID, mappedModel) {
		resetAt := s.nativeFamilyResetForModel(account.ID, mappedModel)
		if !resetAt.IsZero() && s.accountRepo != nil {
			_ = s.accountRepo.SetRateLimited(ctx, account.ID, resetAt)
		}
		return nil, &UpstreamFailoverError{
			StatusCode:             http.StatusTooManyRequests,
			ResponseBody:           []byte(`{"type":"error","error":{"type":"rate_limit_error","message":"Antigravity native account model family is exhausted."}}`),
			ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			PassthroughVerbatim:    true,
			RetryableOnSameAccount: false,
		}
	}

	cli, err := s.getClient(ctx, account)
	if err != nil {
		uErr := classifyNativeUpstreamErr(err, "getClient")
		if uErr.StatusCode == http.StatusUnauthorized {
			s.pauseAccountForReauth(ctx, account, err.Error(), uErr.ResponseBody)
		}
		return nil, uErr
	}

	s.ensureMetricsLoop(ctx, account, s.resolveProxyURL(ctx, account))

	// Reuse the legacy pure Claude -> Gemini translator, then keep only its
	// inner request: the native envelope is built by wrapNativeV1Internal so
	// Claude gets the same checkpoint profile, session id and defaults as
	// every other native request.
	transformOpts := s.claudeTransformOptions(ctx)
	wrapped, err := antigravity.TransformClaudeToGeminiWithOptions(&claudeReq, cli.ProjectID(), mappedModel, transformOpts)
	if err != nil {
		return nil, writeClaudeProtocolError(c, http.StatusBadRequest, "invalid_request_error", "Invalid request")
	}
	innerBody, err := extractNativeInnerRequest(wrapped)
	if err != nil {
		return nil, fmt.Errorf("native claude: extract inner request: %w", err)
	}

	// Schema normalization only. Tool INJECTION stays off for Claude
	// (nativeSkipsToolInjection), so the model sees exactly the caller's own
	// declarations, matching real agy.exe.
	innerBody, toolReport, err := preprocessNativeBody(innerBody, false, "", s.resolveMcpDiscoveryMode(ctx), s.resolveToolCallMode(ctx))
	if err != nil {
		return nil, fmt.Errorf("native claude: tool preprocess: %w", err)
	}

	// Surface the one case where sub2api declines to add thinking: the caller
	// pinned max_tokens at or below the provider's 1024 budget floor, so no
	// valid thinking budget exists. Never let that pass unnoticed — a WARN log
	// plus a response header make the downgrade observable to both operators
	// and clients (an accidental low max_tokens is otherwise invisible).
	if maxOut, suppressed := claudeThinkingSuppressedInBody(innerBody, mappedModel); suppressed {
		slog.WarnContext(ctx, "native claude: thinking disabled, caller max_tokens below provider minimum budget",
			slog.Int64("account_id", account.ID),
			slog.String("model", originalModel),
			slog.String("wire_model", mappedModel),
			slog.Int("max_output_tokens", maxOut),
			slog.Int("min_thinking_budget", nativeClaudeMinThinkingBudget),
			slog.String("remedy", "raise max_tokens above the minimum thinking budget to re-enable thinking"))
		c.Header("x-sub2api-thinking", "disabled_max_tokens_below_min_budget")
	}

	envelope, err := wrapNativeV1Internal(cli.ProjectID(), mappedModel, innerBody)
	if err != nil {
		return nil, fmt.Errorf("native claude: envelope: %w", err)
	}

	slog.InfoContext(ctx, "native: claude request prepared",
		slog.Int64("account_id", account.ID),
		slog.String("model", originalModel),
		slog.String("wire_model", mappedModel),
		slog.Bool("stream", claudeReq.Stream),
		slog.Bool("thinking", thinkingEnabled),
		slog.Bool("tool_injection", false),
		slog.Int("builtin_tools_count", len(toolReport.BuiltinTools)),
		slog.Int("schemas_normalized", toolReport.Normalized))

	// Antigravity only streams; a non-streaming client request is served by
	// collecting the stream and converting once (same as legacy).
	resp, err := cli.RawRequest(ctx, "/v1internal:streamGenerateContent?alt=sse", envelope)
	if err != nil {
		uErr := classifyNativeUpstreamErr(err, "upstream")
		if uErr.StatusCode == http.StatusUnauthorized {
			s.pauseAccountForReauth(ctx, account, err.Error(), uErr.ResponseBody)
		}
		return nil, uErr
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		logNativeUpstreamError(ctx, account.ID, originalModel, mappedModel, "streamGenerateContent", claudeReq.Stream, resp.StatusCode, resp.Header, raw, envelope)
		return nil, s.claudeUpstreamFailure(ctx, account, mappedModel, resp, raw)
	}

	requestID := resp.Header.Get("x-request-id")
	if requestID != "" {
		c.Header("x-request-id", requestID)
	}

	var streamRes *antigravityStreamResult
	if claudeReq.Stream {
		streamRes, err = claudeStreamingResponseShared(s.settingService, c, resp, startTime, originalModel)
	} else {
		streamRes, err = claudeStreamToNonStreamingShared(s.settingService, c, resp, startTime, originalModel)
	}
	if err != nil {
		return nil, err
	}
	usage := streamRes.usage
	if usage == nil {
		usage = &ClaudeUsage{}
	}

	return &ForwardResult{
		RequestID:        requestID,
		Usage:            *usage,
		Model:            originalModel,
		UpstreamModel:    mappedModel,
		Stream:           claudeReq.Stream,
		Duration:         time.Since(startTime),
		FirstTokenMs:     streamRes.firstTokenMs,
		ClientDisconnect: streamRes.clientDisconnect,
	}, nil
}

// claudeTransformOptions mirrors the legacy resolver. The identity patch is
// forced on because the Antigravity upstream rejects requests without it
// (legacy learned this the hard way: missing identity -> HTTP 429).
func (s *AntigravityNativeGatewayService) claudeTransformOptions(ctx context.Context) antigravity.TransformOptions {
	opts := antigravity.DefaultTransformOptions()
	if s.settingService != nil {
		opts.IdentityPatch = s.settingService.GetIdentityPatchPrompt(ctx)
	}
	opts.EnableIdentityPatch = true
	return opts
}

// extractNativeInnerRequest peels the `request` object out of the envelope
// produced by TransformClaudeToGeminiWithOptions. The native gateway rebuilds
// its own envelope, so passing the legacy one through unchanged would hit
// wrapNativeV1Internal's prebuilt-envelope passthrough and skip the native
// per-model defaults.
//
// The legacy translator also sets toolConfig.functionCallingConfig.mode to
// "VALIDATED". Real agy.exe sends Claude requests with no toolConfig at all
// (hash-bound captures, 2026-09-02), so drop it here and let the native
// defaults leave it unset.
func extractNativeInnerRequest(wrapped []byte) ([]byte, error) {
	if !gjson.ValidBytes(wrapped) {
		return nil, fmt.Errorf("transformed body is not valid JSON")
	}
	inner := gjson.GetBytes(wrapped, "request")
	if !inner.Exists() || !inner.IsObject() {
		return nil, fmt.Errorf("transformed body has no request object")
	}
	out := []byte(inner.Raw)
	if gjson.GetBytes(out, "toolConfig").Exists() {
		stripped, err := sjson.DeleteBytes(out, "toolConfig")
		if err != nil {
			return nil, fmt.Errorf("strip toolConfig: %w", err)
		}
		out = stripped
	}
	return out, nil
}

// claudeUpstreamFailure maps a non-200 upstream response to the same failover
// semantics ForwardGemini uses, but with an Anthropic-shaped body so Claude
// clients can parse the error.
func (s *AntigravityNativeGatewayService) claudeUpstreamFailure(
	ctx context.Context,
	account *Account,
	mappedModel string,
	resp *http.Response,
	raw []byte,
) error {
	retryable := true
	if isAntigravityVersionRejection(resp.StatusCode, raw) {
		slog.WarnContext(ctx, "native claude: upstream rejected version, forcing fingerprint refresh",
			slog.Int64("account_id", account.ID),
			slog.Int("status", resp.StatusCode))
		fingerprint.ForceRefresh()
	}
	if resp.StatusCode == http.StatusForbidden {
		upstreamMsg := extractAgyErrorMessage(raw)
		switch classifyForbiddenType(string(raw)) {
		case forbiddenTypeValidation:
			s.pauseAccountForValidation(ctx, account, upstreamMsg, raw, extractValidationURL(string(raw)))
		case forbiddenTypeViolation:
			s.pauseAccountForViolation(ctx, account, upstreamMsg, raw)
		}
		retryable = false
	}
	if (resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized) &&
		nativeBodyIndicatesReauth(raw) {
		s.pauseAccountForReauth(ctx, account, extractAgyErrorMessage(raw), raw)
		retryable = false
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		s.nativeMarkFamilyExhaustedInCache(ctx, account, mappedModel, raw)
	}

	return &UpstreamFailoverError{
		StatusCode:             resp.StatusCode,
		ResponseBody:           claudeErrorBody(resp.StatusCode, raw),
		ResponseHeaders:        http.Header{"Content-Type": []string{"application/json"}},
		RetryableOnSameAccount: retryable,
	}
}

// claudeErrorBody renders an Anthropic-shaped error envelope, preserving the
// upstream message when one can be extracted.
func claudeErrorBody(status int, raw []byte) []byte {
	msg := strings.TrimSpace(extractAntigravityErrorMessage(raw))
	msg = sanitizeUpstreamErrorMessage(msg)
	if msg == "" {
		msg = fmt.Sprintf("upstream returned HTTP %d", status)
	}
	errType := "api_error"
	switch status {
	case http.StatusUnauthorized:
		errType = "authentication_error"
	case http.StatusForbidden:
		errType = "permission_error"
	case http.StatusTooManyRequests:
		errType = "rate_limit_error"
	case http.StatusBadRequest:
		errType = "invalid_request_error"
	}
	payload, err := json.Marshal(map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    errType,
			"message": msg,
		},
	})
	if err != nil {
		return []byte(`{"type":"error","error":{"type":"api_error","message":"upstream request failed"}}`)
	}
	return payload
}

// claudeThinkingSuppressedInBody is the []byte form of claudeThinkingSuppressed
// used by the request path, which handles the inner request as raw JSON.
func claudeThinkingSuppressedInBody(body []byte, wireModel string) (int, bool) {
	if !nativeIsClaudeModel(wireModel) || !gjson.ValidBytes(body) {
		return 0, false
	}
	gc := gjson.GetBytes(body, "generationConfig")
	if !gc.IsObject() {
		return 0, false
	}
	if gc.Get("thinkingConfig.thinkingBudget").Exists() {
		return 0, false
	}
	maxOut := gc.Get("maxOutputTokens")
	if !maxOut.Exists() {
		return 0, false
	}
	budget := thinkingBudgetForModel(wireModel)
	if budget <= 0 || int(maxOut.Int()) > budget {
		return 0, false
	}
	return int(maxOut.Int()), true
}
