package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestNativeClaudeModelDetection(t *testing.T) {
	claude := []string{
		"claude-sonnet-4-6",
		"claude-opus-4-6-thinking",
		"models/claude-sonnet-4-6",
		"CLAUDE-SONNET-4-6",
	}
	for _, m := range claude {
		if !nativeIsClaudeModel(m) {
			t.Errorf("nativeIsClaudeModel(%q) = false, want true", m)
		}
		if !nativeSkipsToolInjection(m) {
			t.Errorf("nativeSkipsToolInjection(%q) = false, want true", m)
		}
	}
	for _, m := range []string{"gemini-3.8-flash-high", "gemini-3.7-flash", "models/gemini-3.1-pro-low"} {
		if nativeIsClaudeModel(m) {
			t.Errorf("nativeIsClaudeModel(%q) = true, want false", m)
		}
		if nativeSkipsToolInjection(m) {
			t.Errorf("nativeSkipsToolInjection(%q) = true, want false for Gemini", m)
		}
	}
	// GPT-OSS is also non-Gemini: real agy.exe does not receive sub2api's
	// Gemini-only aggregator surface for it either.
	if !nativeSkipsToolInjection("gpt-oss-120b-medium") {
		t.Error("nativeSkipsToolInjection(gpt-oss-120b-medium) = false, want true")
	}
}

func TestNativeClaudeOutputTokenDefaults(t *testing.T) {
	// Captured from real agy.exe 1.1.24/1.1.25 on 2026-09-02.
	for _, m := range []string{"claude-sonnet-4-6", "claude-opus-4-6-thinking"} {
		if got := defaultNativeMaxOutputTokens(m); got != 64000 {
			t.Errorf("defaultNativeMaxOutputTokens(%q) = %d, want 64000", m, got)
		}
		if got := maxOutputTokensCapForModel(m); got != 64000 {
			t.Errorf("maxOutputTokensCapForModel(%q) = %d, want 64000", m, got)
		}
		if got := clampMaxOutputTokens(200000, m); got != 64000 {
			t.Errorf("clampMaxOutputTokens(200000, %q) = %v, want 64000", m, got)
		}
	}
	// Gemini keeps its historical defaults and caps.
	if got := defaultNativeMaxOutputTokens("gemini-3.8-flash-medium"); got != 16384 {
		t.Errorf("gemini default = %d, want 16384", got)
	}
	if got := maxOutputTokensCapForModel("gemini-3.8-flash-medium"); got != 65536 {
		t.Errorf("gemini flash cap = %d, want 65536", got)
	}
	if got := maxOutputTokensCapForModel("gemini-pro-agent"); got != 65535 {
		t.Errorf("gemini pro cap = %d, want 65535", got)
	}
}

func TestWrapNativeV1Internal_ClaudeDefaultsMatchCapture(t *testing.T) {
	body := []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`)
	for _, model := range []string{"claude-sonnet-4-6", "claude-opus-4-6-thinking"} {
		out, err := wrapNativeV1Internal("proj", model, body)
		if err != nil {
			t.Fatalf("wrap %s: %v", model, err)
		}
		gc := "request.generationConfig"
		if got := gjson.GetBytes(out, gc+".maxOutputTokens").Int(); got != 64000 {
			t.Errorf("%s maxOutputTokens = %d, want 64000: %s", model, got, out)
		}
		if got := gjson.GetBytes(out, gc+".thinkingConfig.thinkingBudget").Int(); got != 1024 {
			t.Errorf("%s thinkingBudget = %d, want 1024", model, got)
		}
		if !gjson.GetBytes(out, gc+".thinkingConfig.includeThoughts").Bool() {
			t.Errorf("%s includeThoughts missing", model)
		}
		if got := gjson.GetBytes(out, "model").String(); got != model {
			t.Errorf("model = %q, want %q", got, model)
		}
		// Captured Claude requests carry no toolConfig at all.
		if gjson.GetBytes(out, "request.toolConfig").Exists() {
			t.Errorf("%s: toolConfig injected, want absent: %s", model, out)
		}
	}
}

// Tool INJECTION must be off for Claude: the caller's own declarations pass
// through untouched, no call_mcp_tool aggregator is declared, and no MCP
// catalog / single-name instruction text is prepended to systemInstruction.
func TestPreprocessNativeBody_ClaudeSkipsToolInjection(t *testing.T) {
	body := []byte(`{
		"contents":[{"role":"user","parts":[{"text":"hi"}]}],
		"systemInstruction":{"role":"user","parts":[{"text":"CALLER SYSTEM PROMPT"}]},
		"tools":[{"functionDeclarations":[
			{"name":"mcp__files_read","parametersJsonSchema":{"type":"object","properties":{"path":{"type":"string"}}}},
			{"name":"read"}
		]}]
	}`)

	out, report, err := preprocessNativeBody(body, false, "call_mcp_tool", "both", "single_name")
	if err != nil {
		t.Fatalf("preprocess: %v", err)
	}
	if report.AggregatorOn {
		t.Error("AggregatorOn = true, want false for Claude")
	}
	names := map[string]bool{}
	gjson.GetBytes(out, "tools.0.functionDeclarations").ForEach(func(_, v gjson.Result) bool {
		names[v.Get("name").String()] = true
		return true
	})
	if !names["mcp__files_read"] {
		t.Errorf("caller mcp tool was dropped: %s", out)
	}
	if !names["read"] {
		t.Errorf("caller builtin tool was dropped: %s", out)
	}
	if names[defaultMcpAggregatorName] {
		t.Errorf("aggregator declaration was injected: %s", out)
	}
	if names[defaultListToolsName] {
		t.Errorf("agy_list_tools declaration was injected: %s", out)
	}
	sysText := gjson.GetBytes(out, "systemInstruction.parts.0.text").String()
	if !strings.Contains(sysText, "CALLER SYSTEM PROMPT") {
		t.Errorf("caller system prompt lost: %q", sysText)
	}
	if strings.Contains(sysText, mcpCatalogStartMarker) || strings.Contains(sysText, singleNameInstructionsStartMarker) {
		t.Errorf("tool instruction text was injected: %q", sysText)
	}
	// JSON-Schema conversion still runs so upstream accepts caller tools.
	if got := gjson.GetBytes(out, "tools.0.functionDeclarations.0.parameters.type").String(); got != "OBJECT" {
		t.Errorf("schema not normalized to Gemini form: %s", out)
	}
	if gjson.GetBytes(out, "tools.0.functionDeclarations.0.parametersJsonSchema").Exists() {
		t.Errorf("parametersJsonSchema not removed: %s", out)
	}
}

func TestExtractNativeInnerRequest(t *testing.T) {
	wrapped := []byte(`{"project":"p","model":"claude-sonnet-4-6","requestType":"agent","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"sessionId":"7"}}`)
	inner, err := extractNativeInnerRequest(wrapped)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if gjson.GetBytes(inner, "contents.0.parts.0.text").String() != "hi" {
		t.Fatalf("inner request lost contents: %s", inner)
	}
	if gjson.GetBytes(inner, "model").Exists() || gjson.GetBytes(inner, "requestType").Exists() {
		t.Fatalf("envelope fields leaked into inner request: %s", inner)
	}

	// Real agy.exe sends Claude requests with no toolConfig; the legacy
	// translator adds mode=VALIDATED, so it must be stripped here.
	withToolConfig := []byte(`{"model":"claude-sonnet-4-6","request":{"contents":[],"toolConfig":{"functionCallingConfig":{"mode":"VALIDATED"}}}}`)
	strippedInner, err := extractNativeInnerRequest(withToolConfig)
	if err != nil {
		t.Fatalf("extract with toolConfig: %v", err)
	}
	if gjson.GetBytes(strippedInner, "toolConfig").Exists() {
		t.Fatalf("toolConfig was not stripped: %s", strippedInner)
	}

	if _, err := extractNativeInnerRequest([]byte(`{"model":"x"}`)); err == nil {
		t.Fatal("expected error when request object is absent")
	}
	if _, err := extractNativeInnerRequest([]byte(`{`)); err == nil {
		t.Fatal("expected error on invalid JSON")
	}
}

func TestClaudeErrorBodyShape(t *testing.T) {
	cases := map[int]string{
		http.StatusUnauthorized:    "authentication_error",
		http.StatusForbidden:       "permission_error",
		http.StatusTooManyRequests: "rate_limit_error",
		http.StatusBadRequest:      "invalid_request_error",
		http.StatusBadGateway:      "api_error",
	}
	for status, wantType := range cases {
		raw := claudeErrorBody(status, nil)
		var parsed struct {
			Type  string `json:"type"`
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("status %d: invalid JSON %s", status, raw)
		}
		if parsed.Type != "error" {
			t.Errorf("status %d: type = %q, want error", status, parsed.Type)
		}
		if parsed.Error.Type != wantType {
			t.Errorf("status %d: error.type = %q, want %q", status, parsed.Error.Type, wantType)
		}
		if parsed.Error.Message == "" {
			t.Errorf("status %d: empty message", status)
		}
	}
}

// Upstream enforces Anthropic's `max_tokens` > `thinking.budget_tokens` rule
// and answers 400 INVALID_ARGUMENT otherwise (observed live 2026-09-02 with
// maxOutputTokens=1024 + captured budget 1024). When the caller's ceiling
// cannot host the captured budget, thinking is disabled instead of raising
// their ceiling or failing the request.
func TestWrapNativeV1Internal_ClaudeThinkingRespectsCallerMaxTokens(t *testing.T) {
	cases := []struct {
		name           string
		maxOutput      string
		wantMaxOutput  int64
		wantBudget     int64
		wantIncludeThg bool
	}{
		{name: "ceiling equals budget", maxOutput: `"maxOutputTokens":1024,`, wantMaxOutput: 1024, wantBudget: 0, wantIncludeThg: false},
		{name: "ceiling below budget", maxOutput: `"maxOutputTokens":512,`, wantMaxOutput: 512, wantBudget: 0, wantIncludeThg: false},
		{name: "ceiling above budget", maxOutput: `"maxOutputTokens":4096,`, wantMaxOutput: 4096, wantBudget: 1024, wantIncludeThg: true},
		{name: "no ceiling uses capture", maxOutput: "", wantMaxOutput: 64000, wantBudget: 1024, wantIncludeThg: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"generationConfig":{` + tc.maxOutput + `"temperature":1},"contents":[]}`)
			out, err := wrapNativeV1Internal("proj", "claude-sonnet-4-6", body)
			if err != nil {
				t.Fatalf("wrap: %v", err)
			}
			gc := "request.generationConfig"
			if got := gjson.GetBytes(out, gc+".maxOutputTokens").Int(); got != tc.wantMaxOutput {
				t.Errorf("maxOutputTokens = %d, want %d: %s", got, tc.wantMaxOutput, out)
			}
			if got := gjson.GetBytes(out, gc+".thinkingConfig.thinkingBudget").Int(); got != tc.wantBudget {
				t.Errorf("thinkingBudget = %d, want %d: %s", got, tc.wantBudget, out)
			}
			if got := gjson.GetBytes(out, gc+".thinkingConfig.includeThoughts").Bool(); got != tc.wantIncludeThg {
				t.Errorf("includeThoughts = %v, want %v: %s", got, tc.wantIncludeThg, out)
			}
			// The caller's ceiling is never silently raised.
			if tc.wantMaxOutput < 64000 && gjson.GetBytes(out, gc+".maxOutputTokens").Int() > tc.wantMaxOutput {
				t.Errorf("caller maxOutputTokens was raised: %s", out)
			}
		})
	}

	// An explicit caller thinking budget is always preserved verbatim.
	explicit := []byte(`{"generationConfig":{"maxOutputTokens":8192,"thinkingConfig":{"thinkingBudget":4096}},"contents":[]}`)
	out, err := wrapNativeV1Internal("proj", "claude-opus-4-6-thinking", explicit)
	if err != nil {
		t.Fatalf("wrap explicit: %v", err)
	}
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingBudget").Int(); got != 4096 {
		t.Fatalf("explicit budget overwritten: %d (%s)", got, out)
	}
}

func TestNumericConfigValue(t *testing.T) {
	if got, ok := numericConfigValue(json.Number("1024")); !ok || got != 1024 {
		t.Errorf("json.Number = (%d, %v)", got, ok)
	}
	if got, ok := numericConfigValue(float64(2048)); !ok || got != 2048 {
		t.Errorf("float64 = (%d, %v)", got, ok)
	}
	if got, ok := numericConfigValue(4096); !ok || got != 4096 {
		t.Errorf("int = (%d, %v)", got, ok)
	}
	if _, ok := numericConfigValue("nope"); ok {
		t.Error("string must not parse as numeric config value")
	}
}

// The provider floors thinking.budget_tokens at 1024 (live-probed 2026-09-04:
// 128/256/512/1023 all 400, 1024 succeeds), so a caller ceiling at or below
// that cannot host thinking. That downgrade must be detectable rather than
// silent.
func TestClaudeThinkingSuppressionIsDetectable(t *testing.T) {
	cases := []struct {
		name          string
		body          string
		model         string
		wantSuppress  bool
		wantMaxTokens int
	}{
		{
			name:          "ceiling equals floor",
			body:          `{"generationConfig":{"maxOutputTokens":1024},"contents":[]}`,
			model:         "claude-sonnet-4-6",
			wantSuppress:  true,
			wantMaxTokens: 1024,
		},
		{
			name:          "ceiling below floor",
			body:          `{"generationConfig":{"maxOutputTokens":256},"contents":[]}`,
			model:         "claude-opus-4-6-thinking",
			wantSuppress:  true,
			wantMaxTokens: 256,
		},
		{
			name:         "ceiling above floor",
			body:         `{"generationConfig":{"maxOutputTokens":4096},"contents":[]}`,
			model:        "claude-sonnet-4-6",
			wantSuppress: false,
		},
		{
			name:         "explicit caller budget is never called suppressed",
			body:         `{"generationConfig":{"maxOutputTokens":512,"thinkingConfig":{"thinkingBudget":1024}},"contents":[]}`,
			model:        "claude-sonnet-4-6",
			wantSuppress: false,
		},
		{
			name:         "no ceiling means defaults apply",
			body:         `{"generationConfig":{},"contents":[]}`,
			model:        "claude-sonnet-4-6",
			wantSuppress: false,
		},
		{
			name:         "gemini is unaffected",
			body:         `{"generationConfig":{"maxOutputTokens":512},"contents":[]}`,
			model:        "gemini-3.8-flash-medium",
			wantSuppress: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			maxOut, suppressed := claudeThinkingSuppressedInBody([]byte(tc.body), tc.model)
			if suppressed != tc.wantSuppress {
				t.Fatalf("suppressed = %v, want %v", suppressed, tc.wantSuppress)
			}
			if tc.wantSuppress && maxOut != tc.wantMaxTokens {
				t.Fatalf("maxOutputTokens = %d, want %d", maxOut, tc.wantMaxTokens)
			}
		})
	}

	// The map-based predicate used by applyAgyDefaultsToInnerRequest must
	// agree with the body-based one used by the request path.
	inner := map[string]any{"generationConfig": map[string]any{"maxOutputTokens": 1024}}
	if _, suppressed := claudeThinkingSuppressed(inner, "claude-sonnet-4-6"); !suppressed {
		t.Fatal("map predicate disagrees with body predicate")
	}
}

// An explicit caller budget below the provider floor is left untouched so the
// provider's own precise error surfaces instead of a silent rewrite.
func TestWrapNativeV1Internal_ClaudeKeepsExplicitSubFloorBudget(t *testing.T) {
	body := []byte(`{"generationConfig":{"maxOutputTokens":8192,"thinkingConfig":{"thinkingBudget":512}},"contents":[]}`)
	out, err := wrapNativeV1Internal("proj", "claude-sonnet-4-6", body)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingBudget").Int(); got != 512 {
		t.Fatalf("explicit sub-floor budget was rewritten to %d: %s", got, out)
	}
}

// A permanent client-side 400 must not consume same-account retries: observed
// in production on 2026-09-04, a sub-floor thinking budget produced 3
// same-account retries and then 10 account switches on a request that could
// never succeed. Only version rejections may retry on the same account, which
// is exactly what ForwardGemini does.
func TestClaudeUpstreamFailureRetrySemantics(t *testing.T) {
	svc := &AntigravityNativeGatewayService{}
	account := &Account{ID: 7}
	invalidBudget := []byte(`{"error":{"code":400,"message":"{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"thinking.enabled.budget_tokens: Input should be greater than or equal to 1024\"}}","status":"INVALID_ARGUMENT"}}`)

	err := svc.claudeUpstreamFailure(context.Background(), account, "claude-sonnet-4-6",
		&http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{}}, invalidBudget)

	failover, ok := err.(*UpstreamFailoverError)
	if !ok {
		t.Fatalf("expected *UpstreamFailoverError, got %T", err)
	}
	if failover.RetryableOnSameAccount {
		t.Error("invalid-request 400 must not be retried on the same account")
	}
	if failover.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", failover.StatusCode)
	}
	// The provider's precise message must reach the client in Anthropic shape.
	var parsed struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if jsonErr := json.Unmarshal(failover.ResponseBody, &parsed); jsonErr != nil {
		t.Fatalf("response body is not JSON: %s", failover.ResponseBody)
	}
	if parsed.Type != "error" || parsed.Error.Type != "invalid_request_error" {
		t.Errorf("unexpected error envelope: %s", failover.ResponseBody)
	}
	if !strings.Contains(parsed.Error.Message, "1024") {
		t.Errorf("upstream detail lost, client cannot self-correct: %s", failover.ResponseBody)
	}

	// A 5xx is also not a same-account retry (only version rejection is).
	serverErr := svc.claudeUpstreamFailure(context.Background(), account, "claude-sonnet-4-6",
		&http.Response{StatusCode: http.StatusBadGateway, Header: http.Header{}}, []byte(`{"error":{"message":"boom"}}`))
	if fo, ok := serverErr.(*UpstreamFailoverError); !ok || fo.RetryableOnSameAccount {
		t.Errorf("502 must not set RetryableOnSameAccount: %#v", serverErr)
	}
}

// Payload-level 400s are permanent for every account, so they must be
// classified as terminal (no failover). Messages are the provider's own,
// observed live on 2026-09-04.
func TestClaudeRequestIsPermanentlyInvalid(t *testing.T) {
	promptTooLong := []byte(`{"error":{"code":400,"message":"{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"prompt is too long: 2917140 tokens > 1000000 maximum\"}}","status":"INVALID_ARGUMENT"}}`)
	subFloorBudget := []byte(`{"error":{"code":400,"message":"{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"thinking.enabled.budget_tokens: Input should be greater than or equal to 1024\"}}","status":"INVALID_ARGUMENT"}}`)
	maxTokensRule := []byte(`{"error":{"code":400,"message":"{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"` + "`max_tokens`" + ` must be greater than ` + "`thinking.budget_tokens`" + `\"}}","status":"INVALID_ARGUMENT"}}`)

	for name, body := range map[string][]byte{
		"prompt too long":  promptTooLong,
		"sub-floor budget": subFloorBudget,
		"max_tokens rule":  maxTokensRule,
	} {
		if !claudeRequestIsPermanentlyInvalid(body) {
			t.Errorf("%s: expected terminal classification", name)
		}
	}

	// Re-auth keeps its own account-specific handling and must NOT be terminal.
	reauth := []byte(`{"error":{"code":400,"message":"invalid_grant: Re-auth Required","status":"INVALID_ARGUMENT"}}`)
	if claudeRequestIsPermanentlyInvalid(reauth) {
		t.Error("re-auth 400 must stay non-terminal so the account can be paused/rotated")
	}
}

// The client must receive the provider's actionable text, not a generic error.
func TestClaudeUpstreamMessageUnwrapsProviderText(t *testing.T) {
	raw := []byte(`{"error":{"code":400,"message":"{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"prompt is too long: 2917140 tokens > 1000000 maximum\"}}","status":"INVALID_ARGUMENT"}}`)
	got := claudeUpstreamMessage(raw, http.StatusBadRequest)
	if !strings.Contains(got, "1000000 maximum") {
		t.Fatalf("provider detail lost: %q", got)
	}

	if got := claudeUpstreamMessage(nil, http.StatusBadGateway); !strings.Contains(got, "502") {
		t.Fatalf("fallback message missing status: %q", got)
	}
}
