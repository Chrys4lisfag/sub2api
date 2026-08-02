package antigravity

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestAntigravityWireModel(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// Public → wire mappings, verified empirically against
		// daily-cloudcode-pa.sandbox.googleapis.com via probe of
		// /v1internal:fetchAvailableModels (May 2026 snapshot).
		{"gemini-3.5-flash-high", "gemini-3-flash-agent"},
		{"gemini-3.5-flash", "gemini-3.5-flash-low"},
		{"gemini-3.5-flash-medium", "gemini-3.5-flash-low"},
		// Public "Low" → wire "extra-low" (else backend serves Medium tier).
		{"gemini-3.5-flash-low", "gemini-3.5-flash-extra-low"},
		// Gemini 3.6 Flash (2026-07-21) — wire names are identity (no
		// low↔extra-low trap like 3.5). Base ID routes to -medium.
		{"gemini-3.6-flash-high", "gemini-3.6-flash-high"},
		{"gemini-3.6-flash-medium", "gemini-3.6-flash-medium"},
		{"gemini-3.6-flash-low", "gemini-3.6-flash-low"},
		{"gemini-3.6-flash-tiered", "gemini-3.6-flash-tiered"},
		{"gemini-3.6-flash", "gemini-3.6-flash-medium"},
		{"models/gemini-3.6-flash-high", "gemini-3.6-flash-high"},
		{"GEMINI-3.6-FLASH-LOW", "gemini-3.6-flash-low"},
		// 3 Flash legacy variants → base 3-flash.
		{"gemini-3-flash-high", "gemini-3-flash"},
		{"gemini-3-flash-medium", "gemini-3-flash"},
		{"gemini-3-flash-low", "gemini-3-flash"},
		// 3.1 Pro: high needs wire alias "gemini-pro-agent" (literal name → 400).
		{"gemini-3.1-pro-high", "gemini-pro-agent"},
		{"gemini-3.1-pro-low", "gemini-3.1-pro-low"}, // direct, passthrough
		// Gemini 3 Pro is deprecated server-side. We REMOVED it from the
		// public model list rather than auto-rewriting, so any caller still
		// sending a 3-pro-* name gets the literal name passed through; the
		// upstream "no longer available — switch to 3.1 Pro" error then
		// surfaces verbatim. Better than silently serving a different tier.
		{"gemini-3-pro-high", "gemini-3-pro-high"},
		{"gemini-3-pro", "gemini-3-pro"},
		{"gemini-3-pro-low", "gemini-3-pro-low"},
		{"gemini-3-pro-preview", "gemini-3-pro-preview"},
		// `models/` prefix normalization.
		{"models/gemini-3.5-flash-high", "gemini-3-flash-agent"},
		{"models/gemini-3.1-pro-high", "gemini-pro-agent"},
		// Case insensitive on input, returns canonical wire name.
		{"Gemini-3.5-Flash-High", "gemini-3-flash-agent"},
		{"GEMINI-3.1-PRO-HIGH", "gemini-pro-agent"},
		// Pass-through for non-mapped names.
		{"claude-opus-4-7", "claude-opus-4-7"},
		{"claude-sonnet-4-6", "claude-sonnet-4-6"},
		{"gemini-pro-agent", "gemini-pro-agent"}, // already wire — idempotent
		{"gemini-3-flash", "gemini-3-flash"},     // already wire — idempotent
		{"", ""},
	}
	for _, tc := range cases {
		if got := AntigravityWireModel(tc.in); got != tc.want {
			t.Errorf("AntigravityWireModel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestDefaultVariantThinkingLevel(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"gemini-3.5-flash-high", "high"},
		{"gemini-3.5-flash-medium", "medium"},
		{"gemini-3.6-flash-high", "high"},
		{"gemini-3.6-flash-medium", "medium"},
		{"gemini-3.5-flash-low", "low"},
		{"gemini-3.6-flash-low", "low"},
		{"gemini-3.6-flash", ""}, // base has no implicit default
		{"models/gemini-3.5-flash-high", "high"},
		{"gemini-3.5-flash", ""},  // base flash has no implicit default
		{"gemini-3-pro-high", ""}, // not a 3.5 variant
		{"claude-opus-4-7", ""},
		{"", ""},
	}
	for _, tc := range cases {
		if got := DefaultVariantThinkingLevel(tc.in); got != tc.want {
			t.Errorf("DefaultVariantThinkingLevel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestApplyWireModelToBody_RewritesModelField(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-high","request":{"contents":[]}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "model").String(); got != "gemini-3-flash-agent" {
		t.Fatalf("model not rewritten: %s", out)
	}
}

func TestApplyWireModelToBody_AddsThinkingLevelForHigh(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-high","request":{"generationConfig":{}}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingLevel").String(); got != "high" {
		t.Fatalf("thinkingLevel not injected: %s", out)
	}
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.includeThoughts").Bool(); !got {
		t.Fatalf("includeThoughts not set: %s", out)
	}
}

func TestApplyWireModelToBody_AddsThinkingLevelToBareAndSDKShapes(t *testing.T) {
	cases := []struct{ name, body, levelPath string }{
		{"bare REST", `{"model":"gemini-3.5-flash-high","generationConfig":{}}`, "generationConfig.thinkingConfig.thinkingLevel"},
		{"SDK config", `{"model":"gemini-3.5-flash-high","config":{}}`, "config.thinkingConfig.thinkingLevel"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := ApplyWireModelToBody([]byte(tc.body))
			if got := gjson.GetBytes(out, tc.levelPath).String(); got != "high" {
				t.Fatalf("thinkingLevel at %s: got %q in %s", tc.levelPath, got, out)
			}
			if gjson.GetBytes(out, "request").Exists() {
				t.Fatalf("unexpected request wrapper added: %s", out)
			}
		})
	}
}

func TestApplyWireModelToBody_RespectsBareExplicitThinkingLevel(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-high","generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "generationConfig.thinkingConfig.thinkingLevel").String(); got != "low" {
		t.Fatalf("bare explicit thinkingLevel clobbered: %s", out)
	}
	if gjson.GetBytes(out, "request").Exists() {
		t.Fatalf("duplicate wrapped thinking config added: %s", out)
	}
}

func TestApplyWireModelToBody_AddsThinkingLevelForMedium(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-medium","request":{}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingLevel").String(); got != "medium" {
		t.Fatalf("thinkingLevel not injected: %s", out)
	}
}

func TestApplyWireModelToBody_RespectsExplicitThinkingLevel(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-high","request":{"generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingLevel").String(); got != "low" {
		t.Fatalf("explicit thinkingLevel was clobbered: got %q want low", got)
	}
}

func TestApplyWireModelToBody_RespectsExplicitThinkingBudget(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-high","request":{"generationConfig":{"thinkingConfig":{"thinkingBudget":512}}}}`)
	out := ApplyWireModelToBody(body)
	// thinkingLevel should NOT be added when budget is explicit.
	if gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingLevel").Exists() {
		t.Fatalf("thinkingLevel injected despite explicit thinkingBudget: %s", out)
	}
}

func TestApplyWireModelToBody_PassthroughForUnknown(t *testing.T) {
	body := []byte(`{"model":"claude-opus-4-7","request":{}}`)
	out := ApplyWireModelToBody(body)
	if string(out) != string(body) {
		t.Fatalf("body mutated unexpectedly: in=%s out=%s", body, out)
	}
}

func TestApplyWireModelToBody_PassthroughForBodyWithoutModel(t *testing.T) {
	body := []byte(`{"metadata":{"ideType":"ANTIGRAVITY"}}`)
	out := ApplyWireModelToBody(body)
	if string(out) != string(body) {
		t.Fatalf("body mutated for model-less payload: in=%s out=%s", body, out)
	}
}

func TestApplyWireModelToBody_PassthroughForInvalidJSON(t *testing.T) {
	body := []byte(`not json`)
	out := ApplyWireModelToBody(body)
	if string(out) != string(body) {
		t.Fatalf("invalid JSON mutated: in=%s out=%s", body, out)
	}
}

func TestExtractSessionID(t *testing.T) {
	body := []byte(`{"request":{"sessionId":"abc123  "}}`)
	if got := ExtractSessionID(body); got != "abc123" {
		t.Fatalf("ExtractSessionID got %q want abc123", got)
	}
	if got := ExtractSessionID([]byte(`{"request":{}}`)); got != "" {
		t.Fatalf("ExtractSessionID returned %q for empty session", got)
	}
	if got := ExtractSessionID(nil); got != "" {
		t.Fatalf("ExtractSessionID returned %q for nil", got)
	}
}

// ---------------------------------------------------------------------------
// ResolveWireFromBody
// ---------------------------------------------------------------------------

func TestResolveWireFromBody_BaseFlashPicksWireFromThinkingLevel(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantWire string
	}{
		// pre-wrap body (Gemini-format, used by native gateway pre-envelope)
		{
			name:     "high → flash-agent (pre-wrap)",
			body:     `{"model":"gemini-3.5-flash","generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`,
			wantWire: "gemini-3-flash-agent",
		},
		{
			name:     "medium → flash-low (pre-wrap)",
			body:     `{"model":"gemini-3.5-flash","generationConfig":{"thinkingConfig":{"thinkingLevel":"medium"}}}`,
			wantWire: "gemini-3.5-flash-low",
		},
		{
			name:     "low → extra-low (pre-wrap)",
			body:     `{"model":"gemini-3.5-flash","generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`,
			wantWire: "gemini-3.5-flash-extra-low",
		},
		{
			name:     "minimal → extra-low (pre-wrap)",
			body:     `{"model":"gemini-3.5-flash","generationConfig":{"thinkingConfig":{"thinkingLevel":"minimal"}}}`,
			wantWire: "gemini-3.5-flash-extra-low",
		},
		// post-wrap body (v1internal envelope, used by legacy ApplyWireModelToBody)
		{
			name:     "high → flash-agent (post-wrap)",
			body:     `{"model":"gemini-3.5-flash","request":{"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}}`,
			wantWire: "gemini-3-flash-agent",
		},
		{
			name:     "snake_case key honored",
			body:     `{"model":"gemini-3.5-flash","request":{"generationConfig":{"thinkingConfig":{"thinking_level":"high"}}}}`,
			wantWire: "gemini-3-flash-agent",
		},
		// absent → mid (matches AntigravityWireModel default)
		{
			name:     "no thinkingLevel → mid wire",
			body:     `{"model":"gemini-3.5-flash","generationConfig":{}}`,
			wantWire: "gemini-3.5-flash-low",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveWireFromBody("gemini-3.5-flash", []byte(tc.body))
			if got != tc.wantWire {
				t.Errorf("got %q want %q", got, tc.wantWire)
			}
		})
	}
}

func TestResolveWireFromBody_SuffixedVariantsIgnoreBodyLevel(t *testing.T) {
	// -high variant must keep mapping to flash-agent regardless of body.
	body := []byte(`{"model":"gemini-3.5-flash-high","generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`)
	if got := ResolveWireFromBody("gemini-3.5-flash-high", body); got != "gemini-3-flash-agent" {
		t.Errorf("suffix should win: got %q want gemini-3-flash-agent", got)
	}
	// -low variant must keep mapping to extra-low regardless of body.
	body = []byte(`{"model":"gemini-3.5-flash-low","generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`)
	if got := ResolveWireFromBody("gemini-3.5-flash-low", body); got != "gemini-3.5-flash-extra-low" {
		t.Errorf("suffix should win: got %q want gemini-3.5-flash-extra-low", got)
	}
}

func TestResolveSemanticRetryModel_LowersExplicitTierAliases(t *testing.T) {
	cases := []struct{ name, model, level, want string }{
		{name: "3.6 high to medium", model: "gemini-3.6-flash-high", level: "MEDIUM", want: "gemini-3.6-flash-medium"},
		{name: "3.6 medium to low", model: "gemini-3.6-flash-medium", level: "LOW", want: "gemini-3.6-flash-low"},
		{name: "3.6 low stays low", model: "gemini-3.6-flash-low", level: "LOW", want: "gemini-3.6-flash-low"},
		{name: "3.5 high to medium", model: "gemini-3.5-flash-high", level: "MEDIUM", want: "gemini-3.5-flash-medium"},
		{name: "3.5 medium to low", model: "gemini-3.5-flash-medium", level: "LOW", want: "gemini-3.5-flash-low"},
		{name: "3.5 wire high to medium", model: "gemini-3-flash-agent", level: "MEDIUM", want: "gemini-3.5-flash-medium"},
		{name: "3.1 high stays high at medium", model: "gemini-3.1-pro-high", level: "MEDIUM", want: "gemini-3.1-pro-high"},
		{name: "3.1 high to low", model: "gemini-3.1-pro-high", level: "LOW", want: "gemini-3.1-pro-low"},
		{name: "3.1 wire high to low", model: "gemini-pro-agent", level: "LOW", want: "gemini-3.1-pro-low"},
		{name: "suffixless unchanged", model: "gemini-3.6-flash", level: "MEDIUM", want: "gemini-3.6-flash"},
		{name: "unknown unchanged", model: "custom-model", level: "LOW", want: "custom-model"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"` + tc.level + `"}}}`)
			if got := ResolveSemanticRetryModel(tc.model, body); got != tc.want {
				t.Fatalf("ResolveSemanticRetryModel(%q, %s) = %q, want %q", tc.model, body, got, tc.want)
			}
		})
	}
}

func TestResolveWireFromBody_36FlashPicksWireFromThinkingLevel(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantWire string
	}{
		// pre-wrap body (Gemini-format)
		{
			name:     "3.6 high → -high",
			body:     `{"model":"gemini-3.6-flash","generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`,
			wantWire: "gemini-3.6-flash-high",
		},
		{
			name:     "3.6 high from OMP SDK config → -high",
			body:     `{"model":"gemini-3.6-flash","config":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}`,
			wantWire: "gemini-3.6-flash-high",
		},
		{
			name:     "3.6 medium → -medium",
			body:     `{"model":"gemini-3.6-flash","generationConfig":{"thinkingConfig":{"thinkingLevel":"medium"}}}`,
			wantWire: "gemini-3.6-flash-medium",
		},
		{
			name:     "3.6 low → -low (NO extra-low trap, unlike 3.5)",
			body:     `{"model":"gemini-3.6-flash","generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`,
			wantWire: "gemini-3.6-flash-low",
		},
		{
			name:     "3.6 minimal → -low",
			body:     `{"model":"gemini-3.6-flash","generationConfig":{"thinkingConfig":{"thinkingLevel":"minimal"}}}`,
			wantWire: "gemini-3.6-flash-low",
		},
		// post-wrap body
		{
			name:     "3.6 HIGH uppercase (post-wrap)",
			body:     `{"model":"gemini-3.6-flash","request":{"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}}`,
			wantWire: "gemini-3.6-flash-high",
		},
		{
			name:     "3.6 snake_case key honored",
			body:     `{"model":"gemini-3.6-flash","request":{"generationConfig":{"thinkingConfig":{"thinking_level":"low"}}}}`,
			wantWire: "gemini-3.6-flash-low",
		},
		// absent → medium (matches suffixless base default)
		{
			name:     "3.6 no thinkingLevel → -medium",
			body:     `{"model":"gemini-3.6-flash","generationConfig":{}}`,
			wantWire: "gemini-3.6-flash-medium",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveWireFromBody("gemini-3.6-flash", []byte(tc.body))
			if got != tc.wantWire {
				t.Errorf("got %q want %q", got, tc.wantWire)
			}
		})
	}
}

func TestResolveWireFromBody_36NoBodyFallsBackToMedium(t *testing.T) {
	if got := ResolveWireFromBody("gemini-3.6-flash", nil); got != "gemini-3.6-flash-medium" {
		t.Errorf("nil body: got %q want gemini-3.6-flash-medium", got)
	}
	if got := ResolveWireFromBody("gemini-3.6-flash", []byte(`not json`)); got != "gemini-3.6-flash-medium" {
		t.Errorf("invalid json: got %q want gemini-3.6-flash-medium", got)
	}
}

func TestResolveWireFromBody_36SuffixedIgnoresBodyLevel(t *testing.T) {
	// -high variant must pin -high regardless of body override.
	body := []byte(`{"model":"gemini-3.6-flash-high","generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`)
	if got := ResolveWireFromBody("gemini-3.6-flash-high", body); got != "gemini-3.6-flash-high" {
		t.Errorf("suffix should win: got %q want gemini-3.6-flash-high", got)
	}
}

func TestResolveWireFromBody_NoBodyFallsBackToWireModel(t *testing.T) {
	if got := ResolveWireFromBody("gemini-3.5-flash", nil); got != "gemini-3.5-flash-low" {
		t.Errorf("nil body fallback: got %q", got)
	}
	if got := ResolveWireFromBody("gemini-3.5-flash", []byte(`not json`)); got != "gemini-3.5-flash-low" {
		t.Errorf("invalid json fallback: got %q", got)
	}
}

func TestApplyWireModelToBody_BaseFlashRoutesByThinkingLevel(t *testing.T) {
	// High in body should rewrite both the model field AND keep the level
	// the caller set so the backend sees a coherent (model, thinkingLevel) pair.
	body := []byte(`{"model":"gemini-3.5-flash","request":{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "model").String(); got != "gemini-3-flash-agent" {
		t.Errorf("model not rewritten: %s", out)
	}
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingLevel").String(); got != "high" {
		t.Errorf("explicit thinkingLevel was clobbered: %s", out)
	}
}

// TestResolveWireFromBody_ProSlider locks the Gemini 3.1 Pro slider
// dispatch: a single suffixless picker entry whose body level picks
// between the two real agy Pro variants.
func TestResolveWireFromBody_ProSlider(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantWire string
	}{
		{
			name:     "low → gemini-3.1-pro-low",
			body:     `{"model":"gemini-3.1-pro","generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`,
			wantWire: "gemini-3.1-pro-low",
		},
		{
			name:     "minimal → gemini-3.1-pro-low",
			body:     `{"model":"gemini-3.1-pro","generationConfig":{"thinkingConfig":{"thinkingLevel":"minimal"}}}`,
			wantWire: "gemini-3.1-pro-low",
		},
		{
			name:     "medium → gemini-pro-agent (rounds up; agy has no medium pro)",
			body:     `{"model":"gemini-3.1-pro","generationConfig":{"thinkingConfig":{"thinkingLevel":"medium"}}}`,
			wantWire: "gemini-pro-agent",
		},
		{
			name:     "high → gemini-pro-agent",
			body:     `{"model":"gemini-3.1-pro","generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`,
			wantWire: "gemini-pro-agent",
		},
		{
			name:     "empty level → default High (gemini-pro-agent)",
			body:     `{"model":"gemini-3.1-pro"}`,
			wantWire: "gemini-pro-agent",
		},
		{
			name:     "no body → default High",
			body:     ``,
			wantWire: "gemini-pro-agent",
		},
		{
			name:     "wrapped form (request.generationConfig)",
			body:     `{"request":{"model":"gemini-3.1-pro","generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}}`,
			wantWire: "gemini-3.1-pro-low",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveWireFromBody("gemini-3.1-pro", []byte(tc.body))
			if got != tc.wantWire {
				t.Errorf("got %q want %q", got, tc.wantWire)
			}
		})
	}
}

// TestResolveWireFromBody_ProSuffixedVariantsIgnoreBodyLevel — suffixed
// picker entries (existing back-compat path) should still resolve via
// their suffix, ignoring any body level.
func TestResolveWireFromBody_ProSuffixedVariantsIgnoreBodyLevel(t *testing.T) {
	// -high suffix wins regardless of body level.
	body := []byte(`{"model":"gemini-3.1-pro-high","generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`)
	if got := ResolveWireFromBody("gemini-3.1-pro-high", body); got != "gemini-pro-agent" {
		t.Errorf("suffix should win: got %q want gemini-pro-agent", got)
	}
	// -low suffix is direct passthrough.
	body = []byte(`{"model":"gemini-3.1-pro-low","generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`)
	if got := ResolveWireFromBody("gemini-3.1-pro-low", body); got != "gemini-3.1-pro-low" {
		t.Errorf("suffix should win: got %q want gemini-3.1-pro-low", got)
	}
}
