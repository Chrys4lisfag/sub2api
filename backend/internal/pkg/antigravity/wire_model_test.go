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
		{"models/gemini-3.5-flash-high", "high"},
		{"gemini-3.5-flash", ""},      // base flash has no implicit default
		{"gemini-3-pro-high", ""},     // not a 3.5 variant
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
