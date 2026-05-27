package antigravity

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestAntigravityWireModel(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// Public → wire mappings from CLIProxyAPI PR #3490.
		{"gemini-3.5-flash-high", "gemini-3-flash-agent"},
		{"gemini-3.5-flash", "gemini-3.5-flash-low"},
		{"gemini-3.5-flash-medium", "gemini-3.5-flash-low"},
		{"gemini-3-flash-high", "gemini-3-flash"},
		{"gemini-3-flash-medium", "gemini-3-flash"},
		{"gemini-3-flash-low", "gemini-3-flash"},
		// `models/` prefix normalization.
		{"models/gemini-3.5-flash-high", "gemini-3-flash-agent"},
		// Case insensitive on input, returns canonical wire name.
		{"Gemini-3.5-Flash-High", "gemini-3-flash-agent"},
		// Pass-through for non-3.x or already-wire names.
		{"gemini-3-pro-high", "gemini-3-pro-high"},
		{"gemini-3.1-pro-preview", "gemini-3.1-pro-preview"},
		{"claude-opus-4-7", "claude-opus-4-7"},
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
