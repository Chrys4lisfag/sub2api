package antigravity

import (
	"strings"
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
		// Gemini 3.7 Flash — exact identity wire IDs.
		{"gemini-3.7-flash-high", "gemini-3.7-flash-high"},
		{"gemini-3.7-flash-medium", "gemini-3.7-flash-medium"},
		{"gemini-3.7-flash-low", "gemini-3.7-flash-low"},
		{"models/gemini-3.7-flash-high", "gemini-3.7-flash-high"},
		{"GEMINI-3.7-FLASH-LOW", "gemini-3.7-flash-low"},
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
		{"gemini-3.7-flash-high", ""},
		{"gemini-3.7-flash-medium", ""},
		{"gemini-3.7-flash-low", ""},
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

func TestDefaultVariantThinkingBudget(t *testing.T) {
	cases := []struct {
		in         string
		wantBudget int
		wantOK     bool
	}{
		{"gemini-3.7-flash-high", -1, true},
		{"gemini-3.7-flash-medium", 4000, true},
		{"gemini-3.7-flash-low", 1000, true},
		{"models/gemini-3.7-flash-medium", 4000, true},
		{"GEMINI-3.7-FLASH-LOW", 1000, true},
		{"gemini-3.7-flash", 0, false},
		{"gemini-3.6-flash-medium", 0, false},
	}
	for _, tc := range cases {
		budget, ok := DefaultVariantThinkingBudget(tc.in)
		if budget != tc.wantBudget || ok != tc.wantOK {
			t.Errorf("DefaultVariantThinkingBudget(%q) = (%d, %v), want (%d, %v)", tc.in, budget, ok, tc.wantBudget, tc.wantOK)
		}
	}
}

func TestResolveWireFromBody_Gemini37VirtualAlias(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "bare high", body: `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}`, want: "gemini-3.7-flash-high"},
		{name: "sdk medium", body: `{"config":{"thinkingConfig":{"thinkingLevel":"medium"}}}`, want: "gemini-3.7-flash-medium"},
		{name: "snake low", body: `{"generationConfig":{"thinkingConfig":{"thinking_level":"low"}}}`, want: "gemini-3.7-flash-low"},
		{name: "wrapped minimal", body: `{"request":{"config":{"thinkingConfig":{"thinkingLevel":"minimal"}}}}`, want: "gemini-3.7-flash-low"},
		{name: "missing defaults medium", body: `{"contents":[]}`, want: "gemini-3.7-flash-medium"},
		{name: "unknown defaults medium", body: `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"unexpected"}}}`, want: "gemini-3.7-flash-medium"},
		{name: "invalid json defaults medium", body: `{`, want: "gemini-3.7-flash-medium"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveWireFromBody("gemini-3.7-flash", []byte(tc.body)); got != tc.want {
				t.Fatalf("wire model = %q, want %q", got, tc.want)
			}
		})
	}

	pinned := []byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`)
	if got := ResolveWireFromBody("gemini-3.7-flash-high", pinned); got != "gemini-3.7-flash-high" {
		t.Fatalf("explicit tier was rerouted: %q", got)
	}
}

func TestNormalizeGemini37BaseRequestBody(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wire       string
		path       string
		wantBudget int64
	}{
		{name: "bare low", body: `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"low","thinking_budget":99}},"value":9007199254740993}`, wire: "gemini-3.7-flash-low", path: "generationConfig.thinkingConfig", wantBudget: 1000},
		{name: "sdk medium", body: `{"config":{"thinkingConfig":{"thinking_level":"medium","thinkingBudget":99}}}`, wire: "gemini-3.7-flash-medium", path: "config.thinkingConfig", wantBudget: 4000},
		{name: "wrapped high", body: `{"request":{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}}`, wire: "gemini-3.7-flash-high", path: "request.generationConfig.thinkingConfig", wantBudget: -1},
		{name: "wrapped sdk low", body: `{"request":{"config":{"thinkingConfig":{"thinking_level":"low"}}}}`, wire: "gemini-3.7-flash-low", path: "request.config.thinkingConfig", wantBudget: 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := NormalizeGemini37BaseRequestBody([]byte(tc.body), tc.wire)
			if got := gjson.GetBytes(out, tc.path+".thinkingBudget").Int(); got != tc.wantBudget {
				t.Fatalf("thinkingBudget = %d, want %d: %s", got, tc.wantBudget, out)
			}
			if !gjson.GetBytes(out, tc.path+".includeThoughts").Bool() {
				t.Fatalf("includeThoughts missing: %s", out)
			}
			for _, base := range []string{"generationConfig.thinkingConfig", "config.thinkingConfig", "request.generationConfig.thinkingConfig", "request.config.thinkingConfig"} {
				for _, key := range []string{"thinkingLevel", "thinking_level", "thinking_budget"} {
					if gjson.GetBytes(out, base+"."+key).Exists() {
						t.Fatalf("stale %s remains: %s", base+"."+key, out)
					}
				}
			}
			if strings.Contains(tc.body, "9007199254740993") && !strings.Contains(string(out), `"value":9007199254740993`) {
				t.Fatalf("large integer changed: %s", out)
			}
		})
	}
}

func TestApplyWireModelToBody_Gemini37VirtualAlias(t *testing.T) {
	body := []byte(`{"model":"gemini-3.7-flash","config":{"thinkingConfig":{"thinkingLevel":"high"}}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "model").String(); got != "gemini-3.7-flash-high" {
		t.Fatalf("model = %q, want high tier: %s", got, out)
	}
	if got := gjson.GetBytes(out, "config.thinkingConfig.thinkingBudget").Int(); got != -1 {
		t.Fatalf("budget = %d, want -1: %s", got, out)
	}
	if gjson.GetBytes(out, "config.thinkingConfig.thinkingLevel").Exists() {
		t.Fatalf("thinkingLevel reached wire body: %s", out)
	}

	defaulted := ApplyWireModelToBody([]byte(`{"model":"gemini-3.7-flash","contents":[]}`))
	if got := gjson.GetBytes(defaulted, "model").String(); got != "gemini-3.7-flash-medium" {
		t.Fatalf("default model = %q, want medium: %s", got, defaulted)
	}
	if got := gjson.GetBytes(defaulted, "generationConfig.thinkingConfig.thinkingBudget").Int(); got != 4000 {
		t.Fatalf("default budget = %d, want 4000: %s", got, defaulted)
	}
}

func TestAntigravityWireModel_Gemini38Tiers(t *testing.T) {
	cases := map[string]string{
		"gemini-3.8-flash-high":        "gemini-3.8-flash-high",
		"gemini-3.8-flash-medium":      "gemini-3.8-flash-medium",
		"gemini-3.8-flash-low":         "gemini-3.8-flash-low",
		"models/gemini-3.8-flash-high": "gemini-3.8-flash-high",
		"GEMINI-3.8-FLASH-LOW":         "gemini-3.8-flash-low",
		"gemini-3.8-flash":             "gemini-3.8-flash-medium",
	}
	for in, want := range cases {
		if got := AntigravityWireModel(in); got != want {
			t.Errorf("AntigravityWireModel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResolveWireFromBody_Gemini38VirtualAlias(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "bare high", body: `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}`, want: "gemini-3.8-flash-high"},
		{name: "sdk medium", body: `{"config":{"thinkingConfig":{"thinkingLevel":"medium"}}}`, want: "gemini-3.8-flash-medium"},
		{name: "snake low", body: `{"generationConfig":{"thinkingConfig":{"thinking_level":"low"}}}`, want: "gemini-3.8-flash-low"},
		{name: "wrapped minimal", body: `{"request":{"config":{"thinkingConfig":{"thinkingLevel":"minimal"}}}}`, want: "gemini-3.8-flash-low"},
		{name: "missing defaults medium", body: `{"contents":[]}`, want: "gemini-3.8-flash-medium"},
		{name: "unknown defaults medium", body: `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"unexpected"}}}`, want: "gemini-3.8-flash-medium"},
		{name: "invalid json defaults medium", body: `{`, want: "gemini-3.8-flash-medium"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveWireFromBody("gemini-3.8-flash", []byte(tc.body)); got != tc.want {
				t.Fatalf("wire model = %q, want %q", got, tc.want)
			}
		})
	}

	pinned := []byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`)
	if got := ResolveWireFromBody("gemini-3.8-flash-high", pinned); got != "gemini-3.8-flash-high" {
		t.Fatalf("explicit 3.8 tier was rerouted: %q", got)
	}
}

func TestDefaultVariantThinkingBudget_Gemini38(t *testing.T) {
	cases := []struct {
		in         string
		wantBudget int
		wantOK     bool
	}{
		{"gemini-3.8-flash-high", -1, true},
		{"gemini-3.8-flash-medium", 4000, true},
		{"gemini-3.8-flash-low", 1000, true},
		{"models/gemini-3.8-flash-medium", 4000, true},
		{"GEMINI-3.8-FLASH-LOW", 1000, true},
		{"gemini-3.8-flash", 0, false},
	}
	for _, tc := range cases {
		budget, ok := DefaultVariantThinkingBudget(tc.in)
		if budget != tc.wantBudget || ok != tc.wantOK {
			t.Errorf("DefaultVariantThinkingBudget(%q) = (%d, %v), want (%d, %v)", tc.in, budget, ok, tc.wantBudget, tc.wantOK)
		}
	}
}

func TestApplyWireModelToBody_Gemini38VirtualAlias(t *testing.T) {
	body := []byte(`{"model":"gemini-3.8-flash","config":{"thinkingConfig":{"thinkingLevel":"high"}}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "model").String(); got != "gemini-3.8-flash-high" {
		t.Fatalf("model = %q, want high tier: %s", got, out)
	}
	if got := gjson.GetBytes(out, "config.thinkingConfig.thinkingBudget").Int(); got != -1 {
		t.Fatalf("budget = %d, want -1: %s", got, out)
	}
	if gjson.GetBytes(out, "config.thinkingConfig.thinkingLevel").Exists() {
		t.Fatalf("thinkingLevel reached wire body: %s", out)
	}

	defaulted := ApplyWireModelToBody([]byte(`{"model":"gemini-3.8-flash","contents":[]}`))
	if got := gjson.GetBytes(defaulted, "model").String(); got != "gemini-3.8-flash-medium" {
		t.Fatalf("default model = %q, want medium: %s", got, defaulted)
	}
	if got := gjson.GetBytes(defaulted, "generationConfig.thinkingConfig.thinkingBudget").Int(); got != 4000 {
		t.Fatalf("default budget = %d, want 4000: %s", got, defaulted)
	}
}

func TestGemini38SemanticRetryLadder(t *testing.T) {
	medium, ok := LowerNumericBudgetTierOnce("models/GEMINI-3.8-FLASH-HIGH")
	if !ok || medium != "gemini-3.8-flash-medium" {
		t.Fatalf("high lower = (%q, %v), want medium", medium, ok)
	}
	low, ok := LowerNumericBudgetTierOnce(medium)
	if !ok || low != "gemini-3.8-flash-low" {
		t.Fatalf("medium lower = (%q, %v), want low", low, ok)
	}
	if got, ok := LowerNumericBudgetTierOnce(low); ok || got != low {
		t.Fatalf("low lower = (%q, %v), want unchanged", got, ok)
	}

	body := []byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"MEDIUM","thinking_budget":-1}}}`)
	out := ApplyGemini37RetryThinkingBudget(body, medium)
	if got := gjson.GetBytes(out, "generationConfig.thinkingConfig.thinkingBudget").Int(); got != 4000 {
		t.Fatalf("retry budget = %d, want 4000: %s", got, out)
	}
	if gjson.GetBytes(out, "generationConfig.thinkingConfig.thinkingLevel").Exists() {
		t.Fatalf("stale thinkingLevel remains: %s", out)
	}

	if got := ResolveSemanticRetryModel("gemini-3.8-flash-high", []byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`)); got != "gemini-3.8-flash-low" {
		t.Fatalf("retry model = %q, want gemini-3.8-flash-low", got)
	}
	if got := ResolveSemanticRetryModel("gemini-3.8-flash-medium", []byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"minimal"}}}`)); got != "gemini-3.8-flash-low" {
		t.Fatalf("retry model = %q, want gemini-3.8-flash-low", got)
	}
}

func TestIsNumericBudgetVirtualAlias(t *testing.T) {
	for _, in := range []string{"gemini-3.7-flash", "gemini-3.8-flash", "models/GEMINI-3.8-FLASH"} {
		if !IsNumericBudgetVirtualAlias(in) {
			t.Errorf("IsNumericBudgetVirtualAlias(%q) = false, want true", in)
		}
	}
	for _, in := range []string{"gemini-3.8-flash-high", "gemini-3.6-flash", "gemini-3.5-flash", ""} {
		if IsNumericBudgetVirtualAlias(in) {
			t.Errorf("IsNumericBudgetVirtualAlias(%q) = true, want false", in)
		}
	}
}

func TestGemini37SemanticRetryBudget(t *testing.T) {
	medium, ok := LowerGemini37TierOnce("models/GEMINI-3.7-FLASH-HIGH")
	if !ok || medium != "gemini-3.7-flash-medium" {
		t.Fatalf("high lower = (%q, %v), want medium", medium, ok)
	}
	low, ok := LowerGemini37TierOnce(medium)
	if !ok || low != "gemini-3.7-flash-low" {
		t.Fatalf("medium lower = (%q, %v), want low", low, ok)
	}
	if got, ok := LowerGemini37TierOnce(low); ok || got != low {
		t.Fatalf("low lower = (%q, %v), want unchanged", got, ok)
	}

	body := []byte(`{"request":{"generationConfig":{"thinkingConfig":{"thinkingLevel":"MEDIUM","thinking_budget":-1}},"contents":[{"parts":[{"functionCall":{"args":{"large":9007199254740993}}}]}]}}`)
	out := ApplyGemini37RetryThinkingBudget(body, medium)
	thinking := "request.generationConfig.thinkingConfig"
	if got := gjson.GetBytes(out, thinking+".thinkingBudget").Int(); got != 4000 {
		t.Fatalf("retry budget = %d, want 4000: %s", got, out)
	}
	if !gjson.GetBytes(out, thinking+".includeThoughts").Bool() {
		t.Fatalf("includeThoughts missing: %s", out)
	}
	if gjson.GetBytes(out, thinking+".thinkingLevel").Exists() || gjson.GetBytes(out, thinking+".thinking_budget").Exists() {
		t.Fatalf("stale thinking directive remains: %s", out)
	}
	if !strings.Contains(string(out), `"large":9007199254740993`) {
		t.Fatalf("large integer changed: %s", out)
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
	body := []byte(`{"model":"gemini-3.5-flash-high","request":{"contents":[]}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingLevel").String(); got != "high" {
		t.Fatalf("thinkingLevel not injected: %s", out)
	}
}

func TestApplyWireModelToBody_AddsThinkingLevelToBareAndSDKShapes(t *testing.T) {
	cases := []struct {
		name string
		body string
		path string
	}{
		{
			name: "bare REST shape",
			body: `{"model":"gemini-3.5-flash-high","contents":[]}`,
			path: "generationConfig.thinkingConfig.thinkingLevel",
		},
		{
			name: "OMP SDK shape",
			body: `{"model":"gemini-3.5-flash-high","config":{}}`,
			path: "config.thinkingConfig.thinkingLevel",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := ApplyWireModelToBody([]byte(tc.body))
			if got := gjson.GetBytes(out, tc.path).String(); got != "high" {
				t.Fatalf("thinkingLevel at %s: got %q want high, output: %s", tc.path, got, out)
			}
		})
	}
}

func TestApplyWireModelToBody_RespectsBareExplicitThinkingLevel(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-high","generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "generationConfig.thinkingConfig.thinkingLevel").String(); got != "low" {
		t.Fatalf("explicit level clobbered: %s", out)
	}
}

func TestApplyWireModelToBody_AddsThinkingLevelForMedium(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-medium","request":{"contents":[]}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingLevel").String(); got != "medium" {
		t.Fatalf("thinkingLevel not injected: %s", out)
	}
}

func TestApplyWireModelToBody_RespectsExplicitThinkingLevel(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-medium","request":{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}}`)
	out := ApplyWireModelToBody(body)
	if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingLevel").String(); got != "high" {
		t.Fatalf("explicit level clobbered: %s", out)
	}
}

func TestApplyWireModelToBody_RespectsExplicitThinkingBudget(t *testing.T) {
	body := []byte(`{"model":"gemini-3.5-flash-medium","request":{"generationConfig":{"thinkingConfig":{"thinkingBudget":2048}}}}`)
	out := ApplyWireModelToBody(body)
	if gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingLevel").Exists() {
		t.Fatalf("unexpected thinkingLevel injected when budget is explicit: %s", out)
	}
}

func TestApplyWireModelToBody_PassthroughForUnknown(t *testing.T) {
	body := []byte(`{"model":"unknown-model"}`)
	out := ApplyWireModelToBody(body)
	if string(out) != string(body) {
		t.Fatalf("body mutated for unknown model: %s", out)
	}
}

func TestApplyWireModelToBody_PassthroughForBodyWithoutModel(t *testing.T) {
	body := []byte(`{"contents":[]}`)
	out := ApplyWireModelToBody(body)
	if string(out) != string(body) {
		t.Fatalf("body mutated: %s", out)
	}
}

func TestApplyWireModelToBody_PassthroughForInvalidJSON(t *testing.T) {
	body := []byte(`not json`)
	out := ApplyWireModelToBody(body)
	if string(out) != string(body) {
		t.Fatalf("body mutated: %s", out)
	}
}

func TestApplyWireModelToBody_Gemini37FlashDefaultThinkingBudget(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		budgetPath string
		wantBudget int64
		wantModel  string
	}{
		{
			name:       "wrapped v1internal request high",
			body:       `{"model":"gemini-3.7-flash-high","request":{"contents":[]}}`,
			budgetPath: "request.generationConfig.thinkingConfig.thinkingBudget",
			wantBudget: -1,
			wantModel:  "gemini-3.7-flash-high",
		},
		{
			name:       "bare REST request medium",
			body:       `{"model":"gemini-3.7-flash-medium","generationConfig":{}}`,
			budgetPath: "generationConfig.thinkingConfig.thinkingBudget",
			wantBudget: 4000,
			wantModel:  "gemini-3.7-flash-medium",
		},
		{
			name:       "SDK config request low",
			body:       `{"model":"gemini-3.7-flash-low","config":{}}`,
			budgetPath: "config.thinkingConfig.thinkingBudget",
			wantBudget: 1000,
			wantModel:  "gemini-3.7-flash-low",
		},
		{
			name:       "prefixed model name medium",
			body:       `{"model":"models/gemini-3.7-flash-medium","request":{}}`,
			budgetPath: "request.generationConfig.thinkingConfig.thinkingBudget",
			wantBudget: 4000,
			wantModel:  "gemini-3.7-flash-medium",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := ApplyWireModelToBody([]byte(tc.body))
			if got := gjson.GetBytes(out, tc.budgetPath).Int(); got != tc.wantBudget {
				t.Fatalf("thinkingBudget at %s: got %d want %d, output: %s", tc.budgetPath, got, tc.wantBudget, out)
			}
			if got := gjson.GetBytes(out, "model").String(); got != tc.wantModel {
				t.Fatalf("model: got %q want %q, output: %s", got, tc.wantModel, out)
			}
			if got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.includeThoughts").Bool(); !got && strings.HasPrefix(tc.name, "wrapped") {
				t.Fatalf("includeThoughts not set: %s", out)
			}
		})
	}
}

func TestApplyWireModelToBody_Gemini37FlashPreservesExplicitSettings(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		checkLevel string
		wantLevel  string
		checkBudg  string
		wantBudg   int64
	}{
		{
			name:       "explicit camelCase thinkingLevel",
			body:       `{"model":"gemini-3.7-flash-medium","request":{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}}`,
			checkLevel: "request.generationConfig.thinkingConfig.thinkingLevel",
			wantLevel:  "high",
		},
		{
			name:       "explicit snake_case thinking_level",
			body:       `{"model":"gemini-3.7-flash-medium","request":{"generationConfig":{"thinkingConfig":{"thinking_level":"low"}}}}`,
			checkLevel: "request.generationConfig.thinkingConfig.thinking_level",
			wantLevel:  "low",
		},
		{
			name:      "explicit camelCase thinkingBudget",
			body:      `{"model":"gemini-3.7-flash-medium","request":{"generationConfig":{"thinkingConfig":{"thinkingBudget":2048}}}}`,
			checkBudg: "request.generationConfig.thinkingConfig.thinkingBudget",
			wantBudg:  2048,
		},
		{
			name:      "explicit snake_case thinking_budget",
			body:      `{"model":"gemini-3.7-flash-medium","request":{"generationConfig":{"thinkingConfig":{"thinking_budget":1024}}}}`,
			checkBudg: "request.generationConfig.thinkingConfig.thinking_budget",
			wantBudg:  1024,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := ApplyWireModelToBody([]byte(tc.body))
			if tc.checkLevel != "" {
				if got := gjson.GetBytes(out, tc.checkLevel).String(); got != tc.wantLevel {
					t.Fatalf("thinkingLevel at %s: got %q want %q, output: %s", tc.checkLevel, got, tc.wantLevel, out)
				}
				if gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingBudget").Exists() {
					t.Fatalf("unexpected default thinkingBudget injected when level is explicit: %s", out)
				}
			}
			if tc.checkBudg != "" {
				if got := gjson.GetBytes(out, tc.checkBudg).Int(); got != tc.wantBudg {
					t.Fatalf("thinkingBudget at %s: got %d want %d, output: %s", tc.checkBudg, got, tc.wantBudg, out)
				}
			}
		})
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
		{name: "3.7 high to medium", model: "gemini-3.7-flash-high", level: "MEDIUM", want: "gemini-3.7-flash-medium"},
		{name: "3.7 high to low", model: "gemini-3.7-flash-high", level: "LOW", want: "gemini-3.7-flash-low"},
		{name: "3.7 medium to low", model: "gemini-3.7-flash-medium", level: "LOW", want: "gemini-3.7-flash-low"},
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

func TestResolveWireFromBody_Gemini37FlashExactTiers(t *testing.T) {
	cases := []struct {
		name  string
		model string
		body  string
		want  string
	}{
		{
			name:  "high tier no body",
			model: "gemini-3.7-flash-high",
			body:  "",
			want:  "gemini-3.7-flash-high",
		},
		{
			name:  "medium tier prefixed",
			model: "models/gemini-3.7-flash-medium",
			body:  `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`,
			want:  "gemini-3.7-flash-medium",
		},
		{
			name:  "low tier uppercase",
			model: "GEMINI-3.7-FLASH-LOW",
			body:  `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}}`,
			want:  "gemini-3.7-flash-low",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveWireFromBody(tc.model, []byte(tc.body))
			if got != tc.want {
				t.Fatalf("ResolveWireFromBody(%q): got %q want %q", tc.model, got, tc.want)
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
