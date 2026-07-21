package antigravity

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// AntigravityWireModel resolves public Antigravity model IDs to the backend
// model keys currently accepted by Google's Antigravity generation endpoint.
//
// The public model IDs (gemini-3.5-flash, gemini-3.5-flash-high,
// gemini-3.5-flash-medium) are the names users + the model picker see. The
// wire model names (gemini-3-flash-agent, gemini-3.5-flash-low,
// gemini-3-flash) are what Google's cloudcode-pa backend currently accepts.
// Google may collapse these aliases server-side later, but for now the
// public→wire mapping is mandatory: sending the public name returns 400.
//
// Mirrors router-for-me/CLIProxyAPI PR #3490 (Nov 2026) ::
// internal/misc/antigravity_version.go::AntigravityWireModel.
//
// Anything not in the table is passed through unchanged so the existing
// Claude / Gemini 3 Pro / Gemini 3.1 flows keep working.
func AntigravityWireModel(modelName string) string {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(modelName, "models/")))
	switch normalized {
	// Gemini 3.6 Flash variants (rolled onto daily-cloudcode-pa 2026-07,
	// not yet in prod cloudcode-pa as of 2026-07-21). Sub2api's native
	// gateway defaults to the DAILY endpoint via agymimic
	// (internal.EndpointDailySandbox), so 3.6 users hit it directly.
	//
	// Wire names match public IDs verbatim — no "extra-low"-style rename
	// trap like 3.5 flash. Verified via /v1internal:fetchAvailableModels
	// (2026-07-21): displayName tier labels ("High"/"Medium"/"Low")
	// align 1:1 with the wire suffix, and thinkingBudget values
	// (10000/4000/1000) match the tier semantics we already trust for
	// 3-flash-agent/3.5-flash-low/3.5-flash-extra-low respectively.
	case "gemini-3.6-flash-high",
		"gemini-3.6-flash-medium",
		"gemini-3.6-flash-low",
		"gemini-3.6-flash-tiered":
		return normalized
	case "gemini-3.6-flash":
		// Suffixless picker → wire "-medium" (mirrors 3.5 flash base
		// → -low default). Body thinkingLevel can still re-route via
		// ResolveWireFromBody.
		return "gemini-3.6-flash-medium"
	// Gemini 3.5 Flash variants
	case "gemini-3.5-flash-high":
		return "gemini-3-flash-agent"
	case "gemini-3.5-flash", "gemini-3.5-flash-medium":
		return "gemini-3.5-flash-low"
	case "gemini-3.5-flash-low":
		// Public "Low" must map to wire "extra-low" — passthrough would
		// silently serve the MEDIUM tier per the daily-cloudcode-pa
		// fetchAvailableModels probe. Real agy.exe sends extra-low.
		return "gemini-3.5-flash-extra-low"
	// Gemini 3 Flash legacy variants (kept for back-compat)
	case "gemini-3-flash-high", "gemini-3-flash-medium", "gemini-3-flash-low":
		return "gemini-3-flash"
	// Gemini 3.1 Pro variants
	case "gemini-3.1-pro-high":
		// Public "3.1 Pro High" → wire alias "gemini-pro-agent" which the
		// daily endpoint accepts without a thinking-budget mandate.
		// Sending the literal public name returns 400 INVALID_ARGUMENT.
		// Real agy's model picker uses gemini-pro-agent for "Gemini 3.1
		// Pro (High)" — verified via probe of /v1internal:fetchAvailableModels.
		return "gemini-pro-agent"
	// Gemini 3 Pro variants are NOT remapped — Google deprecated them on
	// daily-cloudcode-pa (the backend returns "Gemini 3 Pro is no longer
	// available. Please switch to Gemini 3.1 Pro"). They were removed from
	// DefaultModels so admins can't pick them; if a legacy config still
	// sends one, we let it pass through to surface the upstream error
	// rather than silently rewriting to a different tier.
	default:
		return modelName
	}
}

// ResolveWireFromBody returns the wire model name, taking the caller's
// explicit `thinkingConfig.thinkingLevel` into account when the public
// name is the suffix-less Gemini 3.5 Flash base.
//
// This lets an upstream client expose a single model entry
// (`gemini-3.5-flash`) and pick the real backend tier via the slider:
//
//	body.thinkingLevel == "high"          → wire gemini-3-flash-agent       (high)
//	body.thinkingLevel ∈ {medium,""}      → wire gemini-3.5-flash-low       (mid)
//	body.thinkingLevel ∈ {low, minimal}   → wire gemini-3.5-flash-extra-low (low)
//
// Suffixed variants (-high / -medium / -low) keep the existing
// AntigravityWireModel behavior — the suffix wins, body thinkingLevel
// does not re-route the wire. This preserves the explicit-tier contract
// for clients that pin a specific variant.
//
// body may be the bare Gemini request (pre-v1internal wrap, key path
// `generationConfig.thinkingConfig.thinkingLevel`) or the wrapped form
// (`request.generationConfig...`); both are checked.
func ResolveWireFromBody(publicName string, body []byte) string {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(publicName, "models/")))
	switch normalized {
	case "gemini-3.5-flash":
		if len(body) == 0 || !gjson.ValidBytes(body) {
			return AntigravityWireModel(publicName)
		}
		switch extractThinkingLevel(body) {
		case "high":
			return "gemini-3-flash-agent"
		case "minimal", "low":
			return "gemini-3.5-flash-extra-low"
		case "medium", "":
			return "gemini-3.5-flash-low"
		default:
			return AntigravityWireModel(publicName)
		}
	case "gemini-3.1-pro":
		// Pro tier only ships two flavors on agy: low + agent (high).
		// Mirror Flash's slider-driven dispatch so omp can ship a single
		// `gemini-3.1-pro` picker entry and the body's
		// thinkingConfig.thinkingLevel picks the wire variant.
		//
		//   slider=low    → wire gemini-3.1-pro-low  (passthrough)
		//   slider=med    → wire gemini-pro-agent    (no separate medium
		//                                             tier exists; round up
		//                                             to high)
		//   slider=high   → wire gemini-pro-agent    (3.1 Pro High)
		//
		// No body / no level: default to High (matches agy's IDE default
		// for the Pro picker).
		if len(body) == 0 || !gjson.ValidBytes(body) {
			return "gemini-pro-agent"
		}
		switch extractThinkingLevel(body) {
		case "minimal", "low":
			return "gemini-3.1-pro-low"
		case "medium", "high", "":
			return "gemini-pro-agent"
		default:
			return AntigravityWireModel(publicName)
		}
	default:
		return AntigravityWireModel(publicName)
	}
}

// extractThinkingLevel returns the first non-empty thinkingLevel found in
// either the pre-wrap (`generationConfig…`) or post-wrap
// (`request.generationConfig…`) location, normalized to lowercase.
// Returns "" when neither is set.
func extractThinkingLevel(body []byte) string {
	keys := []string{
		"generationConfig.thinkingConfig.thinkingLevel",
		"generationConfig.thinkingConfig.thinking_level",
		"request.generationConfig.thinkingConfig.thinkingLevel",
		"request.generationConfig.thinkingConfig.thinking_level",
	}
	for _, k := range keys {
		if v := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, k).String())); v != "" {
			return v
		}
	}
	return ""
}

// DefaultVariantThinkingLevel returns the implied thinking effort level for
// public Gemini 3.5/3.6 Flash variant model IDs. Returns "" when the
// model does not carry an implicit default (caller's explicit thinking
// config wins regardless).
func DefaultVariantThinkingLevel(modelName string) string {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(modelName, "models/")))
	switch normalized {
	case "gemini-3.5-flash-high", "gemini-3.6-flash-high":
		return "high"
	case "gemini-3.5-flash-medium", "gemini-3.6-flash-medium":
		return "medium"
	case "gemini-3.6-flash-low":
		return "low"
	default:
		return ""
	}
}

// ApplyWireModelToBody rewrites the outgoing Antigravity request body so
// its top-level `model` field carries the wire-format model name, and
// fills in an implicit `thinkingConfig.thinkingLevel` for variants that
// require one.
//
// Returns the input unchanged if the body is not valid JSON, has no
// `model` field, or already specifies a thinking configuration. The
// payload bytes are NEVER mutated in place — callers receive a fresh
// slice when changes are applied.
func ApplyWireModelToBody(body []byte) []byte {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return body
	}
	publicName := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	if publicName == "" {
		return body
	}

	wire := ResolveWireFromBody(publicName, body)
	out := body
	if wire != publicName {
		if updated, err := sjson.SetBytes(out, "model", wire); err == nil {
			out = updated
		}
	}
	out = applyDefaultThinkingLevel(out, publicName)
	return out
}

// applyDefaultThinkingLevel injects request.generationConfig.thinkingConfig
// for variants that imply one. Skipped when the caller already supplied an
// explicit thinking level OR budget (either camelCase or snake_case keys).
// Skipped when there is no implicit level for the given model.
func applyDefaultThinkingLevel(body []byte, publicModelName string) []byte {
	level := DefaultVariantThinkingLevel(publicModelName)
	if level == "" {
		return body
	}
	const thinkingPath = "request.generationConfig.thinkingConfig"
	for _, key := range []string{
		thinkingPath + ".thinkingLevel",
		thinkingPath + ".thinking_level",
		thinkingPath + ".thinkingBudget",
		thinkingPath + ".thinking_budget",
	} {
		if gjson.GetBytes(body, key).Exists() {
			return body
		}
	}
	updated, err := sjson.SetBytes(body, thinkingPath+".thinkingLevel", level)
	if err != nil {
		return body
	}
	if withThoughts, errSet := sjson.SetBytes(updated, thinkingPath+".includeThoughts", true); errSet == nil {
		updated = withThoughts
	}
	return updated
}

// ExtractSessionID returns the per-request session identifier from the
// Antigravity request body. Returns "" when none is present. Used to
// populate the `X-Machine-Session-Id` header on outbound calls so Google
// can correlate streamed turns with the originating Antigravity session.
func ExtractSessionID(body []byte) string {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return ""
	}
	return strings.TrimSpace(gjson.GetBytes(body, "request.sessionId").String())
}
