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
	case "gemini-3.5-flash-high":
		return "gemini-3-flash-agent"
	case "gemini-3.5-flash", "gemini-3.5-flash-medium":
		return "gemini-3.5-flash-low"
	case "gemini-3-flash-high", "gemini-3-flash-medium", "gemini-3-flash-low":
		return "gemini-3-flash"
	default:
		return modelName
	}
}

// DefaultVariantThinkingLevel returns the implied thinking effort level for
// public Gemini 3.5 Flash variant model IDs. Returns "" when the model
// does not carry an implicit default (caller's explicit thinking config
// wins regardless).
func DefaultVariantThinkingLevel(modelName string) string {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(modelName, "models/")))
	switch normalized {
	case "gemini-3.5-flash-high":
		return "high"
	case "gemini-3.5-flash-medium":
		return "medium"
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

	wire := AntigravityWireModel(publicName)
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
