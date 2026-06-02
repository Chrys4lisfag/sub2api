// Transparent agy_list_tools MCP discovery loop.
//
// When the global setting SettingKeyAntigravityNativeListToolsEmulation is
// enabled (or the per-account credential `list_tools_emulation` overrides),
// this loop runs BEFORE the normal streaming pipeline:
//
//  1. Inject `agy_list_tools` function declaration into the upstream
//     request body alongside `call_mcp_tool`.
//  2. POST a NON-STREAMING upstream request.
//  3. Parse the response for a `functionCall{name: "agy_list_tools"}`.
//  4. If found: synthesize a `functionResponse` containing the catalog
//     (filtered by optional `server` arg), append assistant.call +
//     user.response to the body's contents[], and loop.
//  5. If not found OR budget exhausted: return the final response bytes.
//
// The downstream client never observes the agy_list_tools roundtrip —
// it only sees the FINAL response (which uses call_mcp_tool for actual
// MCP work, which the existing back-translator rewrites to mcp__*).
//
// Why this is client-agnostic: the discovery roundtrip is entirely
// server↔upstream. Clients (omp, claude-code, codex, etc.) never need to
// know about agy_list_tools or implement special dispatch logic.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	api "github.com/koval/agymimic/api"
)

// injectAgyListToolsIntoBody mutates the request body JSON, adding the
// agy_list_tools function declaration to the first tools[] entry. No-op
// if the body has no tools[], parse fails, or the declaration already
// exists (idempotent).
//
// Returns the (possibly modified) body bytes.
func injectAgyListToolsIntoBody(body []byte) []byte {
	if len(body) == 0 {
		return body
	}
	var inner map[string]any
	if err := json.Unmarshal(body, &inner); err != nil {
		return body
	}
	// Caller may have double-wrapped as {"request": {...}} — peel.
	if r, ok := inner["request"].(map[string]any); ok && len(inner) == 1 {
		if injectAgyListToolsIntoInner(r) {
			out, err := json.Marshal(inner)
			if err != nil {
				return body
			}
			return out
		}
		return body
	}
	if injectAgyListToolsIntoInner(inner) {
		out, err := json.Marshal(inner)
		if err != nil {
			return body
		}
		return out
	}
	return body
}

// injectAgyListToolsIntoInner appends the agy_list_tools declaration to
// inner.tools[0].functionDeclarations if not already present. Returns
// true if a modification was made.
func injectAgyListToolsIntoInner(inner map[string]any) bool {
	if inner == nil {
		return false
	}
	toolsAny, ok := inner["tools"].([]any)
	if !ok || len(toolsAny) == 0 {
		// No tools list → create one with agy_list_tools only. Aggregator
		// preprocessing should have already created the tools[] entry,
		// but be defensive.
		inner["tools"] = []any{
			map[string]any{
				"functionDeclarations": []any{buildAgyListToolsDecl()},
			},
		}
		return true
	}
	first, ok := toolsAny[0].(map[string]any)
	if !ok {
		return false
	}
	fds, _ := first["functionDeclarations"].([]any)
	// Idempotent check.
	for _, fd := range fds {
		m, _ := fd.(map[string]any)
		if m == nil {
			continue
		}
		if name, _ := m["name"].(string); name == defaultListToolsName {
			return false
		}
	}
	fds = append(fds, buildAgyListToolsDecl())
	first["functionDeclarations"] = fds
	return true
}

// extractAgyListToolsCall scans a non-streaming Gemini response body for
// the first `functionCall{name: "agy_list_tools"}` in candidates[0].
//
// Returns (callArgs, true) on match. callArgs is the args object the
// model supplied (may contain {"server": "..."}).
//
// Returns (nil, false) when:
//   - body unparseable
//   - no candidates / no parts / no functionCall
//   - functionCall name != defaultListToolsName
//   - the response contains OTHER content alongside (text, other tool
//     calls) — we only loop on PURE discovery turns to avoid swallowing
//     legitimate model output
func extractAgyListToolsCall(respBody []byte) (map[string]any, bool) {
	if len(respBody) == 0 {
		return nil, false
	}
	var root map[string]any
	if err := json.Unmarshal(respBody, &root); err != nil {
		return nil, false
	}
	// agymimic wraps non-streaming responses in {response: {candidates, ...}}.
	// Peel one wrap if present.
	if r, ok := root["response"].(map[string]any); ok && len(root) <= 2 {
		root = r
	}
	cands, ok := root["candidates"].([]any)
	if !ok || len(cands) == 0 {
		return nil, false
	}
	cand, ok := cands[0].(map[string]any)
	if !ok {
		return nil, false
	}
	content, ok := cand["content"].(map[string]any)
	if !ok {
		return nil, false
	}
	parts, ok := content["parts"].([]any)
	if !ok || len(parts) == 0 {
		return nil, false
	}
	// Walk parts to find agy_list_tools, AND verify nothing else of
	// substance is present. If model also emitted text or another tool
	// call, we don't loop — let the caller's normal flow handle it.
	var found map[string]any
	otherSubstance := false
	for _, partAny := range parts {
		part, ok := partAny.(map[string]any)
		if !ok {
			continue
		}
		if fc, ok := part["functionCall"].(map[string]any); ok {
			name, _ := fc["name"].(string)
			if name == defaultListToolsName {
				args, _ := fc["args"].(map[string]any)
				if args == nil {
					args = map[string]any{}
				}
				if found == nil {
					found = args
				}
				// Multiple agy_list_tools in one turn — only honor first.
				continue
			}
			// Another tool call → don't intercept.
			otherSubstance = true
		}
		if t, ok := part["text"].(string); ok && t != "" {
			// Substantive text alongside the discovery call → don't
			// intercept (model gave a real reply, possibly preamble).
			// Empty text or thoughtSignature-only is fine.
			otherSubstance = true
		}
	}
	if found == nil || otherSubstance {
		return nil, false
	}
	return found, true
}

// appendAssistantCallAndUserResponse mutates the body JSON, appending
// two new contents[] turns:
//
//  1. assistant turn carrying the model's agy_list_tools functionCall
//     (so upstream sees its own prior output in the next request)
//  2. user turn carrying the synthesized functionResponse
//
// This is the standard Gemini tool-call → tool-result pattern.
func appendAssistantCallAndUserResponse(body []byte, callArgs, response map[string]any) ([]byte, error) {
	var inner map[string]any
	if err := json.Unmarshal(body, &inner); err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}
	target := inner
	wrapped := false
	if r, ok := inner["request"].(map[string]any); ok && len(inner) == 1 {
		target = r
		wrapped = true
	}
	contents, _ := target["contents"].([]any)
	// Append assistant.functionCall turn.
	contents = append(contents, map[string]any{
		"role": "model",
		"parts": []any{
			map[string]any{
				"functionCall": map[string]any{
					"name": defaultListToolsName,
					"args": callArgs,
				},
			},
		},
	})
	// Append user.functionResponse turn.
	contents = append(contents, map[string]any{
		"role": "user",
		"parts": []any{
			map[string]any{
				"functionResponse": map[string]any{
					"name":     defaultListToolsName,
					"response": response,
				},
			},
		},
	})
	target["contents"] = contents
	if wrapped {
		// Re-wrap.
		out, err := json.Marshal(inner)
		if err != nil {
			return nil, fmt.Errorf("encode body: %w", err)
		}
		return out, nil
	}
	out, err := json.Marshal(inner)
	if err != nil {
		return nil, fmt.Errorf("encode body: %w", err)
	}
	return out, nil
}

// resolveAgyListToolsLoop runs the discovery loop. Each iteration:
//
//  1. Ensure body has agy_list_tools decl (idempotent)
//  2. Wrap in v1internal envelope
//  3. POST non-streaming to upstream
//  4. Parse response
//  5. If agy_list_tools detected: synthesize response, append to body, loop
//  6. Else: return the final response bytes (caller writes to client)
//
// Returns (finalRespBody, iterationCount, error). Budget cap is
// `listToolsCallBudget`. On budget exhaustion, returns the last response
// + a budget-exhausted note in the synthesized contents so the model can
// give up gracefully.
func (s *AntigravityNativeGatewayService) resolveAgyListToolsLoop(
	ctx context.Context,
	cli *api.Client,
	wireModel string,
	body []byte,
	toolReport toolPrepReport,
) ([]byte, int, error) {
	// Always inject agy_list_tools — idempotent, so re-injection on
	// later iterations is a no-op.
	body = injectAgyListToolsIntoBody(body)

	iterations := 0
	var lastResp []byte

	for iterations <= listToolsCallBudget {
		envelope, err := wrapNativeV1Internal(cli.ProjectID(), wireModel, body)
		if err != nil {
			return nil, iterations, fmt.Errorf("list-tools loop: envelope: %w", err)
		}
		// Always non-streaming for the loop.
		path := "/v1internal:generateContent"
		resp, err := cli.RawRequest(ctx, path, envelope)
		if err != nil {
			return nil, iterations, fmt.Errorf("list-tools loop: upstream: %w", err)
		}
		raw, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, iterations, fmt.Errorf("list-tools loop: read response: %w", readErr)
		}
		if resp.StatusCode != http.StatusOK {
			// Surface upstream error to caller for normal failover.
			return raw, iterations, &UpstreamFailoverError{
				StatusCode:             resp.StatusCode,
				ResponseBody:           raw,
				ResponseHeaders:        resp.Header,
				PassthroughVerbatim:    true,
				RetryableOnSameAccount: false,
			}
		}
		lastResp = raw
		iterations++

		callArgs, found := extractAgyListToolsCall(raw)
		if !found {
			// Done — model emitted real output (text or call_mcp_tool).
			return raw, iterations - 1, nil
		}
		// Budget guard: if this was the last allowed iteration, return a
		// budget-exhausted synthetic response that tells the model to
		// stop asking and use call_mcp_tool directly.
		if iterations >= listToolsCallBudget {
			slog.WarnContext(ctx, "native: agy_list_tools budget exhausted",
				slog.Int("iterations", iterations),
				slog.Int("budget", listToolsCallBudget))
			budgetResp := map[string]any{
				"error": "list_tools_budget_exhausted",
				"hint":  "You have called agy_list_tools too many times. Use call_mcp_tool with one of the literal (ServerName, ToolName) pairs from the catalog already provided in the system instructions. Do not call agy_list_tools again this turn.",
			}
			body, err = appendAssistantCallAndUserResponse(body, callArgs, budgetResp)
			if err != nil {
				return raw, iterations, fmt.Errorf("list-tools loop: append budget-exhausted: %w", err)
			}
			// Re-issue one final time so the model can respond with
			// call_mcp_tool given the budget-exhausted hint.
			envelope, err = wrapNativeV1Internal(cli.ProjectID(), wireModel, body)
			if err != nil {
				return nil, iterations, fmt.Errorf("list-tools loop: final envelope: %w", err)
			}
			resp, err := cli.RawRequest(ctx, path, envelope)
			if err != nil {
				return nil, iterations, fmt.Errorf("list-tools loop: final upstream: %w", err)
			}
			raw, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return raw, iterations, nil
		}

		// Synthesize response for THIS iteration's call.
		serverFilter, _ := callArgs["server"].(string)
		response := synthesizeListToolsResponse(toolReport.McpTools, serverFilter)
		body, err = appendAssistantCallAndUserResponse(body, callArgs, response)
		if err != nil {
			return raw, iterations, fmt.Errorf("list-tools loop: append turns: %w", err)
		}
	}
	// Shouldn't reach here — loop always returns inside.
	return lastResp, iterations, nil
}

// agyListToolsSSEEvent wraps a non-streaming JSON response as a single
// SSE `data:` event so clients that requested streaming still receive a
// valid SSE stream (with one event followed by stream end).
//
// Gemini SSE format: `data: {...json...}\n\n`.
func agyListToolsSSEEvent(respBody []byte) []byte {
	out := make([]byte, 0, len(respBody)+10)
	out = append(out, []byte("data: ")...)
	out = append(out, respBody...)
	out = append(out, '\n', '\n')
	return out
}

// newListToolsCallID generates a stable-ish ID for a synthesized
// functionResponse turn. Used only when we need to correlate by ID in
// future variants — current Gemini contract uses name-based matching.
func newListToolsCallID() string {
	return "agy-discovery-" + uuid.NewString()
}
