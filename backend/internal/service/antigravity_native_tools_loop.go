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
	"bytes"
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

// agyListToolsCallInfo carries everything the loop needs to decide
// whether to intercept the model's response and how to reconstruct the
// assistant turn for the next upstream request.
type agyListToolsCallInfo struct {
	// CallArgs is the args object of the first agy_list_tools functionCall
	// the model emitted (typically {"server": "..."} or empty).
	CallArgs map[string]any
	// AssistantParts is the EXACT parts array the model emitted in this
	// turn — preserved verbatim so we can include text/thinking/etc. in
	// the assistant turn when re-issuing. May contain non-agy_list_tools
	// content (text, thoughtSignature). We use this as-is when only the
	// agy_list_tools call is present (alongside text/thinking is OK).
	AssistantParts []any
	// HasOtherFunctionCall is true when the response also contains a
	// functionCall whose name is NOT agy_list_tools (e.g. call_mcp_tool
	// or a direct mcp__* call). In this case we DON'T intercept; we
	// strip the agy_list_tools call from the response and let the
	// other call(s) flow through to the client normally.
	HasOtherFunctionCall bool
	// AgyListToolsPartIndex is the index of the agy_list_tools part in
	// AssistantParts (so the stripper knows what to remove).
	AgyListToolsPartIndex int
}

// extractAgyListToolsCall scans a non-streaming Gemini response body for
// an agy_list_tools functionCall in candidates[0].
//
// Returns (info, true) when agy_list_tools is found. The caller inspects
// info.HasOtherFunctionCall to decide:
//   - false → intercept (re-issue with synthetic response)
//   - true  → strip agy_list_tools and pass remaining response to client
//
// Returns (nil, false) when no agy_list_tools functionCall exists in the
// response (caller passes through to client unchanged).
//
// Critical behavior change vs the original: we DO intercept when the
// model emits text+agy_list_tools in the same turn (the common case for
// reasoning models). Prior version's "mixed content" guard caused
// agy_list_tools to leak to the client, which then errored with
// "Tool agy_list_tools not found" since the discovery tool is
// server-only by design.
func extractAgyListToolsCall(respBody []byte) (*agyListToolsCallInfo, bool) {
	if len(respBody) == 0 {
		return nil, false
	}
	var root map[string]any
	if err := json.Unmarshal(respBody, &root); err != nil {
		return nil, false
	}
	// agymimic wraps upstream responses as {response: {candidates,...},
	// usageMetadata: ..., modelVersion: ..., ...} with 3+ top-level keys.
	// Peel whenever `response` carries `candidates` (regardless of how
	// many sibling fields exist at the outer level). Previous guard
	// `len(root) <= 2` was too restrictive — real upstream responses
	// have 3+ outer fields, peel didn't fire, candidates lookup at the
	// outer level failed, and agy_list_tools calls leaked to the client.
	if r, ok := root["response"].(map[string]any); ok {
		if _, hasCands := r["candidates"]; hasCands {
			root = r
		}
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
	info := &agyListToolsCallInfo{
		AssistantParts:        parts,
		AgyListToolsPartIndex: -1,
	}
	for i, partAny := range parts {
		part, ok := partAny.(map[string]any)
		if !ok {
			continue
		}
		fc, hasFC := part["functionCall"].(map[string]any)
		if !hasFC {
			continue
		}
		name, _ := fc["name"].(string)
		if name == defaultListToolsName {
			if info.AgyListToolsPartIndex < 0 {
				args, _ := fc["args"].(map[string]any)
				if args == nil {
					args = map[string]any{}
				}
				info.CallArgs = args
				info.AgyListToolsPartIndex = i
			}
			continue
		}
		// Any other functionCall (call_mcp_tool, mcp__*, builtin tool)
		// means we should NOT intercept — let the client dispatch the
		// real work and strip our discovery call.
		info.HasOtherFunctionCall = true
	}
	if info.AgyListToolsPartIndex < 0 {
		return nil, false
	}
	return info, true
}

// stripAgyListToolsFromResponse removes any agy_list_tools functionCall
// part from candidates[0].content.parts. Used to sanitize responses
// before forwarding to the client when we decided NOT to intercept
// (e.g. model emitted agy_list_tools alongside a real call_mcp_tool —
// the agy_list_tools must not reach the client because no client has
// that tool registered).
//
// Returns the rewritten bytes; on parse error returns the input
// unchanged (defensive — never break a working response).
func stripAgyListToolsFromResponse(respBody []byte) []byte {
	if len(respBody) == 0 {
		return respBody
	}
	var root map[string]any
	if err := json.Unmarshal(respBody, &root); err != nil {
		return respBody
	}
	target := root
	// Same peel logic as extractAgyListToolsCall.
	if r, ok := root["response"].(map[string]any); ok {
		if _, hasCands := r["candidates"]; hasCands {
			target = r
		}
	}
	cands, _ := target["candidates"].([]any)
	changed := false
	for _, candAny := range cands {
		cand, _ := candAny.(map[string]any)
		if cand == nil {
			continue
		}
		content, _ := cand["content"].(map[string]any)
		if content == nil {
			continue
		}
		parts, _ := content["parts"].([]any)
		if len(parts) == 0 {
			continue
		}
		kept := make([]any, 0, len(parts))
		for _, p := range parts {
			pm, _ := p.(map[string]any)
			if pm != nil {
				if fc, ok := pm["functionCall"].(map[string]any); ok {
					if name, _ := fc["name"].(string); name == defaultListToolsName {
						changed = true
						continue
					}
				}
			}
			kept = append(kept, p)
		}
		if changed {
			content["parts"] = kept
		}
	}
	if !changed {
		return respBody
	}
	out, err := json.Marshal(root)
	if err != nil {
		return respBody
	}
	return out
}

// appendAssistantTurnAndUserResponse appends the model's full assistant
// turn (preserving text, thinking, and the agy_list_tools call) plus a
// user turn carrying the synthesized functionResponse.
//
// Why we preserve the full assistant content: reasoning models routinely
// emit explanatory text + a tool call in the same turn. If we replaced
// the assistant turn with a synthetic functionCall-only message, upstream
// would see history loss and may behave unpredictably (re-explain itself,
// hallucinate prior context, etc.). Including the original parts verbatim
// makes the re-issue indistinguishable from a normal client-driven
// tool-call cycle.
func appendAssistantTurnAndUserResponse(body []byte, assistantParts []any, response map[string]any) ([]byte, error) {
	var inner map[string]any
	if err := json.Unmarshal(body, &inner); err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}
	target := inner
	if r, ok := inner["request"].(map[string]any); ok && len(inner) == 1 {
		target = r
	}
	contents, _ := target["contents"].([]any)
	// Assistant turn: use the model's verbatim parts (text + agy_list_tools call).
	contents = append(contents, map[string]any{
		"role":  "model",
		"parts": assistantParts,
	})
	// User turn: functionResponse for agy_list_tools.
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
//
// ---------------------------------------------------------------------------
// TODO(streaming-aware-discovery, 2026-06-14): the current loop forces
// the upstream call to be non-streaming so the response body can be
// fully buffered + scanned. Caller (ForwardGemini) then emits the entire
// response as a single SSE `data:` event via flushBufferedNativeResponse.
// This kills token-by-token streaming and the "thinking trace" UX in
// clients like omp. As of 2026-06-14 the loop is gated to agy_mimic
// mode only (single_name skips it entirely because mcp__* are declared
// directly; discovery is redundant there).
//
// To restore agy_list_tools discovery WITHOUT losing streaming in
// single_name (or in agy_mimic), the loop needs a redesign. Sketch:
//
//  1. Switch upstream call to /v1internal:streamGenerateContent
//     (streaming) instead of /v1internal:generateContent.
//  2. Read SSE chunks as they arrive. Maintain a small peek buffer
//     (~8 KiB or 3 chunks, whichever first) before flushing anything
//     to the client.
//  3. While peek buffer is filling:
//     a. Parse each chunk's candidates[].content.parts. If any part
//     has functionCall{name:"agy_list_tools"}: DISCARD buffer,
//     abort upstream connection, synthesize functionResponse,
//     append assistant.call + user.response to body, re-issue.
//     Client never sees the buffered bytes.
//     b. If peek-buffer fills WITHOUT detecting agy_list_tools:
//     commit — flush buffered chunks to client + stream remainder
//     through normally. The model's response includes text /
//     call_mcp_tool / etc. as usual.
//  4. Budget cap stays the same (max N iterations).
//
// Tricky bits to think through:
//   - SSE chunks may split mid-functionCall. Need a partial-JSON
//     parser that buffers across chunks until a complete part is
//     reconstructible.
//   - thoughtSignature parts (Gemini's reasoning stream) arrive
//     before the functionCall part. Peek buffer must hold ≥ first
//     few chunks to be sure we've seen any functionCall the model
//     intends.
//   - Mid-stream agy_list_tools detection AFTER bytes have been
//     flushed (peek window passed). We can't undo a flush. Either
//     accept that and strip the call from the stream (it'd reach
//     client as text we filter), or harden the peek window to be
//     large enough that this case is rare.
//   - Re-issue: when we abort + re-issue, we need to pass the SAME
//     ctx / cli / wireModel / body shape. Keep the wrapping helper
//     factored so both paths use the same envelope builder.
//
// Alternative approach if streaming-buffer is too risky: speculative
// double-call. Fire streaming + non-streaming in parallel; cancel one
// once we know whether the model called agy_list_tools. Wastes one
// upstream call per request but keeps streaming UX clean.
//
// Until either redesign lands, leave single_name mode without
// discovery; users who explicitly want it switch to agy_mimic.
// ---------------------------------------------------------------------------
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

		info, found := extractAgyListToolsCall(raw)
		if !found {
			// No agy_list_tools in this response — model emitted real
			// output (text and/or call_mcp_tool). Pass through.
			return raw, iterations - 1, nil
		}
		if info.HasOtherFunctionCall {
			// Model emitted agy_list_tools alongside a real call
			// (call_mcp_tool, mcp__*, builtin). Don't intercept the
			// loop — strip the discovery call so the client never sees
			// it, then forward the rest. The other call(s) get
			// dispatched by the client normally; the model's catalog
			// already gives it enough info to know what was discovered.
			cleaned := stripAgyListToolsFromResponse(raw)
			slog.InfoContext(ctx, "native: agy_list_tools call mixed with real call — stripped + passed through",
				slog.Int("iterations", iterations))
			return cleaned, iterations - 1, nil
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
			body, err = appendAssistantTurnAndUserResponse(body, info.AssistantParts, budgetResp)
			if err != nil {
				return raw, iterations, fmt.Errorf("list-tools loop: append budget-exhausted: %w", err)
			}
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
			// Final response could STILL contain agy_list_tools (model
			// ignored the budget hint). Strip defensively.
			return stripAgyListToolsFromResponse(raw), iterations, nil
		}

		// Synthesize response for THIS iteration's call and re-issue
		// with the model's full assistant turn (text + call) preserved.
		serverFilter, _ := info.CallArgs["server"].(string)
		response := synthesizeListToolsResponse(toolReport.McpTools, serverFilter)
		body, err = appendAssistantTurnAndUserResponse(body, info.AssistantParts, response)
		if err != nil {
			return raw, iterations, fmt.Errorf("list-tools loop: append turns: %w", err)
		}
	}
	// Shouldn't reach here — loop always returns inside.
	return lastResp, iterations, nil
}

// agyListToolsSSEEvent wraps a non-streaming JSON response as a single
// SSE `data:` event so clients that requested streaming still receive a
// agyListToolsSSEEvent wraps a non-streaming JSON response as a single
// SSE `data:` event so clients that requested streaming still receive a
// valid SSE stream.
//
// Critical: the JSON MUST be compacted (no literal newlines) before
// emission. SSE uses newlines to terminate events — a literal `\n` in
// the payload would split the event and the client would see a
// truncated JSON object ("Expected '}'" parse error). Gemini's
// non-streaming generateContent response is often pretty-printed with
// indentation, so we re-marshal through json.Compact to strip all
// whitespace.
//
// Gemini SSE format: `data: {compacted_json}\n\n`.
func agyListToolsSSEEvent(respBody []byte) []byte {
	var compact bytes.Buffer
	if err := json.Compact(&compact, respBody); err != nil {
		// Fall back to raw if compaction fails (unlikely — but never
		// silently produce a broken SSE event). Strip embedded newlines
		// as a best-effort defensive measure.
		safe := bytes.ReplaceAll(respBody, []byte{'\n'}, []byte{' '})
		safe = bytes.ReplaceAll(safe, []byte{'\r'}, []byte{' '})
		out := make([]byte, 0, len(safe)+10)
		out = append(out, []byte("data: ")...)
		out = append(out, safe...)
		out = append(out, '\n', '\n')
		return out
	}
	out := make([]byte, 0, compact.Len()+10)
	out = append(out, []byte("data: ")...)
	out = append(out, compact.Bytes()...)
	out = append(out, '\n', '\n')
	return out
}

// newListToolsCallID generates a stable-ish ID for a synthesized
// functionResponse turn. Used only when we need to correlate by ID in
// future variants — current Gemini contract uses name-based matching.
func newListToolsCallID() string { //nolint:unused // ID correlator retained for future list_tools functionResponse variants
	return "agy-discovery-" + uuid.NewString()
}
