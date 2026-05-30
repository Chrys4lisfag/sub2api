// Runtime glue for the antigravity_native tool aggregator:
//
//   - preprocessNativeBody    — JSON unmarshal → applyToolPreprocessing →
//                                marshal. Returns the rewritten body bytes
//                                + a report the streaming layer uses to
//                                back-translate model output.
//   - accountToolAggregatorEnabled — reads the per-account credential flag
//                                that controls whether MCP tools get hidden
//                                behind call_mcp_tool.
//   - rewriteAggregatedFunctionCalls — walks one SSE chunk's candidates[]
//                                parts[] looking for {functionCall: {name:
//                                "call_mcp_tool", args: {ServerName, ToolName,
//                                Arguments}}} and rewrites them back into
//                                {functionCall: {name: "mcp__server_tool",
//                                args: Arguments}} so omp's tool dispatch
//                                sees the original mcp__ prefix.
//
// Without the rewrite, the model would emit call_mcp_tool and omp would
// either reject (no such tool) or treat it opaquely. With the rewrite,
// the model sees the small aggregator surface AND omp sees its native
// tool names — best of both.
package service

import (
	"encoding/json"
	"strings"
)

// preprocessNativeBody unmarshals the incoming Gemini-format body,
// applies the schema normalizer + (optionally) the call_mcp_tool
// aggregator, and re-marshals.
//
// `body` is the raw request body the upstream gateway handler passed to
// us (the Gemini-format inner request, not the v1internal envelope).
// Returns the rewritten bytes + the report.
//
// On JSON parse error: returns the original body unchanged + a zero
// report. We never block a request on preprocessing failure.
func preprocessNativeBody(body []byte, useAggregator bool) ([]byte, toolPrepReport, error) {
	if len(body) == 0 {
		return body, toolPrepReport{AggregatorOn: useAggregator}, nil
	}
	var inner map[string]any
	if err := json.Unmarshal(body, &inner); err != nil {
		// Pass-through — don't fail the request, just skip preprocessing.
		return body, toolPrepReport{AggregatorOn: useAggregator}, nil
	}
	// Handle the double-wrap shape consistently with wrapNativeV1Internal:
	// caller might have already nested as {"request": {...}}.
	if r, ok := inner["request"].(map[string]any); ok && len(inner) == 1 {
		inner = r
	}
	report := applyToolPreprocessing(inner, useAggregator)
	out, err := json.Marshal(inner)
	if err != nil {
		return body, report, nil
	}
	return out, report, nil
}

// accountToolAggregatorEnabled returns true when the per-account
// credentials map contains `"tool_aggregator": true`, OR the env var
// SUB2API_NATIVE_TOOL_AGGREGATOR_DEFAULT=true is set (rollout switch).
//
// Default behavior: aggregator ON for every native account, because the
// dominant failure mode the fix targets is omp + 200+ MCP tools. Admins
// can disable per-account by setting `tool_aggregator: false` in the
// account credentials JSONB.
func accountToolAggregatorEnabled(account *Account) bool {
	if account == nil {
		return true
	}
	if v, ok := account.Credentials["tool_aggregator"].(bool); ok {
		return v
	}
	if s, ok := account.Credentials["tool_aggregator"].(string); ok {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "false", "off", "0", "no":
			return false
		case "true", "on", "1", "yes":
			return true
		}
	}
	return true // default ON
}

// rewriteAggregatedFunctionCalls walks a single SSE chunk's `candidates`
// array and rewrites any `functionCall` whose name is `call_mcp_tool`,
// `list_mcp_tools`, or `read_mcp_tool_schema` back into a shape omp
// expects:
//
//   - call_mcp_tool        → mcp__<server>_<tool>(...Arguments...)
//   - list_mcp_tools       → synthesize a `mcp__<server>__list` no-op or
//                            keep as-is so omp can route to a builtin
//                            (we just inline the catalog inside args).
//   - read_mcp_tool_schema → keep as-is (omp doesn't have a matching tool;
//                            the model uses this purely for its own
//                            context — schema injected via list/read calls
//                            in the prompt prefix or via a sidecar).
//
// Returns the rewritten payload bytes if any changes were made; otherwise
// returns the input unchanged. Idempotent.
func rewriteAggregatedFunctionCalls(payload []byte, report toolPrepReport) []byte {
	if !report.AggregatorOn || len(report.McpTools) == 0 {
		return payload
	}
	if len(payload) == 0 {
		return payload
	}

	var root map[string]any
	if err := json.Unmarshal(payload, &root); err != nil {
		return payload
	}

	cands, ok := root["candidates"].([]any)
	if !ok || len(cands) == 0 {
		return payload
	}
	changed := false
	for _, candAny := range cands {
		cand, ok := candAny.(map[string]any)
		if !ok {
			continue
		}
		content, ok := cand["content"].(map[string]any)
		if !ok {
			continue
		}
		parts, ok := content["parts"].([]any)
		if !ok {
			continue
		}
		for _, partAny := range parts {
			part, ok := partAny.(map[string]any)
			if !ok {
				continue
			}
			fc, ok := part["functionCall"].(map[string]any)
			if !ok {
				continue
			}
			name, _ := fc["name"].(string)
			if name != "call_mcp_tool" {
				continue
			}
			args, _ := fc["args"].(map[string]any)
			if args == nil {
				continue
			}
			server, _ := args["ServerName"].(string)
			tool, _ := args["ToolName"].(string)
			inner, _ := args["Arguments"].(map[string]any)
			handle, found := report.resolveMcpHandle(server, tool)
			if !found {
				// Leave as-is — omp will surface "unknown tool" error to
				// the user / model. Better than silently dropping the call.
				continue
			}
			fc["name"] = handle.FullName
			// Replace args with the inner Arguments object the model
			// supplied. If inner is nil / non-object, fall back to empty
			// args (model violated the call_mcp_tool schema).
			if inner == nil {
				fc["args"] = map[string]any{}
			} else {
				fc["args"] = inner
			}
			changed = true
		}
	}
	if !changed {
		return payload
	}
	out, err := json.Marshal(root)
	if err != nil {
		return payload
	}
	return out
}

// rewriteSSELineFunctionCalls runs rewriteAggregatedFunctionCalls on the
// JSON payload inside an SSE `data: ...` line, preserving the `data: `
// prefix and trailing CR/LF. Non-data lines pass through unchanged.
//
// Cheap fast-path: when the line doesn't contain `call_mcp_tool` we
// return the input untouched without parsing JSON.
func rewriteSSELineFunctionCalls(line []byte, report toolPrepReport) []byte {
	if !report.AggregatorOn || len(report.McpTools) == 0 {
		return line
	}
	if len(line) == 0 {
		return line
	}
	// Fast-path: only parse if the keyword appears in the line.
	if !containsToken(line, []byte("call_mcp_tool")) &&
		!containsToken(line, []byte("list_mcp_tools")) &&
		!containsToken(line, []byte("read_mcp_tool_schema")) {
		return line
	}
	// Find SSE prefix + payload split.
	// Lines look like:  data: {...json...}\n
	// (with optional CR before LF.)
	prefix := []byte("data:")
	idx := indexOf(line, prefix)
	if idx < 0 {
		return line
	}
	payloadStart := idx + len(prefix)
	// Skip optional single leading space.
	if payloadStart < len(line) && line[payloadStart] == ' ' {
		payloadStart++
	}
	// Strip trailing CR/LF before parsing.
	end := len(line)
	for end > payloadStart && (line[end-1] == '\n' || line[end-1] == '\r') {
		end--
	}
	payload := line[payloadStart:end]
	rewritten := rewriteAggregatedFunctionCalls(payload, report)
	if string(rewritten) == string(payload) {
		return line
	}
	// Rebuild line: prefix + space + rewritten + original CR/LF tail.
	tail := line[end:]
	out := make([]byte, 0, len(line)+len(rewritten))
	out = append(out, line[:idx+len(prefix)]...)
	out = append(out, ' ')
	out = append(out, rewritten...)
	out = append(out, tail...)
	return out
}

// containsToken — substring search without allocating (bytes.Contains
// works the same but this avoids importing bytes here just for one call).
func containsToken(haystack, needle []byte) bool {
	return indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle []byte) int {
	n := len(needle)
	if n == 0 || len(haystack) < n {
		return -1
	}
	first := needle[0]
outer:
	for i := 0; i+n <= len(haystack); i++ {
		if haystack[i] != first {
			continue
		}
		for j := 1; j < n; j++ {
			if haystack[i+j] != needle[j] {
				continue outer
			}
		}
		return i
	}
	return -1
}
