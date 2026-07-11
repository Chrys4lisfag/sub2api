// Runtime glue for the antigravity_native tool aggregator:
//
//   - preprocessNativeBody    — JSON unmarshal → applyToolPreprocessing →
//     marshal. Returns the rewritten body bytes
//   - a report the streaming layer uses to
//     back-translate model output.
//   - accountToolAggregatorEnabled — reads the per-account credential flag
//     that controls whether MCP tools get hidden
//     behind call_mcp_tool.
//   - rewriteAggregatedFunctionCalls — walks one SSE chunk's candidates[]
//     parts[] looking for {functionCall: {name:
//     "call_mcp_tool", args: {ServerName, ToolName,
//     Arguments}}} and rewrites them back into
//     {functionCall: {name: "mcp__server_tool",
//     args: Arguments}} so omp's tool dispatch
//     sees the original mcp__ prefix.
//
// Without the rewrite, the model would emit call_mcp_tool and omp would
// either reject (no such tool) or treat it opaquely. With the rewrite,
// the model sees the small aggregator surface AND omp sees its native
// tool names — best of both.
package service

import (
	"encoding/json"
	"log/slog"
	"sort"
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
func preprocessNativeBody(body []byte, useAggregator bool, aggregatorName, discoveryMode, toolCallMode string) ([]byte, toolPrepReport, error) {
	if aggregatorName == "" {
		aggregatorName = defaultMcpAggregatorName
	}
	if discoveryMode == "" {
		discoveryMode = "both"
	}
	if toolCallMode == "" {
		toolCallMode = "single_name"
	}
	if len(body) == 0 {
		return body, toolPrepReport{
			AggregatorOn:   useAggregator,
			AggregatorName: aggregatorName,
			DiscoveryMode:  discoveryMode,
			ToolCallMode:   toolCallMode,
		}, nil
	}
	var inner map[string]any
	if err := json.Unmarshal(body, &inner); err != nil {
		return body, toolPrepReport{
			AggregatorOn:   useAggregator,
			AggregatorName: aggregatorName,
			DiscoveryMode:  discoveryMode,
			ToolCallMode:   toolCallMode,
		}, nil
	}
	if r, ok := inner["request"].(map[string]any); ok && len(inner) == 1 {
		inner = r
	}
	// Normalize Google GenAI SDK shape ({model, contents, config:{...}})
	// to Gemini REST API shape (top-level tools/systemInstruction/
	// generationConfig). omp's @google/genai client serializes requests
	// in the new SDK form; cloudcode-pa rejects the `config` field
	// (verified 2026-06-14 via direct curl: HTTP 400 with `Invalid JSON
	// payload received. Unknown name "config" at 'request': Cannot find
	// field.`). No-op when the body is already REST-shaped.
	normalizeOmpGeminiSDKShape(inner)
	report := applyToolPreprocessing(inner, useAggregator, aggregatorName, discoveryMode, toolCallMode)
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

// accountMcpAggregatorName returns the on-wire function name to use for
// the MCP aggregator. Reads `mcp_aggregator_name` from per-account
// credentials; defaults to "call_mcp_tool" (agy parity) when empty,
// missing, or invalid.
//
// Validation: the name must match Gemini's functionDeclaration name
// convention — leading letter or underscore, then [A-Za-z0-9_]. Invalid
// names fall back to the default so a typo in admin UI can't break the
// gateway.
func accountMcpAggregatorName(account *Account) string {
	if account == nil {
		return defaultMcpAggregatorName
	}
	raw, _ := account.Credentials["mcp_aggregator_name"].(string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultMcpAggregatorName
	}
	if !isValidMcpAggregatorName(raw) {
		return defaultMcpAggregatorName
	}
	return raw
}

// isValidMcpAggregatorName enforces the Gemini functionDeclaration name
// rules: 1-64 chars, leading [A-Za-z_], rest [A-Za-z0-9_].
func isValidMcpAggregatorName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r == '_':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// rewriteAggregatedFunctionCalls walks a single SSE chunk's `candidates`
// array and rewrites any `functionCall` whose name is `call_mcp_tool`,
// `list_mcp_tools`, or `read_mcp_tool_schema` back into a shape omp
// expects:
//
//   - call_mcp_tool        → mcp__<server>_<tool>(...Arguments...)
//   - list_mcp_tools       → synthesize a `mcp__<server>__list` no-op or
//     keep as-is so omp can route to a builtin
//     (we just inline the catalog inside args).
//   - read_mcp_tool_schema → keep as-is (omp doesn't have a matching tool;
//     the model uses this purely for its own
//     context — schema injected via list/read calls
//     in the prompt prefix or via a sidecar).
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
	// Peel agymimic envelope when present so the loop's wrapped payloads
	// are walked correctly. Mutation continues in-place on the shared
	// map; json.Marshal(root) at the end preserves any outer fields.
	target := root
	if r, ok := root["response"].(map[string]any); ok {
		if _, hasCands := r["candidates"]; hasCands {
			target = r
		}
	}

	cands, ok := target["candidates"].([]any)
	if !ok || len(cands) == 0 {
		return payload
	}
	rewriteCount := 0
	resolveFailures := 0
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
			aggName := report.AggregatorName
			if aggName == "" {
				aggName = defaultMcpAggregatorName
			}
			if name != aggName {
				continue
			}
			args, _ := fc["args"].(map[string]any)
			if args == nil {
				slog.Warn("native: call_mcp_tool with nil args — leaving as-is",
					"name", name)
				continue
			}
			server, _ := args["ServerName"].(string)
			tool, _ := args["ToolName"].(string)
			inner, _ := args["Arguments"].(map[string]any)
			handle, found := report.resolveMcpHandle(server, tool)
			if !found {
				// Log with nearest candidates so we can diagnose without
				// asking the user for screenshots. The candidates list
				// helps spot whether the requested tool genuinely doesn't
				// exist or the model paraphrased a name we should have
				// caught.
				slog.Warn("native: call_mcp_tool failed to resolve — leaving as-is for omp to report",
					"server_requested", server,
					"tool_requested", tool,
					"available_servers", report.availableServerNames(),
					"nearest_candidates", report.nearestMcpHandles(server, tool, 5),
					"total_mcp_tools_in_request", len(report.McpTools))
				resolveFailures++
				continue
			}
			rewriteCount++
			slog.Info("native: call_mcp_tool rewritten",
				"server_requested", server,
				"tool_requested", tool,
				"resolved_to", handle.FullName,
				"arg_keys", sortedMapKeys(inner))
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
	if rewriteCount > 0 || resolveFailures > 0 {
		slog.Info("native: call_mcp_tool rewrite summary",
			"rewritten", rewriteCount,
			"resolve_failures", resolveFailures,
			"available_mcp_tools", len(report.McpTools))
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
	// Fast-path: only parse if the configured aggregator name (or the
	// legacy default) appears in the line.
	aggName := report.AggregatorName
	if aggName == "" {
		aggName = defaultMcpAggregatorName
	}
	if !containsToken(line, []byte(aggName)) &&
		(aggName == defaultMcpAggregatorName ||
			!containsToken(line, []byte(defaultMcpAggregatorName))) {
		// Either the model emitted neither the configured nor the legacy
		// name → no work to do.
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

// availableServerNames returns the sorted, deduplicated list of MCP
// server names present in this report. Used for diagnostic logging
// when a call_mcp_tool resolution fails — caller (or future you
// reading the logs) immediately sees which servers actually were
// available so they can spot model paraphrases / typos.
func (r toolPrepReport) availableServerNames() []string {
	seen := map[string]struct{}{}
	for _, h := range r.McpTools {
		server, _ := splitMcpFullName(h.FullName)
		if server != "" {
			seen[server] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// nearestMcpHandles returns up to `n` MCP tool FullNames sorted by
// Levenshtein distance against `mcp__<server>_<tool>` (dash-normalized).
// Used purely for diagnostic logging: when call_mcp_tool resolution
// fails we want the nearest candidates in the log line so we can spot
// whether the resolver missed something it should have caught, vs the
// model truly hallucinating a non-existent tool.
func (r toolPrepReport) nearestMcpHandles(server, tool string, n int) []string {
	if n <= 0 || len(r.McpTools) == 0 {
		return nil
	}
	serverNorm := strings.ReplaceAll(server, "-", "_")
	toolNorm := strings.ReplaceAll(tool, "-", "_")
	want := "mcp__" + serverNorm + "_" + toolNorm

	type scored struct {
		name string
		dist int
	}
	scoredList := make([]scored, 0, len(r.McpTools))
	for _, h := range r.McpTools {
		scoredList = append(scoredList, scored{name: h.FullName, dist: levenshtein(want, h.FullName)})
	}
	sort.Slice(scoredList, func(i, j int) bool { return scoredList[i].dist < scoredList[j].dist })
	if n > len(scoredList) {
		n = len(scoredList)
	}
	out := make([]string, 0, n)
	for _, s := range scoredList[:n] {
		out = append(out, s.name)
	}
	return out
}

// ---------------------------------------------------------------------------
// Google GenAI SDK config → Gemini REST API top-level shape normalizer
// ---------------------------------------------------------------------------

// sdkGenerationConfigKeys is the set of `config.*` keys that belong under
// REST `generationConfig.*`. Everything else stays where it is (or gets
// lifted by the explicit cases in normalizeOmpGeminiSDKShape).
//
// Reference: https://ai.google.dev/api/generate-content#generationconfig
// — the GenerationConfig message fields. The SDK flattens these into
// the top-level `config` bag alongside tools/systemInstruction; REST
// wants them grouped under generationConfig.
var sdkGenerationConfigKeys = []string{
	"maxOutputTokens",
	"thinkingConfig",
	"temperature",
	"topP",
	"topK",
	"candidateCount",
	"stopSequences",
	"responseMimeType",
	"responseSchema",
	"responseJsonSchema",
	"responseModalities",
	"speechConfig",
	"mediaResolution",
	"seed",
	"audioTimestamp",
	"presencePenalty",
	"frequencyPenalty",
	"responseLogprobs",
	"logprobs",
	"routingConfig",
	"modelSelectionConfig",
}

// sdkTopLevelKeys is the set of `config.*` keys that belong AT the
// REST request root, not nested. Lifted verbatim.
var sdkTopLevelKeys = []string{
	"tools",
	"systemInstruction",
	"toolConfig",
	"safetySettings",
	"cachedContent",
	"labels",
}

// sdkDropKeys is the set of `config.*` keys that are SDK-side runtime
// metadata with no upstream meaning. We drop them silently.
var sdkDropKeys = []string{
	"abortSignal",
	"httpOptions",
	"signal",
}

// normalizeOmpGeminiSDKShape lifts Google GenAI SDK `config` bag shape
// to Gemini REST API top-level keys, removing `config` when done. omp's
// @google/genai client serializes outbound requests as:
//
//	{
//	  "model":    "gemini-3.1-pro",
//	  "contents": [...],
//	  "config":   {
//	    "tools":             [...],
//	    "systemInstruction": {...},
//	    "thinkingConfig":    {...},
//	    "maxOutputTokens":   65536,
//	    "abortSignal":       {...},
//	    ...
//	  }
//	}
//
// cloudcode-pa upstream rejects the `config` field with HTTP 400
// "Invalid JSON payload received. Unknown name 'config' at 'request':
// Cannot find field." (verified 2026-06-14 via direct curl against
// gemini-3.1-pro:generateContent).
//
// We rewrite to the REST shape:
//
//	{
//	  "contents":          [...],
//	  "tools":             [...],
//	  "systemInstruction": {...},
//	  "generationConfig":  {
//	    "thinkingConfig":  {...},
//	    "maxOutputTokens": 65536
//	  }
//	}
//
// Conflict policy: when a key exists at BOTH top-level and inside
// config, the top-level value wins (caller-supplied REST shape > SDK
// shape). This makes the function idempotent and safe for callers that
// already pre-normalized.
//
// Idempotent. No-op when `config` is absent.
func normalizeOmpGeminiSDKShape(inner map[string]any) {
	if inner == nil {
		return
	}
	cfg, ok := inner["config"].(map[string]any)
	if !ok {
		return
	}

	// Step 1: lift top-level REST keys verbatim. Top-level wins on conflict.
	for _, key := range sdkTopLevelKeys {
		v, ok := cfg[key]
		if !ok {
			continue
		}
		if _, present := inner[key]; !present {
			inner[key] = v
		}
	}

	// Step 2: lift generationConfig sub-keys. Build a fresh map merging
	// any pre-existing inner.generationConfig with the SDK-style keys
	// from config. Existing inner.generationConfig values win on conflict.
	gc, _ := inner["generationConfig"].(map[string]any)
	if gc == nil {
		gc = map[string]any{}
	}
	for _, key := range sdkGenerationConfigKeys {
		v, ok := cfg[key]
		if !ok {
			continue
		}
		if _, present := gc[key]; !present {
			gc[key] = v
		}
	}
	if len(gc) > 0 {
		inner["generationConfig"] = gc
	}

	// Step 3: drop `config` entirely. Any SDK-only metadata (abortSignal
	// etc.) goes away with it. We don't need to enumerate sdkDropKeys
	// explicitly because the whole `config` object is being removed —
	// the list exists for documentation only.
	_ = sdkDropKeys
	delete(inner, "config")

	// Step 4: drop the SDK's `model` field at top level. The REST API
	// expects the model in the URL path, not in the request body; some
	// upstream variants tolerate it but real agy never sends it inside
	// the request body. wrapNativeV1Internal will set `envelope.model`
	// from the URL-derived wireModel.
	delete(inner, "model")
}

// ---------------------------------------------------------------------------
// Diagnostic helpers for slog calls in the native tool-aggregator pipeline.
// Kept small and allocation-light so they can run on every request without
// showing up in profiles.
// ---------------------------------------------------------------------------

// sortedMapKeys returns the keys of m sorted alphabetically, capped at 32
// entries. Used to print which arg names the model passed inside a
// call_mcp_tool invocation without dumping arbitrarily large blobs into
// the log line.
func sortedMapKeys(m map[string]any) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	if len(out) > 32 {
		out = out[:32]
	}
	return out
}

// sampleMcpHandleNames returns up to n FullName values from handles,
// preserving order. Returns nil when handles is empty so the slog
// attribute renders as a clean omitted field.
func sampleMcpHandleNames(handles []mcpToolHandle, n int) []string {
	if len(handles) == 0 || n <= 0 {
		return nil
	}
	if n > len(handles) {
		n = len(handles)
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = handles[i].FullName
	}
	return out
}

// sampleStrings returns up to n entries from in, preserving order.
func sampleStrings(in []string, n int) []string {
	if len(in) == 0 || n <= 0 {
		return nil
	}
	if n > len(in) {
		n = len(in)
	}
	return append([]string(nil), in[:n]...)
}
