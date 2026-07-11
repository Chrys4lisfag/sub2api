// Tool-list preprocessing for the antigravity_native gateway. Two pieces:
//
//  1. normalizeToolsForAntigravity — converts each functionDeclaration's
//     `parametersJsonSchema` (raw JSON Schema, draft 2020-12) to the
//     `parameters` Gemini Schema format real agy.exe uses. Strips
//     unsupported keywords, uppercases types, flattens anyOf/oneOf.
//
//  2. applyToolAggregator — replaces all MCP-prefixed function
//     declarations with a single `call_mcp_tool` aggregator (the pattern
//     real agy.exe uses). Reduces effective tool count from 200+ down to
//     ~20, sidestepping Gemini's "empty-args under tool overload" failure
//     mode. The aggregator also adds `list_mcp_tools` + `read_mcp_tool_schema`
//     so the model can discover MCP tool schemas on demand.
//
// Both are middleware: they mutate the inner request body just before
// envelope wrap in wrapNativeV1Internal.
package service

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// applyToolPreprocessing runs the schema normalizer + the tool-call-mode-
// driven preprocessing pipeline. Mutates `inner` in place.
//
// Modes:
//
//	"single_name" — mcp__<server>_<tool> declarations stay in the tools
//	                list (schema-normalized). call_mcp_tool +
//	                agy_list_tools are ALSO declared alongside so the
//	                model has the three valid call paths. A short
//	                single-name-mode instruction block is injected into
//	                systemInstruction. No full catalog enumeration —
//	                the declarations already carry tool names + schemas.
//
//	"agy_mimic"   — mcp__* stripped from declarations; only call_mcp_tool
//	                (and agy_list_tools when discovery_mode allows) is
//	                exposed. Full per-server catalog enumeration injected
//	                in systemInstruction. Matches real agy.exe wire.
//
// `aggregatorName` is the function name model emits to reach the
// call_mcp_tool aggregator. Defaults to "call_mcp_tool" (agy parity).
//
// `discoveryMode` ("prompt" / "list_tool" / "both") controls whether
// agy_list_tools is declared and the agy_mimic catalog form. Works in
// both tool-call modes.
func applyToolPreprocessing(inner map[string]any, useAggregator bool, aggregatorName, discoveryMode, toolCallMode string) toolPrepReport {
	if aggregatorName == "" {
		aggregatorName = defaultMcpAggregatorName
	}
	if discoveryMode == "" {
		discoveryMode = "both"
	}
	if toolCallMode == "" {
		toolCallMode = "single_name"
	}
	report := toolPrepReport{
		Normalized:     0,
		McpTools:       nil,
		BuiltinTools:   nil,
		AggregatorOn:   useAggregator,
		AggregatorName: aggregatorName,
		DiscoveryMode:  discoveryMode,
		ToolCallMode:   toolCallMode,
	}
	if inner == nil {
		return report
	}
	toolsAny, ok := inner["tools"].([]any)
	if !ok || len(toolsAny) == 0 {
		return report
	}
	// Mode flags (duplicated here to avoid an import cycle).
	declaresListTool := discoveryMode == "list_tool" || discoveryMode == "both"
	injectsCatalog := discoveryMode == "prompt" || discoveryMode == "both"
	stripMcp := toolCallMode == "agy_mimic"

	for _, t := range toolsAny {
		tool, ok := t.(map[string]any)
		if !ok {
			continue
		}
		fds, ok := tool["functionDeclarations"].([]any)
		if !ok {
			continue
		}
		var kept []any
		for _, fdAny := range fds {
			fd, ok := fdAny.(map[string]any)
			if !ok {
				kept = append(kept, fdAny)
				continue
			}
			name, _ := fd["name"].(string)
			if useAggregator && strings.HasPrefix(name, "mcp__") {
				// Always record in report — rewriter + resolver need this
				// list in BOTH modes (to translate any call_mcp_tool the
				// model emits and to validate direct mcp__* calls).
				report.McpTools = append(report.McpTools, mcpToolHandle{
					FullName: name,
					Decl:     fd,
				})
				if stripMcp {
					// agy_mimic — hide mcp__* from upstream model so the
					// only path to MCP is through call_mcp_tool.
					continue
				}
				// single_name — schema-normalize and keep in declarations
				// so the model can emit `mcp__<server>_<tool>` directly.
				if convertFunctionDeclarationSchema(fd) {
					report.Normalized++
				}
				kept = append(kept, fd)
				continue
			}
			report.BuiltinTools = append(report.BuiltinTools, name)
			if convertFunctionDeclarationSchema(fd) {
				report.Normalized++
			}
			kept = append(kept, fd)
		}
		if useAggregator && len(report.McpTools) > 0 {
			// call_mcp_tool aggregator: declared in BOTH modes so the
			// model has a fallback when it can't recall an exact mcp__*
			// name (single_name) or as the only path (agy_mimic).
			kept = append(kept, buildCallMcpToolDecl(aggregatorName))
			// TEMPORARY (2026-06-14): agy_list_tools is only declared in
			// agy_mimic mode. In single_name mode the model sees every
			// mcp__* tool directly in declarations so the discovery tool
			// is redundant; AND the discovery loop in
			// AntigravityNativeGatewayService.ForwardGemini forces non-
			// streaming upstream calls + single-event SSE flush, killing
			// token streaming UX for the client. Until streaming-aware
			// discovery lands (see TODO in resolveAgyListToolsLoop), we
			// keep the tool gated behind agy_mimic. The old behavior is
			// preserved below in a commented block so the streaming-aware
			// rework can be slotted in without re-deriving the gating.
			if declaresListTool && stripMcp {
				kept = append(kept, buildAgyListToolsDecl())
			}
			// Pre-2026-06-14 behavior — restore once streaming-aware
			// discovery is implemented:
			//   if declaresListTool {
			//       kept = append(kept, buildAgyListToolsDecl())
			//   }
		}
		tool["functionDeclarations"] = kept
	}
	if useAggregator && len(report.McpTools) > 0 {
		if stripMcp {
			// agy_mimic — full per-server catalog enumeration because
			// the model has no other way to discover what's available.
			injectMcpCatalogIntoSystemInstruction(inner, report.McpTools, aggregatorName, discoveryMode, injectsCatalog, declaresListTool)
		} else {
			// single_name — short instruction block. Tools self-describe
			// in declarations; we only need to tell the model HOW to
			// reach them and remind it of the fallback paths.
			//
			// TEMPORARY (2026-06-14): agy_list_tools is gated behind
			// agy_mimic to preserve streaming UX (see decl-append block
			// above + TODO in resolveAgyListToolsLoop). Since we don't
			// declare it in single_name, we also don't mention it in
			// the instructions. The third arg used to be `declaresListTool`;
			// hard-pin to false until streaming-aware discovery lands.
			injectSingleNameInstructionsIntoSystemInstruction(inner, aggregatorName, false)
			// Pre-2026-06-14:
			//   injectSingleNameInstructionsIntoSystemInstruction(inner, aggregatorName, declaresListTool)
		}
	}
	return report
}

// toolPrepReport captures what the preprocessing pipeline did. The
// response-stream back-translator needs McpTools to map call_mcp_tool
// invocations back to the original mcp__server_tool names that omp
// expects to see in tool-call deltas.
type toolPrepReport struct {
	Normalized     int             // count of functionDeclarations whose schema we converted
	McpTools       []mcpToolHandle // MCP tools that were hidden behind the aggregator
	BuiltinTools   []string        // non-MCP tools we kept verbatim
	AggregatorOn   bool
	AggregatorName string // function name model uses to invoke aggregator (default "call_mcp_tool")
	DiscoveryMode  string // "prompt" | "list_tool" | "both"
	ToolCallMode   string // "single_name" | "agy_mimic"
}

// mcpToolHandle remembers an MCP tool's original name + declaration so
// the call_mcp_tool back-translator can rebuild a synthetic tool-call.
type mcpToolHandle struct {
	FullName string         // e.g. "mcp__ida_orchestrator_read_memory"
	Decl     map[string]any // the original functionDeclaration object
}

// ---------------------------------------------------------------------------
// Schema normalization: parametersJsonSchema (JSON Schema) → parameters
// (Gemini Schema). Verified against agy.exe's wire (May 2026 Frida capture).
// ---------------------------------------------------------------------------

// convertFunctionDeclarationSchema converts one declaration's schema
// field from `parametersJsonSchema` (lowercase, with $schema/anyOf/etc.)
// to `parameters` (uppercase OBJECT/STRING, JSON-Schema-subset). Returns
// true if a conversion was applied.
//
// Idempotent: if the declaration already uses `parameters`, no-op.
func convertFunctionDeclarationSchema(fd map[string]any) bool {
	raw, has := fd["parametersJsonSchema"]
	if !has {
		return false
	}
	schema, ok := raw.(map[string]any)
	if !ok {
		// Non-object schema (rare) — drop it; downstream model won't
		// know how to call this tool anyway.
		delete(fd, "parametersJsonSchema")
		return false
	}
	converted := convertSchemaToGeminiSchema(schema)
	delete(fd, "parametersJsonSchema")
	if converted != nil {
		fd["parameters"] = converted
	}
	return true
}

// convertSchemaToGeminiSchema walks a JSON Schema draft 2020-12 object
// and emits the Gemini Schema subset agy uses on the wire:
//
//   - "type" values: lowercase → UPPERCASE (object → OBJECT)
//   - Drop $schema, $id, $ref, $defs, definitions, examples, default
//   - Drop format/pattern (Gemini Schema doesn't honor most formats)
//   - Flatten anyOf/oneOf: pick the first non-null branch; if all
//     branches are non-null we keep the first
//   - allOf: merge properties shallowly
//   - Recurse into properties / items / additionalProperties
//
// Returns nil for empty / unconvertible schemas.
func convertSchemaToGeminiSchema(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}
	// Resolve anyOf/oneOf/allOf first.
	if anyOf, ok := schema["anyOf"].([]any); ok && len(anyOf) > 0 {
		picked := pickFirstNonNullBranch(anyOf)
		if picked != nil {
			return convertSchemaToGeminiSchema(picked)
		}
	}
	if oneOf, ok := schema["oneOf"].([]any); ok && len(oneOf) > 0 {
		picked := pickFirstNonNullBranch(oneOf)
		if picked != nil {
			return convertSchemaToGeminiSchema(picked)
		}
	}
	if allOf, ok := schema["allOf"].([]any); ok && len(allOf) > 0 {
		merged := map[string]any{}
		for _, b := range allOf {
			if bm, ok := b.(map[string]any); ok {
				for k, v := range bm {
					if _, present := merged[k]; !present {
						merged[k] = v
					}
				}
			}
		}
		for k, v := range schema {
			if k == "allOf" {
				continue
			}
			if _, present := merged[k]; !present {
				merged[k] = v
			}
		}
		return convertSchemaToGeminiSchema(merged)
	}

	out := map[string]any{}

	// type — uppercase the string form. Skip array-of-types (rare).
	if t, ok := schema["type"].(string); ok {
		out["type"] = strings.ToUpper(t)
	}

	// Pass through allowed scalar keywords.
	for _, k := range []string{"description", "title", "enum", "nullable"} {
		if v, ok := schema[k]; ok {
			out[k] = v
		}
	}

	// required — pass through, but only if the type is OBJECT.
	if req, ok := schema["required"]; ok {
		out["required"] = req
	}

	// properties — recurse.
	if props, ok := schema["properties"].(map[string]any); ok {
		newProps := map[string]any{}
		for k, v := range props {
			if vm, ok := v.(map[string]any); ok {
				if conv := convertSchemaToGeminiSchema(vm); conv != nil {
					newProps[k] = conv
				}
			}
		}
		out["properties"] = newProps
	}

	// items — recurse for arrays.
	if items, ok := schema["items"].(map[string]any); ok {
		if conv := convertSchemaToGeminiSchema(items); conv != nil {
			out["items"] = conv
		}
	}

	// additionalProperties — only keep if it's a schema object, drop
	// boolean form (Gemini Schema treats absence as permissive).
	if ap, ok := schema["additionalProperties"].(map[string]any); ok {
		if conv := convertSchemaToGeminiSchema(ap); conv != nil {
			out["additionalProperties"] = conv
		}
	}

	// Sort property keys deterministically so logging diffs are stable.
	// (No-op for runtime behavior.)
	if props, ok := out["properties"].(map[string]any); ok && len(props) > 0 {
		keys := make([]string, 0, len(props))
		for k := range props {
			keys = append(keys, k)
		}
		sort.Strings(keys)
	}

	return out
}

// pickFirstNonNullBranch returns the first branch in an anyOf/oneOf slice
// whose `type` is not "null" / "NULL". If all branches are null, returns
// nil. Used to flatten nullable polyfills like `anyOf: [{type:"string"},
// {type:"null"}]`.
func pickFirstNonNullBranch(branches []any) map[string]any {
	for _, b := range branches {
		bm, ok := b.(map[string]any)
		if !ok {
			continue
		}
		if t, ok := bm["type"].(string); ok && strings.EqualFold(t, "null") {
			continue
		}
		return bm
	}
	return nil
}

// ---------------------------------------------------------------------------
// call_mcp_tool aggregator. Matches the declaration agy.exe emits verbatim.
// ---------------------------------------------------------------------------

// buildCallMcpToolDecl returns the declaration for the call_mcp_tool
// aggregator. Schema matches real agy.exe wire capture (May 2026):
//
//	{
//	  "name": "call_mcp_tool",
//	  "description": "Call a lazy-loaded MCP tool. Read the tool's schema
//	                  file to understand the tool's arguments and usage.",
//	  "parameters": {
//	    "type": "OBJECT",
//	    "properties": {
//	      "ServerName": {"type":"STRING", "description":"Name of the MCP server."},
//	      "ToolName":   {"type":"STRING", "description":"Name of the tool to call."},
//	      "Arguments":  {"description":"Arguments to pass to the tool."},
//	      "toolAction": {"type":"STRING"},
//	      "toolSummary":{"type":"STRING"}
//	    },
//	    "required": ["ServerName","ToolName"]
//	  }
//	}
//
// defaultMcpAggregatorName is the on-wire function name real agy uses
// to reach the MCP aggregator. Per-account credentials may override
// via `mcp_aggregator_name` (see accountMcpAggregatorName).
const defaultMcpAggregatorName = "call_mcp_tool"

func buildCallMcpToolDecl(aggregatorName string) map[string]any {
	if aggregatorName == "" {
		aggregatorName = defaultMcpAggregatorName
	}
	return map[string]any{
		"name": aggregatorName,
		"description": "Invoke an MCP (Model Context Protocol) tool. MUST be called " +
			"as a TOP-LEVEL functionCall — NEVER via `eval`/Python and NEVER as " +
			"`tool.call_mcp_tool(...)` inside an eval cell (that path is not wired " +
			"and will hang). This is the ONLY way to reach an MCP server; there " +
			"are no individual mcp__* tools in the declarations. The MCP catalog " +
			"(server names, tool names, argument schemas) is in the system " +
			"instructions; pick a (ServerName, ToolName) pair from there literally " +
			"and supply Arguments as a JSON object matching the tool's input " +
			"schema. toolAction/toolSummary are short labels for the activity " +
			"feed and may be omitted.",
		"parameters": map[string]any{
			"type": "OBJECT",
			"properties": map[string]any{
				"ServerName": map[string]any{
					"type":        "STRING",
					"description": "Name of the MCP server (e.g. github-official, ida-orchestrator).",
				},
				"ToolName": map[string]any{
					"type":        "STRING",
					"description": "Name of the tool to call within the server (without server prefix).",
				},
				"Arguments": map[string]any{
					"type":        "OBJECT",
					"description": "Arguments to pass to the tool. Must match the tool's input schema.",
				},
				"toolAction": map[string]any{
					"type":        "STRING",
					"description": "Brief 2-5 word summary of what this tool is doing, sentence-capitalized. Examples: 'Reading file', 'Searching repo'.",
				},
				"toolSummary": map[string]any{
					"type":        "STRING",
					"description": "Brief 2-5 word noun phrase describing what this call is about.",
				},
			},
			"required": []any{"ServerName", "ToolName"},
		},
	}
}

// ---------------------------------------------------------------------------
// agy_list_tools — transparent server-side MCP discovery.
//
// When list-tools emulation is enabled (global setting), sub2api injects a
// second function declaration alongside call_mcp_tool. The model can emit
// agy_list_tools as a top-level functionCall to receive the FULL MCP
// catalog as a functionResponse. The roundtrip is handled entirely server
// side: sub2api intercepts the model's call, synthesizes a response, and
// re-issues the upstream request with the assistant.call + user.response
// pair appended. The downstream client never sees agy_list_tools at all.
//
// Why this is more workaround-resistant than catalog-in-systemInstruction
// alone: a tool CALL is an action the model performs, not passive text it
// reads. Performing the discovery commits the model to "MCP mode" — its
// natural follow-up is call_mcp_tool, not a fallback like client-side
// Python eval or manual filesystem inspection.
// ---------------------------------------------------------------------------

// defaultListToolsName is the on-wire function name for the discovery
// helper. Kept stable for now; could be made configurable later.
const defaultListToolsName = "agy_list_tools"

// listToolsCallBudget caps the number of agy_list_tools roundtrips per
// downstream client request. Prevents runaway loops when the model is
// confused and keeps repeating the discovery call.
const listToolsCallBudget = 3

// buildAgyListToolsDecl returns the function declaration the model can
// call to receive the full MCP catalog. Used only when list-tools
// emulation is enabled (see accountListToolsEmulationEnabled).
func buildAgyListToolsDecl() map[string]any {
	return map[string]any{
		"name": defaultListToolsName,
		"description": "List available MCP (Model Context Protocol) tools and their " +
			"argument schemas. Call `agy_list_tools` BEFORE `" + defaultMcpAggregatorName +
			"` when you need to discover what MCP servers and tools are " +
			"available, or to inspect the input schema of a specific tool. " +
			"Returns a JSON object keyed by server name, with each server " +
			"mapped to its tool list. Pass an optional `server` argument to " +
			"filter to one server's tools. MUST be invoked as a TOP-LEVEL " +
			"functionCall — NEVER via `eval`/Python and NEVER as " +
			"`tool.agy_list_tools(...)` inside an eval cell.",
		"parameters": map[string]any{
			"type": "OBJECT",
			"properties": map[string]any{
				"server": map[string]any{
					"type":        "STRING",
					"description": "Optional: filter the returned catalog to one server's tools (e.g. \"electerm\", \"github-official\"). Omit to receive all servers.",
				},
			},
		},
	}
}

// synthesizeListToolsResponse builds the JSON object returned as the
// `response` field of the functionResponse when the model calls
// agy_list_tools. Shape:
//
//	{
//	  "servers": {
//	    "electerm": [
//	      {
//	        "name": "list_electerm_bookmarks",
//	        "description": "...",
//	        "args_schema": { ... }
//	      },
//	      ...
//	    ],
//	    "github-official": [ ... ]
//	  },
//	  "totalServers": 2,
//	  "totalTools": 47,
//	  "filteredBy": "electerm" // omitted when no filter
//	}
//
// `serverFilter` is the optional `server` argument from the model's call.
// Empty → return all servers. Unknown server name → return empty servers
// map + helpful note in `unknownServer` field.
func synthesizeListToolsResponse(mcpTools []mcpToolHandle, serverFilter string) map[string]any {
	filter := strings.TrimSpace(serverFilter)
	// Group tools by server (derived from full name "mcp__<server>_<tool>").
	type toolEntry struct {
		name        string
		description string
		schema      any
	}
	byServer := map[string][]toolEntry{}
	totalTools := 0
	for _, h := range mcpTools {
		server, tool := splitMcpFullName(h.FullName)
		if server == "" || tool == "" {
			continue
		}
		if filter != "" && !serverNameMatches(server, filter) {
			continue
		}
		desc, _ := h.Decl["description"].(string)
		desc = strings.TrimSpace(desc)
		var schema any
		if s, ok := h.Decl["parametersJsonSchema"]; ok {
			schema = s
		} else if s, ok := h.Decl["parameters"]; ok {
			schema = s
		}
		byServer[server] = append(byServer[server], toolEntry{
			name:        tool,
			description: desc,
			schema:      schema,
		})
		totalTools++
	}
	// Serialize to map[string]any with sorted tool order per server (stable
	// for caching + diffing).
	servers := map[string]any{}
	serverNames := make([]string, 0, len(byServer))
	for s := range byServer {
		serverNames = append(serverNames, s)
	}
	sort.Strings(serverNames)
	for _, s := range serverNames {
		entries := byServer[s]
		sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
		arr := make([]any, 0, len(entries))
		for _, e := range entries {
			row := map[string]any{
				"name":        e.name,
				"description": e.description,
			}
			if e.schema != nil {
				row["args_schema"] = e.schema
			}
			arr = append(arr, row)
		}
		servers[s] = arr
	}
	resp := map[string]any{
		"servers":      servers,
		"totalServers": len(servers),
		"totalTools":   totalTools,
	}
	if filter != "" {
		resp["filteredBy"] = filter
		if len(servers) == 0 {
			resp["unknownServer"] = filter
			resp["hint"] = "No server matched. Call agy_list_tools with no `server` arg to see all available servers, or use call_mcp_tool with one of the literal names from the systemInstruction catalog."
		}
	}
	return resp
}

// serverNameMatches compares server names from the catalog (which are
// derived from mcp__<server>_ prefix via underscore convention) against
// the user-friendly form (which may use dashes). Both directions
// normalized to underscores for comparison.
func serverNameMatches(serverFromCatalog, userFilter string) bool {
	a := strings.ReplaceAll(serverFromCatalog, "-", "_")
	b := strings.ReplaceAll(userFilter, "-", "_")
	return strings.EqualFold(a, b)
}

func buildListMcpToolsDecl() map[string]any { //nolint:unused // synthetic MCP discovery tool, retained for list_tool mode variants
	return map[string]any{
		"name": "list_mcp_tools",
		"description": "List all MCP tools available in the current session. Returns a JSON array of " +
			"{server_name, tool_name, description} so you can find the right tool before calling call_mcp_tool.",
		"parameters": map[string]any{
			"type":       "OBJECT",
			"properties": map[string]any{},
		},
	}
}

func buildReadMcpToolSchemaDecl() map[string]any { //nolint:unused // synthetic MCP schema tool, retained for list_tool mode variants
	return map[string]any{
		"name": "read_mcp_tool_schema",
		"description": "Fetch the input schema (JSON Schema) for a specific MCP tool. " +
			"Call this before call_mcp_tool when you need to know what arguments are expected.",
		"parameters": map[string]any{
			"type": "OBJECT",
			"properties": map[string]any{
				"ServerName": map[string]any{
					"type":        "STRING",
					"description": "Name of the MCP server.",
				},
				"ToolName": map[string]any{
					"type":        "STRING",
					"description": "Name of the tool whose schema to fetch.",
				},
			},
			"required": []any{"ServerName", "ToolName"},
		},
	}
}

// ---------------------------------------------------------------------------
// Lookup helpers used by the response-stream back-translator.
// ---------------------------------------------------------------------------

// resolveMcpHandle finds the original mcp__server_tool full name + decl
// from a (ServerName, ToolName) pair the model emitted via call_mcp_tool.
// Returns (handle, true) on match, (zero, false) otherwise.
//
// Matching rules:
//  1. exact match on the full mcp__server_tool name
//  2. fuzzy match: server "ida-orchestrator" matches handle prefix
//     "mcp__ida_orchestrator_" (dashes → underscores)
func (r toolPrepReport) resolveMcpHandle(server, tool string) (mcpToolHandle, bool) {
	serverNorm := strings.ReplaceAll(server, "-", "_")
	toolNorm := strings.ReplaceAll(tool, "-", "_")

	// Tier 1: exact mcp__server_tool match.
	want := "mcp__" + serverNorm + "_" + toolNorm
	for _, h := range r.McpTools {
		if h.FullName == want {
			return h, true
		}
	}

	// Tier 2: server-prefix + tool-suffix exact match. Catches omp's
	// prefix-stripping (server "electerm" + tool "list_bookmarks" maps to
	// real name "mcp__electerm_list_electerm_bookmarks").
	prefix := "mcp__" + serverNorm + "_"
	for _, h := range r.McpTools {
		if strings.HasPrefix(h.FullName, prefix) && strings.HasSuffix(h.FullName, "_"+toolNorm) {
			return h, true
		}
	}

	// Tier 3: server-prefix + Levenshtein-closest tool. Model
	// hallucinations like "get_electerm_bookmarks" (instead of the
	// catalog's "list_electerm_bookmarks") fall through to here. We
	// scan all tools under the requested server, score them by
	// edit-distance against the requested tool name, and pick the
	// closest if it's within a sensible threshold (≤ 40% of the
	// longer string's length — keeps "list" vs "get" matching but
	// rejects wholesale renames like "create_issue" → "list_files").
	var best mcpToolHandle
	bestDist := -1
	for _, h := range r.McpTools {
		if !strings.HasPrefix(h.FullName, prefix) {
			continue
		}
		// extract the per-tool portion under the server prefix
		toolPart := strings.TrimPrefix(h.FullName, prefix)
		d := levenshtein(toolNorm, toolPart)
		if bestDist == -1 || d < bestDist {
			bestDist = d
			best = h
		}
	}
	if bestDist >= 0 {
		longer := len(toolNorm)
		if l := len(strings.TrimPrefix(best.FullName, prefix)); l > longer {
			longer = l
		}
		if longer > 0 && bestDist*100/longer <= 40 {
			return best, true
		}
	}

	// Tier 4: scan ALL handles by Levenshtein on the full name. Catches
	// model hallucinations on BOTH server + tool. Same 40% threshold.
	want = "mcp__" + serverNorm + "_" + toolNorm
	best = mcpToolHandle{}
	bestDist = -1
	for _, h := range r.McpTools {
		d := levenshtein(want, h.FullName)
		if bestDist == -1 || d < bestDist {
			bestDist = d
			best = h
		}
	}
	if bestDist >= 0 {
		longer := len(want)
		if l := len(best.FullName); l > longer {
			longer = l
		}
		if longer > 0 && bestDist*100/longer <= 30 {
			return best, true
		}
	}

	return mcpToolHandle{}, false
}

// levenshtein returns the Levenshtein edit distance between two strings.
// Pure-Go, no deps. Used by resolveMcpHandle's fuzzy tier to recover from
// model hallucinations on MCP tool names.
func levenshtein(a, b string) int {
	if a == b {
		return 0
	}
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	// Two-row dynamic programming table.
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			m := del
			if ins < m {
				m = ins
			}
			if sub < m {
				m = sub
			}
			curr[j] = m
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

// renderMcpToolCatalog returns a JSON-encoded array of
//
//	[{"server_name": "...", "tool_name": "...", "description": "..."}, ...]
//
// suitable as the response body for the list_mcp_tools synthetic tool.
// Used by the back-translator when the model invokes list_mcp_tools.
func (r toolPrepReport) renderMcpToolCatalog() []byte { //nolint:unused // back-translator for list_mcp_tools synthetic tool
	type catalogEntry struct {
		ServerName  string `json:"server_name"`
		ToolName    string `json:"tool_name"`
		Description string `json:"description,omitempty"`
	}
	out := make([]catalogEntry, 0, len(r.McpTools))
	for _, h := range r.McpTools {
		// Reverse the mcp__<server>_<tool> normalization.
		name := strings.TrimPrefix(h.FullName, "mcp__")
		// Heuristic split: first underscore segment is server, rest is tool.
		// Real omp uses sanitized server names, so this is a best-effort
		// readback; the call_mcp_tool model only needs THIS string back.
		idx := strings.Index(name, "_")
		var server, tool string
		if idx < 0 {
			server, tool = name, name
		} else {
			server, tool = name[:idx], name[idx+1:]
		}
		desc, _ := h.Decl["description"].(string)
		out = append(out, catalogEntry{
			ServerName:  server,
			ToolName:    tool,
			Description: desc,
		})
	}
	buf, _ := json.Marshal(out)
	return buf
}

// renderMcpToolSchema returns a JSON-encoded schema for a specific
// (server, tool). Used by read_mcp_tool_schema. Falls back to an empty
// object if no match.
func (r toolPrepReport) renderMcpToolSchema(server, tool string) []byte { //nolint:unused // back-translator for read_mcp_tool_schema synthetic tool
	h, ok := r.resolveMcpHandle(server, tool)
	if !ok {
		return []byte(`{"error":"unknown MCP tool"}`)
	}
	// Prefer parametersJsonSchema (omp's original) but fall back to parameters.
	if sch, ok := h.Decl["parametersJsonSchema"]; ok {
		buf, _ := json.Marshal(sch)
		return buf
	}
	if sch, ok := h.Decl["parameters"]; ok {
		buf, _ := json.Marshal(sch)
		return buf
	}
	return []byte(`{}`)
}

// injectMcpCatalogIntoSystemInstruction prepends an MCP-tool catalog block
// to the request's systemInstruction text part. Format mirrors what real
// agy.exe ships in its prompt context — concise newline-separated entries
// with server, tool, description, and the parsed input-schema JSON so the
// model can construct correct Arguments to call_mcp_tool.
//
// Idempotent on the catalog block: if a prior call already injected the
// marker line, we skip. The catalog block is fenced by sentinel lines so
// future injections can locate-and-replace without doubling up.
//
// Catalog layout (truncated to ~6 KiB total to keep input token budget
// reasonable on large MCP sets):
//
//	## MCP TOOL CATALOG (call via call_mcp_tool) ##
//	server: github-official | tool: create_issue
//	  description: Create a new issue in a repository.
//	  args_schema: {"type":"object","properties":{"owner":{"type":"string"}, ...}}
//	server: github-official | tool: get_pull_request
//	  description: ...
//	  args_schema: {...}
//	## END MCP TOOL CATALOG ##
//
// Mutates `inner` in place. No-op when there are no MCP tools.
func injectMcpCatalogIntoSystemInstruction(inner map[string]any, mcpTools []mcpToolHandle, aggregatorName string, discoveryMode string, fullCatalog bool, declaresListTool bool) {
	if inner == nil || len(mcpTools) == 0 {
		return
	}
	if aggregatorName == "" {
		aggregatorName = defaultMcpAggregatorName
	}
	if discoveryMode == "" {
		discoveryMode = "both" //nolint:ineffassign // normalized default; retained for downstream discovery-mode variants
	}
	catalog := buildMcpCatalogText(mcpTools, aggregatorName, fullCatalog, declaresListTool)
	if catalog == "" {
		return
	}

	sysAny, has := inner["systemInstruction"]
	if !has {
		inner["systemInstruction"] = map[string]any{
			"role":  "user",
			"parts": []any{map[string]any{"text": catalog}},
		}
		return
	}
	sys, ok := sysAny.(map[string]any)
	if !ok {
		inner["systemInstruction"] = map[string]any{
			"role":  "user",
			"parts": []any{map[string]any{"text": catalog}},
		}
		return
	}
	parts, _ := sys["parts"].([]any)
	if len(parts) == 0 {
		sys["parts"] = []any{map[string]any{"text": catalog}}
		return
	}
	first, _ := parts[0].(map[string]any)
	if first == nil {
		parts[0] = map[string]any{"text": catalog}
		sys["parts"] = parts
		return
	}
	existingText, _ := first["text"].(string)
	if strings.Contains(existingText, mcpCatalogStartMarker) {
		return
	}
	first["text"] = catalog + "\n\n" + existingText
	parts[0] = first
	sys["parts"] = parts
}

// ---------------------------------------------------------------------------
// single_name mode — short instruction block
// ---------------------------------------------------------------------------

const (
	singleNameInstructionsStartMarker = "## MCP TOOLS ##"
	singleNameInstructionsEndMarker   = "## END MCP TOOLS ##"
)

// injectSingleNameInstructionsIntoSystemInstruction prepends a SHORT
// instruction block to systemInstruction explaining the three valid
// MCP call paths in single_name mode:
//
//  1. Direct emission of mcp__<server>_<tool> functionCall (preferred —
//     these tools are visible in the declarations already)
//  2. call_mcp_tool({ServerName, ToolName, Arguments}) — fallback when
//     the model is uncertain about the exact name
//  3. agy_list_tools(server?) — discovery / verification
//
// No full catalog enumeration because the model already sees every
// mcp__* tool in its declarations. The instructions exist purely to
// remind the model of the fallback aggregators + anti-fallback rules
// (no Python eval, no paramiko, no bash for MCP-served services).
//
// Idempotent: re-injection on the same systemInstruction is a no-op
// (we check for the start marker before prepending).
func injectSingleNameInstructionsIntoSystemInstruction(inner map[string]any, aggregatorName string, declaresListTool bool) {
	if inner == nil {
		return
	}
	if aggregatorName == "" {
		aggregatorName = defaultMcpAggregatorName
	}
	text := buildSingleNameInstructions(aggregatorName, declaresListTool)
	if text == "" {
		return
	}

	sysAny, has := inner["systemInstruction"]
	if !has {
		inner["systemInstruction"] = map[string]any{
			"role":  "user",
			"parts": []any{map[string]any{"text": text}},
		}
		return
	}
	sys, ok := sysAny.(map[string]any)
	if !ok {
		inner["systemInstruction"] = map[string]any{
			"role":  "user",
			"parts": []any{map[string]any{"text": text}},
		}
		return
	}
	parts, _ := sys["parts"].([]any)
	if len(parts) == 0 {
		sys["parts"] = []any{map[string]any{"text": text}}
		return
	}
	first, _ := parts[0].(map[string]any)
	if first == nil {
		parts[0] = map[string]any{"text": text}
		sys["parts"] = parts
		return
	}
	existingText, _ := first["text"].(string)
	if strings.Contains(existingText, singleNameInstructionsStartMarker) {
		return
	}
	first["text"] = text + "\n\n" + existingText
	parts[0] = first
	sys["parts"] = parts
}

// buildSingleNameInstructions composes the short single_name-mode
// instruction block. Always smaller than ~1 KB regardless of MCP tool
// count (tools self-describe in declarations; this text only points
// the model at the right paths).
func buildSingleNameInstructions(aggregatorName string, declaresListTool bool) string {
	if aggregatorName == "" {
		aggregatorName = defaultMcpAggregatorName
	}
	var b strings.Builder
	_, _ = b.WriteString(singleNameInstructionsStartMarker)
	_ = b.WriteByte('\n')
	b.WriteString("You have direct access to MCP (Model Context Protocol) tools as\n")
	b.WriteString("`mcp__<server>_<tool>` function declarations in your toolset.\n")
	b.WriteString("PREFER calling them directly by name — this is the canonical path.\n\n")

	b.WriteString("Server-side fallback aggregators are also declared:\n\n")
	b.WriteString("1. `")
	b.WriteString(aggregatorName)
	b.WriteString("(ServerName, ToolName, Arguments)` — emit when you cannot\n")
	b.WriteString("   confidently recall the exact mcp__ name. The server translates\n")
	b.WriteString("   the split form to the corresponding mcp__<server>_<tool> call\n")
	b.WriteString("   before dispatching, with fuzzy resolution for paraphrased\n")
	b.WriteString("   server / tool names.\n\n")

	if declaresListTool {
		b.WriteString("2. `agy_list_tools(server?)` — emit to verify a tool exists or to\n")
		b.WriteString("   inspect its argument schema. Returns the authoritative\n")
		b.WriteString("   server-side catalog as a functionResponse. Pass an optional\n")
		b.WriteString("   `server` argument to scope to one server's tools.\n\n")
	}

	b.WriteString("INVOCATION RULES:\n")
	b.WriteString("- All three paths MUST be invoked as TOP-LEVEL functionCalls.\n")
	b.WriteString("- NEVER call `tool.")
	b.WriteString(aggregatorName)
	b.WriteString("(...)` or `tool.agy_list_tools(...)`\n")
	b.WriteString("  inside `eval`/Python code cells — that path is not wired and\n")
	b.WriteString("  will hang or return tool-not-found.\n\n")

	b.WriteString("ANTI-FALLBACK RULE: never use `eval`/Python/paramiko/bash/subprocess\n")
	b.WriteString("to talk to services that have MCP servers registered. If you have\n")
	b.WriteString("opened or used a server, assume it has additional tools (command\n")
	b.WriteString("execution, terminal output, file ops, ...) and prefer those over\n")
	b.WriteString("a client-side workaround.\n\n")

	b.WriteString(singleNameInstructionsEndMarker)
	return b.String()
}

const (
	mcpCatalogStartMarker = "## MCP TOOL CATALOG ##"
	mcpCatalogEndMarker   = "## END MCP TOOL CATALOG ##"
	// Bumped from 6 KiB → 20 KiB so the full catalog (~200 mcp tools,
	// each ~80 bytes name+desc + small schema) fits without truncation
	// in most deployments. ~5 k tokens at typical English density —
	// acceptable input overhead in exchange for never hiding a tool the
	// model is about to need.
	mcpCatalogBudget = 20 * 1024
)

// buildMcpCatalogText constructs the MCP-tool catalog block injected
// into systemInstruction.
//
// Modes:
//   - fullCatalog=true:   includes every tool with description + schema
//     (subject to budget; on overflow we DROP SCHEMAS first, never
//     names, so every tool stays visible by name).
//   - fullCatalog=false:  per-server name lists only — no schemas, no
//     descriptions. The model is told to call agy_list_tools(server=X)
//     for full details.
//
// Tools are grouped by server (alphabetized) so the model sees a
// coherent inventory per MCP server rather than tools scattered across
// the catalog.
//
// declaresListTool=true adds the "if you can't find a tool, call
// agy_list_tools" cue + the anti-fallback rule.
func buildMcpCatalogText(mcpTools []mcpToolHandle, aggregatorName string, fullCatalog bool, declaresListTool bool) string {
	if len(mcpTools) == 0 {
		return ""
	}
	if aggregatorName == "" {
		aggregatorName = defaultMcpAggregatorName
	}

	// Group by server, alphabetized within.
	byServer := map[string][]mcpToolHandle{}
	for _, h := range mcpTools {
		server, _ := splitMcpFullName(h.FullName)
		byServer[server] = append(byServer[server], h)
	}
	serverNames := make([]string, 0, len(byServer))
	for s := range byServer {
		serverNames = append(serverNames, s)
	}
	sort.Strings(serverNames)

	var b strings.Builder
	b.WriteString(mcpCatalogStartMarker)
	_ = b.WriteByte('\n')

	// Preamble — HOW TO INVOKE + anti-fallback + anti-Python-eval.
	b.WriteString("HOW TO INVOKE: emit a TOP-LEVEL functionCall to `")
	b.WriteString(aggregatorName)
	b.WriteString("` with EXACTLY the (ServerName, ToolName) pair listed below.\n")
	b.WriteString("Do NOT paraphrase names. Do NOT invoke MCP tools via `eval`/Python\n")
	b.WriteString("and NEVER as `tool.")
	b.WriteString(aggregatorName)
	b.WriteString("(...)` inside a Python eval cell\n")
	b.WriteString("(that path is not wired and will hang).\n\n")

	if declaresListTool {
		b.WriteString("DISCOVERY: a separate top-level tool `agy_list_tools` is declared.\n")
		b.WriteString("Call `agy_list_tools(server=\"<name>\")` whenever you need to verify\n")
		b.WriteString("a tool exists or to inspect its argument schema — this catalog may\n")
		b.WriteString("be truncated or summarized. The agy_list_tools response is\n")
		b.WriteString("authoritative.\n\n")
	}

	b.WriteString("ANTI-FALLBACK RULE: when a server is listed below, NEVER fall back\n")
	b.WriteString("to client-side `eval`/Python/paramiko/bash/subprocess to talk to\n")
	b.WriteString("that service. If you have already opened or used a server (e.g.\n")
	b.WriteString("opened an electerm bookmark), assume it has additional tools\n")
	b.WriteString("(command execution, terminal output, file ops) — ")
	if declaresListTool {
		b.WriteString("call `agy_list_tools(server=\"<name>\")` to enumerate them.\n\n")
	} else {
		b.WriteString("scan this catalog\nthoroughly before assuming the capability is missing.\n\n")
	}

	b.WriteString("EXAMPLE: to list electerm bookmarks, emit\n")
	b.WriteString("  functionCall{ name: \"")
	b.WriteString(aggregatorName)
	b.WriteString("\", args: { ServerName: \"electerm\",\n")
	b.WriteString("    ToolName: \"list_electerm_bookmarks\", Arguments: {} } }\n\n")

	if !fullCatalog {
		// Minimal mode: just server names + counts, no per-tool details.
		b.WriteString("AVAILABLE SERVERS (call agy_list_tools(server=X) for full tool list + schemas):\n")
		for _, s := range serverNames {
			fmt.Fprintf(&b, "  - %s (%d tools)\n", s, len(byServer[s]))
		}
		b.WriteString("\n")
		b.WriteString(mcpCatalogEndMarker)
		return b.String()
	}

	// Full mode: grouped per-server, schemas included until budget.
	written := b.Len()
	includeSchemas := true

	for _, server := range serverNames {
		// Per-server header.
		header := "# " + server + "\n"
		if written+len(header)+len(mcpCatalogEndMarker)+8 > mcpCatalogBudget {
			b.WriteString("(...remaining servers omitted for token budget — call agy_list_tools to enumerate)\n")
			break
		}
		b.WriteString(header)
		written += len(header)

		tools := byServer[server]
		sort.Slice(tools, func(i, j int) bool { return tools[i].FullName < tools[j].FullName })
		for _, h := range tools {
			_, tool := splitMcpFullName(h.FullName)
			desc, _ := h.Decl["description"].(string)
			desc = strings.TrimSpace(desc)
			if len(desc) > 200 {
				desc = desc[:197] + "..."
			}
			entry := "- " + h.FullName + "\n"
			entry += "  call: " + aggregatorName + "(ServerName=\"" + server + "\", ToolName=\"" + tool + "\", Arguments={...})\n"
			if desc != "" {
				entry += "  description: " + desc + "\n"
			}
			// Schema (skip if we already exceeded the schema budget).
			if includeSchemas {
				schema := h.Decl["parametersJsonSchema"]
				if schema == nil {
					schema = h.Decl["parameters"]
				}
				schemaBytes, _ := json.Marshal(schema)
				if len(schemaBytes) > 0 && string(schemaBytes) != "null" {
					sstr := string(schemaBytes)
					if len(sstr) > 600 {
						sstr = sstr[:597] + "..."
					}
					entry += "  args_schema: " + sstr + "\n"
				}
			}
			// If adding schema-bearing entry overflows, drop schemas for
			// the rest (keep names visible — critical for discovery).
			if includeSchemas && written+len(entry)+len(mcpCatalogEndMarker)+8 > mcpCatalogBudget {
				includeSchemas = false
				b.WriteString("(...schemas omitted for remaining entries — call agy_list_tools(server=X) for schema details)\n")
				entry = "- " + h.FullName + "\n"
				entry += "  call: " + aggregatorName + "(ServerName=\"" + server + "\", ToolName=\"" + tool + "\", Arguments={...})\n"
			}
			// If even names-only overflows, stop entirely.
			if written+len(entry)+len(mcpCatalogEndMarker)+8 > mcpCatalogBudget {
				b.WriteString("(...remaining tools omitted — call agy_list_tools to enumerate)\n")
				goto done
			}
			b.WriteString(entry)
			written += len(entry)
		}
	}
done:
	b.WriteString(mcpCatalogEndMarker)
	return b.String()
}

// splitMcpFullName parses "mcp__<server>_<tool>" → (server, tool).
// Mirrors the namespacing scheme omp's createMCPToolName uses.
// Best-effort: when the server name contains underscores (sanitized
// dashes), we pick the first underscore as the boundary. Edge cases
// where the model later tries (server="ida", tool="orchestrator_read_memory")
// still resolve via resolveMcpHandle's fuzzy match.
func splitMcpFullName(full string) (server, tool string) {
	stripped := strings.TrimPrefix(full, "mcp__")
	idx := strings.Index(stripped, "_")
	if idx <= 0 {
		return stripped, stripped
	}
	return stripped[:idx], stripped[idx+1:]
}
