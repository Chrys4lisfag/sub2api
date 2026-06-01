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
	"sort"
	"strings"
)

// applyToolPreprocessing runs both the schema normalizer and (if enabled
// for this account) the MCP aggregator. Mutates `inner` in place.
//
// Returns a small report describing what was changed, useful for logging
// and for the back-translation step in the response stream.
func applyToolPreprocessing(inner map[string]any, useAggregator bool) toolPrepReport {
	report := toolPrepReport{
		Normalized:    0,
		McpTools:      nil,
		BuiltinTools:  nil,
		AggregatorOn:  useAggregator,
	}
	if inner == nil {
		return report
	}
	toolsAny, ok := inner["tools"].([]any)
	if !ok || len(toolsAny) == 0 {
		return report
	}
	// Each entry is { "functionDeclarations": [ ... ] }. We iterate and
	// rebuild the slice.
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
				// Stash the original tool so we can back-translate the
				// call_mcp_tool invocation in the response stream.
				report.McpTools = append(report.McpTools, mcpToolHandle{
					FullName: name,
					Decl:     fd,
				})
				continue // drop from outbound declarations
			}
			report.BuiltinTools = append(report.BuiltinTools, name)
			// Schema normalize regardless of aggregator mode.
			if convertFunctionDeclarationSchema(fd) {
				report.Normalized++
			}
			kept = append(kept, fd)
		}
		if useAggregator && len(report.McpTools) > 0 {
			kept = append(kept, buildCallMcpToolDecl())
			// NOTE: previously we also appended buildListMcpToolsDecl()
			// and buildReadMcpToolSchemaDecl() here. The model would
			// dutifully call them per call_mcp_tool's description, omp's
			// runtime had no matching tool, and the call failed with
			// "Tool list_mcp_tools not found". Instead of exposing them
			// as model-callable functions, we now INJECT the catalog
			// directly into systemInstruction (see injectMcpCatalogIntoSystemInstruction)
			// so the model sees every MCP tool's name + schema upfront
			// without needing a separate discovery round-trip.
		}
		tool["functionDeclarations"] = kept
	}
	if useAggregator && len(report.McpTools) > 0 {
		injectMcpCatalogIntoSystemInstruction(inner, report.McpTools)
	}
	return report
}

// toolPrepReport captures what the preprocessing pipeline did. The
// response-stream back-translator needs McpTools to map call_mcp_tool
// invocations back to the original mcp__server_tool names that omp
// expects to see in tool-call deltas.
type toolPrepReport struct {
	Normalized   int            // count of functionDeclarations whose schema we converted
	McpTools     []mcpToolHandle // MCP tools that were hidden behind the aggregator
	BuiltinTools []string        // non-MCP tools we kept verbatim
	AggregatorOn bool
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
func buildCallMcpToolDecl() map[string]any {
	return map[string]any{
		"name": "call_mcp_tool",
		"description": "Invoke an MCP tool. The MCP catalog (server names, tool names, " +
			"argument schemas) is provided in the system instructions; pick a (ServerName, " +
			"ToolName) pair from there and supply Arguments as a JSON object matching the " +
			"tool's input schema. toolAction/toolSummary are short labels for the activity " +
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

func buildListMcpToolsDecl() map[string]any {
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

func buildReadMcpToolSchemaDecl() map[string]any {
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
	want := "mcp__" + serverNorm + "_" + toolNorm
	for _, h := range r.McpTools {
		if h.FullName == want {
			return h, true
		}
	}
	// Fuzzy: server-only prefix match — the model may have stripped the
	// server prefix if the tool name was already namespaced.
	prefix := "mcp__" + serverNorm + "_"
	for _, h := range r.McpTools {
		if strings.HasPrefix(h.FullName, prefix) && strings.HasSuffix(h.FullName, "_"+toolNorm) {
			return h, true
		}
	}
	return mcpToolHandle{}, false
}

// renderMcpToolCatalog returns a JSON-encoded array of
//
//	[{"server_name": "...", "tool_name": "...", "description": "..."}, ...]
//
// suitable as the response body for the list_mcp_tools synthetic tool.
// Used by the back-translator when the model invokes list_mcp_tools.
func (r toolPrepReport) renderMcpToolCatalog() []byte {
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
func (r toolPrepReport) renderMcpToolSchema(server, tool string) []byte {
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
func injectMcpCatalogIntoSystemInstruction(inner map[string]any, mcpTools []mcpToolHandle) {
	if inner == nil || len(mcpTools) == 0 {
		return
	}
	catalog := buildMcpCatalogText(mcpTools)
	if catalog == "" {
		return
	}

	// Get-or-create systemInstruction.parts[0].text
	sysAny, has := inner["systemInstruction"]
	if !has {
		// Build a fresh systemInstruction with our catalog as the sole content.
		inner["systemInstruction"] = map[string]any{
			"role": "user",
			"parts": []any{
				map[string]any{"text": catalog},
			},
		}
		return
	}
	sys, ok := sysAny.(map[string]any)
	if !ok {
		// Caller gave us a non-object — overwrite with our shape.
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
	// Prepend catalog to the FIRST text part. Caller's text follows so
	// the model still sees the original system context (identity, tool
	// usage rules, etc.) — the catalog is supplementary.
	first, _ := parts[0].(map[string]any)
	if first == nil {
		parts[0] = map[string]any{"text": catalog}
		sys["parts"] = parts
		return
	}
	existingText, _ := first["text"].(string)
	if strings.Contains(existingText, mcpCatalogStartMarker) {
		// Already injected — idempotent.
		return
	}
	first["text"] = catalog + "\n\n" + existingText
	parts[0] = first
	sys["parts"] = parts
}

const (
	mcpCatalogStartMarker = "## MCP TOOL CATALOG (call via call_mcp_tool) ##"
	mcpCatalogEndMarker   = "## END MCP TOOL CATALOG ##"
	// Soft byte limit on the catalog block to keep prompt token usage in
	// check. ~6 KiB ≈ 1.5k tokens at typical English density.
	mcpCatalogBudget = 6 * 1024
)

func buildMcpCatalogText(mcpTools []mcpToolHandle) string {
	if len(mcpTools) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(mcpCatalogStartMarker)
	b.WriteByte('\n')
	written := b.Len()
	for _, h := range mcpTools {
		server, tool := splitMcpFullName(h.FullName)
		desc, _ := h.Decl["description"].(string)
		desc = strings.TrimSpace(desc)
		if len(desc) > 240 {
			desc = desc[:237] + "..."
		}
		// Schema: prefer parametersJsonSchema (original omp shape, more
		// expressive than the trimmed Gemini Schema we emit on the wire).
		schema := h.Decl["parametersJsonSchema"]
		if schema == nil {
			schema = h.Decl["parameters"]
		}
		schemaBytes, _ := json.Marshal(schema)
		entry := "server: " + server + " | tool: " + tool + "\n"
		if desc != "" {
			entry += "  description: " + desc + "\n"
		}
		if len(schemaBytes) > 0 && string(schemaBytes) != "null" {
			s := string(schemaBytes)
			if len(s) > 800 {
				s = s[:797] + "..."
			}
			entry += "  args_schema: " + s + "\n"
		}
		// Stop adding entries once we've blown the budget. Real agy
		// presents abbreviated entries when over budget — we just
		// truncate so the model gets a fair sample of available tools.
		if written+len(entry)+len(mcpCatalogEndMarker)+8 > mcpCatalogBudget {
			b.WriteString("(...catalog truncated for token budget — call call_mcp_tool with any plausible (ServerName, ToolName) and the upstream will validate)\n")
			break
		}
		b.WriteString(entry)
		written += len(entry)
	}
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
