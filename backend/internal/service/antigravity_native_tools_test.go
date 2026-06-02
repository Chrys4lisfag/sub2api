package service

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestConvertSchemaToGeminiSchema verifies the parametersJsonSchema →
// parameters Gemini Schema conversion produces UPPERCASE types, drops
// JSON-Schema-only keys, and flattens anyOf nullable polyfills.
func TestConvertSchemaToGeminiSchema_BasicObject(t *testing.T) {
	in := map[string]any{
		"$schema":     "https://json-schema.org/draft/2020-12/schema",
		"type":        "object",
		"description": "List files",
		"properties": map[string]any{
			"paths": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "string",
				},
			},
			"limit": map[string]any{
				"type":    "integer",
				"default": 100,
			},
		},
		"required": []any{"paths"},
	}
	out := convertSchemaToGeminiSchema(in)
	if out["type"] != "OBJECT" {
		t.Errorf("type want OBJECT got %v", out["type"])
	}
	if _, has := out["$schema"]; has {
		t.Errorf("$schema should be stripped")
	}
	props := out["properties"].(map[string]any)
	if props["paths"].(map[string]any)["type"] != "ARRAY" {
		t.Errorf("paths.type want ARRAY")
	}
	if props["paths"].(map[string]any)["items"].(map[string]any)["type"] != "STRING" {
		t.Errorf("paths.items.type want STRING")
	}
	if _, has := props["limit"].(map[string]any)["default"]; has {
		t.Errorf("default should be stripped")
	}
	req := out["required"].([]any)
	if len(req) != 1 || req[0] != "paths" {
		t.Errorf("required mismatch: %v", req)
	}
}

func TestConvertSchemaToGeminiSchema_AnyOfNullableFlatten(t *testing.T) {
	// Common JSON Schema "nullable" polyfill: anyOf with type:null branch.
	in := map[string]any{
		"anyOf": []any{
			map[string]any{"type": "string"},
			map[string]any{"type": "null"},
		},
	}
	out := convertSchemaToGeminiSchema(in)
	if out["type"] != "STRING" {
		t.Errorf("anyOf nullable not flattened to STRING, got %v", out)
	}
}

func TestConvertSchemaToGeminiSchema_AllOfMerge(t *testing.T) {
	in := map[string]any{
		"allOf": []any{
			map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "string"}}},
			map[string]any{"properties": map[string]any{"b": map[string]any{"type": "integer"}}},
		},
	}
	out := convertSchemaToGeminiSchema(in)
	if out["type"] != "OBJECT" {
		t.Errorf("allOf merged type want OBJECT got %v", out["type"])
	}
	// Only the FIRST branch's properties get merged (shallow merge by design).
	props := out["properties"].(map[string]any)
	if _, has := props["a"]; !has {
		t.Errorf("allOf merge lost property a: %v", props)
	}
}

// TestPreprocess_NoMcp_NoAggregator covers the non-aggregator codepath:
// the report should not include MCP tools.
func TestPreprocess_NoMcp_NoAggregator(t *testing.T) {
	body := []byte(`{"contents":[],"tools":[{"functionDeclarations":[
		{"name":"find","parametersJsonSchema":{"type":"object","properties":{"paths":{"type":"array","items":{"type":"string"}}},"required":["paths"]}},
		{"name":"read","parametersJsonSchema":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}
	]}]}`)
	out, report, err := preprocessNativeBody(body, false, "", "both")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if report.AggregatorOn {
		t.Errorf("aggregator should be off")
	}
	if len(report.McpTools) != 0 {
		t.Errorf("no mcp tools should be hidden, got %d", len(report.McpTools))
	}
	if report.Normalized < 2 {
		t.Errorf("expected at least 2 normalizations, got %d", report.Normalized)
	}
	// Verify out body uses `parameters` not `parametersJsonSchema`.
	if strings.Contains(string(out), "parametersJsonSchema") {
		t.Errorf("parametersJsonSchema not stripped: %s", out)
	}
	if !strings.Contains(string(out), `"parameters":`) {
		t.Errorf("parameters key missing: %s", out)
	}
}

// TestPreprocess_McpAggregator covers the main fix path: many MCP tools
// should be collapsed into a single call_mcp_tool entry, and the catalog
// should be injected into systemInstruction (NOT exposed as model-callable
// list_mcp_tools / read_mcp_tool_schema declarations — those were dropped
// because omp's runtime didn't have matching tools, and the model would
// get "Tool list_mcp_tools not found" loops).
func TestPreprocess_McpAggregator(t *testing.T) {
	body := []byte(`{"contents":[],"tools":[{"functionDeclarations":[
		{"name":"find","parametersJsonSchema":{"type":"object","properties":{"paths":{"type":"array","items":{"type":"string"}}}}},
		{"name":"mcp__github_official_get_pull_request","description":"Get PR","parametersJsonSchema":{"type":"object","properties":{"owner":{"type":"string"},"repo":{"type":"string"}}}},
		{"name":"mcp__github_official_create_issue","description":"New issue","parametersJsonSchema":{"type":"object","properties":{"owner":{"type":"string"},"repo":{"type":"string"},"title":{"type":"string"}}}},
		{"name":"mcp__ida_orchestrator_read_memory","description":"Read mem","parametersJsonSchema":{"type":"object","properties":{"regions":{"type":"array"}}}}
	]}]}`)
	out, report, err := preprocessNativeBody(body, true, "", "both")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !report.AggregatorOn {
		t.Errorf("aggregator should be on")
	}
	if len(report.McpTools) != 3 {
		t.Errorf("expected 3 mcp tools captured, got %d", len(report.McpTools))
	}
	outStr := string(out)
	// Out body should NOT contain the original mcp__ names in declarations.
	if strings.Count(outStr, `"name":"mcp__`) > 0 {
		t.Errorf("mcp__ names leaked into outbound body")
	}
	// Aggregator MUST contain call_mcp_tool.
	if !strings.Contains(outStr, `"name":"call_mcp_tool"`) {
		t.Errorf("call_mcp_tool declaration missing: %s", out)
	}
	// Aggregator MUST NOT contain the dropped synthetic tools.
	for _, dropped := range []string{"list_mcp_tools", "read_mcp_tool_schema"} {
		if strings.Contains(outStr, `"name":"`+dropped+`"`) {
			t.Errorf("dropped tool %q leaked into outbound declarations", dropped)
		}
	}
	// Catalog MUST be injected into systemInstruction.
	if !strings.Contains(outStr, "MCP TOOL CATALOG") {
		t.Errorf("MCP catalog not injected into systemInstruction: %s", out)
	}
	// Catalog must mention each MCP tool's tool name.
	for _, want := range []string{"get_pull_request", "create_issue", "read_memory"} {
		if !strings.Contains(outStr, want) {
			t.Errorf("catalog missing tool %q: %s", want, out)
		}
	}
}

// TestInjectMcpCatalog_Idempotent — re-injection must not double the
// catalog block.
func TestInjectMcpCatalog_Idempotent(t *testing.T) {
	inner := map[string]any{
		"systemInstruction": map[string]any{
			"role":  "user",
			"parts": []any{map[string]any{"text": "existing system text"}},
		},
	}
	tools := []mcpToolHandle{
		{FullName: "mcp__server_one_tool_a", Decl: map[string]any{"description": "first tool"}},
	}
	injectMcpCatalogIntoSystemInstruction(inner, tools, "", "both", true, true)
	injectMcpCatalogIntoSystemInstruction(inner, tools, "", "both", true, true)
	text := inner["systemInstruction"].(map[string]any)["parts"].([]any)[0].(map[string]any)["text"].(string)
	if strings.Count(text, mcpCatalogStartMarker) != 1 {
		t.Errorf("catalog marker count != 1 after double-inject: %d in %q", strings.Count(text, mcpCatalogStartMarker), text)
	}
	if !strings.Contains(text, "existing system text") {
		t.Errorf("existing system text was lost after inject")
	}
}

// TestRewriteAggregatedFunctionCalls_BasicMatch verifies the SSE
// back-translator rewrites a call_mcp_tool functionCall into the original
// mcp__server_tool name + inner Arguments.
func TestRewriteAggregatedFunctionCalls_BasicMatch(t *testing.T) {
	report := toolPrepReport{
		AggregatorOn: true,
		McpTools: []mcpToolHandle{
			{FullName: "mcp__github_official_get_pull_request", Decl: map[string]any{"name": "mcp__github_official_get_pull_request"}},
		},
	}
	payload := []byte(`{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"call_mcp_tool","args":{"ServerName":"github-official","ToolName":"get_pull_request","Arguments":{"owner":"acme","repo":"x"}}}}
	]}}]}`)
	out := rewriteAggregatedFunctionCalls(payload, report)
	if !strings.Contains(string(out), `"name":"mcp__github_official_get_pull_request"`) {
		t.Fatalf("rewrite missing: %s", out)
	}
	// Verify Arguments unpacked
	var parsed map[string]any
	_ = json.Unmarshal(out, &parsed)
	parts := parsed["candidates"].([]any)[0].(map[string]any)["content"].(map[string]any)["parts"].([]any)
	fc := parts[0].(map[string]any)["functionCall"].(map[string]any)
	args := fc["args"].(map[string]any)
	if args["owner"] != "acme" {
		t.Errorf("Arguments not unwrapped: %v", args)
	}
	// Verify ServerName/ToolName are NOT in the rewritten args.
	if _, has := args["ServerName"]; has {
		t.Errorf("ServerName leaked into args after rewrite")
	}
}

// TestRewriteAggregatedFunctionCalls_AggregatorOff is a no-op.
func TestRewriteAggregatedFunctionCalls_AggregatorOff(t *testing.T) {
	report := toolPrepReport{AggregatorOn: false}
	payload := []byte(`{"candidates":[{"content":{"parts":[{"functionCall":{"name":"call_mcp_tool","args":{}}}]}}]}`)
	out := rewriteAggregatedFunctionCalls(payload, report)
	if string(out) != string(payload) {
		t.Errorf("aggregator off should be no-op")
	}
}

// TestRewriteSSELineFunctionCalls validates the line-level wrapper that
// preserves the SSE `data:` prefix + CRLF tail.
func TestRewriteSSELineFunctionCalls(t *testing.T) {
	report := toolPrepReport{
		AggregatorOn: true,
		McpTools: []mcpToolHandle{
			{FullName: "mcp__serena_find_file", Decl: map[string]any{"name": "mcp__serena_find_file"}},
		},
	}
	line := []byte(`data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"call_mcp_tool","args":{"ServerName":"serena","ToolName":"find_file","Arguments":{"file_mask":"*.go","relative_path":"."}}}}]}}]}` + "\n")
	out := rewriteSSELineFunctionCalls(line, report)
	if !strings.HasPrefix(string(out), "data: {") {
		t.Errorf("data: prefix lost: %s", out)
	}
	if !strings.HasSuffix(string(out), "\n") {
		t.Errorf("trailing newline lost")
	}
	if !strings.Contains(string(out), `"mcp__serena_find_file"`) {
		t.Errorf("rewrite missing: %s", out)
	}
}

func TestAccountToolAggregatorEnabled_Default(t *testing.T) {
	a := &Account{Credentials: map[string]any{}}
	if !accountToolAggregatorEnabled(a) {
		t.Errorf("default should be ON")
	}
}

func TestAccountToolAggregatorEnabled_Disabled(t *testing.T) {
	a := &Account{Credentials: map[string]any{"tool_aggregator": false}}
	if accountToolAggregatorEnabled(a) {
		t.Errorf("explicit false should be OFF")
	}
}

func TestAccountToolAggregatorEnabled_StringForms(t *testing.T) {
	for _, s := range []string{"false", "off", "0", "no", "FALSE"} {
		a := &Account{Credentials: map[string]any{"tool_aggregator": s}}
		if accountToolAggregatorEnabled(a) {
			t.Errorf("string %q should be OFF", s)
		}
	}
}

// TestResolveMcpHandle_HallucinatedToolName — the live deploy caught the
// model hallucinating "get_electerm_bookmarks" when the catalog actually
// advertised "list_electerm_bookmarks". With the Levenshtein fuzzy tier,
// we should still find the right handle.
func TestResolveMcpHandle_HallucinatedToolName(t *testing.T) {
	report := toolPrepReport{
		AggregatorOn: true,
		McpTools: []mcpToolHandle{
			{FullName: "mcp__electerm_list_electerm_bookmarks", Decl: map[string]any{"description": "list"}},
			{FullName: "mcp__electerm_open_electerm_bookmark", Decl: map[string]any{"description": "open"}},
			{FullName: "mcp__electerm_send_electerm_terminal_command", Decl: map[string]any{"description": "send"}},
		},
	}
	// Model hallucinated "get_electerm_bookmarks" instead of "list_electerm_bookmarks".
	got, ok := report.resolveMcpHandle("electerm", "get_electerm_bookmarks")
	if !ok {
		t.Fatal("fuzzy resolve should find list_electerm_bookmarks despite get_/list_ swap")
	}
	if got.FullName != "mcp__electerm_list_electerm_bookmarks" {
		t.Errorf("fuzzy picked wrong tool: %s", got.FullName)
	}
}

// TestResolveMcpHandle_RejectsWildlyDifferent — fuzzy must NOT match when
// the requested name has no meaningful overlap.
func TestResolveMcpHandle_RejectsWildlyDifferent(t *testing.T) {
	report := toolPrepReport{
		AggregatorOn: true,
		McpTools: []mcpToolHandle{
			{FullName: "mcp__electerm_list_electerm_bookmarks", Decl: map[string]any{"description": "list"}},
		},
	}
	// Wholly unrelated tool name on a server that does have other tools.
	// 40% threshold rejects this.
	_, ok := report.resolveMcpHandle("electerm", "execute_terminal_command_with_extra_words")
	if ok {
		t.Error("fuzzy should reject wildly mismatched tool name on existing server")
	}
}

// TestResolveMcpHandle_PrefixStrip — omp's createMCPToolName strips the
// server prefix when redundant. If model emits the short ToolName, we
// still find the handle that has the full prefixed name.
func TestResolveMcpHandle_PrefixStrip(t *testing.T) {
	report := toolPrepReport{
		AggregatorOn: true,
		McpTools: []mcpToolHandle{
			{FullName: "mcp__github_official_get_pull_request", Decl: map[string]any{"description": "pr"}},
		},
	}
	got, ok := report.resolveMcpHandle("github-official", "get_pull_request")
	if !ok {
		t.Fatalf("dash-normalized server + exact tool should resolve, got !ok")
	}
	if got.FullName != "mcp__github_official_get_pull_request" {
		t.Errorf("wrong handle: %s", got.FullName)
	}
}

// TestLevenshtein_Basics — sanity-check the edit-distance helper.
func TestLevenshtein_Basics(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"abc", "abc", 0},
		{"abc", "abd", 1},
		{"list_bookmarks", "get_bookmarks", 3},
		{"", "abc", 3},
		{"abc", "", 3},
	}
	for _, c := range cases {
		got := levenshtein(c.a, c.b)
		if got != c.want {
			t.Errorf("levenshtein(%q,%q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// accountMcpAggregatorName / configurable name plumbing
// ---------------------------------------------------------------------------

func TestAccountMcpAggregatorName_DefaultEmpty(t *testing.T) {
	if got := accountMcpAggregatorName(&Account{Credentials: map[string]any{}}); got != "call_mcp_tool" {
		t.Errorf("empty cred should default to call_mcp_tool, got %q", got)
	}
	if got := accountMcpAggregatorName(nil); got != "call_mcp_tool" {
		t.Errorf("nil account should default to call_mcp_tool, got %q", got)
	}
}

func TestAccountMcpAggregatorName_Custom(t *testing.T) {
	a := &Account{Credentials: map[string]any{"mcp_aggregator_name": "agy_call_mcp_tool"}}
	if got := accountMcpAggregatorName(a); got != "agy_call_mcp_tool" {
		t.Errorf("custom name not respected: got %q", got)
	}
}

func TestAccountMcpAggregatorName_TrimsAndDefaults(t *testing.T) {
	a := &Account{Credentials: map[string]any{"mcp_aggregator_name": "   "}}
	if got := accountMcpAggregatorName(a); got != "call_mcp_tool" {
		t.Errorf("whitespace-only should default, got %q", got)
	}
}

func TestAccountMcpAggregatorName_InvalidFallsBack(t *testing.T) {
	cases := []string{
		"1starts_with_digit",
		"has-dash",
		"has space",
		"has.dot",
		strings.Repeat("a", 65), // too long
	}
	for _, bad := range cases {
		a := &Account{Credentials: map[string]any{"mcp_aggregator_name": bad}}
		if got := accountMcpAggregatorName(a); got != "call_mcp_tool" {
			t.Errorf("invalid %q should fall back to default, got %q", bad, got)
		}
	}
}

func TestIsValidMcpAggregatorName(t *testing.T) {
	good := []string{"a", "_x", "call_mcp_tool", "agy_call_mcp_tool", "FooBar123"}
	for _, g := range good {
		if !isValidMcpAggregatorName(g) {
			t.Errorf("%q should be valid", g)
		}
	}
	bad := []string{"", "1abc", "-x", "a-b", "a b", "a.b", strings.Repeat("a", 65)}
	for _, b := range bad {
		if isValidMcpAggregatorName(b) {
			t.Errorf("%q should be invalid", b)
		}
	}
}

// TestPreprocess_CustomAggregatorName verifies the configured name is
// used in the outbound declaration AND in the catalog text.
func TestPreprocess_CustomAggregatorName(t *testing.T) {
	body := []byte(`{"contents":[],"tools":[{"functionDeclarations":[
		{"name":"mcp__github_official_get_pull_request","description":"Get PR","parametersJsonSchema":{"type":"object","properties":{"owner":{"type":"string"}}}}
	]}]}`)
	out, report, err := preprocessNativeBody(body, true, "agy_call_mcp_tool", "both")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if report.AggregatorName != "agy_call_mcp_tool" {
		t.Errorf("report.AggregatorName not set: %q", report.AggregatorName)
	}
	outStr := string(out)
	if !strings.Contains(outStr, `"name":"agy_call_mcp_tool"`) {
		t.Errorf("custom aggregator decl missing: %s", outStr)
	}
	if strings.Contains(outStr, `"name":"call_mcp_tool"`) {
		t.Errorf("default name leaked into outbound: %s", outStr)
	}
	if !strings.Contains(outStr, "agy_call_mcp_tool") {
		t.Errorf("custom name missing from catalog body: %s", outStr)
	}
}

// TestRewrite_CustomAggregatorName verifies the back-translator honors
// the configured name when matching model output.
func TestRewrite_CustomAggregatorName(t *testing.T) {
	report := toolPrepReport{
		AggregatorOn:   true,
		AggregatorName: "agy_call_mcp_tool",
		McpTools: []mcpToolHandle{
			{FullName: "mcp__github_official_get_pull_request", Decl: map[string]any{}},
		},
	}
	payload := []byte(`{"candidates":[{"content":{"parts":[{"functionCall":{"name":"agy_call_mcp_tool","args":{"ServerName":"github-official","ToolName":"get_pull_request","Arguments":{"owner":"a","repo":"b"}}}}]}}]}`)
	out := rewriteAggregatedFunctionCalls(payload, report)
	if !strings.Contains(string(out), `"name":"mcp__github_official_get_pull_request"`) {
		t.Fatalf("rewrite missing under custom name: %s", out)
	}
	if strings.Contains(string(out), `"name":"agy_call_mcp_tool"`) {
		t.Errorf("custom aggregator name leaked back to omp side: %s", out)
	}
}
