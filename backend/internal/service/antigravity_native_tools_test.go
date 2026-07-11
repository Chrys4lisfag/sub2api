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
	out, report, err := preprocessNativeBody(body, false, "", "both", "single_name")
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
	out, report, err := preprocessNativeBody(body, true, "", "both", "agy_mimic")
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
	out, report, err := preprocessNativeBody(body, true, "agy_call_mcp_tool", "both", "agy_mimic")
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

// TestRewriteAggregatedFunctionCalls_PeelsAgymimicWrapper regression-locks
// the third occurrence of "expected unwrapped, got wrapped" — the agy_list_tools
// loop hands wrapped responses to rewriteAggregatedFunctionCalls BEFORE
// flushBufferedNativeResponse runs its unwrap step. Without peeling here,
// call_mcp_tool was never rewritten, and omp errored with
// "Tool call_mcp_tool not found".
func TestRewriteAggregatedFunctionCalls_PeelsAgymimicWrapper(t *testing.T) {
	report := toolPrepReport{
		AggregatorOn:   true,
		AggregatorName: "call_mcp_tool",
		McpTools: []mcpToolHandle{
			{FullName: "mcp__electerm_list_electerm_bookmarks", Decl: map[string]any{}},
		},
	}
	wrapped := []byte(`{
		"response": {
			"candidates": [{
				"content": {
					"parts": [{
						"functionCall": {
							"name": "call_mcp_tool",
							"args": {
								"ServerName": "electerm",
								"ToolName": "list_electerm_bookmarks",
								"Arguments": {}
							}
						}
					}]
				}
			}]
		},
		"usageMetadata": {"promptTokenCount": 100},
		"modelVersion": "gemini-3.5-flash",
		"responseId": "abc"
	}`)
	out := rewriteAggregatedFunctionCalls(wrapped, report)
	s := string(out)
	if !strings.Contains(s, `"name":"mcp__electerm_list_electerm_bookmarks"`) {
		t.Fatalf("rewriter must rewrite call_mcp_tool inside wrapped response: %s", s)
	}
	if strings.Contains(s, `"name":"call_mcp_tool"`) {
		t.Errorf("call_mcp_tool name should be replaced: %s", s)
	}
}

func TestAvailableServerNames(t *testing.T) {
	r := toolPrepReport{
		McpTools: []mcpToolHandle{
			{FullName: "mcp__electerm_list_electerm_bookmarks"},
			{FullName: "mcp__electerm_send_electerm_terminal_command"},
			{FullName: "mcp__github_official_get_pr"},
			{FullName: "mcp__ida_orchestrator_get_instances"},
		},
	}
	got := r.availableServerNames()
	// Expected sorted, deduplicated.
	want := []string{"electerm", "github", "ida"}
	if len(got) != len(want) {
		t.Fatalf("availableServerNames len mismatch: got %v want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("availableServerNames[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestNearestMcpHandles_RanksByDistance(t *testing.T) {
	r := toolPrepReport{
		McpTools: []mcpToolHandle{
			{FullName: "mcp__ida_orchestrator_get_instances"},
			{FullName: "mcp__ida_orchestrator_decompile"},
			{FullName: "mcp__electerm_list_electerm_bookmarks"},
			{FullName: "mcp__github_official_get_pr"},
		},
	}
	// Model paraphrased "ida" / "get_instances" instead of the real
	// "ida_orchestrator" / "get_instances". The closest handle should
	// be the orchestrator's get_instances; the further apart names
	// (electerm, github) should appear later in the ranking.
	got := r.nearestMcpHandles("ida", "get_instances", 3)
	if len(got) != 3 {
		t.Fatalf("expected 3 candidates, got %d: %v", len(got), got)
	}
	if got[0] != "mcp__ida_orchestrator_get_instances" {
		t.Errorf("closest should be mcp__ida_orchestrator_get_instances, got %q", got[0])
	}
}

func TestNearestMcpHandles_EmptyOnEmptyReport(t *testing.T) {
	r := toolPrepReport{}
	if got := r.nearestMcpHandles("ida", "x", 5); got != nil {
		t.Errorf("empty report should return nil, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// single_name mode (default) — mcp__* declarations pass through alongside
// call_mcp_tool + agy_list_tools fallback aggregators
// ---------------------------------------------------------------------------

// TestPreprocess_SingleNameKeepsMcpDeclarations verifies the new default
// mode: mcp__* tools stay in declarations (schema-normalized) so the
// model can emit them directly, with call_mcp_tool + agy_list_tools
// declared alongside as fallback aggregators.
func TestPreprocess_SingleNameKeepsMcpDeclarations(t *testing.T) {
	body := []byte(`{"contents":[],"tools":[{"functionDeclarations":[
		{"name":"find","parametersJsonSchema":{"type":"object","properties":{"paths":{"type":"array","items":{"type":"string"}}}}},
		{"name":"mcp__github_official_get_pull_request","description":"Get PR","parametersJsonSchema":{"type":"object","properties":{"owner":{"type":"string"}}}},
		{"name":"mcp__electerm_list_electerm_bookmarks","description":"list","parametersJsonSchema":{"type":"object","properties":{}}}
	]}]}`)
	out, report, err := preprocessNativeBody(body, true, "", "both", "single_name")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if report.ToolCallMode != "single_name" {
		t.Errorf("expected report.ToolCallMode=single_name, got %q", report.ToolCallMode)
	}
	if len(report.McpTools) != 2 {
		t.Errorf("expected 2 mcp tools recorded, got %d", len(report.McpTools))
	}
	outStr := string(out)
	// mcp__* declarations MUST remain in the outbound body.
	for _, want := range []string{
		`"name":"mcp__github_official_get_pull_request"`,
		`"name":"mcp__electerm_list_electerm_bookmarks"`,
	} {
		if !strings.Contains(outStr, want) {
			t.Errorf("missing direct decl %s in single_name mode", want)
		}
	}
	// call_mcp_tool aggregator MUST also be present as fallback.
	if !strings.Contains(outStr, `"name":"call_mcp_tool"`) {
		t.Errorf("call_mcp_tool fallback missing: %s", outStr)
	}
	// agy_list_tools is GATED behind agy_mimic mode (2026-06-14) to
	// preserve streaming UX. In single_name mode it must NOT be in
	// declarations. Pre-fix behaviour declared it always.
	if strings.Contains(outStr, `"name":"agy_list_tools"`) {
		t.Errorf("agy_list_tools must NOT be declared in single_name mode (streaming gate): %s", outStr)
	}
	// Schemas normalized — no parametersJsonSchema leak.
	if strings.Contains(outStr, "parametersJsonSchema") {
		t.Errorf("parametersJsonSchema should be normalized to parameters")
	}
	// Short instructions block injected, NOT the full catalog form.
	if !strings.Contains(outStr, singleNameInstructionsStartMarker) {
		t.Errorf("single_name instructions marker missing")
	}
	if strings.Contains(outStr, mcpCatalogStartMarker) {
		t.Errorf("full agy_mimic catalog must NOT be injected in single_name mode")
	}
}

// TestPreprocess_SingleNameNoListToolWhenDiscoveryPrompt verifies that
// when discovery_mode is "prompt" (= no agy_list_tools), the discovery
// tool is not declared even in single_name mode.
func TestPreprocess_SingleNameNoListToolWhenDiscoveryPrompt(t *testing.T) {
	body := []byte(`{"contents":[],"tools":[{"functionDeclarations":[
		{"name":"mcp__electerm_list_electerm_bookmarks","description":"list","parametersJsonSchema":{"type":"object","properties":{}}}
	]}]}`)
	out, _, err := preprocessNativeBody(body, true, "", "prompt", "single_name")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	outStr := string(out)
	if strings.Contains(outStr, `"name":"agy_list_tools"`) {
		t.Errorf("agy_list_tools must NOT be declared when discovery_mode=prompt: %s", outStr)
	}
	if !strings.Contains(outStr, `"name":"call_mcp_tool"`) {
		t.Errorf("call_mcp_tool fallback should still be present: %s", outStr)
	}
	if !strings.Contains(outStr, `"name":"mcp__electerm_list_electerm_bookmarks"`) {
		t.Errorf("mcp__ decl should still be present: %s", outStr)
	}
}

// TestNormalizeToolCallMode locks the enum canonicalization map.
func TestNormalizeToolCallMode(t *testing.T) {
	cases := map[string]string{
		"single_name":   "single_name",
		" SINGLE_NAME ": "single_name",
		"single-name":   "single_name",
		"passthrough":   "single_name",
		"direct":        "single_name",
		"agy_mimic":     "agy_mimic",
		"agy-mimic":     "agy_mimic",
		"agy":           "agy_mimic",
		"mimic":         "agy_mimic",
		"aggregator":    "agy_mimic",
		"":              "",
		"garbage":       "",
	}
	for in, want := range cases {
		if got := normalizeToolCallMode(in); got != want {
			t.Errorf("normalizeToolCallMode(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestBuildSingleNameInstructions_BasicShape verifies the short
// instruction block contains the canonical guidance.
func TestBuildSingleNameInstructions_BasicShape(t *testing.T) {
	out := buildSingleNameInstructions("call_mcp_tool", true)
	for _, want := range []string{
		"## MCP TOOLS ##",
		"## END MCP TOOLS ##",
		"mcp__<server>_<tool>",
		"PREFER calling them directly",
		"call_mcp_tool(ServerName, ToolName, Arguments)",
		"agy_list_tools(server?)",
		"ANTI-FALLBACK RULE",
		"TOP-LEVEL functionCalls",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in single_name instructions:\n%s", want, out)
		}
	}
	// When agy_list_tools is NOT declared, the bullet about it should
	// be omitted.
	out2 := buildSingleNameInstructions("call_mcp_tool", false)
	if strings.Contains(out2, "agy_list_tools(server?)") {
		t.Errorf("agy_list_tools bullet should be absent when declaresListTool=false:\n%s", out2)
	}
}

// TestBuildSingleNameInstructions_CustomAggregatorName threads the
// configured aggregator name through the instructions.
func TestBuildSingleNameInstructions_CustomAggregatorName(t *testing.T) {
	out := buildSingleNameInstructions("agy_call_mcp_tool", true)
	if !strings.Contains(out, "agy_call_mcp_tool(ServerName, ToolName, Arguments)") {
		t.Errorf("custom aggregator name not threaded into instructions: %s", out)
	}
	// Bare "`call_mcp_tool(" (backtick-prefixed) appears only for the
	// default name; the custom name appears as "`agy_call_mcp_tool(".
	// Avoid substring traps where the custom contains the default.
	if strings.Contains(out, "`call_mcp_tool(") {
		t.Errorf("default name leaked into instructions when custom configured: %s", out)
	}
}

// TestPreprocess_AgyMimicDeclaresListTool — counterpart to the
// streaming gate. In agy_mimic mode the discovery loop is the only way
// the model reaches MCP servers, so agy_list_tools MUST still be in
// declarations when discovery_mode allows it.
func TestPreprocess_AgyMimicDeclaresListTool(t *testing.T) {
	body := []byte(`{"contents":[],"tools":[{"functionDeclarations":[
		{"name":"mcp__electerm_list_electerm_bookmarks","description":"list","parametersJsonSchema":{"type":"object","properties":{}}}
	]}]}`)
	for _, dm := range []string{"both", "list_tool"} {
		t.Run("discovery_mode="+dm, func(t *testing.T) {
			out, _, err := preprocessNativeBody(body, true, "", dm, "agy_mimic")
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			outStr := string(out)
			if !strings.Contains(outStr, `"name":"agy_list_tools"`) {
				t.Errorf("agy_list_tools should be declared in agy_mimic+discovery_mode=%s: %s", dm, outStr)
			}
			// mcp__* stripped in agy_mimic (catalog-via-aggregator only)
			if strings.Contains(outStr, `"name":"mcp__electerm_list_electerm_bookmarks"`) {
				t.Errorf("mcp__* should be stripped in agy_mimic: %s", outStr)
			}
		})
	}
}

// TestPreprocess_AgyMimicPromptNoListTool — in agy_mimic with
// discovery_mode=prompt, agy_list_tools is still NOT declared (gating
// is governed by discovery_mode here, not tool_call_mode).
func TestPreprocess_AgyMimicPromptNoListTool(t *testing.T) {
	body := []byte(`{"contents":[],"tools":[{"functionDeclarations":[
		{"name":"mcp__electerm_list_electerm_bookmarks","description":"list","parametersJsonSchema":{"type":"object","properties":{}}}
	]}]}`)
	out, _, err := preprocessNativeBody(body, true, "", "prompt", "agy_mimic")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if strings.Contains(string(out), `"name":"agy_list_tools"`) {
		t.Errorf("agy_list_tools should NOT be declared with discovery_mode=prompt: %s", out)
	}
}

// ---------------------------------------------------------------------------
// normalizeOmpGeminiSDKShape — Gemini SDK config bag → REST API top-level
// ---------------------------------------------------------------------------

func TestNormalizeOmpGeminiSDKShape_FullSdkBag(t *testing.T) {
	inner := map[string]any{
		"model":    "gemini-3.1-pro",
		"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "hi"}}}},
		"config": map[string]any{
			"tools":             []any{map[string]any{"functionDeclarations": []any{}}},
			"systemInstruction": map[string]any{"parts": []any{map[string]any{"text": "sys"}}},
			"thinkingConfig":    map[string]any{"includeThoughts": true, "thinkingLevel": "HIGH"},
			"maxOutputTokens":   float64(65536),
			"abortSignal":       map[string]any{"aborted": false},
		},
	}
	normalizeOmpGeminiSDKShape(inner)
	if _, has := inner["config"]; has {
		t.Error("config key should be removed")
	}
	if _, has := inner["model"]; has {
		t.Error("model key should be removed (lives in URL not body)")
	}
	if _, has := inner["tools"]; !has {
		t.Error("tools should be lifted to top level")
	}
	if _, has := inner["systemInstruction"]; !has {
		t.Error("systemInstruction should be lifted to top level")
	}
	gc, ok := inner["generationConfig"].(map[string]any)
	if !ok {
		t.Fatal("generationConfig must be created when sdk config has gen-config keys")
	}
	if gc["maxOutputTokens"] != float64(65536) {
		t.Errorf("maxOutputTokens not lifted under generationConfig: %v", gc)
	}
	if _, has := gc["thinkingConfig"]; !has {
		t.Errorf("thinkingConfig not lifted under generationConfig: %v", gc)
	}
}

func TestNormalizeOmpGeminiSDKShape_NoConfigIsNoop(t *testing.T) {
	inner := map[string]any{
		"contents":         []any{},
		"tools":            []any{map[string]any{"functionDeclarations": []any{}}},
		"generationConfig": map[string]any{"maxOutputTokens": float64(8192)},
	}
	before, _ := json.Marshal(inner)
	normalizeOmpGeminiSDKShape(inner)
	after, _ := json.Marshal(inner)
	if string(before) != string(after) {
		t.Errorf("REST-shaped body must be unchanged:\nbefore: %s\nafter:  %s", before, after)
	}
}

func TestNormalizeOmpGeminiSDKShape_TopLevelWinsOnConflict(t *testing.T) {
	// Caller already pre-normalized (or sent mixed shape). Existing
	// top-level values must NOT be clobbered by SDK config bag values.
	inner := map[string]any{
		"contents": []any{},
		"tools":    []any{map[string]any{"functionDeclarations": []any{map[string]any{"name": "REAL"}}}},
		"config": map[string]any{
			"tools": []any{map[string]any{"functionDeclarations": []any{map[string]any{"name": "SHOULD_BE_IGNORED"}}}},
		},
	}
	normalizeOmpGeminiSDKShape(inner)
	toolsAny, _ := inner["tools"].([]any)
	t0, _ := toolsAny[0].(map[string]any)
	fds, _ := t0["functionDeclarations"].([]any)
	first, _ := fds[0].(map[string]any)
	if first["name"] != "REAL" {
		t.Errorf("top-level tools should win over config.tools: got %v", first["name"])
	}
}

func TestNormalizeOmpGeminiSDKShape_GenConfigMerge(t *testing.T) {
	// Existing top-level generationConfig values must survive; SDK
	// config-bag keys fill the gaps only.
	inner := map[string]any{
		"generationConfig": map[string]any{"maxOutputTokens": float64(8192)},
		"config": map[string]any{
			"maxOutputTokens": float64(65536), // should NOT clobber
			"thinkingConfig":  map[string]any{"includeThoughts": true},
			"temperature":     float64(0.7),
		},
	}
	normalizeOmpGeminiSDKShape(inner)
	gc, _ := inner["generationConfig"].(map[string]any)
	if gc["maxOutputTokens"] != float64(8192) {
		t.Errorf("existing maxOutputTokens clobbered: %v", gc["maxOutputTokens"])
	}
	if gc["temperature"] != float64(0.7) {
		t.Errorf("temperature not lifted: %v", gc)
	}
	if _, has := gc["thinkingConfig"]; !has {
		t.Errorf("thinkingConfig not lifted: %v", gc)
	}
}

func TestNormalizeOmpGeminiSDKShape_DropsRuntimeMetadata(t *testing.T) {
	inner := map[string]any{
		"contents": []any{},
		"config": map[string]any{
			"abortSignal":     map[string]any{"aborted": false},
			"httpOptions":     map[string]any{"timeout": 30000},
			"maxOutputTokens": float64(1024),
		},
	}
	normalizeOmpGeminiSDKShape(inner)
	// abortSignal / httpOptions should not appear anywhere in the result.
	serialized, _ := json.Marshal(inner)
	if strings.Contains(string(serialized), "abortSignal") {
		t.Errorf("abortSignal must be dropped: %s", serialized)
	}
	if strings.Contains(string(serialized), "httpOptions") {
		t.Errorf("httpOptions must be dropped: %s", serialized)
	}
}

func TestNormalizeOmpGeminiSDKShape_Idempotent(t *testing.T) {
	// Running twice must produce the same result as running once.
	inner := map[string]any{
		"model":    "gemini-3.1-pro",
		"contents": []any{},
		"config": map[string]any{
			"tools":           []any{},
			"thinkingConfig":  map[string]any{"thinkingLevel": "HIGH"},
			"maxOutputTokens": float64(2048),
		},
	}
	normalizeOmpGeminiSDKShape(inner)
	once, _ := json.Marshal(inner)
	normalizeOmpGeminiSDKShape(inner)
	twice, _ := json.Marshal(inner)
	if string(once) != string(twice) {
		t.Errorf("not idempotent:\nonce:  %s\ntwice: %s", once, twice)
	}
}

// TestPreprocess_LiftsOmpSdkShape — integration check that the normalizer
// runs inside preprocessNativeBody so tool preprocessing sees lifted
// tools at the top level (the original bug — mcp__* tools nested under
// config went undiscovered).
func TestPreprocess_LiftsOmpSdkShape(t *testing.T) {
	body := []byte(`{
		"model": "gemini-3.1-pro",
		"contents": [{"role":"user","parts":[{"text":"hi"}]}],
		"config": {
			"tools": [{"functionDeclarations": [
				{"name": "mcp__electerm_list_electerm_bookmarks", "description": "list", "parametersJsonSchema": {"type":"object","properties":{}}}
			]}],
			"thinkingConfig": {"includeThoughts": true, "thinkingLevel": "HIGH"},
			"maxOutputTokens": 65536
		}
	}`)
	out, report, err := preprocessNativeBody(body, true, "", "both", "single_name")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(report.McpTools) != 1 {
		t.Errorf("preprocessing should discover 1 mcp tool after lift, got %d", len(report.McpTools))
	}
	outStr := string(out)
	if strings.Contains(outStr, `"config"`) {
		t.Errorf("config key should be stripped: %s", outStr)
	}
	if !strings.Contains(outStr, `"generationConfig"`) {
		t.Errorf("generationConfig should be present: %s", outStr)
	}
	if !strings.Contains(outStr, `"name":"mcp__electerm_list_electerm_bookmarks"`) {
		t.Errorf("mcp tool should still be in declarations after lift+single_name preprocessing: %s", outStr)
	}
}

// ---------------------------------------------------------------------------
// maxOutputTokens clamp — pro-tier wire models reject 65536 boundary
// ---------------------------------------------------------------------------

func TestMaxOutputTokensCapForModel(t *testing.T) {
	cases := map[string]int{
		"gemini-pro-agent":           65535,
		"gemini-3.1-pro-low":         65535,
		"unknown-future-model":       65535,
		"gemini-3-flash-agent":       65536,
		"gemini-3.5-flash-low":       65536,
		"gemini-3.5-flash-extra-low": 65536,
		"gemini-3-flash":             65536,
	}
	for wire, want := range cases {
		if got := maxOutputTokensCapForModel(wire); got != want {
			t.Errorf("cap for %q: got %d want %d", wire, got, want)
		}
	}
}

func TestClampMaxOutputTokens(t *testing.T) {
	tests := []struct {
		name string
		in   any
		wire string
		want any
	}{
		{"pro_65536_clamps", float64(65536), "gemini-pro-agent", float64(65535)},
		{"pro_65535_keeps", float64(65535), "gemini-pro-agent", float64(65535)},
		{"pro_under_keeps", float64(8192), "gemini-pro-agent", float64(8192)},
		{"pro_int_clamps", 65536, "gemini-pro-agent", 65535},
		{"flash_65536_keeps", float64(65536), "gemini-3-flash-agent", float64(65536)},
		{"flash_huge_clamps_to_cap", float64(100000), "gemini-3-flash-agent", float64(65536)},
		{"negative_passthrough", float64(-1), "gemini-pro-agent", float64(-1)},
		{"zero_passthrough", float64(0), "gemini-pro-agent", float64(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clampMaxOutputTokens(tt.in, tt.wire)
			if got != tt.want {
				t.Errorf("clamp(%v, %q) = %v want %v", tt.in, tt.wire, got, tt.want)
			}
		})
	}
}
