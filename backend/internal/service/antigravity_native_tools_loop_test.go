package service

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildAgyListToolsDecl_HasRequiredFields(t *testing.T) {
	d := buildAgyListToolsDecl()
	if name, _ := d["name"].(string); name != "agy_list_tools" {
		t.Errorf("expected name agy_list_tools, got %v", d["name"])
	}
	desc, _ := d["description"].(string)
	if !strings.Contains(desc, "MCP") || !strings.Contains(desc, "agy_list_tools") {
		t.Errorf("description should mention MCP and agy_list_tools: %s", desc)
	}
	if !strings.Contains(desc, "TOP-LEVEL") {
		t.Errorf("description should warn against Python eval: %s", desc)
	}
	params, _ := d["parameters"].(map[string]any)
	if params == nil || params["type"] != "OBJECT" {
		t.Errorf("expected OBJECT parameters, got %+v", d["parameters"])
	}
}

func TestSynthesizeListToolsResponse_GroupsByServer(t *testing.T) {
	tools := []mcpToolHandle{
		{FullName: "mcp__electerm_list_electerm_bookmarks", Decl: map[string]any{"description": "list bookmarks"}},
		{FullName: "mcp__electerm_open_electerm_bookmark", Decl: map[string]any{"description": "open bookmark"}},
		{FullName: "mcp__github_official_get_pull_request", Decl: map[string]any{"description": "get PR"}},
	}
	resp := synthesizeListToolsResponse(tools, "")
	if resp["totalServers"] != 2 {
		t.Errorf("expected 2 servers, got %v", resp["totalServers"])
	}
	if resp["totalTools"] != 3 {
		t.Errorf("expected 3 tools, got %v", resp["totalTools"])
	}
	servers := resp["servers"].(map[string]any)
	electerm, ok := servers["electerm"].([]any)
	if !ok || len(electerm) != 2 {
		t.Errorf("expected 2 electerm tools, got %+v", servers["electerm"])
	}
}

func TestSynthesizeListToolsResponse_FilterByServer(t *testing.T) {
	tools := []mcpToolHandle{
		{FullName: "mcp__electerm_list_electerm_bookmarks", Decl: map[string]any{"description": "list"}},
		{FullName: "mcp__github_official_get_pull_request", Decl: map[string]any{"description": "get PR"}},
	}
	resp := synthesizeListToolsResponse(tools, "electerm")
	if resp["totalServers"] != 1 {
		t.Errorf("filter should give 1 server, got %v", resp["totalServers"])
	}
	if resp["filteredBy"] != "electerm" {
		t.Errorf("filteredBy missing or wrong: %v", resp["filteredBy"])
	}
}

func TestSynthesizeListToolsResponse_UnknownServerFilterHasHint(t *testing.T) {
	tools := []mcpToolHandle{
		{FullName: "mcp__electerm_x", Decl: map[string]any{}},
	}
	resp := synthesizeListToolsResponse(tools, "totally-fake")
	if resp["unknownServer"] != "totally-fake" {
		t.Errorf("expected unknownServer marker, got %v", resp["unknownServer"])
	}
	if _, ok := resp["hint"].(string); !ok {
		t.Errorf("expected hint string when unknown server filtered")
	}
}

func TestServerNameMatches_DashUnderscoreEquiv(t *testing.T) {
	if !serverNameMatches("github_official", "github-official") {
		t.Error("dash/underscore should match")
	}
	if !serverNameMatches("github-official", "github_official") {
		t.Error("dash/underscore should match (reverse)")
	}
	if !serverNameMatches("Foo", "foo") {
		t.Error("case-insensitive should match")
	}
	if serverNameMatches("electerm", "github") {
		t.Error("distinct names should not match")
	}
}

func TestInjectAgyListToolsIntoBody_AddsDecl(t *testing.T) {
	body := []byte(`{"contents":[],"tools":[{"functionDeclarations":[
		{"name":"call_mcp_tool"}
	]}]}`)
	out := injectAgyListToolsIntoBody(body)
	if !strings.Contains(string(out), `"name":"agy_list_tools"`) {
		t.Fatalf("agy_list_tools decl missing: %s", out)
	}
	// Original call_mcp_tool decl should be preserved.
	if !strings.Contains(string(out), `"name":"call_mcp_tool"`) {
		t.Fatalf("call_mcp_tool decl should be preserved: %s", out)
	}
}

func TestInjectAgyListToolsIntoBody_Idempotent(t *testing.T) {
	body := []byte(`{"contents":[],"tools":[{"functionDeclarations":[
		{"name":"call_mcp_tool"},{"name":"agy_list_tools"}
	]}]}`)
	out := injectAgyListToolsIntoBody(body)
	count := strings.Count(string(out), `"name":"agy_list_tools"`)
	if count != 1 {
		t.Fatalf("re-injection should be no-op, found %d copies", count)
	}
}

func TestInjectAgyListToolsIntoBody_NoTools_CreatesEntry(t *testing.T) {
	body := []byte(`{"contents":[]}`)
	out := injectAgyListToolsIntoBody(body)
	if !strings.Contains(string(out), `"name":"agy_list_tools"`) {
		t.Fatalf("should create tools entry: %s", out)
	}
}

func TestExtractAgyListToolsCall_BasicMatch(t *testing.T) {
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"agy_list_tools","args":{"server":"electerm"}}}
	]}}]}`)
	args, ok := extractAgyListToolsCall(body)
	if !ok {
		t.Fatal("expected match")
	}
	if args["server"] != "electerm" {
		t.Errorf("expected server=electerm, got %v", args)
	}
}

func TestExtractAgyListToolsCall_NoArgs(t *testing.T) {
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"agy_list_tools"}}
	]}}]}`)
	args, ok := extractAgyListToolsCall(body)
	if !ok {
		t.Fatal("expected match even without args")
	}
	if args == nil {
		t.Error("args should be non-nil empty map")
	}
}

func TestExtractAgyListToolsCall_NotAgy(t *testing.T) {
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"call_mcp_tool","args":{}}}
	]}}]}`)
	if _, ok := extractAgyListToolsCall(body); ok {
		t.Fatal("call_mcp_tool should NOT match")
	}
}

func TestExtractAgyListToolsCall_MixedContent_NoIntercept(t *testing.T) {
	// Model emitted text + agy_list_tools → we should NOT intercept
	// because there's real model output to preserve.
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"text":"Let me check what's available..."},
		{"functionCall":{"name":"agy_list_tools","args":{}}}
	]}}]}`)
	if _, ok := extractAgyListToolsCall(body); ok {
		t.Fatal("mixed text+agy_list_tools should NOT intercept")
	}
}

func TestExtractAgyListToolsCall_MixedOtherCall_NoIntercept(t *testing.T) {
	// Model emitted call_mcp_tool + agy_list_tools → don't intercept.
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"agy_list_tools","args":{}}},
		{"functionCall":{"name":"call_mcp_tool","args":{"ServerName":"x","ToolName":"y","Arguments":{}}}}
	]}}]}`)
	if _, ok := extractAgyListToolsCall(body); ok {
		t.Fatal("mixed agy_list_tools+call_mcp_tool should NOT intercept")
	}
}

func TestExtractAgyListToolsCall_AgymimicWrapper(t *testing.T) {
	// agymimic wraps as {response: {...}} — extractor should peel it.
	body := []byte(`{"response":{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"agy_list_tools","args":{}}}
	]}}]}}`)
	if _, ok := extractAgyListToolsCall(body); !ok {
		t.Fatal("should peel agymimic envelope")
	}
}

func TestAppendAssistantCallAndUserResponse_AddsTwoTurns(t *testing.T) {
	body := []byte(`{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`)
	callArgs := map[string]any{"server": "electerm"}
	resp := map[string]any{"servers": map[string]any{"electerm": []any{}}}
	out, err := appendAssistantCallAndUserResponse(body, callArgs, resp)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output not valid JSON: %v", err)
	}
	contents := parsed["contents"].([]any)
	if len(contents) != 3 {
		t.Fatalf("expected 3 turns (original + assistant.call + user.response), got %d", len(contents))
	}
	// 2nd turn should be assistant.functionCall(agy_list_tools)
	t2 := contents[1].(map[string]any)
	if t2["role"] != "model" {
		t.Errorf("turn 2 role should be model: %v", t2["role"])
	}
	t2parts := t2["parts"].([]any)
	if fc, ok := t2parts[0].(map[string]any)["functionCall"].(map[string]any); !ok || fc["name"] != "agy_list_tools" {
		t.Errorf("turn 2 should be functionCall(agy_list_tools): %v", t2parts)
	}
	// 3rd turn should be user.functionResponse
	t3 := contents[2].(map[string]any)
	if t3["role"] != "user" {
		t.Errorf("turn 3 role should be user: %v", t3["role"])
	}
	t3parts := t3["parts"].([]any)
	fr, ok := t3parts[0].(map[string]any)["functionResponse"].(map[string]any)
	if !ok || fr["name"] != "agy_list_tools" {
		t.Errorf("turn 3 should be functionResponse(agy_list_tools): %v", t3parts)
	}
}

func TestAppendAssistantCallAndUserResponse_HandlesWrappedRequest(t *testing.T) {
	// Body might be wrapped as {request: {contents: ...}}.
	body := []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`)
	out, err := appendAssistantCallAndUserResponse(body, map[string]any{}, map[string]any{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output not valid JSON: %v", err)
	}
	// Should preserve wrap.
	inner, ok := parsed["request"].(map[string]any)
	if !ok {
		t.Fatalf("wrap should be preserved: %v", parsed)
	}
	contents := inner["contents"].([]any)
	if len(contents) != 3 {
		t.Errorf("expected 3 turns under wrap, got %d", len(contents))
	}
}

func TestAgyListToolsSSEEvent_FormatValid(t *testing.T) {
	body := []byte(`{"candidates":[]}`)
	evt := agyListToolsSSEEvent(body)
	s := string(evt)
	if !strings.HasPrefix(s, "data: ") {
		t.Errorf("missing data: prefix: %q", s)
	}
	if !strings.HasSuffix(s, "\n\n") {
		t.Errorf("missing trailing \\n\\n: %q", s)
	}
}

// TestAgyListToolsSSEEvent_CompactsPrettyJSON verifies the regression
// fix for an omp "Expected '}'" parse error. Gemini non-streaming
// responses can be pretty-printed with literal newlines + indentation;
// SSE uses newlines to terminate events, so the JSON MUST be compacted
// before emission or the client sees a truncated payload.
func TestAgyListToolsSSEEvent_CompactsPrettyJSON(t *testing.T) {
	pretty := []byte("{\n  \"candidates\": [\n    {\n      \"content\": {\n        \"parts\": []\n      }\n    }\n  ]\n}")
	out := agyListToolsSSEEvent(pretty)
	s := string(out)
	if !strings.HasPrefix(s, "data: ") {
		t.Fatalf("missing data: prefix: %q", s)
	}
	if !strings.HasSuffix(s, "\n\n") {
		t.Fatalf("missing trailing \\n\\n: %q", s)
	}
	payload := s[len("data: ") : len(s)-2]
	if strings.ContainsAny(payload, "\n\r") {
		t.Fatalf("payload contains literal newline/CR — would split SSE event: %q", payload)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("payload not valid JSON after compaction: %v\n%s", err, payload)
	}
}

func TestAgyListToolsSSEEvent_HandlesInvalidJSONDefensively(t *testing.T) {
	junk := []byte("not\njson\n{}")
	out := agyListToolsSSEEvent(junk)
	s := string(out)
	payload := s[len("data: ") : len(s)-2]
	if strings.ContainsAny(payload, "\n\r") {
		t.Fatalf("fallback path must also strip newlines: %q", payload)
	}
}
