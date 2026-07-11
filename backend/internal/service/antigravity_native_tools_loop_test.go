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
	servers, _ := resp["servers"].(map[string]any)
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
	info, ok := extractAgyListToolsCall(body)
	if !ok {
		t.Fatal("expected match")
	}
	if info.CallArgs["server"] != "electerm" {
		t.Errorf("expected server=electerm, got %v", info.CallArgs)
	}
	if info.HasOtherFunctionCall {
		t.Error("no other function call present")
	}
}

func TestExtractAgyListToolsCall_NoArgs(t *testing.T) {
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"agy_list_tools"}}
	]}}]}`)
	info, ok := extractAgyListToolsCall(body)
	if !ok {
		t.Fatal("expected match even without args")
	}
	if info.CallArgs == nil {
		t.Error("CallArgs should be non-nil empty map")
	}
}

func TestExtractAgyListToolsCall_NotAgy(t *testing.T) {
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"call_mcp_tool","args":{}}}
	]}}]}`)
	if _, ok := extractAgyListToolsCall(body); ok {
		t.Fatal("call_mcp_tool alone should NOT match (no agy_list_tools present)")
	}
}

// TestExtractAgyListToolsCall_TextPlusAgyListTools_DoesIntercept covers
// the regression that broke omp: reasoning models emit explanatory text
// + the discovery call together. Prior version refused to intercept
// because of a "mixed content" guard; the discovery call then leaked
// to the client which errored with "Tool agy_list_tools not found".
func TestExtractAgyListToolsCall_TextPlusAgyListTools_DoesIntercept(t *testing.T) {
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"text":"Let me check what's available..."},
		{"functionCall":{"name":"agy_list_tools","args":{}}}
	]}}]}`)
	info, ok := extractAgyListToolsCall(body)
	if !ok {
		t.Fatal("text + agy_list_tools MUST intercept (text-only mixed content is fine)")
	}
	if info.HasOtherFunctionCall {
		t.Error("text is not a function call — HasOtherFunctionCall should be false")
	}
	if len(info.AssistantParts) != 2 {
		t.Errorf("AssistantParts should preserve all 2 parts, got %d", len(info.AssistantParts))
	}
}

func TestExtractAgyListToolsCall_MixedWithOtherCall_StripPath(t *testing.T) {
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"agy_list_tools","args":{}}},
		{"functionCall":{"name":"call_mcp_tool","args":{"ServerName":"x","ToolName":"y","Arguments":{}}}}
	]}}]}`)
	info, ok := extractAgyListToolsCall(body)
	if !ok {
		t.Fatal("agy_list_tools+call_mcp_tool: should report match with HasOtherFunctionCall=true")
	}
	if !info.HasOtherFunctionCall {
		t.Error("HasOtherFunctionCall must be true when other functionCall present")
	}
}

func TestExtractAgyListToolsCall_AgymimicWrapper(t *testing.T) {
	body := []byte(`{"response":{"candidates":[{"content":{"parts":[
		{"functionCall":{"name":"agy_list_tools","args":{}}}
	]}}]}}`)
	if _, ok := extractAgyListToolsCall(body); !ok {
		t.Fatal("should peel agymimic envelope")
	}
}

func TestStripAgyListToolsFromResponse_RemovesOnlyDiscoveryCall(t *testing.T) {
	body := []byte(`{"candidates":[{"content":{"parts":[
		{"text":"hi"},
		{"functionCall":{"name":"agy_list_tools","args":{}}},
		{"functionCall":{"name":"call_mcp_tool","args":{"ServerName":"x","ToolName":"y","Arguments":{}}}}
	]}}]}`)
	out := stripAgyListToolsFromResponse(body)
	if strings.Contains(string(out), "agy_list_tools") {
		t.Errorf("agy_list_tools should be stripped: %s", out)
	}
	if !strings.Contains(string(out), "call_mcp_tool") {
		t.Errorf("call_mcp_tool should remain: %s", out)
	}
	if !strings.Contains(string(out), `"text":"hi"`) {
		t.Errorf("text should remain: %s", out)
	}
}

func TestStripAgyListToolsFromResponse_NoOpWhenAbsent(t *testing.T) {
	body := []byte(`{"candidates":[{"content":{"parts":[{"text":"hi"}]}}]}`)
	out := stripAgyListToolsFromResponse(body)
	if string(out) != string(body) {
		t.Errorf("no-op expected when no agy_list_tools present: %s -> %s", body, out)
	}
}

func TestAppendAssistantTurnAndUserResponse_PreservesText(t *testing.T) {
	body := []byte(`{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`)
	assistantParts := []any{
		map[string]any{"text": "Let me check"},
		map[string]any{"functionCall": map[string]any{"name": "agy_list_tools", "args": map[string]any{"server": "electerm"}}},
	}
	resp := map[string]any{"servers": map[string]any{"electerm": []any{}}}
	out, err := appendAssistantTurnAndUserResponse(body, assistantParts, resp)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output not valid JSON: %v", err)
	}
	contents, _ := parsed["contents"].([]any)
	if len(contents) != 3 {
		t.Fatalf("expected 3 turns, got %d", len(contents))
	}
	t2 := contents[1].(map[string]any)
	if t2["role"] != "model" {
		t.Errorf("turn 2 role should be model: %v", t2["role"])
	}
	t2parts := t2["parts"].([]any)
	if len(t2parts) != 2 {
		t.Fatalf("assistant turn should preserve both text + call parts, got %d", len(t2parts))
	}
	if _, ok := t2parts[0].(map[string]any)["text"]; !ok {
		t.Errorf("first part should be text: %v", t2parts[0])
	}
	if fc, ok := t2parts[1].(map[string]any)["functionCall"].(map[string]any); !ok || fc["name"] != "agy_list_tools" {
		t.Errorf("second part should be functionCall(agy_list_tools): %v", t2parts[1])
	}
	t3 := contents[2].(map[string]any)
	if t3["role"] != "user" {
		t.Errorf("turn 3 role should be user: %v", t3["role"])
	}
}

func TestAppendAssistantTurnAndUserResponse_HandlesWrappedRequest(t *testing.T) {
	body := []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`)
	parts := []any{map[string]any{"functionCall": map[string]any{"name": "agy_list_tools"}}}
	out, err := appendAssistantTurnAndUserResponse(body, parts, map[string]any{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output not valid JSON: %v", err)
	}
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

// TestExtractAgyListToolsCall_RealUpstreamShape regression-locks the
// real wire shape captured from chat-history when omp showed "Tool
// agy_list_tools not found". The previous extractor refused to peel
// because the agymimic wrapper had >2 top-level keys (response,
// usageMetadata, modelVersion, responseId, ...). Result: candidates
// were searched at the WRONG level and the call leaked to the client.
func TestExtractAgyListToolsCall_RealUpstreamShape(t *testing.T) {
	body := []byte(`{
		"response": {
			"candidates": [{
				"content": {
					"parts": [
						{"text": "**My Current Inventory of MCP Tools**\nuser wants list...", "thought": true},
						{
							"functionCall": {"args": {}, "id": "2v9et4by", "name": "agy_list_tools"},
							"thoughtSignature": "EuYNCuMNAQw"
						}
					],
					"role": "model"
				}
			}],
			"usageMetadata": {"promptTokenCount": 1000, "candidatesTokenCount": 50},
			"modelVersion": "gemini-3.5-flash-extra-low",
			"responseId": "abc123"
		},
		"someOtherField": "value"
	}`)
	info, ok := extractAgyListToolsCall(body)
	if !ok {
		t.Fatal("MUST intercept real upstream agymimic-wrapped response (regression: previous len(root) <= 2 guard prevented peel)")
	}
	if info.HasOtherFunctionCall {
		t.Error("thought-text + agy_list_tools is not 'other function call'")
	}
	if info.CallArgs == nil {
		t.Error("CallArgs should be the empty args object, not nil")
	}
	if len(info.AssistantParts) != 2 {
		t.Errorf("expected 2 assistant parts (thought-text + functionCall), got %d", len(info.AssistantParts))
	}
}

// TestStripAgyListToolsFromResponse_RealUpstreamShape mirrors the
// extractor regression test for the strip path.
func TestStripAgyListToolsFromResponse_RealUpstreamShape(t *testing.T) {
	body := []byte(`{
		"response": {
			"candidates": [{
				"content": {
					"parts": [
						{"functionCall": {"args": {}, "name": "agy_list_tools"}},
						{"functionCall": {"args": {"ServerName": "electerm", "ToolName": "list_electerm_bookmarks", "Arguments": {}}, "name": "call_mcp_tool"}}
					]
				}
			}]
		},
		"usageMetadata": {"promptTokenCount": 100}
	}`)
	out := stripAgyListToolsFromResponse(body)
	if strings.Contains(string(out), "agy_list_tools") {
		t.Errorf("agy_list_tools should be stripped even when wrapped in agymimic envelope: %s", out)
	}
	if !strings.Contains(string(out), "call_mcp_tool") {
		t.Errorf("call_mcp_tool should remain: %s", out)
	}
}
