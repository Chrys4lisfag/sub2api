package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestChatHistoryLogService_DisabledIsNoop(t *testing.T) {
	tmp := t.TempDir()
	svc := NewChatHistoryLogService(tmp, 1024*1024, false)
	svc.Start(context.Background())
	defer svc.Stop()
	svc.Log(ChatHistoryEntry{AccountID: 1})
	// Wait a beat then verify nothing was written.
	time.Sleep(50 * time.Millisecond)
	entries, _ := os.ReadDir(tmp)
	if len(entries) != 0 {
		t.Errorf("expected no files when disabled, got %d", len(entries))
	}
}

func TestChatHistoryLogService_WritesEntry(t *testing.T) {
	tmp := t.TempDir()
	svc := NewChatHistoryLogService(tmp, 1024*1024, true)
	svc.Start(context.Background())
	defer svc.Stop()
	svc.Log(ChatHistoryEntry{
		AccountID: 42,
		Platform:  "antigravity_native",
		Model:     "gemini-3.1-pro",
		Request:   map[string]any{"contents": []any{map[string]any{"role": "user"}}},
		Response:  map[string]any{"candidates": []any{}},
	})
	// Give writer goroutine time to flush.
	time.Sleep(150 * time.Millisecond)
	// One plain .jsonl file should exist (not gzipped yet since size < 50 MiB).
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one log file")
	}
	var found bool
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".jsonl") {
			found = true
			b, _ := os.ReadFile(filepath.Join(tmp, e.Name()))
			if !strings.Contains(string(b), `"account_id":42`) {
				t.Errorf("entry missing in file: %s", b)
			}
		}
	}
	if !found {
		t.Errorf("no .jsonl file found among entries: %v", entries)
	}
}

func TestChatHistoryLogService_Redacts(t *testing.T) {
	entry := ChatHistoryEntry{
		Request: map[string]any{
			"headers": map[string]any{
				"Authorization":  "Bearer secret-token",
				"x-goog-api-key": "abc123",
			},
			"contents": []any{
				map[string]any{
					"refresh_token": "refresh-zzz",
					"nested": map[string]any{
						"password": "hunter2",
					},
				},
			},
		},
	}
	redactChatHistoryEntry(&entry)
	buf, _ := json.Marshal(entry)
	s := string(buf)
	for _, leak := range []string{"secret-token", "abc123", "refresh-zzz", "hunter2"} {
		if strings.Contains(s, leak) {
			t.Errorf("leak detected after redaction: %q in %s", leak, s)
		}
	}
	for _, marker := range []string{`"[REDACTED]"`} {
		if !strings.Contains(s, marker) {
			t.Errorf("expected redaction marker %q in %s", marker, s)
		}
	}
}

func TestAccountAllowsChatHistory(t *testing.T) {
	if !AccountAllowsChatHistory(nil) {
		t.Error("nil account should allow")
	}
	if !AccountAllowsChatHistory(&Account{Credentials: map[string]any{}}) {
		t.Error("empty credentials should allow")
	}
	if AccountAllowsChatHistory(&Account{Credentials: map[string]any{"chat_history_enabled": false}}) {
		t.Error("bool false should opt out")
	}
	if AccountAllowsChatHistory(&Account{Credentials: map[string]any{"chat_history_enabled": "off"}}) {
		t.Error("string 'off' should opt out")
	}
	if !AccountAllowsChatHistory(&Account{Credentials: map[string]any{"chat_history_enabled": "yes"}}) {
		t.Error("string 'yes' should allow")
	}
}

func TestExtractToolCallNames(t *testing.T) {
	resp := map[string]any{
		"candidates": []any{
			map[string]any{
				"content": map[string]any{
					"parts": []any{
						map[string]any{"functionCall": map[string]any{"name": "call_mcp_tool"}},
						map[string]any{"text": "hello"},
						map[string]any{"functionCall": map[string]any{"name": "agy_list_tools"}},
					},
				},
			},
		},
	}
	names := extractToolCallNamesFromResponse(resp)
	if len(names) != 2 || names[0] != "call_mcp_tool" || names[1] != "agy_list_tools" {
		t.Errorf("unexpected names: %v", names)
	}
}

// ---------------------------------------------------------------------------
// MCP discovery mode tests
// ---------------------------------------------------------------------------

func TestNormalizeMcpDiscoveryMode(t *testing.T) {
	cases := map[string]string{
		"prompt":         "prompt",
		"  PROMPT  ":     "prompt",
		"prompt_only":    "prompt",
		"prompt-only":    "prompt",
		"list_tool":      "list_tool",
		"list-tool":      "list_tool",
		"list_tool_only": "list_tool",
		"both":           "both",
		"all":            "both",
		"full":           "both",
		"":               "",
		"garbage":        "",
		"BoTh":           "both",
	}
	for in, want := range cases {
		if got := normalizeMcpDiscoveryMode(in); got != want {
			t.Errorf("normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestModeFlags(t *testing.T) {
	if !modeDeclaresListTool("both") || !modeDeclaresListTool("list_tool") {
		t.Error("both / list_tool should declare agy_list_tools")
	}
	if modeDeclaresListTool("prompt") {
		t.Error("prompt should NOT declare agy_list_tools")
	}
	if !modeInjectsCatalog("both") || !modeInjectsCatalog("prompt") {
		t.Error("both / prompt should inject full catalog")
	}
	if modeInjectsCatalog("list_tool") {
		t.Error("list_tool should NOT inject full catalog (minimal hint only)")
	}
}

// ---------------------------------------------------------------------------
// catalog + truncation tests
// ---------------------------------------------------------------------------

func TestBuildMcpCatalogText_FullMode(t *testing.T) {
	tools := []mcpToolHandle{
		{FullName: "mcp__electerm_list_electerm_bookmarks", Decl: map[string]any{"description": "list"}},
		{FullName: "mcp__electerm_send_electerm_terminal_command", Decl: map[string]any{"description": "send"}},
		{FullName: "mcp__github_get_pr", Decl: map[string]any{"description": "get pr"}},
	}
	out := buildMcpCatalogText(tools, "call_mcp_tool", true, true)
	if !strings.Contains(out, "agy_list_tools") {
		t.Error("full mode + declaresListTool should mention agy_list_tools in HOW-TO")
	}
	if !strings.Contains(out, "ANTI-FALLBACK") {
		t.Error("missing anti-fallback rule")
	}
	if !strings.Contains(out, "mcp__electerm_send_electerm_terminal_command") {
		t.Error("send_terminal_command should be present in full catalog")
	}
	if !strings.Contains(out, "# electerm") || !strings.Contains(out, "# github") {
		t.Error("expected per-server section headers")
	}
}

func TestBuildMcpCatalogText_ListToolMode(t *testing.T) {
	tools := []mcpToolHandle{
		{FullName: "mcp__electerm_list_electerm_bookmarks", Decl: map[string]any{"description": "list"}},
		{FullName: "mcp__electerm_send_electerm_terminal_command", Decl: map[string]any{"description": "send"}},
	}
	out := buildMcpCatalogText(tools, "call_mcp_tool", false, true)
	// In list_tool mode we show only server names + counts.
	if !strings.Contains(out, "AVAILABLE SERVERS") {
		t.Error("expected AVAILABLE SERVERS section in minimal mode")
	}
	if !strings.Contains(out, "electerm (2 tools)") {
		t.Errorf("expected server name + count, got: %s", out)
	}
	// Full names should NOT be enumerated in minimal mode.
	if strings.Contains(out, "mcp__electerm_send_electerm_terminal_command") {
		t.Error("minimal mode should not enumerate full tool names")
	}
}

func TestBuildMcpCatalogText_NoListToolNoAdvertise(t *testing.T) {
	tools := []mcpToolHandle{
		{FullName: "mcp__electerm_x", Decl: map[string]any{"description": "x"}},
	}
	out := buildMcpCatalogText(tools, "call_mcp_tool", true, false)
	if strings.Contains(out, "DISCOVERY:") {
		t.Error("declaresListTool=false should NOT include DISCOVERY section")
	}
}
