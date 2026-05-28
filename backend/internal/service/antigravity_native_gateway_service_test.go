package service

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestWrapNativeV1Internal_BareGeminiBody verifies the common case: a plain
// Gemini-format request body gets wrapped in the v1internal envelope with
// project/model/userAgent/requestId fields populated.
func TestWrapNativeV1Internal_BareGeminiBody(t *testing.T) {
	body := []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`)
	out, err := wrapNativeV1Internal("proj-123", "gemini-3-flash", body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(out, &envelope); err != nil {
		t.Fatalf("envelope is not valid json: %v\nraw: %s", err, out)
	}
	if envelope["project"] != "proj-123" {
		t.Errorf("project: want proj-123, got %v", envelope["project"])
	}
	if envelope["model"] != "gemini-3-flash" {
		t.Errorf("model: want gemini-3-flash, got %v", envelope["model"])
	}
	if envelope["userAgent"] != "antigravity" {
		t.Errorf("userAgent: want antigravity, got %v", envelope["userAgent"])
	}
	rid, ok := envelope["requestId"].(string)
	if !ok || !strings.HasPrefix(rid, "agent-") {
		t.Errorf("requestId: want agent-<uuid>, got %v", envelope["requestId"])
	}
	req, ok := envelope["request"].(map[string]any)
	if !ok {
		t.Fatalf("request: want map, got %T", envelope["request"])
	}
	if _, ok := req["contents"]; !ok {
		t.Errorf("request.contents missing — original Gemini body was not preserved")
	}
}

// TestWrapNativeV1Internal_IdempotentPassthrough verifies that if the
// caller hands us an already-wrapped envelope (contains
// "userAgent":"antigravity"), we don't re-wrap.
func TestWrapNativeV1Internal_IdempotentPassthrough(t *testing.T) {
	already := []byte(`{"project":"proj-x","model":"gemini-3-pro","request":{"contents":[]},"userAgent":"antigravity","requestId":"agent-abc"}`)
	out, err := wrapNativeV1Internal("ignored", "ignored-model", already)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != string(already) {
		t.Errorf("idempotent passthrough broken:\nwant: %s\ngot:  %s", already, out)
	}
}

// TestWrapNativeV1Internal_DoubleWrapUnwrap verifies that if the caller
// gave us {"request": {...}} (a half-wrapped body — caller already added the
// request envelope but not the userAgent/project/model wrapper) we unwrap
// it once before re-wrapping into the full envelope.
func TestWrapNativeV1Internal_DoubleWrapUnwrap(t *testing.T) {
	body := []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}}`)
	out, err := wrapNativeV1Internal("proj-y", "claude-opus-4-6-thinking", body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(out, &envelope); err != nil {
		t.Fatalf("envelope is not valid json: %v\nraw: %s", err, out)
	}
	if envelope["userAgent"] != "antigravity" {
		t.Errorf("userAgent: want antigravity, got %v", envelope["userAgent"])
	}
	req, ok := envelope["request"].(map[string]any)
	if !ok {
		t.Fatalf("request: want map, got %T", envelope["request"])
	}
	// Inner should be the contents map, NOT another {"request":{...}} layer.
	if _, double := req["request"]; double {
		t.Errorf("double-wrap not unwrapped — request.request still present: %v", req)
	}
	if _, ok := req["contents"]; !ok {
		t.Errorf("request.contents missing after unwrap")
	}
}

// TestWrapNativeV1Internal_EmptyProject verifies that an empty projectID is
// dropped from the envelope (omitted, not emitted as ""). agymimic's auth
// flow may not yet have discovered the project on a fresh import — the
// upstream backend tolerates a missing project field but rejects "".
func TestWrapNativeV1Internal_EmptyProject(t *testing.T) {
	body := []byte(`{"contents":[]}`)
	out, err := wrapNativeV1Internal("", "gemini-3-flash", body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(out, &envelope); err != nil {
		t.Fatalf("invalid envelope: %v", err)
	}
	if _, present := envelope["project"]; present {
		t.Errorf("project key should be absent when projectID is empty, got %v", envelope["project"])
	}
}

// TestWrapNativeV1Internal_InvalidJSON returns a clear error rather than
// silently emitting a malformed envelope.
func TestWrapNativeV1Internal_InvalidJSON(t *testing.T) {
	body := []byte(`{not valid json`)
	_, err := wrapNativeV1Internal("proj", "m", body)
	if err == nil {
		t.Fatal("expected error on invalid JSON body")
	}
}

// TestChooseGeminiAction covers the 3 branches: explicit action wins,
// otherwise streamGenerateContent when stream=true, generateContent
// otherwise.
func TestChooseGeminiAction(t *testing.T) {
	cases := []struct {
		action string
		stream bool
		want   string
	}{
		{"countTokens", true, "countTokens"},
		{"countTokens", false, "countTokens"},
		{"", true, "streamGenerateContent"},
		{"", false, "generateContent"},
	}
	for _, c := range cases {
		got := chooseGeminiAction(c.action, c.stream)
		if got != c.want {
			t.Errorf("chooseGeminiAction(%q, %v) = %q, want %q", c.action, c.stream, got, c.want)
		}
	}
}
