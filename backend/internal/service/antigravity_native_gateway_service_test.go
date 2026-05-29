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

// ---------------------------------------------------------------------------
// unwrapAgyResponseEnvelope*
// ---------------------------------------------------------------------------

// TestUnwrapBody_StripsResponseWrapper covers the agymimic non-streaming
// shape: {"response":{...},"traceId":"…","metadata":{}}. Standard Gemini SDKs
// expect the inner object at the top level.
func TestUnwrapBody_StripsResponseWrapper(t *testing.T) {
	in := []byte(`{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]}}],"usageMetadata":{"promptTokenCount":3}},"traceId":"abc","metadata":{}}`)
	out := unwrapAgyResponseEnvelopeBody(in)
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unwrapped body is not valid JSON: %v\nraw: %s", err, out)
	}
	if _, ok := got["candidates"]; !ok {
		t.Fatalf("candidates missing at top level: %s", out)
	}
	if _, ok := got["usageMetadata"]; !ok {
		t.Fatalf("usageMetadata missing at top level: %s", out)
	}
	if _, ok := got["response"]; ok {
		t.Fatalf("response key should be gone: %s", out)
	}
}

// TestUnwrapBody_PassthroughForAlreadyCanonical confirms a payload that
// already has candidates at the top is returned untouched (so the unwrapper
// is safe to chain through any forwarder, including the legacy antigravity
// gateway if we ever fold paths together).
func TestUnwrapBody_PassthroughForAlreadyCanonical(t *testing.T) {
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]}}]}`)
	out := unwrapAgyResponseEnvelopeBody(in)
	if string(out) != string(in) {
		t.Fatalf("canonical body mutated: got %s", out)
	}
}

func TestUnwrapBody_PassthroughForInvalidJSON(t *testing.T) {
	in := []byte(`not-json`)
	out := unwrapAgyResponseEnvelopeBody(in)
	if string(out) != string(in) {
		t.Fatalf("invalid JSON mutated: got %s", out)
	}
}

// TestUnwrapLine_StreamingChunk covers the streaming case — each `data:` line
// carries the same envelope and needs the same unwrap, but the `data:` prefix
// and trailing newline must survive.
func TestUnwrapLine_StreamingChunk(t *testing.T) {
	in := []byte(`data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"Hi"}]}}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1,"totalTokenCount":4}},"traceId":"x","metadata":{}}` + "\n")
	out := unwrapAgyResponseEnvelopeLine(in)
	s := string(out)
	if !strings.HasPrefix(s, "data: {") {
		t.Fatalf("data prefix lost: %q", s)
	}
	if !strings.HasSuffix(s, "\n") {
		t.Fatalf("trailing newline lost: %q", s)
	}
	if strings.Contains(s, `"response":`) {
		t.Fatalf("response wrapper still present: %q", s)
	}
	if !strings.Contains(s, `"candidates":`) || !strings.Contains(s, `"usageMetadata":`) {
		t.Fatalf("candidates/usage not surfaced: %q", s)
	}
}

// TestUnwrapLine_PreservesCRLF — Windows / proxied clients sometimes emit
// CRLF SSE framing; the unwrapper must keep CR + LF as-is so downstream
// parsers don't choke.
func TestUnwrapLine_PreservesCRLF(t *testing.T) {
	in := []byte(`data: {"response":{"candidates":[]}}` + "\r\n")
	out := unwrapAgyResponseEnvelopeLine(in)
	if !strings.HasSuffix(string(out), "\r\n") {
		t.Fatalf("CRLF lost: %q", string(out))
	}
}

// TestUnwrapLine_PassthroughForNonDataLines — keep-alive comments and blank
// keep-alive lines must not be rewritten.
func TestUnwrapLine_PassthroughForNonDataLines(t *testing.T) {
	for _, in := range [][]byte{
		[]byte("\n"),
		[]byte(": keep-alive\n"),
		[]byte("event: ping\n"),
	} {
		out := unwrapAgyResponseEnvelopeLine(in)
		if string(out) != string(in) {
			t.Fatalf("non-data line mutated: in=%q out=%q", in, out)
		}
	}
}

// ---------------------------------------------------------------------------
// inspectGeminiResponseForAnomalies
// ---------------------------------------------------------------------------

func TestInspectAnomalies_CleanText(t *testing.T) {
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hi"}]},"finishReason":"STOP"}]}`)
	a, _ := inspectGeminiResponseForAnomalies(in)
	if a != "" {
		t.Fatalf("clean text response flagged: %s", a)
	}
}

func TestInspectAnomalies_CleanFunctionCall(t *testing.T) {
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"find","args":{"paths":["."]}}}]},"finishReason":"STOP"}]}`)
	a, _ := inspectGeminiResponseForAnomalies(in)
	if a != "" {
		t.Fatalf("clean function call flagged: %s", a)
	}
}

func TestInspectAnomalies_EmptyFunctionArgs(t *testing.T) {
	// This is the omp/Zod failure case — model returned find() with no args
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"find","args":{}}}]}}]}`)
	a, d := inspectGeminiResponseForAnomalies(in)
	if a != "empty_function_args" {
		t.Fatalf("expected empty_function_args, got %q", a)
	}
	if d["function"] != "find" {
		t.Fatalf("expected function=find in details, got %v", d)
	}
}

func TestInspectAnomalies_StopWithoutContent(t *testing.T) {
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":""}]},"finishReason":"STOP"}]}`)
	a, _ := inspectGeminiResponseForAnomalies(in)
	if a != "stop_without_content" {
		t.Fatalf("expected stop_without_content, got %q", a)
	}
}

func TestInspectAnomalies_NoCandidates(t *testing.T) {
	in := []byte(`{"candidates":[]}`)
	a, _ := inspectGeminiResponseForAnomalies(in)
	if a != "no_candidates" {
		t.Fatalf("expected no_candidates, got %q", a)
	}
}

func TestInspectAnomalies_PassthroughForInvalidJSON(t *testing.T) {
	a, _ := inspectGeminiResponseForAnomalies([]byte("garbage"))
	if a != "" {
		t.Fatalf("invalid JSON should not flag, got %q", a)
	}
}

// ---------------------------------------------------------------------------
// extractDataPayload
// ---------------------------------------------------------------------------

func TestExtractDataPayload(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{"plain data", `data: {"candidates":[]}` + "\n", `{"candidates":[]}`},
		{"data done", "data: [DONE]\n", ""},
		{"non data event", "event: ping\n", ""},
		{"empty line", "\n", ""},
		{"data with crlf", "data: {\"x\":1}\r\n", `{"x":1}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractDataPayload([]byte(tc.in))
			if string(got) != tc.want {
				t.Fatalf("extractDataPayload(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
