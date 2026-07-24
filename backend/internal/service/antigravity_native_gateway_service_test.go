package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
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
	if !ok || !strings.HasPrefix(rid, "checkpoint/") {
		t.Errorf("requestId: want checkpoint/<uuid>, got %v", envelope["requestId"])
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

func TestInspectAnomalies_ThoughtOnlyStop(t *testing.T) {
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"private reasoning","thought":true}]},"finishReason":"STOP"}]}`)
	a, _ := inspectGeminiResponseForAnomalies(in)
	if a != "stop_without_content" {
		t.Fatalf("expected thought-only STOP to be unusable content, got %q", a)
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

// ---------------------------------------------------------------------------
// inspectStreamChunk
// ---------------------------------------------------------------------------

// Streaming chunks come in three flavors during a normal completion:
//
//	chunk 1: text fragment
//	chunk 2: text fragment
//	chunk N: thoughtSignature + text="" + finishReason=STOP  ← benign tail
//
// The original anomaly inspector only looked at the LAST chunk and
// raised stop_without_content even though chunks 1..N-1 carried text.
// inspectStreamChunk is per-chunk and the streamGeminiToClient loop
// accumulates the booleans across the entire stream.
func TestInspectStreamChunk_TextChunk(t *testing.T) {
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hi"}]}}]}`)
	saw, fn, empty := inspectStreamChunk(in)
	if !saw || fn || empty != "" {
		t.Fatalf("text chunk: sawText=%v sawFn=%v emptyArgs=%q want sawText=true rest empty", saw, fn, empty)
	}
}

func TestInspectStreamChunk_FinalThoughtSignatureOnly(t *testing.T) {
	// This is the chunk that previously triggered the false-positive.
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"thoughtSignature":"abc","text":""}]},"finishReason":"STOP"}]}`)
	saw, fn, empty := inspectStreamChunk(in)
	if saw || fn || empty != "" {
		t.Fatalf("final-tail chunk should report nothing: sawText=%v sawFn=%v emptyArgs=%q", saw, fn, empty)
	}
}

func TestInspectStreamChunk_ThoughtTextIsNotAnswer(t *testing.T) {
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"private reasoning","thought":true}]}}]}`)
	saw, fn, empty := inspectStreamChunk(in)
	if saw || fn || empty != "" {
		t.Fatalf("thought text must not count as answer content: sawText=%v sawFn=%v emptyArgs=%q", saw, fn, empty)
	}
}

func TestInspectStreamChunk_FunctionCallWithArgs(t *testing.T) {
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"find","args":{"paths":["."]}}}]}}]}`)
	saw, fn, empty := inspectStreamChunk(in)
	if saw || !fn || empty != "" {
		t.Fatalf("good function call: sawText=%v sawFn=%v emptyArgs=%q want sawFn=true rest false", saw, fn, empty)
	}
}

func TestInspectStreamChunk_FunctionCallEmptyArgs(t *testing.T) {
	// The actual omp/Zod failure pattern.
	in := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"find","args":{}}}]}}]}`)
	saw, fn, empty := inspectStreamChunk(in)
	if saw || !fn || empty != "find" {
		t.Fatalf("empty-args fn call: sawText=%v sawFn=%v emptyArgs=%q want sawFn=true emptyArgs=find", saw, fn, empty)
	}
}

func TestInspectStreamChunk_InvalidJSON(t *testing.T) {
	saw, fn, empty := inspectStreamChunk([]byte("garbage"))
	if saw || fn || empty != "" {
		t.Fatalf("invalid JSON should be silent: %v %v %q", saw, fn, empty)
	}
}

// TestExtractAgyErrorMessage covers the upstream-body -> human-readable
// summary path used by the 403 surface (the message we put into
// account.error_message and into the test-dialog content event).
func TestExtractAgyErrorMessage(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "PERMISSION_DENIED verify",
			body: `{"error":{"code":403,"message":"Verify your account to continue.","status":"PERMISSION_DENIED"}}`,
			want: "PERMISSION_DENIED: Verify your account to continue.",
		},
		{
			name: "message only",
			body: `{"error":{"message":"nope"}}`,
			want: "nope",
		},
		{
			name: "status only",
			body: `{"error":{"status":"UNAUTHENTICATED"}}`,
			want: "UNAUTHENTICATED",
		},
		{
			name: "empty body",
			body: ``,
			want: "",
		},
		{
			name: "garbage body",
			body: `not json`,
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractAgyErrorMessage([]byte(tc.body))
			if got != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
		})
	}
}

// TestVerifyFlow_ParsesUpstream403 exercises the parsing helpers the
// VALIDATION_REQUIRED flow chains together (classifyForbiddenType ->
// extractValidationURL -> extractAgyErrorMessage) against a Google-
// shaped 403 body that includes both the structured
// details[].metadata.validation_url field AND the URL inside the human
// message. The point is to lock the parse outputs that the new
// pauseAccountForValidation + TestConnection surface depend on.
func TestVerifyFlow_ParsesUpstream403(t *testing.T) {
	body := `{
		"error": {
			"code": 403,
			"status": "PERMISSION_DENIED",
			"message": "Your current account is not eligible for Antigravity. Verify your account to continue. https://accounts.google.com/signin/continue?sarp=1&continue=https://developers.google.com/gemini-code-assist/auth/auth_success_gemini&plt=AKgnsbsTOKEN&flowName=GlifWebSignIn&authuser",
			"details": [{
				"@type": "type.googleapis.com/google.rpc.ErrorInfo",
				"reason": "VALIDATION_REQUIRED",
				"metadata": {
					"validation_url": "https://accounts.google.com/signin/continue?sarp=1&continue=https://developers.google.com/gemini-code-assist/auth/auth_success_gemini&plt=AKgnsbsTOKEN&flowName=GlifWebSignIn&authuser"
				}
			}]
		}
	}`
	if got := classifyForbiddenType(body); got != forbiddenTypeValidation {
		t.Errorf("classifyForbiddenType: got %q want validation", got)
	}
	url := extractValidationURL(body)
	if !strings.Contains(url, "accounts.google.com/signin/continue") {
		t.Errorf("extractValidationURL: missing google signin URL, got %q", url)
	}
	if !strings.Contains(url, "plt=AKgnsbsTOKEN") {
		t.Errorf("extractValidationURL: missing plt token (the per-account piece omp/the dialog needs), got %q", url)
	}
	msg := extractAgyErrorMessage([]byte(body))
	if !strings.Contains(msg, "PERMISSION_DENIED") || !strings.Contains(msg, "Verify your account") {
		t.Errorf("extractAgyErrorMessage: missing status/message, got %q", msg)
	}
}

// TestIsUpstreamRateLimitPayload covers the new pre-write guard that
// catches the "HTTP 200 OK with 429 inside an SSE event" case so the
// failover loop can rotate accounts before c.Writer.Write commits gin
// headers. The string shapes mirror live cloudcode-pa bodies pulled
// from production logs (compact + indented JSON variants).
func TestIsUpstreamRateLimitPayload(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "google compact 429",
			body: `data: {"error":{"code":429,"message":"Individual quota reached.","status":"RESOURCE_EXHAUSTED"}}`,
			want: true,
		},
		{
			name: "google indented 429",
			body: `data: {
  "error": {
    "code": 429,
    "message": "Individual quota reached.",
    "status": "RESOURCE_EXHAUSTED"
  }
}`,
			want: true,
		},
		{
			name: "google QUOTA_EXHAUSTED metadata reason",
			body: `{"error":{"details":[{"reason":"QUOTA_EXHAUSTED"}]}}`,
			want: true,
		},
		{
			name: "normal candidates chunk",
			body: `data: {"candidates":[{"content":{"parts":[{"text":"hi"}]}}]}`,
			want: false,
		},
		{
			name: "different error (validation)",
			body: `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Verify your account"}}`,
			want: false,
		},
		{
			name: "empty body",
			body: ``,
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isUpstreamRateLimitPayload([]byte(tc.body))
			if got != tc.want {
				t.Errorf("got %v want %v\nbody=%q", got, tc.want, tc.body)
			}
		})
	}
}

func TestStreamGeminiToClient_ThoughtOnlyStopFailsOverBeforeWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader(
			"data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"private reasoning\",\"thought\":true}]},\"finishReason\":\"STOP\"}]}}\n\n",
		)),
	}

	svc := &AntigravityNativeGatewayService{}
	result, err := svc.streamGeminiToClient(
		context.Background(), c, 39, resp, time.Now(),
		"gemini-3.6-flash", "gemini-3.6-flash-high", toolPrepReport{},
	)
	if result != nil {
		t.Fatalf("thought-only STOP result = %#v, want nil", result)
	}
	var failoverErr *UpstreamFailoverError
	if !errors.As(err, &failoverErr) {
		t.Fatalf("thought-only STOP error = %v, want UpstreamFailoverError", err)
	}
	if failoverErr.RetryableOnSameAccount {
		t.Fatal("thought-only STOP should switch account, not retry same account")
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("thought-only STOP committed %d client bytes before failover: %q", rec.Body.Len(), rec.Body.String())
	}
}

func TestStreamGeminiToClient_ThoughtThenAnswerFlushesOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	upstream := strings.Join([]string{
		`data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"private reasoning","thought":true}]}}]}}`,
		"",
		`data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"visible answer"}]},"finishReason":"STOP"}]}}`,
		"",
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(upstream)),
	}

	svc := &AntigravityNativeGatewayService{}
	result, err := svc.streamGeminiToClient(
		context.Background(), c, 39, resp, time.Now(),
		"gemini-3.6-flash", "gemini-3.6-flash-high", toolPrepReport{},
	)
	if err != nil {
		t.Fatalf("thought+answer stream returned error: %v", err)
	}
	if result == nil {
		t.Fatal("thought+answer stream returned nil result")
	}
	out := rec.Body.String()
	if strings.Count(out, "private reasoning") != 1 || strings.Count(out, "visible answer") != 1 {
		t.Fatalf("buffered stream was not flushed exactly once: %q", out)
	}
	if strings.Index(out, "private reasoning") > strings.Index(out, "visible answer") {
		t.Fatalf("buffered thought reordered after answer: %q", out)
	}
}

func TestPassNonStreamingGemini_ThoughtOnlyStopFailsOverBeforeWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(
			`{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"private reasoning","thought":true}]},"finishReason":"STOP"}]}}`,
		)),
	}

	svc := &AntigravityNativeGatewayService{}
	result, err := svc.passNonStreamingGemini(
		context.Background(), c, 39, resp, time.Now(),
		"gemini-3.6-flash", "gemini-3.6-flash-high", toolPrepReport{},
	)
	if result != nil {
		t.Fatalf("thought-only STOP result = %#v, want nil", result)
	}
	var failoverErr *UpstreamFailoverError
	if !errors.As(err, &failoverErr) {
		t.Fatalf("thought-only STOP error = %v, want UpstreamFailoverError", err)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("non-streaming thought-only STOP committed %d client bytes: %q", rec.Body.Len(), rec.Body.String())
	}
}

func TestStreamGeminiToClient_SafetyFinishPassesThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(
			"data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"SAFETY\"}]}}\n\n",
		)),
	}

	svc := &AntigravityNativeGatewayService{}
	result, err := svc.streamGeminiToClient(
		context.Background(), c, 39, resp, time.Now(),
		"gemini-3.6-flash", "gemini-3.6-flash-high", toolPrepReport{},
	)
	if err != nil || result == nil {
		t.Fatalf("SAFETY finish result=%#v err=%v, want passthrough success", result, err)
	}
	if !strings.Contains(rec.Body.String(), `"finishReason":"SAFETY"`) {
		t.Fatalf("SAFETY finish was not passed through: %q", rec.Body.String())
	}
}

func TestStreamGeminiToClient_UnterminatedFinalAnswerIsFlushed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(
			`data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"final answer"}]},"finishReason":"STOP"}]}}`,
		)),
	}

	svc := &AntigravityNativeGatewayService{}
	result, err := svc.streamGeminiToClient(
		context.Background(), c, 39, resp, time.Now(),
		"gemini-3.6-flash", "gemini-3.6-flash-high", toolPrepReport{},
	)
	if err != nil || result == nil {
		t.Fatalf("unterminated answer result=%#v err=%v", result, err)
	}
	if strings.Count(rec.Body.String(), "final answer") != 1 {
		t.Fatalf("unterminated final chunk not flushed exactly once: %q", rec.Body.String())
	}
}

func TestStreamGeminiToClient_MissingFinishSynthesizesOther(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(
			"data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"answer\"}]}}]}}\n\n",
		)),
	}

	svc := &AntigravityNativeGatewayService{}
	result, err := svc.streamGeminiToClient(
		context.Background(), c, 39, resp, time.Now(),
		"gemini-3.6-flash", "gemini-3.6-flash-high", toolPrepReport{},
	)
	if err != nil || result == nil {
		t.Fatalf("missing-finish stream result=%#v err=%v", result, err)
	}
	if !strings.Contains(rec.Body.String(), `"finishReason":"OTHER"`) {
		t.Fatalf("missing-finish stream lacks synthetic OTHER terminator: %q", rec.Body.String())
	}
}
