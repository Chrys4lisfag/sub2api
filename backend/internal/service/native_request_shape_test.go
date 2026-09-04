package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// Probe matrix recorded live on 2026-09-04 against the real upstream:
//
//	unsigned functionCall mid-history (163 parts)      -> HTTP 200
//	signed-first + unsigned-second parallel call        -> HTTP 200
//	trailing model turn, unsigned functionCall          -> 400 "missing a thought_signature"
//	trailing model turn, signed functionCall            -> 400 "ending with a model turn"
//	trailing model turn, text only                      -> 400 "ending with a model turn"
//
// So the trailing model turn is the fault and a missing signature is only how
// upstream reports it. These tests pin that detection.
func TestGeminiRequestEndsWithModelTurn(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		want      bool
		wantShape string
	}{
		{
			name: "trailing model turn with unsigned function call",
			body: `{"contents":[
				{"role":"user","parts":[{"text":"read a"}]},
				{"role":"model","parts":[{"functionCall":{"name":"default_api:read","args":{"path":"a"}}}]}
			]}`,
			want:      true,
			wantShape: "functionCalls=1 signed=0",
		},
		{
			name: "trailing model turn with signed function call",
			body: `{"contents":[
				{"role":"user","parts":[{"text":"read a"}]},
				{"role":"model","parts":[{"functionCall":{"name":"default_api:read","args":{}},"thoughtSignature":"sig"}]}
			]}`,
			want:      true,
			wantShape: "functionCalls=1 signed=1",
		},
		{
			name:      "trailing model turn with text only",
			body:      `{"contents":[{"role":"user","parts":[{"text":"hi"}]},{"role":"model","parts":[{"text":"hello"}]}]}`,
			want:      true,
			wantShape: "texts=1",
		},
		{
			name:      "trailing assistant alias",
			body:      `{"contents":[{"role":"user","parts":[{"text":"hi"}]},{"role":"assistant","parts":[{"text":"hello"}]}]}`,
			want:      true,
			wantShape: "texts=1",
		},
		{
			name: "well-formed: ends with functionResponse",
			body: `{"contents":[
				{"role":"user","parts":[{"text":"read a"}]},
				{"role":"model","parts":[{"functionCall":{"name":"default_api:read","args":{}}}]},
				{"role":"user","parts":[{"functionResponse":{"name":"default_api:read","response":{"result":"x"}}}]}
			]}`,
			want: false,
		},
		{
			name: "wrapped v1internal envelope is inspected too",
			body: `{"model":"gemini-3.8-flash","request":{"contents":[
				{"role":"user","parts":[{"text":"hi"}]},
				{"role":"model","parts":[{"functionCall":{"name":"read","args":{}}}]}
			]}}`,
			want:      true,
			wantShape: "functionCalls=1 signed=0",
		},
		{name: "empty body", body: "", want: false},
		{name: "invalid json", body: "{", want: false},
		{name: "no contents", body: `{"generationConfig":{}}`, want: false},
		{name: "empty contents", body: `{"contents":[]}`, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, shape := geminiRequestEndsWithModelTurn([]byte(tc.body))
			if got != tc.want {
				t.Fatalf("endsWithModelTurn = %v, want %v", got, tc.want)
			}
			if tc.wantShape != "" && shape != tc.wantShape {
				t.Fatalf("shape = %q, want %q", shape, tc.wantShape)
			}
		})
	}
}

// The message must name the real problem AND pre-empt the misleading upstream
// signature error, since that is what users will search for.
func TestTrailingModelTurnMessageIsActionable(t *testing.T) {
	msg := nativeTrailingModelTurnMessage
	for _, want := range []string{"model/assistant turn", "functionResponse", "thought_signature"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message missing %q: %s", want, msg)
		}
	}
}

// The shape summary is for logs: it must never carry text, arguments or
// signature material.
func TestTurnShapeSummaryLeaksNothing(t *testing.T) {
	body := `{"contents":[{"role":"model","parts":[
		{"text":"SECRET-TEXT","thought":true},
		{"functionCall":{"name":"read","args":{"path":"SECRET-PATH"}},"thoughtSignature":"SECRET-SIG"}
	]}]}`
	_, shape := geminiRequestEndsWithModelTurn([]byte(body))
	for _, leak := range []string{"SECRET-TEXT", "SECRET-PATH", "SECRET-SIG"} {
		if strings.Contains(shape, leak) {
			t.Fatalf("shape summary leaked %q: %s", leak, shape)
		}
	}
	if !strings.Contains(shape, "functionCalls=1 signed=1") || !strings.Contains(shape, "thoughts=1") {
		t.Fatalf("shape summary lost structure: %s", shape)
	}
}

// Regression guard for the production panic on 2026-09-04: both protocol error
// writers MUST return a non-nil error. Handlers treat a nil error as success
// and dereference the (nil) *ForwardResult while recording usage, which
// panicked the usage worker at gemini_v1beta_handler.go:598.
func TestProtocolErrorWritersReturnNonNilError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("gemini", func(t *testing.T) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/x", nil)

		err := writeGeminiProtocolError(c, http.StatusBadRequest, "INVALID_ARGUMENT", nativeTrailingModelTurnMessage)
		if err == nil {
			t.Fatal("writeGeminiProtocolError returned nil error; handlers would deref a nil result")
		}
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
		if got := gjson.Get(rec.Body.String(), "error.status").String(); got != "INVALID_ARGUMENT" {
			t.Fatalf("error.status = %q: %s", got, rec.Body.String())
		}
		if got := gjson.Get(rec.Body.String(), "error.message").String(); !strings.Contains(got, "model/assistant turn") {
			t.Fatalf("message not propagated: %s", rec.Body.String())
		}
	})

	t.Run("claude", func(t *testing.T) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/x", nil)

		err := writeClaudeProtocolError(c, http.StatusBadRequest, "invalid_request_error", "boom")
		if err == nil {
			t.Fatal("writeClaudeProtocolError returned nil error; handlers would deref a nil result")
		}
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
		if got := gjson.Get(rec.Body.String(), "error.type").String(); got != "invalid_request_error" {
			t.Fatalf("error.type = %q: %s", got, rec.Body.String())
		}
	})
}
