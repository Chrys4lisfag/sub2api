package service

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func issue4812RequestBody(t *testing.T, rootType string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "redacted"}}}},
		"tools": []any{map[string]any{"functionDeclarations": []any{map[string]any{
			"name": "get_weather", "description": "redacted",
			"parameters": map[string]any{
				"type":       rootType,
				"properties": map[string]any{"city": map[string]any{"type": map[bool]string{true: "STRING", false: "string"}[rootType == "OBJECT"]}},
				"required":   []any{"city"},
			},
		}}}},
		"toolConfig": map[string]any{"functionCallingConfig": map[string]any{
			"mode": "ANY", "allowedFunctionNames": []any{"get_weather"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func assertIssue4812Envelope(t *testing.T, wrapped []byte, model string) {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal(wrapped, &envelope); err != nil {
		t.Fatal(err)
	}
	request := envelope["request"].(map[string]any)
	declaration := request["tools"].([]any)[0].(map[string]any)["functionDeclarations"].([]any)[0].(map[string]any)
	parameters := declaration["parameters"].(map[string]any)
	city := parameters["properties"].(map[string]any)["city"].(map[string]any)
	config := request["toolConfig"].(map[string]any)["functionCallingConfig"].(map[string]any)
	allowed := config["allowedFunctionNames"].([]any)
	if envelope["model"] != model {
		t.Fatalf("model changed: %v", envelope["model"])
	}
	if declaration["name"] != "get_weather" || parameters["type"] != "object" || city["type"] != "string" {
		t.Fatalf("declaration changed: name=%v root=%v city=%v", declaration["name"], parameters["type"], city["type"])
	}
	if config["mode"] != "ANY" || len(allowed) != 1 || allowed[0] != "get_weather" {
		t.Fatalf("function calling config changed: mode=%v allowed=%v", config["mode"], allowed)
	}
}

func TestIssue4812LegacyWirePreservesExactFunctionCallingConfig(t *testing.T) {
	for _, rootType := range []string{"object", "OBJECT"} {
		t.Run(rootType, func(t *testing.T) {
			cleaned, err := cleanGeminiRequest(issue4812RequestBody(t, rootType))
			if err != nil {
				t.Fatal(err)
			}
			wrapped, err := (&AntigravityGatewayService{}).wrapV1InternalRequest("redacted-project", "gemini-3.6-flash", cleaned)
			if err != nil {
				t.Fatal(err)
			}
			assertIssue4812Envelope(t, wrapped, "gemini-3.6-flash")
		})
	}
}

func TestIssue4812NativeWirePreservesExactFunctionCallingConfig(t *testing.T) {
	processed, _, err := preprocessNativeBody(issue4812RequestBody(t, "object"), false, "", "both", "single_name")
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := wrapNativeV1Internal("redacted-project", "gemini-3.6-flash-high", processed)
	if err != nil {
		t.Fatal(err)
	}
	assertIssue4812Envelope(t, wrapped, "gemini-3.6-flash-high")
}

func TestIssue4812LegacyCollectorCapturesSuccessAndMalformed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("ANTIGRAVITY_AB_CAPTURE_DIR", t.TempDir())
	for _, test := range []struct {
		finishReason string
		wantOutcome  string
	}{
		{finishReason: "STOP", wantOutcome: "success"},
		{finishReason: "MALFORMED_FUNCTION_CALL", wantOutcome: "malformed_function_call"},
	} {
		t.Run(test.finishReason, func(t *testing.T) {
			payload := `{"response":{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"get_weather","args":{"city":"redacted"}}}]},"finishReason":"` + test.finishReason + `"}]}}`
			resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("data: " + payload + "\n\n"))}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			result, err := (&AntigravityGatewayService{settingService: &SettingService{}}).handleGeminiStreamToNonStreaming(c, resp, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if result.diagnosticOutcome != test.wantOutcome {
				t.Fatalf("outcome=%q want=%q", result.diagnosticOutcome, test.wantOutcome)
			}
			if !strings.Contains(string(result.upstreamResponse), test.finishReason) || !strings.Contains(string(result.convertedResponse), `"name":"get_weather"`) {
				t.Fatalf("paired capture incomplete: upstream=%s converted=%s", result.upstreamResponse, result.convertedResponse)
			}
		})
	}
}

func TestIssue4812NativeCaptureClassifiesMalformedAndRedacts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dir := t.TempDir()
	t.Setenv("ANTIGRAVITY_AB_CAPTURE_DIR", dir)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/antigravity-native/v1beta/models/gemini-3.6-flash-high:generateContent", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(
			`{"response":{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"get_weather","args":{"city":"private-city"}}}]},"finishReason":"MALFORMED_FUNCTION_CALL"}]}}`,
		)),
	}
	outbound := []byte(`{"request":{"contents":[{"parts":[{"text":"private-prompt"}]}],"accessToken":"private-token","toolConfig":{"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["get_weather"]}}}}`)
	result, err := (&AntigravityNativeGatewayService{}).passNonStreamingGemini(
		c.Request.Context(), c, 39, resp, time.Now(),
		"gemini-3.6-flash-high", "gemini-3.6-flash-high", "generateContent", outbound, toolPrepReport{},
	)
	if err != nil || result == nil {
		t.Fatalf("result=%v err=%v", result, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("capture entries=%d err=%v", len(entries), err)
	}
	file, err := os.Open(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var captured map[string]any
	if err := json.NewDecoder(reader).Decode(&captured); err != nil {
		t.Fatal(err)
	}
	if captured["outcome"] != "malformed_function_call" {
		t.Fatalf("capture outcome=%v", captured["outcome"])
	}
	serialized, _ := json.Marshal(captured)
	for _, secret := range []string{"private-prompt", "private-token", "private-city"} {
		if strings.Contains(string(serialized), secret) {
			t.Fatalf("capture leaked %q", secret)
		}
	}
}
