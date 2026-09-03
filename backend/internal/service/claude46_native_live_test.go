package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
)

// TestLiveClaude46NativeEnvelope exercises the real native Claude request
// path — Anthropic body -> legacy pure translator -> inner request ->
// preprocessNativeBody with tool injection OFF -> wrapNativeV1Internal ->
// real cloudcode-pa POST — and asserts the serialized envelope matches the
// hash-bound agy.exe capture (maxOutputTokens 64000, thinkingBudget 1024,
// includeThoughts true, no toolConfig) and that the provider answers with
// non-empty Gemini-shaped SSE text.
//
// Gated on SUB2API_AGY_LIVE_CREDENTIALS_FILE; the credential file is expected
// to live only in an ACL-protected private directory and is never logged.
func TestLiveClaude46NativeEnvelope(t *testing.T) {
	credFile := os.Getenv("SUB2API_AGY_LIVE_CREDENTIALS_FILE")
	if credFile == "" {
		t.Skip("skipping live test: SUB2API_AGY_LIVE_CREDENTIALS_FILE environment variable is not set")
	}

	data, err := os.ReadFile(credFile)
	require.NoError(t, err, "failed to read live credentials file")

	var creds liveCredentials
	require.NoError(t, json.Unmarshal(data, &creds), "failed to parse live credentials JSON")
	require.True(t, strings.HasPrefix(creds.Authorization, "Bearer "), "credentials authorization must be bearer")
	require.NotEmpty(t, creds.Project, "credentials project missing")
	require.NotEmpty(t, creds.URL, "credentials url missing")

	endpoint, err := url.Parse(creds.URL)
	require.NoError(t, err)
	require.Equal(t, "https", strings.ToLower(endpoint.Scheme))
	allowedHosts := map[string]bool{
		"cloudcode-pa.googleapis.com":                  true,
		"daily-cloudcode-pa.googleapis.com":            true,
		"daily-cloudcode-pa.sandbox.googleapis.com":    true,
		"autopush-cloudcode-pa.sandbox.googleapis.com": true,
	}
	require.True(t, allowedHosts[strings.ToLower(endpoint.Hostname())], "provider URL host is not allowlisted")
	require.Equal(t, "/v1internal:streamGenerateContent", endpoint.Path)
	endpoint.RawQuery = "alt=sse"
	endpoint.Fragment = ""
	endpoint.User = nil
	safeProviderURL := endpoint.String()

	client := &http.Client{Timeout: 90 * time.Second}

	cases := []struct {
		model string
		// maxTokens is the Anthropic max_tokens the client sends. "" omits it.
		maxTokens string
		// wantMaxOutputTokens is the value that must reach the wire.
		wantMaxOutputTokens int64
		// wantThinkingBudget is the synthesized budget: the captured 1024
		// when it fits under the caller's ceiling, else 0 (thinking off) so
		// upstream's `max_tokens > thinking.budget_tokens` rule holds.
		wantThinkingBudget  int64
		wantIncludeThoughts bool
	}{
		// Caller ceiling equals the captured budget: thinking must be turned
		// off rather than 400 or silently raising the caller's ceiling.
		{model: "claude-sonnet-4-6", maxTokens: `"max_tokens":1024,`, wantMaxOutputTokens: 1024, wantThinkingBudget: 0, wantIncludeThoughts: false},
		// Caller value above the captured Claude ceiling is clamped to 64000.
		{model: "claude-sonnet-4-6", maxTokens: `"max_tokens":200000,`, wantMaxOutputTokens: 64000, wantThinkingBudget: 1024, wantIncludeThoughts: true},
		// No caller value -> synthesize the captured agy.exe defaults.
		{model: "claude-opus-4-6-thinking", maxTokens: "", wantMaxOutputTokens: 64000, wantThinkingBudget: 1024, wantIncludeThoughts: true},
	}

	for _, tc := range cases {
		model := tc.model
		t.Run(model+"/"+strings.TrimSuffix(strings.TrimPrefix(tc.maxTokens, `"max_tokens":`), ",")+"default", func(t *testing.T) {
			claudeBody := []byte(`{"model":"` + model + `",` + tc.maxTokens + `"stream":true,` +
				`"system":"You are a helpful assistant.",` +
				`"messages":[{"role":"user","content":"Reply with exactly: OK"}],` +
				`"tools":[{"name":"mcp__files_read","description":"read a file","input_schema":{"type":"object","properties":{"path":{"type":"string"}}}}]}`)

			var claudeReq antigravity.ClaudeRequest
			require.NoError(t, json.Unmarshal(claudeBody, &claudeReq))

			opts := antigravity.DefaultTransformOptions()
			opts.EnableIdentityPatch = true
			wrapped, err := antigravity.TransformClaudeToGeminiWithOptions(&claudeReq, creds.Project, model, opts)
			require.NoError(t, err)

			inner, err := extractNativeInnerRequest(wrapped)
			require.NoError(t, err)
			require.False(t, gjson.GetBytes(inner, "toolConfig").Exists(), "toolConfig must be stripped")

			// Tool injection OFF, exactly as the gateway does for Claude.
			inner, report, err := preprocessNativeBody(inner, false, "", "both", "single_name")
			require.NoError(t, err)
			require.False(t, report.AggregatorOn)

			var declared []string
			gjson.GetBytes(inner, "tools.0.functionDeclarations").ForEach(func(_, v gjson.Result) bool {
				declared = append(declared, v.Get("name").String())
				return true
			})
			require.Contains(t, declared, "mcp__files_read", "caller tool must survive")
			require.NotContains(t, declared, defaultMcpAggregatorName, "aggregator must not be injected")
			require.NotContains(t, declared, defaultListToolsName, "discovery tool must not be injected")

			envelope, err := wrapNativeV1Internal(creds.Project, model, inner)
			require.NoError(t, err)

			gc := "request.generationConfig"
			require.Equal(t, tc.wantMaxOutputTokens, gjson.GetBytes(envelope, gc+".maxOutputTokens").Int(),
				"maxOutputTokens must honor the caller and clamp at the captured Claude ceiling")
			require.Equal(t, tc.wantThinkingBudget, gjson.GetBytes(envelope, gc+".thinkingConfig.thinkingBudget").Int())
			require.Equal(t, tc.wantIncludeThoughts, gjson.GetBytes(envelope, gc+".thinkingConfig.includeThoughts").Bool())
			require.False(t, gjson.GetBytes(envelope, "request.toolConfig").Exists())
			require.Equal(t, model, gjson.GetBytes(envelope, "model").String())

			req, err := http.NewRequest(http.MethodPost, safeProviderURL, bytes.NewReader(envelope))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", creds.Authorization)
			if creds.UserAgent != "" {
				req.Header.Set("User-Agent", creds.UserAgent)
			} else {
				req.Header.Set("User-Agent", "antigravity")
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("provider request failed for %s: %T", model, err)
			}
			defer func() { _ = resp.Body.Close() }()

			// Log model + status only — never credentials, project, prompt or content.
			t.Logf("model=%s status=%d", model, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
				// Only the upstream error message/status enum is logged; it is
				// provider diagnostics, never request or response content.
				t.Logf("upstream_error status=%s message=%s",
					gjson.GetBytes(raw, "error.status").String(),
					gjson.GetBytes(raw, "error.message").String())
			}
			require.Equal(t, http.StatusOK, resp.StatusCode)

			scanner := bufio.NewScanner(resp.Body)
			scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
			hasText := false
			for scanner.Scan() {
				line := scanner.Text()
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				payload := strings.TrimPrefix(line, "data: ")
				if payload == "[DONE]" || !gjson.Valid(payload) {
					continue
				}
				parts := gjson.Get(payload, "response.candidates.0.content.parts")
				if !parts.Exists() {
					parts = gjson.Get(payload, "candidates.0.content.parts")
				}
				for _, part := range parts.Array() {
					if strings.TrimSpace(part.Get("text").String()) != "" {
						hasText = true
						break
					}
				}
				if hasText {
					break
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatalf("provider stream read failed for %s: %T", model, err)
			}
			require.True(t, hasText, "expected at least one non-empty SSE text part")
		})
	}
}
