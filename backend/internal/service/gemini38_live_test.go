package service

import (
	"bufio"
	"bytes"
	"encoding/json"
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

func TestLiveGemini38VirtualAliasSlider(t *testing.T) {
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
	require.NoError(t, err, "failed to parse provider URL")
	require.Equal(t, "https", strings.ToLower(endpoint.Scheme), "provider URL must use HTTPS")
	allowedHosts := map[string]bool{
		"cloudcode-pa.googleapis.com":                  true,
		"daily-cloudcode-pa.googleapis.com":            true,
		"daily-cloudcode-pa.sandbox.googleapis.com":    true,
		"autopush-cloudcode-pa.sandbox.googleapis.com": true,
	}
	require.True(t, allowedHosts[strings.ToLower(endpoint.Hostname())], "provider URL host is not allowlisted")
	require.Equal(t, "/v1internal:streamGenerateContent", endpoint.Path, "provider URL path is not allowed")
	endpoint.RawQuery = "alt=sse"
	endpoint.Fragment = ""
	endpoint.User = nil
	safeProviderURL := endpoint.String()

	cases := []struct {
		name       string
		level      string
		wantWire   string
		wantBudget int64
	}{
		{name: "low", level: "low", wantWire: "gemini-3.8-flash-low", wantBudget: 1000},
		{name: "medium", level: "medium", wantWire: "gemini-3.8-flash-medium", wantBudget: 4000},
		{name: "high", level: "high", wantWire: "gemini-3.8-flash-high", wantBudget: -1},
		{name: "absent-defaults-medium", wantWire: "gemini-3.8-flash-medium", wantBudget: 4000},
	}

	client := &http.Client{Timeout: 60 * time.Second}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bareBody := []byte(`{"contents":[{"role":"user","parts":[{"text":"Hello"}]}]}`)
			if tc.level != "" {
				bareBody = []byte(`{"contents":[{"role":"user","parts":[{"text":"Hello"}]}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"` + tc.level + `"}}}`)
			}

			wireModel := antigravity.ResolveWireFromBody("gemini-3.8-flash", bareBody)
			require.Equal(t, tc.wantWire, wireModel)
			bareBody = antigravity.NormalizeNumericBudgetTierBody(bareBody, wireModel)
			require.Equal(t, tc.wantBudget, gjson.GetBytes(bareBody, "generationConfig.thinkingConfig.thinkingBudget").Int())
			require.True(t, gjson.GetBytes(bareBody, "generationConfig.thinkingConfig.includeThoughts").Bool())
			require.False(t, gjson.GetBytes(bareBody, "generationConfig.thinkingConfig.thinkingLevel").Exists())

			wrapped, err := wrapNativeV1Internal(creds.Project, wireModel, bareBody)
			require.NoError(t, err, "failed to wrap request")

			modelInEnvelope := gjson.GetBytes(wrapped, "model").String()
			require.Equal(t, tc.wantWire, modelInEnvelope)

			budgetInEnvelope := gjson.GetBytes(wrapped, "request.generationConfig.thinkingConfig.thinkingBudget").Int()
			require.Equal(t, tc.wantBudget, budgetInEnvelope)

			includeThoughts := gjson.GetBytes(wrapped, "request.generationConfig.thinkingConfig.includeThoughts").Bool()
			require.True(t, includeThoughts)
			require.False(t, gjson.GetBytes(wrapped, "request.generationConfig.thinkingConfig.thinkingLevel").Exists())

			req, err := http.NewRequest(http.MethodPost, safeProviderURL, bytes.NewReader(wrapped))
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
				t.Fatalf("provider request failed for level %s: %T", tc.name, err)
			}
			defer resp.Body.Close()

			// Log tier, wire model, and status only — never log credentials, project, prompt, or response content.
			t.Logf("level=%s wire=%s status=%d", tc.name, wireModel, resp.StatusCode)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			scanner := bufio.NewScanner(resp.Body)
			hasText := false
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "data: ") {
					dataStr := strings.TrimPrefix(line, "data: ")
					if dataStr == "[DONE]" {
						continue
					}
					if gjson.Valid(dataStr) {
						parts := gjson.Get(dataStr, "response.candidates.0.content.parts")
						if parts.Exists() && len(parts.Array()) > 0 {
							for _, part := range parts.Array() {
								if text := part.Get("text").String(); strings.TrimSpace(text) != "" {
									hasText = true
									break
								}
							}
						}
					}
				}
				if hasText {
					break
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatalf("provider stream read failed for level %s: %T", tc.name, err)
			}
			require.True(t, hasText, "expected at least one non-empty SSE text part")
		})
	}
}
