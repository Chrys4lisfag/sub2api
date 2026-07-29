package diagnosticcapture

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func TestCaptureStrictlyRedactsSensitiveBodies(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(captureDirEnv, dir)
	t.Setenv(captureMaxFilesEnv, "10")
	t.Setenv(captureMaxBytesEnv, "1048576")

	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "private-request-id")
	ctx = context.WithValue(ctx, ctxkey.ClientRequestID, "private-client-id")
	record := Record{
		Route:       "antigravity-native",
		RequestPath: "/antigravity-native/v1beta/models/gemini-3.6-flash-high:generateContent",
		Model:       "gemini-3.6-flash-high",
		WireModel:   "gemini-3.6-flash-high",
		Action:      "generateContent",
		AccountID:   42,
		Outcome:     "stop_without_content",
		OutboundRequest: []byte(`{
			"project":"private-project", "unknown_field":"private-unknown",
			"request":{"contents":[{"role":"user","parts":[{"text":"private prompt"}]}],
			"tools":[{"functionDeclarations":[{"name":"get_weather","description":"private description","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}]}],
			"toolConfig":{"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["get_weather"]}},
			"accessToken":"private-access-token"}}`),
		UpstreamResponse:  []byte(`{"response":{"candidates":[{"content":{"parts":[{"thought":true,"text":"private reasoning","thoughtSignature":"private-signature"},{"functionCall":{"name":"get_weather","args":{"city":"private city"}}}]},"finishReason":"MALFORMED_FUNCTION_CALL"}]}}`),
		ConvertedResponse: []byte(`{"candidates":[{"content":{"parts":[{"functionResponse":{"name":"get_weather","response":{"weather":"private result"}}}]},"finishReason":"STOP"}]}`),
	}
	require.NoError(t, Capture(ctx, record))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	file, err := os.Open(filepath.Join(dir, entries[0].Name()))
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer reader.Close()
	var captured bundle
	require.NoError(t, json.NewDecoder(reader).Decode(&captured))

	encoded, err := json.Marshal(captured)
	require.NoError(t, err)
	serialized := string(encoded)
	for _, secret := range []string{
		"private-request-id", "private-client-id", "private-project", "private-unknown",
		"private prompt", "private description", "private-access-token", "private reasoning",
		"private-signature", "private city", "private result",
	} {
		require.NotContains(t, serialized, secret)
	}
	require.Contains(t, serialized, "get_weather")
	require.Contains(t, serialized, `"mode":"ANY"`)
	require.Contains(t, serialized, `"finishReason":"MALFORMED_FUNCTION_CALL"`)
	require.NotEmpty(t, captured.RequestIDHash)
	require.NotEmpty(t, captured.OutboundRequest.SHA256)
	require.True(t, captured.OutboundRequest.JSONValid)

	if runtime.GOOS != "windows" {
		info, err := entries[0].Info()
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
}

func TestBuildArtifactInvalidJSONStoresOnlyDigest(t *testing.T) {
	remaining := 1024
	captured := buildArtifact([]byte("private malformed payload"), &remaining)
	require.False(t, captured.JSONValid)
	require.Empty(t, captured.Content)
	require.NotEmpty(t, captured.SHA256)
}

func TestBuildArtifactOmitsOversizeRedactedJSON(t *testing.T) {
	remaining := 8
	captured := buildArtifact([]byte(`{"text":"private prompt","mode":"ANY"}`), &remaining)
	require.True(t, captured.JSONValid)
	require.True(t, captured.Truncated)
	require.Empty(t, captured.Content)
	require.Equal(t, 0, remaining)
}

func TestCaptureDisabledWritesNothing(t *testing.T) {
	t.Setenv(captureDirEnv, "")
	require.False(t, Enabled())
	require.NoError(t, Capture(context.Background(), Record{OutboundRequest: []byte(`{"text":"private"}`)}))
}

func TestNormalizeKeyHandlesCredentialVariants(t *testing.T) {
	for _, key := range []string{"access_token", "Access-Token", "access.token"} {
		require.Equal(t, "accesstoken", normalizeKey(key))
	}
	require.True(t, strings.HasSuffix(normalizeKey("client_secret"), "secret"))
}
