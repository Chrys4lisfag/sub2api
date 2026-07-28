package handler

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestCaptureSemanticEmptyRequest_WritesOneCompressedExactBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dir := t.TempDir()
	t.Setenv(semanticEmptyCaptureDirEnv, dir)
	t.Setenv(semanticEmptyCaptureMaxFilesEnv, "10")
	t.Setenv(semanticEmptyCaptureMaxBytesEnv, "4194304")

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "req-capture-123")
	ctx = context.WithValue(ctx, ctxkey.ClientRequestID, "client-capture-456")
	c.Request = httptest.NewRequest("POST", "/antigravity-native/v1beta/models/gemini-3.6-flash:streamGenerateContent", nil).WithContext(ctx)

	body := []byte(`{
  "contents": [{"role": "user", "parts": [{"text": "specific <prompt>"}]}],
  "generationConfig": {"thinkingConfig": {"thinkingLevel": "high"}}
}`)
	failoverErr := &service.UpstreamFailoverError{
		StatusCode:             502,
		ResponseBody:           []byte(`{"error":{"code":502,"message":"STOP without usable content"}}`),
		Kind:                   service.FailoverKindSemanticEmpty,
		DiagnosticResponseBody: []byte(`data: {"candidates":[{"finishReason":"STOP"}]}`),
	}

	captureSemanticEmptyRequest(c, body, "gemini-3.6-flash", "streamGenerateContent", true, 42, failoverErr)
	captureSemanticEmptyRequest(c, body, "gemini-3.6-flash", "streamGenerateContent", true, 43, failoverErr)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "same client request must be captured only once")
	require.Contains(t, entries[0].Name(), "req-capture-123")
	require.Equal(t, ".gz", filepath.Ext(entries[0].Name()))

	file, err := os.Open(filepath.Join(dir, entries[0].Name()))
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer reader.Close()

	var bundle semanticEmptyCaptureBundle
	require.NoError(t, json.NewDecoder(reader).Decode(&bundle))
	require.Equal(t, "req-capture-123", bundle.RequestID)
	require.Equal(t, "client-capture-456", bundle.ClientRequestID)
	require.Equal(t, int64(42), bundle.AccountID)
	require.Equal(t, "semantic_empty", bundle.FailureKind)
	require.Equal(t, len(body), bundle.RequestBodyBytes)
	require.Equal(t, len(body), bundle.CapturedBodyBytes)
	require.False(t, bundle.RequestBodyTruncated)
	require.Equal(t, body, bundle.RequestBody)
	require.Equal(t, failoverErr.ResponseBody, bundle.UpstreamErrorBody)
	require.Equal(t, failoverErr.DiagnosticResponseBody, bundle.UpstreamResponseBody)

	if runtime.GOOS != "windows" {
		info, err := entries[0].Info()
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		dirInfo, err := os.Stat(dir)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
	}
}

func TestCaptureSemanticEmptyRequest_RespectsByteCap(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dir := t.TempDir()
	t.Setenv(semanticEmptyCaptureDirEnv, dir)
	t.Setenv(semanticEmptyCaptureMaxBytesEnv, "8")

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1beta/models/model:streamGenerateContent", nil)
	body := []byte(`{"long":"request body"}`)
	captureSemanticEmptyRequest(c, body, "model", "streamGenerateContent", true, 1, &service.UpstreamFailoverError{
		StatusCode: 502,
		Kind:       service.FailoverKindSemanticEmpty,
	})

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	file, err := os.Open(filepath.Join(dir, entries[0].Name()))
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer reader.Close()
	var bundle semanticEmptyCaptureBundle
	require.NoError(t, json.NewDecoder(reader).Decode(&bundle))
	require.True(t, bundle.RequestBodyTruncated)
	require.Equal(t, 8, bundle.CapturedBodyBytes)
	require.Equal(t, body[:8], bundle.RequestBody)
}

func TestCaptureSemanticEmptyRequest_BoundsAllBodiesAndAllowsRetryAfterWriteFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	parent := t.TempDir()
	dir := filepath.Join(parent, "captures")
	require.NoError(t, os.WriteFile(dir, []byte("not a directory"), 0o600))
	t.Setenv(semanticEmptyCaptureDirEnv, dir)
	t.Setenv(semanticEmptyCaptureMaxBytesEnv, "12")

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	body := []byte(`{}`)
	failoverErr := &service.UpstreamFailoverError{
		StatusCode:             502,
		ResponseBody:           []byte(`{"e":1}`),
		Kind:                   service.FailoverKindSemanticEmpty,
		DiagnosticResponseBody: []byte("abcdef"),
	}

	captureSemanticEmptyRequest(c, body, "model", "streamGenerateContent", true, 1, failoverErr)
	_, captured := c.Get(semanticEmptyCaptureContextKey)
	require.False(t, captured, "failed write must not suppress a later capture attempt")

	require.NoError(t, os.Remove(dir))
	require.NoError(t, os.Mkdir(dir, 0o700))
	captureSemanticEmptyRequest(c, body, "model", "streamGenerateContent", true, 2, failoverErr)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, ".gz", filepath.Ext(entries[0].Name()))

	file, err := os.Open(filepath.Join(dir, entries[0].Name()))
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer reader.Close()
	var bundle semanticEmptyCaptureBundle
	require.NoError(t, json.NewDecoder(reader).Decode(&bundle))
	require.Equal(t, len(failoverErr.DiagnosticResponseBody), bundle.UpstreamResponseBodyBytes)
	require.Equal(t, 3, bundle.CapturedUpstreamResponseBodyBytes)
	require.True(t, bundle.UpstreamResponseBodyTruncated)
	require.Equal(t, []byte("abc"), bundle.UpstreamResponseBody)
}

func TestPruneSemanticEmptyCaptures_KeepsNewest(t *testing.T) {
	dir := t.TempDir()
	for i := range 3 {
		bundle := semanticEmptyCaptureBundle{
			Version:           1,
			CapturedAt:        time.Date(2026, 7, 28, 17, 0, i, 0, time.UTC),
			RequestID:         "req-" + string(rune('a'+i)),
			RequestBodySHA256: "0123456789abcdef",
			RequestBody:       []byte(`{}`),
		}
		require.NoError(t, writeSemanticEmptyCapture(context.Background(), dir, bundle))
	}
	require.NoError(t, pruneSemanticEmptyCaptures(dir, 2))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.NotContains(t, entries[0].Name(), "req-a")
}
