package handler

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const (
	semanticEmptyCaptureDirEnv      = "SEMANTIC_EMPTY_CAPTURE_DIR"
	semanticEmptyCaptureMaxFilesEnv = "SEMANTIC_EMPTY_CAPTURE_MAX_FILES"
	semanticEmptyCaptureMaxBytesEnv = "SEMANTIC_EMPTY_CAPTURE_MAX_BYTES"

	defaultSemanticEmptyCaptureMaxFiles = 100
	defaultSemanticEmptyCaptureMaxBytes = 4 << 20
	semanticEmptyCaptureContextKey      = "semantic_empty_request_captured"
)

var semanticEmptyCaptureMu sync.Mutex

func semanticEmptyCaptureEnabled() bool {
	return strings.TrimSpace(os.Getenv(semanticEmptyCaptureDirEnv)) != ""
}

type semanticEmptyCaptureBundle struct {
	Version                           int       `json:"version"`
	CapturedAt                        time.Time `json:"captured_at"`
	RequestID                         string    `json:"request_id,omitempty"`
	ClientRequestID                   string    `json:"client_request_id,omitempty"`
	RequestPath                       string    `json:"request_path,omitempty"`
	Model                             string    `json:"model"`
	Action                            string    `json:"action"`
	Stream                            bool      `json:"stream"`
	AccountID                         int64     `json:"account_id"`
	FailureKind                       string    `json:"failure_kind"`
	UpstreamStatus                    int       `json:"upstream_status"`
	RequestBodyBytes                  int       `json:"request_body_bytes"`
	CapturedBodyBytes                 int       `json:"captured_body_bytes"`
	RequestBodySHA256                 string    `json:"request_body_sha256"`
	RequestBodyTruncated              bool      `json:"request_body_truncated"`
	RequestBody                       []byte    `json:"request_body_base64"`
	UpstreamErrorBodyBytes            int       `json:"upstream_error_body_bytes"`
	CapturedUpstreamErrorBodyBytes    int       `json:"captured_upstream_error_body_bytes"`
	UpstreamErrorBodyTruncated        bool      `json:"upstream_error_body_truncated"`
	UpstreamErrorBody                 []byte    `json:"upstream_error_body_base64,omitempty"`
	UpstreamResponseBodyBytes         int       `json:"upstream_response_body_bytes"`
	CapturedUpstreamResponseBodyBytes int       `json:"captured_upstream_response_body_bytes"`
	UpstreamResponseBodyTruncated     bool      `json:"upstream_response_body_truncated"`
	UpstreamResponseBody              []byte    `json:"upstream_response_body_base64,omitempty"`
}

// captureSemanticEmptyRequest persists one exact inbound Gemini body per client
// request when semantic-empty failover first fires. Capture is opt-in through
// SEMANTIC_EMPTY_CAPTURE_DIR because prompts may contain sensitive user data.
// Files are gzip-compressed JSON, created mode 0600 inside a mode 0700 directory.
func captureSemanticEmptyRequest(
	c *gin.Context,
	body []byte,
	model string,
	action string,
	stream bool,
	accountID int64,
	failoverErr *service.UpstreamFailoverError,
) {
	if c == nil || failoverErr == nil || failoverErr.Kind != service.FailoverKindSemanticEmpty {
		return
	}
	dir := strings.TrimSpace(os.Getenv(semanticEmptyCaptureDirEnv))
	if dir == "" {
		return
	}
	if captured, exists := c.Get(semanticEmptyCaptureContextKey); exists {
		if done, _ := captured.(bool); done {
			return
		}
	}

	ctx := context.Background()
	requestPath := ""
	if c.Request != nil {
		ctx = c.Request.Context()
		if c.Request.URL != nil {
			requestPath = c.Request.URL.Path
		}
	}
	requestID, _ := ctx.Value(ctxkey.RequestID).(string)
	clientRequestID, _ := ctx.Value(ctxkey.ClientRequestID).(string)

	maxBytes := positiveEnvInt(semanticEmptyCaptureMaxBytesEnv, defaultSemanticEmptyCaptureMaxBytes)
	remainingBytes := maxBytes
	capturedBody, truncated := consumeCaptureBytes(body, &remainingBytes)
	capturedUpstreamError, upstreamErrorTruncated := consumeCaptureBytes(failoverErr.ResponseBody, &remainingBytes)
	capturedUpstreamResponse, upstreamResponseTruncated := consumeCaptureBytes(failoverErr.DiagnosticResponseBody, &remainingBytes)
	digest := sha256.Sum256(body)
	bundle := semanticEmptyCaptureBundle{
		Version:                           1,
		CapturedAt:                        time.Now().UTC(),
		RequestID:                         strings.TrimSpace(requestID),
		ClientRequestID:                   strings.TrimSpace(clientRequestID),
		RequestPath:                       requestPath,
		Model:                             model,
		Action:                            action,
		Stream:                            stream,
		AccountID:                         accountID,
		FailureKind:                       failoverErr.Kind.String(),
		UpstreamStatus:                    failoverErr.StatusCode,
		RequestBodyBytes:                  len(body),
		CapturedBodyBytes:                 len(capturedBody),
		RequestBodySHA256:                 hex.EncodeToString(digest[:]),
		RequestBodyTruncated:              truncated,
		RequestBody:                       append([]byte(nil), capturedBody...),
		UpstreamErrorBodyBytes:            len(failoverErr.ResponseBody),
		CapturedUpstreamErrorBodyBytes:    len(capturedUpstreamError),
		UpstreamErrorBodyTruncated:        upstreamErrorTruncated,
		UpstreamErrorBody:                 append([]byte(nil), capturedUpstreamError...),
		UpstreamResponseBodyBytes:         len(failoverErr.DiagnosticResponseBody),
		CapturedUpstreamResponseBodyBytes: len(capturedUpstreamResponse),
		UpstreamResponseBodyTruncated:     upstreamResponseTruncated,
		UpstreamResponseBody:              append([]byte(nil), capturedUpstreamResponse...),
	}

	if err := writeSemanticEmptyCapture(ctx, dir, bundle); err != nil {
		logger.FromContext(ctx).Error("gateway.semantic_empty_capture_failed", zap.Error(err))
		return
	}
	c.Set(semanticEmptyCaptureContextKey, true)
}

func writeSemanticEmptyCapture(ctx context.Context, dir string, bundle semanticEmptyCaptureBundle) error {
	semanticEmptyCaptureMu.Lock()
	defer semanticEmptyCaptureMu.Unlock()

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create capture directory: %w", err)
	}
	dirInfo, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("inspect capture directory: %w", err)
	}
	if dirInfo.Mode()&os.ModeSymlink != 0 || !dirInfo.IsDir() {
		return fmt.Errorf("capture path must be a real directory")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("secure capture directory: %w", err)
	}

	file, err := os.CreateTemp(dir, ".semantic-empty-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary capture file: %w", err)
	}
	tempPath := file.Name()
	keepTemp := false
	defer func() {
		if !keepTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("secure temporary capture file: %w", err)
	}

	gzipWriter := gzip.NewWriter(file)
	encodeErr := json.NewEncoder(gzipWriter).Encode(bundle)
	gzipErr := gzipWriter.Close()
	closeErr := file.Close()
	if encodeErr != nil || gzipErr != nil || closeErr != nil {
		switch {
		case encodeErr != nil:
			return fmt.Errorf("encode capture: %w", encodeErr)
		case gzipErr != nil:
			return fmt.Errorf("compress capture: %w", gzipErr)
		default:
			return fmt.Errorf("close capture: %w", closeErr)
		}
	}

	requestPart := sanitizeCaptureFilename(bundle.RequestID)
	if requestPart == "" {
		if len(bundle.RequestBodySHA256) >= 12 {
			requestPart = bundle.RequestBodySHA256[:12]
		} else {
			requestPart = "unknown"
		}
	}
	randomPart := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(tempPath), ".semantic-empty-"), ".tmp")
	filename := fmt.Sprintf("semantic-empty-%s-%s-%s.json.gz", bundle.CapturedAt.Format("20060102T150405.000000000Z"), requestPart, randomPart)
	path := filepath.Join(dir, filename)
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publish capture file: %w", err)
	}
	keepTemp = true
	if err := pruneSemanticEmptyCaptures(dir, positiveEnvInt(semanticEmptyCaptureMaxFilesEnv, defaultSemanticEmptyCaptureMaxFiles)); err != nil {
		logger.FromContext(ctx).Warn("gateway.semantic_empty_capture_prune_failed", zap.Error(err))
	}
	logger.FromContext(ctx).Warn("gateway.semantic_empty_request_captured",
		zap.String("capture_file", path),
		zap.String("request_id", bundle.RequestID),
		zap.Int("request_body_bytes", bundle.RequestBodyBytes),
		zap.Bool("request_body_truncated", bundle.RequestBodyTruncated),
		zap.String("request_body_sha256", bundle.RequestBodySHA256),
	)
	return nil
}

func pruneSemanticEmptyCaptures(dir string, maxFiles int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	captures := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "semantic-empty-") && strings.HasSuffix(entry.Name(), ".json.gz") {
			captures = append(captures, entry)
		}
	}
	sort.Slice(captures, func(i, j int) bool { return captures[i].Name() < captures[j].Name() })
	for len(captures) > maxFiles {
		if err := os.Remove(filepath.Join(dir, captures[0].Name())); err != nil {
			return err
		}
		captures = captures[1:]
	}
	return nil
}

func consumeCaptureBytes(data []byte, remaining *int) ([]byte, bool) {
	if len(data) <= *remaining {
		*remaining -= len(data)
		return data, false
	}
	captured := data[:*remaining]
	*remaining = 0
	return captured, true
}

func positiveEnvInt(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func sanitizeCaptureFilename(value string) string {
	value = strings.TrimSpace(value)
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
