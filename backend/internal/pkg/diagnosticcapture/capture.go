package diagnosticcapture

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
)

const (
	captureDirEnv      = "ANTIGRAVITY_AB_CAPTURE_DIR"
	captureMaxFilesEnv = "ANTIGRAVITY_AB_CAPTURE_MAX_FILES"
	captureMaxBytesEnv = "ANTIGRAVITY_AB_CAPTURE_MAX_BYTES"

	defaultCaptureMaxFiles = 100
	defaultCaptureMaxBytes = 1 << 20
)

var captureMu sync.Mutex

// Record contains one upstream attempt. Bodies are redacted before serialization.
type Record struct {
	Route             string
	RequestPath       string
	Model             string
	WireModel         string
	Action            string
	Stream            bool
	AccountID         int64
	Outcome           string
	OutboundRequest   []byte
	UpstreamResponse  []byte
	ConvertedResponse []byte
}

type artifact struct {
	Bytes     int             `json:"bytes"`
	SHA256    string          `json:"sha256"`
	JSONValid bool            `json:"json_valid"`
	Truncated bool            `json:"redacted_content_omitted"`
	Content   json.RawMessage `json:"redacted_json,omitempty"`
}

type bundle struct {
	Version             int       `json:"version"`
	CapturedAt          time.Time `json:"captured_at"`
	RequestIDHash       string    `json:"request_id_sha256,omitempty"`
	ClientRequestIDHash string    `json:"client_request_id_sha256,omitempty"`
	Route               string    `json:"route"`
	RequestPath         string    `json:"request_path,omitempty"`
	Model               string    `json:"model"`
	WireModel           string    `json:"wire_model,omitempty"`
	Action              string    `json:"action"`
	Stream              bool      `json:"stream"`
	AccountID           int64     `json:"account_id"`
	Outcome             string    `json:"outcome"`
	OutboundRequest     artifact  `json:"outbound_request"`
	UpstreamResponse    artifact  `json:"upstream_response"`
	ConvertedResponse   artifact  `json:"converted_response"`
}

func Enabled() bool {
	return strings.TrimSpace(os.Getenv(captureDirEnv)) != ""
}

func Capture(ctx context.Context, record Record) error {
	dir := strings.TrimSpace(os.Getenv(captureDirEnv))
	if dir == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	requestID, _ := ctx.Value(ctxkey.RequestID).(string)
	clientRequestID, _ := ctx.Value(ctxkey.ClientRequestID).(string)
	remaining := positiveEnvInt(captureMaxBytesEnv, defaultCaptureMaxBytes)
	captured := bundle{
		Version:             1,
		CapturedAt:          time.Now().UTC(),
		RequestIDHash:       hashString(requestID),
		ClientRequestIDHash: hashString(clientRequestID),
		Route:               record.Route,
		RequestPath:         record.RequestPath,
		Model:               record.Model,
		WireModel:           record.WireModel,
		Action:              record.Action,
		Stream:              record.Stream,
		AccountID:           record.AccountID,
		Outcome:             record.Outcome,
		OutboundRequest:     buildArtifact(record.OutboundRequest, &remaining),
		UpstreamResponse:    buildArtifact(record.UpstreamResponse, &remaining),
		ConvertedResponse:   buildArtifact(record.ConvertedResponse, &remaining),
	}
	return writeBundle(dir, captured)
}

func buildArtifact(raw []byte, remaining *int) artifact {
	digest := sha256.Sum256(raw)
	out := artifact{Bytes: len(raw), SHA256: hex.EncodeToString(digest[:])}
	if len(raw) == 0 {
		out.JSONValid = true
		out.Content = json.RawMessage("null")
		return out
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return out
	}
	out.JSONValid = true
	redacted, err := json.Marshal(redactValue(value, ""))
	if err != nil {
		out.JSONValid = false
		return out
	}
	if len(redacted) > *remaining {
		out.Truncated = true
		*remaining = 0
		return out
	}
	*remaining -= len(redacted)
	out.Content = redacted
	return out
}

func redactValue(value any, parentKey string) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			normalized := normalizeKey(key)
			if redactWholeField(normalized, normalizeKey(parentKey)) {
				out[key] = redactedSummary(child)
				continue
			}
			out[key] = redactValue(child, key)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, child := range typed {
			out[i] = redactValue(child, parentKey)
		}
		return out
	case string:
		if preserveStringField(normalizeKey(parentKey)) {
			return typed
		}
		return redactedSummary(typed)
	default:
		return typed
	}
}

func preserveStringField(key string) bool {
	switch key {
	case "name", "type", "role", "finishreason", "mode", "status", "action", "model",
		"wiremodel", "requesttype", "mimetype", "required", "allowedfunctionnames", "propertyordering":
		return true
	default:
		return false
	}
}

func redactWholeField(key, parent string) bool {
	switch key {
	case "text", "description", "prompt", "thought", "reasoning", "thoughtsignature",
		"authorization", "accesstoken", "refreshtoken", "apikey", "token", "password",
		"credential", "credentials", "secret", "args", "arguments", "data", "uri", "url":
		return true
	}
	if strings.HasSuffix(key, "token") || strings.HasSuffix(key, "secret") || strings.HasSuffix(key, "password") {
		return true
	}
	if key == "response" && (parent == "functionresponse" || parent == "toolresponse") {
		return true
	}
	if key == "output" && (parent == "functionresponse" || parent == "toolresponse") {
		return true
	}
	if strings.HasSuffix(key, "id") {
		return true
	}
	return false
}

func redactedSummary(value any) map[string]any {
	raw, _ := json.Marshal(value)
	digest := sha256.Sum256(raw)
	return map[string]any{
		"redacted": true,
		"bytes":    len(raw),
		"sha256":   hex.EncodeToString(digest[:]),
	}
}

func normalizeKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	key = strings.NewReplacer("_", "", "-", "", ".", "").Replace(key)
	return key
}

func writeBundle(dir string, captured bundle) error {
	captureMu.Lock()
	defer captureMu.Unlock()

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create capture directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("inspect capture directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("capture path must be a real directory")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("secure capture directory: %w", err)
	}

	file, err := os.CreateTemp(dir, ".antigravity-ab-*.tmp")
	if err != nil {
		return fmt.Errorf("create capture file: %w", err)
	}
	tempPath := file.Name()
	published := false
	defer func() {
		if !published {
			_ = os.Remove(tempPath)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("secure capture file: %w", err)
	}
	gzipWriter := gzip.NewWriter(file)
	encodeErr := json.NewEncoder(gzipWriter).Encode(captured)
	gzipErr := gzipWriter.Close()
	closeErr := file.Close()
	if encodeErr != nil {
		return fmt.Errorf("encode capture: %w", encodeErr)
	}
	if gzipErr != nil {
		return fmt.Errorf("compress capture: %w", gzipErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close capture: %w", closeErr)
	}

	randomPart := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(tempPath), ".antigravity-ab-"), ".tmp")
	name := fmt.Sprintf("antigravity-ab-%s-%s.json.gz", captured.CapturedAt.Format("20060102T150405.000000000Z"), randomPart)
	if err := os.Rename(tempPath, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("publish capture: %w", err)
	}
	published = true
	return prune(dir, positiveEnvInt(captureMaxFilesEnv, defaultCaptureMaxFiles))
}

func prune(dir string, maxFiles int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	captures := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "antigravity-ab-") && strings.HasSuffix(entry.Name(), ".json.gz") {
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

func positiveEnvInt(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func hashString(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
