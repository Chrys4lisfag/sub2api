// Server-side chat history logger for diagnostic purposes.
//
// Captures, per request, the full upstream Gemini-format request body +
// the aggregated response body, along with metadata (account, model,
// discovery mode, tool calls observed, agy_list_tools iterations,
// timing). Writes are async (buffered channel → single writer goroutine)
// so request hot path is never blocked.
//
// Storage: one JSONL file per UTC day under `path`. Files are rolled
// when:
//   - day changes (UTC midnight)
//   - current file exceeds 50 MiB uncompressed
//
// On rotation the just-closed file is gzipped in place
// (`YYYY-MM-DD.jsonl` → `YYYY-MM-DD.jsonl.gz`). A background sweeper
// runs every hour AND on every rotation, summing on-disk sizes and
// deleting the oldest gzipped files until the total fits under the
// configured `maxBytes` cap (hard ceiling — defaults to 500 MiB).
//
// Sensitive headers (Authorization, x-goog-api-key) and OAuth/refresh
// tokens are redacted before serialization.
//
// Per-account opt-out is supported via the credential
// `chat_history_enabled: false` so privacy-sensitive accounts can skip
// logging individually even when the global toggle is on.
package service

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// ChatHistoryEntry is one row in the JSONL log.
//
// `Request` / `Response` are the parsed JSON objects (we deep-copy to
// redact secrets before serialization). When the response is streamed
// SSE, we aggregate all candidate parts into a single synthetic
// `{candidates:[{content:{parts:[...]}}], usageMetadata:{...}}` shape so
// downstream analysis can treat streamed + non-streamed identically.
type ChatHistoryEntry struct {
	Timestamp              time.Time      `json:"ts"`
	RequestID              string         `json:"request_id,omitempty"`
	AccountID              int64          `json:"account_id"`
	Platform               string         `json:"platform"`
	Model                  string         `json:"model"`
	WireModel              string         `json:"wire_model,omitempty"`
	Stream                 bool           `json:"stream"`
	DiscoveryMode          string         `json:"discovery_mode,omitempty"`
	AggregatorName         string         `json:"aggregator_name,omitempty"`
	ClientIP               string         `json:"client_ip,omitempty"`
	Request                map[string]any `json:"request,omitempty"`
	Response               map[string]any `json:"response,omitempty"`
	ToolCallsSeen          []string       `json:"tool_calls_seen,omitempty"`
	AgyListToolsIterations int            `json:"agy_list_tools_iterations,omitempty"`
	DurationMs             int64          `json:"duration_ms,omitempty"`
	FirstTokenMs           int64          `json:"first_token_ms,omitempty"`
	Error                  string         `json:"error,omitempty"`
}

// ChatHistoryLogService writes ChatHistoryEntry rows to daily-rolled
// JSONL files with a global size cap.
type ChatHistoryLogService struct {
	// Static config (set at construction).
	path     string
	maxBytes int64

	// Live state from settings (changes via SetEnabled / SetMaxBytes).
	enabled        atomic.Bool
	maxBytesAtomic atomic.Int64

	// Async ingest.
	ch   chan ChatHistoryEntry
	stop chan struct{}
	done chan struct{}

	// File state — owned by the single writer goroutine; no external
	// lock needed.
	curFile     *os.File
	curDay      string // "2026-06-02" UTC
	curBytes    int64
	dropped     atomic.Uint64
	writeErrors atomic.Uint64
}

// NewChatHistoryLogService builds the service in a NOT-YET-STARTED
// state. Call Start(ctx) to launch the writer goroutine + background
// sweeper.
//
// `path` is the storage directory (created if missing).
// `maxBytes` is the total on-disk size cap (hard ceiling). Pass 0 to
// disable the cap (NOT recommended).
func NewChatHistoryLogService(path string, maxBytes int64, enabled bool) *ChatHistoryLogService {
	s := &ChatHistoryLogService{
		path:     path,
		maxBytes: maxBytes,
		ch:       make(chan ChatHistoryEntry, 200),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	s.enabled.Store(enabled)
	s.maxBytesAtomic.Store(maxBytes)
	return s
}

// SetEnabled toggles logging at runtime (called from settings update).
// While disabled, Log() is a no-op; in-flight buffered entries still
// drain to disk.
func (s *ChatHistoryLogService) SetEnabled(v bool) {
	if s == nil {
		return
	}
	s.enabled.Store(v)
}

// SetMaxBytes updates the size cap at runtime.
func (s *ChatHistoryLogService) SetMaxBytes(v int64) {
	if s == nil || v < 0 {
		return
	}
	s.maxBytesAtomic.Store(v)
}

// IsEnabled returns the live enabled state. Hot-path check in gateway —
// avoid building entries if disabled.
func (s *ChatHistoryLogService) IsEnabled() bool {
	if s == nil {
		return false
	}
	return s.enabled.Load()
}

// Start launches the background writer + size-sweeper. Idempotent: a
// second call is a no-op.
func (s *ChatHistoryLogService) Start(ctx context.Context) {
	if s == nil {
		return
	}
	if err := os.MkdirAll(s.path, 0o755); err != nil {
		slog.Warn("chat history: mkdir failed", "path", s.path, "err", err)
		return
	}
	go s.run(ctx)
}

// Stop gracefully drains the queue and closes the current file. Safe to
// call multiple times.
func (s *ChatHistoryLogService) Stop() {
	if s == nil {
		return
	}
	select {
	case <-s.stop:
		return
	default:
		close(s.stop)
	}
	<-s.done
}

// Log enqueues an entry for async write. Non-blocking: if the channel
// is full (writer goroutine behind), the entry is dropped and a
// dropped-counter is incremented. Never blocks the calling request.
func (s *ChatHistoryLogService) Log(entry ChatHistoryEntry) {
	if s == nil || !s.enabled.Load() {
		return
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}
	// Redact in place before enqueue so the writer goroutine doesn't
	// re-walk the payload.
	redactChatHistoryEntry(&entry)
	select {
	case s.ch <- entry:
	default:
		s.dropped.Add(1)
	}
}

// DroppedCount returns the number of entries dropped due to backpressure.
// Surface via metrics or a debug endpoint if useful.
func (s *ChatHistoryLogService) DroppedCount() uint64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
}

// WriteErrorCount returns the number of write failures (disk full, IO
// errors, etc.).
func (s *ChatHistoryLogService) WriteErrorCount() uint64 {
	if s == nil {
		return 0
	}
	return s.writeErrors.Load()
}

// ---------------------------------------------------------------------------
// internals: writer goroutine, rotation, eviction
// ---------------------------------------------------------------------------

func (s *ChatHistoryLogService) run(ctx context.Context) {
	defer close(s.done)
	defer s.closeCurrent()
	sweepTicker := time.NewTicker(1 * time.Hour)
	defer sweepTicker.Stop()
	// Sweep once on startup to enforce cap after restart.
	s.sweepIfOverCap()

	for {
		select {
		case <-s.stop:
			// Drain remaining entries best-effort.
			for {
				select {
				case e := <-s.ch:
					s.writeEntry(e)
				default:
					return
				}
			}
		case <-ctx.Done():
			return
		case e := <-s.ch:
			s.writeEntry(e)
		case <-sweepTicker.C:
			s.sweepIfOverCap()
		}
	}
}

func (s *ChatHistoryLogService) writeEntry(e ChatHistoryEntry) {
	day := e.Timestamp.UTC().Format("2006-01-02")
	if err := s.rollIfNeeded(day); err != nil {
		s.writeErrors.Add(1)
		slog.Warn("chat history: roll failed", "err", err)
		return
	}
	if s.curFile == nil {
		return
	}
	buf, err := json.Marshal(e)
	if err != nil {
		s.writeErrors.Add(1)
		slog.Warn("chat history: marshal failed", "err", err)
		return
	}
	buf = append(buf, '\n')
	n, err := s.curFile.Write(buf)
	if err != nil {
		s.writeErrors.Add(1)
		slog.Warn("chat history: write failed", "err", err)
		return
	}
	s.curBytes += int64(n)
	if s.curBytes >= 50*1024*1024 {
		// File reached the per-file roll threshold — close + gzip +
		// sweep + open fresh.
		if err := s.closeAndGzipCurrent(); err != nil {
			slog.Warn("chat history: close/gzip failed", "err", err)
		}
		s.sweepIfOverCap()
	}
}

func (s *ChatHistoryLogService) rollIfNeeded(day string) error {
	if s.curFile != nil && s.curDay == day {
		return nil
	}
	// Day changed (or first write) — close current then open new.
	if s.curFile != nil {
		if err := s.closeAndGzipCurrent(); err != nil {
			return err
		}
		s.sweepIfOverCap()
	}
	path := filepath.Join(s.path, day+".jsonl")
	// Open in append mode so re-opens after restart preserve data for
	// the same day.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat %s: %w", path, err)
	}
	s.curFile = f
	s.curDay = day
	s.curBytes = st.Size()
	return nil
}

func (s *ChatHistoryLogService) closeCurrent() {
	if s.curFile == nil {
		return
	}
	if err := s.closeAndGzipCurrent(); err != nil {
		slog.Warn("chat history: shutdown close/gzip failed", "err", err)
	}
}

// closeAndGzipCurrent closes the current open file then re-opens it for
// reading, streams it through gzip into `.jsonl.gz`, and removes the
// original `.jsonl`. Resets curFile/curDay state.
func (s *ChatHistoryLogService) closeAndGzipCurrent() error {
	if s.curFile == nil {
		return nil
	}
	plainPath := s.curFile.Name()
	if err := s.curFile.Close(); err != nil {
		s.curFile = nil
		s.curDay = ""
		s.curBytes = 0
		return fmt.Errorf("close %s: %w", plainPath, err)
	}
	s.curFile = nil
	s.curDay = ""
	s.curBytes = 0

	// Skip gzip when the file is empty (avoid littering empty .gz files
	// on rapid roll/restart cycles).
	if st, err := os.Stat(plainPath); err == nil && st.Size() == 0 {
		_ = os.Remove(plainPath)
		return nil
	}

	src, err := os.Open(plainPath)
	if err != nil {
		return fmt.Errorf("reopen %s: %w", plainPath, err)
	}
	defer func() { _ = src.Close() }()

	gzPath := plainPath + ".gz.tmp"
	dst, err := os.Create(gzPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", gzPath, err)
	}
	gw := gzip.NewWriter(dst)
	if _, err := io.Copy(gw, src); err != nil {
		_ = gw.Close()
		_ = dst.Close()
		_ = os.Remove(gzPath)
		return fmt.Errorf("gzip %s: %w", plainPath, err)
	}
	if err := gw.Close(); err != nil {
		_ = dst.Close()
		_ = os.Remove(gzPath)
		return fmt.Errorf("gzip close %s: %w", plainPath, err)
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(gzPath)
		return fmt.Errorf("close %s: %w", gzPath, err)
	}
	finalPath := plainPath + ".gz"
	if err := os.Rename(gzPath, finalPath); err != nil {
		_ = os.Remove(gzPath)
		return fmt.Errorf("rename %s -> %s: %w", gzPath, finalPath, err)
	}
	if err := os.Remove(plainPath); err != nil {
		slog.Warn("chat history: remove plain after gzip failed", "path", plainPath, "err", err)
	}
	return nil
}

// sweepIfOverCap totals the on-disk size of *.jsonl.gz under `path`. If
// total exceeds maxBytes, deletes oldest gzipped files until under the
// cap. The currently-open .jsonl is excluded from the total (its size
// is bounded by the per-file roll threshold).
func (s *ChatHistoryLogService) sweepIfOverCap() {
	cap := s.maxBytesAtomic.Load()
	if cap <= 0 {
		return
	}
	entries, err := os.ReadDir(s.path)
	if err != nil {
		slog.Warn("chat history: sweep readdir failed", "err", err)
		return
	}
	type fileInfo struct {
		name string
		size int64
		mod  time.Time
	}
	var gz []fileInfo
	var total int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl.gz") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		gz = append(gz, fileInfo{name: e.Name(), size: info.Size(), mod: info.ModTime()})
		total += info.Size()
	}
	if total <= cap {
		return
	}
	// Sort by modtime ascending (oldest first).
	sort.Slice(gz, func(i, j int) bool { return gz[i].mod.Before(gz[j].mod) })
	deleted := 0
	for _, f := range gz {
		if total <= cap {
			break
		}
		full := filepath.Join(s.path, f.name)
		if err := os.Remove(full); err != nil {
			slog.Warn("chat history: sweep remove failed", "path", full, "err", err)
			continue
		}
		total -= f.size
		deleted++
	}
	if deleted > 0 {
		slog.Info("chat history: swept oldest files",
			"deleted", deleted, "remaining_bytes", total, "cap_bytes", cap)
	}
}

// ---------------------------------------------------------------------------
// redaction
// ---------------------------------------------------------------------------

var redactedKeys = map[string]bool{
	"authorization":       true,
	"x-goog-api-key":      true,
	"x-goog-user-project": true,
	"access_token":        true,
	"refresh_token":       true,
	"id_token":            true,
	"client_secret":       true,
	"api_key":             true,
	"password":            true,
	"private_key":         true,
	"passphrase":          true,
}

// redactChatHistoryEntry walks the Request/Response objects in place,
// replacing any value whose key matches a sensitive name with the
// literal string "[REDACTED]". Case-insensitive.
func redactChatHistoryEntry(e *ChatHistoryEntry) {
	if e == nil {
		return
	}
	redactWalk(e.Request)
	redactWalk(e.Response)
}

func redactWalk(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if redactedKeys[strings.ToLower(k)] {
				t[k] = "[REDACTED]"
				continue
			}
			redactWalk(val)
		}
	case []any:
		for _, item := range t {
			redactWalk(item)
		}
	}
}

// ---------------------------------------------------------------------------
// helpers for the gateway integration layer
// ---------------------------------------------------------------------------

// AccountAllowsChatHistory returns true unless the account's credentials
// contain `chat_history_enabled: false`. Caller should still gate on
// the global toggle (IsEnabled) — this is the per-account override
// check only.
func AccountAllowsChatHistory(account *Account) bool {
	if account == nil {
		return true
	}
	if v, ok := account.Credentials["chat_history_enabled"].(bool); ok {
		return v
	}
	if s, ok := account.Credentials["chat_history_enabled"].(string); ok {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "false", "off", "0", "no":
			return false
		case "true", "on", "1", "yes":
			return true
		}
	}
	return true
}

// extractToolCallNamesFromResponse walks a Gemini-format response body
// (single candidates structure) and returns every functionCall name
// observed in part order. Used to populate ChatHistoryEntry.ToolCallsSeen
// for fast post-hoc analytics.
func extractToolCallNamesFromResponse(resp map[string]any) []string {
	if resp == nil {
		return nil
	}
	var names []string
	cands, _ := resp["candidates"].([]any)
	for _, c := range cands {
		cm, _ := c.(map[string]any)
		if cm == nil {
			continue
		}
		content, _ := cm["content"].(map[string]any)
		if content == nil {
			continue
		}
		parts, _ := content["parts"].([]any)
		for _, p := range parts {
			pm, _ := p.(map[string]any)
			if pm == nil {
				continue
			}
			fc, _ := pm["functionCall"].(map[string]any)
			if fc == nil {
				continue
			}
			if name, ok := fc["name"].(string); ok && name != "" {
				names = append(names, name)
			}
		}
	}
	return names
}
