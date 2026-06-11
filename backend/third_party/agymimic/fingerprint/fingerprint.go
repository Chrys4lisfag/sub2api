// Package fingerprint discovers the live agy.exe identity (version string,
// Go build version, SHA512) and refreshes it periodically so callers don't
// hardcode constants that drift each release.
//
// Wire:
//   - GET https://antigravity-cli-auto-updater-974169037036.us-central1.run.app/manifests/{platform}.json
//     → { "version": "1.0.3", "url": "https://.../cli_windows_x64.exe", "sha512": "..." }
//   - GET <url> → the actual agy binary
//   - debug/buildinfo.Read(binary) → BuildInfo.GoVersion (the long
//     "go1.27-20260427-RC04 cl/906595525 +5fb2392a6f X:boringcrypto,simd")
//
// The Refresher caches per-platform results, sha512-verifies every download,
// and only re-downloads when the manifest's version changes. Default poll
// interval is 6h, suitable for backend-side scheduling.
package fingerprint

import (
	"context"
	"crypto/sha512"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	I "github.com/koval/agymimic/internal"
)

// ManifestBaseURL is the canonical auto-updater base used by install.cmd /
// install.sh / install.ps1. Hardcoded by Google; not user-tunable.
const ManifestBaseURL = "https://antigravity-cli-auto-updater-974169037036.us-central1.run.app"

// Manifest mirrors the JSON shape served at /manifests/{platform}.json.
type Manifest struct {
	Version string `json:"version"`
	URL     string `json:"url"`
	SHA512  string `json:"sha512"`
}

// Snapshot is the resolved identity for one platform. All fields are
// observable by the Antigravity backend and should match real agy.exe.
type Snapshot struct {
	// Platform key (e.g. "windows_amd64", "darwin_arm64"). Drives manifest URL.
	Platform string `json:"platform"`

	// Version is the manifest version (e.g. "1.0.3"). Used for User-Agent
	// "antigravity/<version>" and feature-flag context.
	Version string `json:"version"`

	// GoVersion is the long Go build string extracted from the binary via
	// debug/buildinfo (e.g. "go1.27-20260427-RC04 cl/906595525 +5fb2392a6f
	// X:boringcrypto,simd"). Goes into Unleash metrics platformVersion.
	GoVersion string `json:"go_version"`

	// SHA512 of the downloaded binary, hex-lowercase. Cross-checked against
	// manifest before extraction; recorded so callers can prove they used
	// the same binary.
	SHA512 string `json:"sha512"`

	// FetchedAt is when this snapshot was produced.
	FetchedAt time.Time `json:"fetched_at"`

	// BinaryPath is the on-disk cache location (deterministic per platform
	// + version) if Refresher.CacheDir was set. Empty when no cache.
	BinaryPath string `json:"binary_path,omitempty"`
}

// Options configures the Refresher goroutine.
type Options struct {
	// Platform forces a specific {os}_{arch} key. Empty = use the current
	// process platform; the sub2api server typically wants the platform
	// of the upstream that the backend most strictly fingerprints, which
	// is darwin/arm64 because the cloudcode-pa UA validator rejects other
	// tails ("no longer supported"). Set to "darwin_arm64" in production.
	Platform string

	// Interval between manifest checks. Default 6h. Set to 0 to disable
	// background polling (one-shot Refresh only).
	Interval time.Duration

	// HTTPClient overrides the default. Use this to wire a per-account
	// proxy or a TLS-fingerprint client if you must look like an end-user
	// download.
	HTTPClient *http.Client

	// CacheDir is the on-disk directory where binaries are cached. If
	// empty, the binary is downloaded to a temp file, scanned, and
	// removed. With a CacheDir, re-runs at the same version skip the
	// download entirely.
	CacheDir string

	// OnUpdate fires when a new snapshot is published (manifest version
	// changed). Use this to invalidate User-Agent headers, regenerate
	// per-account metrics clients, etc.
	OnUpdate func(*Snapshot)

	// Logger is optional. If nil, log lines are silently dropped.
	Logger func(format string, args ...any)
}

// Refresher polls the manifest endpoint and exposes the latest snapshot.
// Safe for concurrent reads.
type Refresher struct {
	opts Options

	snap atomic.Pointer[Snapshot]

	mu     sync.Mutex
	cancel context.CancelFunc

	httpc *http.Client
}

// CurrentPlatform returns the {os}_{arch} key used by the manifest URLs.
// Maps Go GOOS/GOARCH to the upstream naming (windows/amd64 →
// windows_amd64, darwin/arm64 → darwin_arm64, linux/amd64 →
// linux_amd64). Architecture aliases are normalized.
func CurrentPlatform() string {
	os := runtime.GOOS
	arch := runtime.GOARCH
	switch arch {
	case "amd64", "x86_64":
		arch = "amd64"
	case "arm64", "aarch64":
		arch = "arm64"
	}
	return os + "_" + arch
}

// New constructs a Refresher. Run Start to begin background polling, or
// call Refresh manually for one-shot use.
func New(opts Options) *Refresher {
	if opts.Platform == "" {
		opts.Platform = CurrentPlatform()
	}
	if opts.Interval == 0 {
		opts.Interval = 6 * time.Hour
	}
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Minute}
	}
	if opts.Logger == nil {
		opts.Logger = func(string, ...any) {}
	}
	return &Refresher{opts: opts, httpc: hc}
}

// Start begins background polling. Returns immediately; runs until Stop.
// Calling Start twice is a no-op (second call returns without spawning).
func (r *Refresher) Start(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	go r.loop(ctx)
}

// Stop halts background polling. Subsequent Snapshot() calls still return
// the last cached value.
func (r *Refresher) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
}

func (r *Refresher) loop(ctx context.Context) {
	// Eager first refresh — block once so consumers can read a populated
	// snapshot immediately after Start returns.
	if s, err := r.Refresh(ctx); err == nil {
		r.opts.Logger("fingerprint: initial refresh ok version=%s go=%s", s.Version, s.GoVersion)
	} else {
		r.opts.Logger("fingerprint: initial refresh failed: %v", err)
	}
	t := time.NewTicker(r.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := r.Refresh(ctx); err != nil {
				r.opts.Logger("fingerprint: refresh failed: %v", err)
			}
		}
	}
}

// Snapshot returns the most recently resolved snapshot, or nil if Refresh
// has not yet succeeded. Reads are atomic and lock-free.
func (r *Refresher) Snapshot() *Snapshot {
	return r.snap.Load()
}

// Refresh fetches the current manifest, downloads the binary if the
// version changed (or no cache exists), extracts the Go build version,
// and atomically swaps in the new Snapshot. Returns the new snapshot.
func (r *Refresher) Refresh(ctx context.Context) (*Snapshot, error) {
	m, err := r.fetchManifest(ctx, r.opts.Platform)
	if err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	// Skip download if cached snapshot already matches this version.
	if prev := r.snap.Load(); prev != nil && prev.Version == m.Version && prev.GoVersion != "" {
		return prev, nil
	}
	binPath, sum, err := r.downloadBinary(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	if !strings.EqualFold(sum, m.SHA512) {
		// Best-effort cleanup before bailing.
		if r.opts.CacheDir == "" {
			_ = os.Remove(binPath)
		}
		return nil, fmt.Errorf("sha512 mismatch: manifest=%s got=%s", m.SHA512, sum)
	}
	bi, err := buildinfo.ReadFile(binPath)
	if err != nil {
		if r.opts.CacheDir == "" {
			_ = os.Remove(binPath)
		}
		return nil, fmt.Errorf("buildinfo: %w", err)
	}
	snap := &Snapshot{
		Platform:   r.opts.Platform,
		Version:    m.Version,
		GoVersion:  bi.GoVersion,
		SHA512:     sum,
		FetchedAt:  time.Now().UTC(),
		BinaryPath: binPath,
	}
	if r.opts.CacheDir == "" {
		_ = os.Remove(binPath)
		snap.BinaryPath = ""
	}
	r.snap.Store(snap)
	if r.opts.OnUpdate != nil {
		r.opts.OnUpdate(snap)
	}
	return snap, nil
}

func (r *Refresher) fetchManifest(ctx context.Context, platform string) (*Manifest, error) {
	url := ManifestBaseURL + "/manifests/" + platform + ".json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}
	var m Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	if m.Version == "" || m.URL == "" || m.SHA512 == "" {
		return nil, fmt.Errorf("manifest missing fields: %+v", m)
	}
	return &m, nil
}

// downloadBinary writes the binary to CacheDir (deterministic path) or to
// a temp file, returning the path plus the lowercase hex sha512 of the
// content. Callers verify against Manifest.SHA512.
func (r *Refresher) downloadBinary(ctx context.Context, m *Manifest) (string, string, error) {
	var path string
	if r.opts.CacheDir != "" {
		if err := os.MkdirAll(r.opts.CacheDir, 0o755); err != nil {
			return "", "", err
		}
		path = filepath.Join(r.opts.CacheDir, fmt.Sprintf("agy-%s-%s.bin", r.opts.Platform, m.Version))
		// Reuse cached binary if present + sha512 matches.
		if sum, err := sha512File(path); err == nil && strings.EqualFold(sum, m.SHA512) {
			return path, sum, nil
		}
	} else {
		f, err := os.CreateTemp("", "agy-fingerprint-*.bin")
		if err != nil {
			return "", "", err
		}
		path = f.Name()
		f.Close()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.URL, nil)
	if err != nil {
		return "", "", err
	}
	resp, err := r.httpc.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("download status %s", resp.Status)
	}
	out, err := os.Create(path)
	if err != nil {
		return "", "", err
	}
	h := sha512.New()
	if _, err := io.Copy(io.MultiWriter(out, h), resp.Body); err != nil {
		out.Close()
		return "", "", err
	}
	if err := out.Close(); err != nil {
		return "", "", err
	}
	return path, hex.EncodeToString(h.Sum(nil)), nil
}

func sha512File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha512.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Install starts a background Refresher and wires its updates into
// agymimic's internal version cache. After Install returns, every
// subsequent call to internal.LatestAntigravityVersion() /
// internal.LatestGoVersion() — and through them, every header and
// metrics payload mimic emits — reflects the live agy.exe identity.
//
// Returns the running Refresher so callers can Stop() it during shutdown
// and query Snapshot() for ad-hoc reads.
func Install(ctx context.Context, opts Options) *Refresher {
	prev := opts.OnUpdate
	opts.OnUpdate = func(s *Snapshot) {
		I.SetLiveFingerprint(s.Version, s.GoVersion)
		if prev != nil {
			prev(s)
		}
	}
	r := New(opts)
	// Wire the force-refresh hook so gateway code that detects upstream
	// "no longer supported" errors can trigger an out-of-cycle manifest
	// pull without coupling to the refresher type directly. Logs every
	// dispatch + outcome (success-with-version-change, success-unchanged,
	// failure) so operators can verify the refresh actually happened.
	I.SetForceRefreshFingerprint(func() {
		before := I.LatestAntigravityVersion()
		opts.Logger("fingerprint: FORCE refresh dispatched (current version=%s)", before)
		refreshCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := r.Refresh(refreshCtx)
		if err != nil {
			opts.Logger("fingerprint: FORCE refresh FAILED (kept version=%s): %v", before, err)
			return
		}
		if s.Version != before {
			opts.Logger("fingerprint: FORCE refresh ok — version %s → %s sha512=%s...", before, s.Version, firstN(s.SHA512, 12))
		} else {
			opts.Logger("fingerprint: FORCE refresh ok — version unchanged at %s (manifest reports same)", s.Version)
		}
	})

	// Eager SYNCHRONOUS initial refresh: block here until the manifest is
	// fetched (or fails). Previously the eager refresh ran inside the
	// background loop goroutine, which meant the first 100-300 ms of
	// requests after Install could still use the stale DefaultAntigravityVersion.
	// Bounded by a 30 s deadline so we don't hang process startup if the
	// manifest endpoint is unreachable; on timeout we keep the cached
	// default and let the background loop retry every Interval.
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if s, err := r.Refresh(initCtx); err == nil {
		opts.Logger("fingerprint: startup refresh ok version=%s go=%s sha512=%s...", s.Version, s.GoVersion, firstN(s.SHA512, 12))
	} else {
		opts.Logger("fingerprint: startup refresh FAILED (using fallback %s): %v", I.DefaultAntigravityVersion, err)
	}
	cancel()

	r.Start(ctx)
	return r
}

// firstN returns the first n chars of s, or s if shorter. Used to keep
// startup log lines compact.
func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// ForceRefresh asks the installed Refresher (the one returned by
// Install) to fetch a fresh manifest right now. Callers from outside
// the agymimic module use this when they detect that the upstream
// rejected the currently advertised Antigravity version — e.g. sub2api's
// native gateway sees `HTTP 400 "no longer supported"`. Re-entrant safe
// (the underlying refresher serializes via its mutex) and no-op when no
// Install has run yet. The refresh dispatches on a goroutine.
func ForceRefresh() {
	I.ForceRefreshFingerprint()
}
