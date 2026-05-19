package geminicli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

// Runtime-mutable wire fingerprint:
//
// The real Gemini CLI versions move every 2-3 weeks. Sticking to a hardcoded
// constant means our impersonation drifts from the latest accepted UA + once
// Google's classifier flags it we get 4xx with no clear signal.
//
// Default* values are the in-binary fallback when the auto-updater hasn't run
// yet (or has failed). The current value is held in an atomic.Pointer so the
// background updater can swap it without locking readers on the hot UA path.

const (
	// DefaultGeminiCLIVersion is the bundled fallback CLI version when the
	// auto-updater has not yet contacted npm. Bump alongside meaningful
	// upstream releases.
	DefaultGeminiCLIVersion = "0.42.0"

	// DefaultGoogleAPIClientHeader is the bundled fallback value for the
	// x-goog-api-client header. The trailing token reflects the Node runtime
	// the gemini-cli ships with; track via observed UA in real-world error
	// logs (see issue google-gemini/gemini-cli#26572).
	DefaultGoogleAPIClientHeader = "gl-node/24.14.0"

	// npmLatestURL points at npm's metadata endpoint for the current stable
	// gemini-cli release. Returns JSON with a top-level `version` field.
	npmLatestURL = "https://registry.npmjs.org/@google/gemini-cli/latest"

	// minAutoUpdateInterval is the hard floor on the auto-update poll
	// interval. Don't hammer npm.
	minAutoUpdateInterval = 30 * time.Minute

	// DefaultAutoUpdateInterval is the recommended poll cadence. Six hours
	// matches the sub2api fork-sync workflow; gives roughly four refreshes
	// per day.
	DefaultAutoUpdateInterval = 6 * time.Hour

	// autoUpdateHTTPTimeout bounds each individual npm fetch.
	autoUpdateHTTPTimeout = 20 * time.Second
)

// versionPattern restricts what we accept from npm to the semver MAJOR.MINOR.PATCH
// shape that the Gemini CLI publishes. Anything else (pre-release, build metadata,
// garbage) is rejected -- we'd rather stay on the previous good value than ship
// a UA Google won't recognize.
var versionPattern = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

// cliVersionPtr and apiClientHeaderPtr hold the live UA fingerprint. Reads are
// lock-free via atomic.Pointer. The auto-updater is the only writer.
var (
	cliVersionPtr      atomic.Pointer[string]
	apiClientHeaderPtr atomic.Pointer[string]
)

func init() {
	v := DefaultGeminiCLIVersion
	h := DefaultGoogleAPIClientHeader
	cliVersionPtr.Store(&v)
	apiClientHeaderPtr.Store(&h)
}

// GeminiCLIVersion returns the current Gemini CLI version string used in
// outbound User-Agent headers. Safe to call concurrently.
func GeminiCLIVersion() string {
	if p := cliVersionPtr.Load(); p != nil {
		return *p
	}
	return DefaultGeminiCLIVersion
}

// GoogleAPIClientHeader returns the current value for the x-goog-api-client
// header sent on Code Assist + OAuth token endpoint requests.
func GoogleAPIClientHeader() string {
	if p := apiClientHeaderPtr.Load(); p != nil {
		return *p
	}
	return DefaultGoogleAPIClientHeader
}

// SetGeminiCLIVersion overrides the live CLI version string. Rejects empty
// input and anything that does not look like a valid semver release tag.
// Returns true when the new value was accepted.
func SetGeminiCLIVersion(v string) bool {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" || !versionPattern.MatchString(trimmed) {
		return false
	}
	cliVersionPtr.Store(&trimmed)
	return true
}

// SetGoogleAPIClientHeader overrides the live x-goog-api-client header value.
// Rejects empty input.
func SetGoogleAPIClientHeader(v string) bool {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return false
	}
	apiClientHeaderPtr.Store(&trimmed)
	return true
}

// FetchLatestNpmVersion polls npm's `@google/gemini-cli/latest` metadata
// endpoint and returns the stable release version. Errors out instead of
// returning empty so callers can keep the previous good value on failure.
func FetchLatestNpmVersion(ctx context.Context) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, autoUpdateHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, npmLatestURL, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	// Identify the poller so npm operators can tell traffic apart from CLI
	// installs. Plain sub2api UA; nothing here pretends to be the real CLI.
	req.Header.Set("User-Agent", "sub2api-geminicli-updater/1.0")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("npm status %d: %s", resp.StatusCode, strings.TrimSpace(string(preview)))
	}

	var payload struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	if !versionPattern.MatchString(payload.Version) {
		return "", fmt.Errorf("unexpected version shape %q", payload.Version)
	}
	return payload.Version, nil
}

// StartVersionAutoUpdater launches a background goroutine that periodically
// fetches the latest gemini-cli version from npm and updates the live UA
// fingerprint. interval is clamped to minAutoUpdateInterval to avoid
// hammering npm. Honors ctx cancellation for clean shutdown.
//
// Safe to call multiple times -- each call spawns its own goroutine, but
// callers should only invoke once during app boot (typically from main.go
// after initializeApplication).
func StartVersionAutoUpdater(ctx context.Context, interval time.Duration) {
	if interval < minAutoUpdateInterval {
		interval = minAutoUpdateInterval
	}

	go func() {
		// Slight startup delay so we don't race with the rest of the boot
		// sequence (DB migrations, embed unpack, etc.). 20 seconds is long
		// enough for the HTTP listener to come up.
		startupDelay := time.NewTimer(20 * time.Second)
		select {
		case <-ctx.Done():
			startupDelay.Stop()
			return
		case <-startupDelay.C:
		}

		// Run one immediate poll, then settle into the periodic cadence.
		runPoll(ctx)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runPoll(ctx)
			}
		}
	}()
}

// runPoll performs a single npm fetch + version swap. Designed to be safe
// to call concurrently; the underlying atomic.Pointer handles writer races,
// though in practice only one updater goroutine exists per process.
func runPoll(ctx context.Context) {
	latest, err := FetchLatestNpmVersion(ctx)
	if err != nil {
		slog.Warn("geminicli auto-updater poll failed",
			slog.String("err", err.Error()),
			slog.String("kept_version", GeminiCLIVersion()),
		)
		return
	}
	current := GeminiCLIVersion()
	if latest == current {
		slog.Debug("geminicli auto-updater no change",
			slog.String("version", current),
		)
		return
	}
	if SetGeminiCLIVersion(latest) {
		slog.Info("geminicli auto-updater applied new version",
			slog.String("previous", current),
			slog.String("current", latest),
		)
	}
}
