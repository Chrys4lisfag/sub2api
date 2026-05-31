package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// AutoUpdaterReleasesURL — Cloud-Run service agy.exe polls for new builds.
// Returns JSON [{version, execution_id}, ...] sorted newest-first.
// Project number 974169037036 = Google's Antigravity-CLI updater project.
const AutoUpdaterReleasesURL = "https://antigravity-auto-updater-974169037036.us-central1.run.app/releases"

// DefaultGoVersion is the platformVersion advertised when no live fingerprint
// has been published. Captured from a real agy.exe build at the time of
// recording; fingerprint.Refresher overrides this in production via
// SetLiveFingerprint.
const DefaultGoVersion = "go1.27-20260427-RC04 cl/906595525 +5fb2392a6f X:boringcrypto,simd"

var (
	versionMu     sync.RWMutex
	cachedVersion = DefaultAntigravityVersion
	cachedGoVer   = DefaultGoVersion
	versionExpiry time.Time
)

// LatestAntigravityVersion returns the version string to advertise. If we've
// fetched the live latest from the auto-updater within the last 6h, returns
// that; otherwise returns DefaultAntigravityVersion.
func LatestAntigravityVersion() string {
	versionMu.RLock()
	defer versionMu.RUnlock()
	if cachedVersion != "" && time.Now().Before(versionExpiry) {
		return cachedVersion
	}
	return DefaultAntigravityVersion
}

// LatestGoVersion returns the Go build string to advertise in Unleash
// metrics platformVersion. Falls back to DefaultGoVersion when no live
// fingerprint has been published.
func LatestGoVersion() string {
	versionMu.RLock()
	defer versionMu.RUnlock()
	if cachedGoVer != "" {
		return cachedGoVer
	}
	return DefaultGoVersion
}

// SetLiveFingerprint atomically updates both the advertised Antigravity
// version and the Go build string. fingerprint.Refresher calls this on each
// successful manifest fetch. Either argument may be empty to leave that
// field at its current value.
func SetLiveFingerprint(version, goVersion string) {
	versionMu.Lock()
	defer versionMu.Unlock()
	if version != "" {
		cachedVersion = version
	}
	if goVersion != "" {
		cachedGoVer = goVersion
	}
	versionExpiry = time.Now().Add(24 * time.Hour)
}

// RefreshAntigravityVersion fetches the current Antigravity build from the
// public updater and caches it. Safe to call concurrently.
// Returns the resolved version (or fallback on error).
func RefreshAntigravityVersion(ctx context.Context) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, AutoUpdaterReleasesURL, nil)
	c := &http.Client{Timeout: 10 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return DefaultAntigravityVersion, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return DefaultAntigravityVersion, fmt.Errorf("updater %d", resp.StatusCode)
	}
	var list []struct {
		Version     string `json:"version"`
		ExecutionID string `json:"execution_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return DefaultAntigravityVersion, err
	}
	if len(list) == 0 || list[0].Version == "" {
		return DefaultAntigravityVersion, errors.New("empty releases list")
	}
	versionMu.Lock()
	cachedVersion = list[0].Version
	versionExpiry = time.Now().Add(6 * time.Hour)
	versionMu.Unlock()
	return cachedVersion, nil
}

// ---------------------------------------------------------------------------
// Force-refresh hook — lets the gateway layer signal "current advertised
// version is being rejected upstream; please refresh now". The fingerprint
// package wires the real refresher into this slot during fingerprint.Install;
// callers that detect "no longer supported" / similar errors call
// ForceRefreshFingerprint() and the refresher fetches the current manifest
// out-of-cycle, updates the cache, and the next request uses the new value.
//
// Defense in depth: even if Google rolls a new mandatory version BEFORE our
// 6 h poll wakes up, the first rejected request triggers a refresh and the
// retry succeeds.

var (
	forceRefreshMu sync.RWMutex
	forceRefreshFn func()
)

// SetForceRefreshFingerprint registers the callback the fingerprint
// refresher uses to perform an out-of-cycle manifest pull. Pass nil to
// clear. Subsequent calls overwrite. Safe for concurrent use.
func SetForceRefreshFingerprint(fn func()) {
	forceRefreshMu.Lock()
	forceRefreshFn = fn
	forceRefreshMu.Unlock()
}

// ForceRefreshFingerprint asks the registered refresher to fetch a fresh
// manifest immediately. No-op when no refresher is installed. The call
// dispatches the refresh on a goroutine so the caller (typically deep
// inside the gateway request path) doesn't block. Idempotent — multiple
// rapid calls coalesce because the refresher's mutex serializes them.
func ForceRefreshFingerprint() {
	forceRefreshMu.RLock()
	fn := forceRefreshFn
	forceRefreshMu.RUnlock()
	if fn != nil {
		go fn()
	}
}
