// Package service — Native Antigravity account quality tracker.
//
// Contract (authoritative, package-level; no methods, no accessor):
//
//	RecordNativeAccountQuality(accountID int64, model string, latency time.Duration, semanticEmpty bool)
//	NativeAccountQualityPenalty(accountID int64, model string) float64  // [0,1]
//	NativeAccountLatencyEWMA(accountID int64, model string) time.Duration
//	ResetNativeAccountQualityForTest()
//
// # Design goals
//
//   - Bounded, thread-safe EWMA per (accountID, REQUESTED model) — the caller's
//     originally-requested model name is the key, wire-model (post
//     ResolveWireFromBody) is only ever attached as slog metadata by callers.
//     This preserves 3.6 / 3.1 identity through the tracker: `gemini-3.6-flash`
//     and `gemini-3.6-flash-high` are distinct EWMA rows even though they map
//     to the same wire model, because they are distinct REQUESTED models.
//   - Low-sample neutral: any read below `qualityLowSampleFloor` samples
//     returns the neutral value (penalty = 0, latency = 0). Fresh accounts
//     are never over-penalized on first requests.
//   - Bounded penalty: EWMA of semantic-empty rate is clamped to [0, 1].
//     Latency EWMA is clamped ≥ 0 and rounded to ms internally.
//   - No account disabling / downgrade / cache invalidation — the tracker
//     is passive. Only Record* mutates; Penalty/Latency are pure reads.
//     Consumers (scheduling, sticky-escape) apply their own thresholds and
//     own the decision, per the target contract.
//
// # Global state, on purpose
//
// Main ratified a synchronized package-global tracker: the two producers
// (Native gateway service instances during ForwardGemini) and the sole
// consumer (Distribution's scheduling code in gateway_scheduling.go) all
// resolve to the SAME process-wide EWMA state. A per-service tracker would
// diverge in a two-service test rig and defeat the acceptance test that
// verifies isolation across account IDs and models. Bounded LRU keeps
// memory pathological accounts from unbounded growth.
package service

import (
	"container/list"
	"context"
	"math"
	"sync"
	"time"
)

// Tunables. Deliberately unexported — consumers own their thresholds and
// compose against the raw penalty/latency values (parent's constraint).
const (
	// qualityDefaultCapacity is the max number of (accountID, model) rows
	// kept in the LRU. Excess rows evict the least-recently-updated key.
	// 4096 fits >= 1000 accounts × ~4 requested models each without any
	// eviction under normal operation.
	qualityDefaultCapacity = 4096

	// qualityAlpha is the EWMA smoothing factor. New samples contribute
	// alpha, prior state contributes (1-alpha). 0.2 means ~5 samples for a
	// step change to become visible — fast enough to catch a suddenly-bad
	// account but slow enough that a single fluke doesn't flip a stable one.
	qualityAlpha = 0.2

	// qualityLowSampleFloor is the neutrality floor: reads below this
	// sample count return 0 (neutral). Prevents cold-start bias on the
	// FIRST request to a fresh account/model pair.
	qualityLowSampleFloor = 5

	// qualityMaxPenalty caps the semantic-empty EWMA read. Contract range
	// is [0, 1]; we return no more than this even if repeated failures
	// would drive the EWMA higher (it can't — 1 is the arithmetic ceiling
	// when every sample is 1.0 — but the clamp is a defense-in-depth).
	qualityMaxPenalty = 1.0

	// qualityMaxLatency clamps individual latency samples fed into the
	// EWMA. Prevents a pathological upstream hang (e.g. TCP read stuck at
	// 10 min) from dominating the EWMA and hiding a subsequent recovery.
	qualityMaxLatency = 10 * time.Minute
)

// nativeQualityKey is the composite EWMA key. Value semantics — safe as a
// map key without allocation.
type nativeQualityKey struct {
	accountID int64
	model     string // caller's REQUESTED model, verbatim (no wire remap).
}

// nativeQualityEntry holds the EWMA state for one key + the LRU element
// pointing at it. `element.Value` aliases *nativeQualityEntry — we don't
// re-marshal on eviction.
type nativeQualityEntry struct {
	key           nativeQualityKey
	element       *list.Element
	semEmptyEWMA  float64 // in [0, 1]. 1.0 = every observed request was semantic-empty.
	latencyMsEWMA float64 // in ms, >= 0.
	samples       int
	updatedAt     time.Time
}

// nativeQualityTracker is the concrete store. All operations take the
// single mutex; reads are cheap enough that a separate RWMutex would only
// add contention on the LRU head-of-list update every access performs.
type nativeQualityTracker struct {
	mu      sync.Mutex
	entries map[nativeQualityKey]*nativeQualityEntry
	order   *list.List // list of *nativeQualityEntry; front = most recently updated
	cap     int
	alpha   float64
	minSamp int
	maxPen  float64
	nowFn   func() time.Time
}

func newNativeQualityTracker(capHint int, alpha float64, minSamp int, maxPen float64, now func() time.Time) *nativeQualityTracker {
	if capHint <= 0 {
		capHint = qualityDefaultCapacity
	}
	if alpha <= 0 || alpha > 1 {
		alpha = qualityAlpha
	}
	if minSamp <= 0 {
		minSamp = qualityLowSampleFloor
	}
	if maxPen <= 0 || maxPen > 1 {
		maxPen = qualityMaxPenalty
	}
	if now == nil {
		now = time.Now
	}
	return &nativeQualityTracker{
		entries: make(map[nativeQualityKey]*nativeQualityEntry, capHint),
		order:   list.New(),
		cap:     capHint,
		alpha:   alpha,
		minSamp: minSamp,
		maxPen:  maxPen,
		nowFn:   now,
	}
}

// record updates the EWMA for (accountID, model). semanticEmpty=true feeds
// a 1.0 sample into the semantic-empty EWMA; false feeds 0.0. Latency is
// always fed (clamped ≥ 0 and ≤ qualityMaxLatency) — the contract asks for
// end-to-end latency on all Native responses, semantic-empty included, so
// consumers can see that "empty" responses are also slow.
func (t *nativeQualityTracker) record(accountID int64, model string, latency time.Duration, semanticEmpty bool) {
	if t == nil {
		return
	}
	// Ignore records with an empty model key — the map still accepts an
	// empty string, but empty means the caller lost the requested model
	// somewhere and the sample is uncorrelatable. Better silent-drop than
	// polluting a shared "" row.
	if model == "" {
		return
	}

	// Clamp latency into [0, qualityMaxLatency]. Negative can happen if a
	// clock jumps or a caller passes a bad startTime; we don't want a
	// negative sample to pull the EWMA below zero.
	if latency < 0 {
		latency = 0
	} else if latency > qualityMaxLatency {
		latency = qualityMaxLatency
	}
	latencyMs := float64(latency.Milliseconds())

	var semSample float64
	if semanticEmpty {
		semSample = 1.0
	}

	key := nativeQualityKey{accountID: accountID, model: model}

	t.mu.Lock()
	defer t.mu.Unlock()

	entry, ok := t.entries[key]
	if !ok {
		entry = &nativeQualityEntry{
			key:           key,
			semEmptyEWMA:  semSample,
			latencyMsEWMA: latencyMs,
			samples:       1,
			updatedAt:     t.nowFn(),
		}
		entry.element = t.order.PushFront(entry)
		t.entries[key] = entry
		t.evictLocked()
		return
	}

	// Standard EWMA folding: new = α * sample + (1-α) * prior.
	entry.semEmptyEWMA = clampNativeQuality01(t.alpha*semSample + (1-t.alpha)*entry.semEmptyEWMA)
	entry.latencyMsEWMA = math.Max(0, t.alpha*latencyMs+(1-t.alpha)*entry.latencyMsEWMA)
	entry.samples++
	entry.updatedAt = t.nowFn()
	t.order.MoveToFront(entry.element)
}

// evictLocked drops the LRU tail until the map fits under cap. Called
// under t.mu.
func (t *nativeQualityTracker) evictLocked() {
	for t.order.Len() > t.cap {
		back := t.order.Back()
		if back == nil {
			return
		}
		victim := back.Value.(*nativeQualityEntry)
		t.order.Remove(back)
		delete(t.entries, victim.key)
	}
}

// penalty returns the semantic-empty EWMA clamped to [0, maxPen], or 0
// when the row hasn't reached the low-sample floor. Nil-safe.
func (t *nativeQualityTracker) penalty(accountID int64, model string) float64 {
	if t == nil || model == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[nativeQualityKey{accountID: accountID, model: model}]
	if !ok || entry.samples < t.minSamp {
		return 0
	}
	if entry.semEmptyEWMA <= 0 {
		return 0
	}
	if entry.semEmptyEWMA >= t.maxPen {
		return t.maxPen
	}
	return entry.semEmptyEWMA
}

// latencyEWMA returns the latency EWMA as a time.Duration, or 0 when the
// row is below the low-sample floor. Nil-safe.
func (t *nativeQualityTracker) latencyEWMA(accountID int64, model string) time.Duration {
	if t == nil || model == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[nativeQualityKey{accountID: accountID, model: model}]
	if !ok || entry.samples < t.minSamp {
		return 0
	}
	ms := entry.latencyMsEWMA
	if ms < 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// samplesFor is a test-only helper that returns the observed sample count
// for a key. Used by focused tests to prove RecordNativeAccountQuality
// actually landed (vs. testing only the eventual penalty).
func (t *nativeQualityTracker) samplesFor(accountID int64, model string) int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[nativeQualityKey{accountID: accountID, model: model}]
	if !ok {
		return 0
	}
	return entry.samples
}

// reset drops every EWMA row. Test-only.
func (t *nativeQualityTracker) reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries = make(map[nativeQualityKey]*nativeQualityEntry, t.cap)
	t.order.Init()
}

func clampNativeQuality01(x float64) float64 {
	if math.IsNaN(x) || x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}
	return x
}

// ─────────────────────────────────────────────────────────────────────────────
// Package-level authoritative API (see file header for contract).
// All symbols nil-safe against the global tracker being unset.
// ─────────────────────────────────────────────────────────────────────────────

// defaultNativeQualityTracker is the process-wide EWMA store. Populated at
// package init so callers never see a nil tracker; ResetNativeAccountQualityForTest
// clears it in place, preserving the pointer.
var defaultNativeQualityTracker = newNativeQualityTracker(
	qualityDefaultCapacity, qualityAlpha, qualityLowSampleFloor, qualityMaxPenalty, time.Now,
)

func recordNativeAccountQuality(ctx context.Context, accountID int64, fallbackModel string, latency time.Duration, semanticEmpty bool) {
	model := fallbackModel
	if qualityModel, ok := NativeQualityModelFromContext(ctx); ok {
		model = qualityModel
	}
	RecordNativeAccountQuality(accountID, model, latency, semanticEmpty)
}

// RecordNativeAccountQuality feeds one observation into the (accountID, model)
// EWMA. `model` MUST be the caller's originally-requested model name (never
// the wire-remapped form) so 3.6/3.1 requests remain distinguishable in the
// quality state. `latency` is end-to-end from request start through
// stream/non-stream completion; negative or absurdly-large values are
// clamped defensively. `semanticEmpty=true` marks a `stop_without_content`
// / thought-only STOP; false marks a normal successful response.
func RecordNativeAccountQuality(accountID int64, model string, latency time.Duration, semanticEmpty bool) {
	defaultNativeQualityTracker.record(accountID, model, latency, semanticEmpty)
}

// NativeAccountQualityPenalty returns the exponentially-weighted moving
// average of the semantic-empty rate for (accountID, model). Range is
// [0, 1]: 0 = clean or no data (< low-sample floor), higher = more
// semantic-empty responses observed. Consumers apply their own threshold.
func NativeAccountQualityPenalty(accountID int64, model string) float64 {
	return defaultNativeQualityTracker.penalty(accountID, model)
}

// NativeAccountLatencyEWMA returns the exponentially-weighted end-to-end
// latency for (accountID, model). Zero means "no data" (below low-sample
// floor / unseen key) — callers MUST NOT treat 0 as "fast".
func NativeAccountLatencyEWMA(accountID int64, model string) time.Duration {
	return defaultNativeQualityTracker.latencyEWMA(accountID, model)
}

// ResetNativeAccountQualityForTest clears every EWMA row. Only for tests
// that need deterministic starting state; harmless in production but
// pointless there (nobody calls it outside tests).
func ResetNativeAccountQualityForTest() {
	defaultNativeQualityTracker.reset()
}
