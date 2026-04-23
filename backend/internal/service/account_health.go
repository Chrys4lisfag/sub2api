package service

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"
)

// AccountHealthTracker maintains a bounded sliding-window of per-account
// success/error outcomes in process memory. Combined with current load, it
// produces a scalar "how attractive is this account right now" score that the
// scheduler uses to tie-break same-priority candidates.
//
// Design:
//   - Fixed-size ring per account (healthRingSize events, default 20).
//   - Events older than healthWindow (1h) are ignored when scoring.
//   - Score = wilson lower-bound of error rate with Min-sample guard (0.5 neutral).
//   - CombinedScore blends health with load via piecewise penalties tuned in
//     tuning/health_load_weights.py. Lower score == better candidate.
//
// The tracker is thread-safe and hooked synchronously on every upstream
// response, so scheduling decisions always see the latest state (no cache
// staleness).

const (
	healthRingSize     = 20
	healthWindow       = time.Hour
	healthMinSamples   = 5
	healthNeutralScore = 0.5

	// Piecewise coefficients (source: tuning/health_load_weights.py).
	// Passed all 10 anchor scenarios on 2026-04-23 sweep.
	healthKnee    = 0.3
	healthSlopeLo = 1.0
	healthSlopeHi = 3.0
	loadKnee      = 0.2
	loadSlopeLo   = 0.3
	loadSlopeHi   = 1.5
)

type accountHealthOutcome struct {
	at      time.Time
	isError bool
}

type accountHealthState struct {
	mu     sync.Mutex
	events [healthRingSize]accountHealthOutcome
	head   int
	count  int
}

// AccountHealthTracker is the concrete store. A process-wide singleton lives
// in defaultAccountHealthTracker; callers should use the free functions below
// (RecordSuccess, RecordError, HealthScore, CombinedScore) unless they need
// isolated trackers for tests.
type AccountHealthTracker struct {
	m sync.Map // int64 accountID -> *accountHealthState
}

var defaultAccountHealthTracker = &AccountHealthTracker{}

// DefaultAccountHealthTracker returns the process-wide tracker used by the
// free functions. Exposed so wire_gen can pass it into services that need a
// typed dependency.
func DefaultAccountHealthTracker() *AccountHealthTracker {
	return defaultAccountHealthTracker
}

// RecordSuccess notes a successful upstream response from an account.
func RecordSuccess(accountID int64) {
	defaultAccountHealthTracker.Record(accountID, time.Now(), false)
}

// RecordError notes an upstream error from an account (any 4xx/5xx we decided
// to mark rate-limited or failed).
func RecordError(accountID int64) {
	defaultAccountHealthTracker.Record(accountID, time.Now(), true)
}

// HealthScore returns the Wilson lower bound of the recent error rate for an
// account. 0.0 == perfect, 1.0 == all errors. Under-sampled accounts return
// healthNeutralScore (0.5).
func HealthScore(accountID int64) float64 {
	return defaultAccountHealthTracker.Score(accountID)
}

// CombinedScore is the piecewise blend of health and load used by the
// scheduler. Lower == better. See tuning/health_load_weights.py for the
// scenario catalog that pinned the coefficients.
func CombinedScore(accountID int64, loadRate float64) float64 {
	return defaultAccountHealthTracker.CombinedScore(accountID, loadRate)
}

// Record appends an outcome (at a caller-supplied timestamp, so hydration
// from DB can replay historical events in chronological order).
func (t *AccountHealthTracker) Record(accountID int64, at time.Time, isError bool) {
	v, _ := t.m.LoadOrStore(accountID, &accountHealthState{})
	h := v.(*accountHealthState)
	h.mu.Lock()
	h.events[h.head] = accountHealthOutcome{at: at, isError: isError}
	h.head = (h.head + 1) % healthRingSize
	if h.count < healthRingSize {
		h.count++
	}
	h.mu.Unlock()
}

// Score returns the per-account Wilson lower-bound error rate over the
// configured window. Neutral when under-sampled.
func (t *AccountHealthTracker) Score(accountID int64) float64 {
	v, ok := t.m.Load(accountID)
	if !ok {
		return healthNeutralScore
	}
	h := v.(*accountHealthState)
	h.mu.Lock()
	defer h.mu.Unlock()
	cutoff := time.Now().Add(-healthWindow)
	errors, total := 0, 0
	for i := 0; i < h.count; i++ {
		e := h.events[i]
		if e.at.Before(cutoff) {
			continue
		}
		total++
		if e.isError {
			errors++
		}
	}
	if total < healthMinSamples {
		return healthNeutralScore
	}
	return wilsonLowerBound(errors, total)
}

// CombinedScore == healthPenalty(HealthScore) + loadPenalty(loadRate).
func (t *AccountHealthTracker) CombinedScore(accountID int64, loadRate float64) float64 {
	h := t.Score(accountID)
	return healthPenalty(h) + loadPenalty(loadRate)
}

// wilsonLowerBound is the 95% Wilson lower confidence bound for a binomial
// proportion. Handles zero-errors edge case (returns 0, not NaN).
func wilsonLowerBound(errors, total int) float64 {
	if total <= 0 {
		return 0
	}
	const z = 1.96
	p := float64(errors) / float64(total)
	n := float64(total)
	centre := p + z*z/(2*n)
	margin := z * math.Sqrt(p*(1-p)/n+z*z/(4*n*n))
	lower := (centre - margin) / (1 + z*z/n)
	if lower < 0 {
		return 0
	}
	return lower
}

// loadPenalty is piecewise linear: gentle below the knee so a few users on a
// healthy account doesn't bounce traffic off it, steep above so a saturated
// account yields to emptier peers even with worse health.
func loadPenalty(loadRate float64) float64 {
	if loadRate < 0 {
		loadRate = 0
	}
	if loadRate < loadKnee {
		return loadSlopeLo * loadRate
	}
	return loadSlopeLo*loadKnee + loadSlopeHi*(loadRate-loadKnee)
}

// healthPenalty is piecewise linear: small fluctuations stay linear, but a
// "broken" account (Wilson > healthKnee) gets a steep penalty so it's
// effectively last-resort.
func healthPenalty(wilson float64) float64 {
	if wilson < 0 {
		wilson = 0
	}
	if wilson < healthKnee {
		return healthSlopeLo * wilson
	}
	return healthSlopeLo*healthKnee + healthSlopeHi*(wilson-healthKnee)
}

// HealthEventRow represents a single historical outcome used for startup
// hydration. IsError mirrors the in-memory flag.
type HealthEventRow struct {
	AccountID int64
	CreatedAt time.Time
	IsError   bool
}

// AccountHealthEventSource is the minimal repo interface required to rehydrate
// the tracker on service startup.
type AccountHealthEventSource interface {
	ListRecentHealthEvents(ctx context.Context, window time.Duration, perAccountLimit int) ([]HealthEventRow, error)
}

// HydrateAccountHealth pre-populates the process-wide tracker from persistent
// logs. Intended to run once synchronously during service startup so that the
// first scheduling decision after a restart already reflects recent history.
//
// Errors are logged, not fatal: the worst case is a cold start with all
// accounts at neutral score for the first few requests, which recovers fast.
func HydrateAccountHealth(ctx context.Context, src AccountHealthEventSource) {
	if src == nil {
		return
	}
	rows, err := src.ListRecentHealthEvents(ctx, healthWindow, healthRingSize)
	if err != nil {
		slog.Warn("account_health_hydrate_failed", "err", err)
		return
	}
	for _, r := range rows {
		defaultAccountHealthTracker.Record(r.AccountID, r.CreatedAt, r.IsError)
	}
	slog.Info("account_health_hydrated", "events", len(rows))
}
