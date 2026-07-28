// Package service — native selection recency (optional, process-safe).
//
// This file defines a small OPTIONAL interface, NativeSelectionRecency,
// implemented by ConcurrencyCache backends that want to expose a
// process-safe, atomic "recently selected" claim primitive for
// Native-platform accounts. It exists solely to spread NEW (signature-free)
// Native sessions across otherwise-equal candidates without violating
// account capacity or sticky continuation.
//
// Non-goals:
//   - Not a rate limiter, not a lock, not a session binding.
//   - Never removes an account from the eligible candidate pool: losing the
//     claim race merely deprioritizes an account among equal-tier
//     alternatives. Every account remains a valid fallback.
package service

import (
	"context"
	"sync"
	"time"
)

// NativeSelectionRecency is the OPTIONAL extension implemented by a
// ConcurrencyCache backend to provide atomic, short-TTL claim tracking of
// recent Native-platform account selections across scheduler processes.
//
// The scheduler ONLY consults this interface for new (no-sticky,
// no-thoughtSignature) sessions on the Native platform. Existing sticky
// continuations and non-Native platforms are unaffected.
type NativeSelectionRecency interface {
	// ReserveNativeAccountForSelection atomically marks accountID as
	// recently selected for at most ttl. Returns:
	//   - (true, nil):  the caller won the claim within TTL; safe to prefer.
	//   - (false, nil): another selector currently holds an active claim;
	//                   prefer alternatives if any exist, but the account
	//                   remains eligible as fallback.
	//   - (_, err):     fail-open — caller MUST treat as "won" so that a
	//                   Redis outage never blocks selection.
	ReserveNativeAccountForSelection(ctx context.Context, accountID int64, ttl time.Duration) (bool, error)
}

// nativeSelectionRecency returns the recency backend when the underlying
// ConcurrencyCache implements it, else nil (feature absent — scheduler
// silently falls back to plain shuffle-based spread).
func (s *ConcurrencyService) nativeSelectionRecency() NativeSelectionRecency {
	if s == nil || s.cache == nil {
		return nil
	}
	if r, ok := s.cache.(NativeSelectionRecency); ok {
		return r
	}
	return nil
}

// ReserveNativeAccountForSelection delegates to the optional recency backend
// with fail-open semantics: when no backend is wired, returns (true, nil)
// so the caller behaves exactly as if the feature were disabled.
const nativeSelectionRecencyBackendTimeout = 150 * time.Millisecond

func (s *ConcurrencyService) ReserveNativeAccountForSelection(ctx context.Context, accountID int64, ttl time.Duration) (bool, error) {
	if ttl <= 0 || accountID <= 0 {
		return true, nil
	}
	r := s.nativeSelectionRecency()
	if r == nil {
		return true, nil
	}
	reserveCtx, cancel := context.WithTimeout(ctx, nativeSelectionRecencyBackendTimeout)
	defer cancel()
	won, err := r.ReserveNativeAccountForSelection(reserveCtx, accountID, ttl)
	if err != nil {
		// Fail-open on backend error: refusing selection because Redis is
		// slow would violate "all accounts remain eligible fallback".
		return true, err
	}
	return won, nil
}

// InMemoryNativeSelectionRecency is a process-local NativeSelectionRecency
// implementation intended for tests and single-process deployments. It is
// safe for concurrent use and enforces the same atomic SET-NX-EX semantics
// as a Redis backend, so barrier-based tests can validate spread behavior
// without a live Redis.
type InMemoryNativeSelectionRecency struct {
	mu      sync.Mutex
	entries map[int64]time.Time
	// clock indirection so tests can advance time deterministically; nil
	// means "use time.Now()".
	clock func() time.Time
}

// NewInMemoryNativeSelectionRecency constructs an in-memory recency
// backend backed by a mutex-guarded map with lazy expiry pruning.
func NewInMemoryNativeSelectionRecency() *InMemoryNativeSelectionRecency {
	return &InMemoryNativeSelectionRecency{entries: make(map[int64]time.Time)}
}

// SetClockForTest overrides the wall clock; nil restores time.Now.
func (r *InMemoryNativeSelectionRecency) SetClockForTest(clock func() time.Time) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.clock = clock
	r.mu.Unlock()
}

// ReserveNativeAccountForSelection atomically claims accountID for ttl.
func (r *InMemoryNativeSelectionRecency) ReserveNativeAccountForSelection(_ context.Context, accountID int64, ttl time.Duration) (bool, error) {
	if r == nil || ttl <= 0 || accountID <= 0 {
		return true, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if r.clock != nil {
		now = r.clock()
	}
	if exp, ok := r.entries[accountID]; ok && now.Before(exp) {
		return false, nil
	}
	if len(r.entries) > 1024 {
		for k, v := range r.entries {
			if !now.Before(v) {
				delete(r.entries, k)
			}
		}
	}
	r.entries[accountID] = now.Add(ttl)
	return true, nil
}

// ResetForTest wipes all outstanding claims.
func (r *InMemoryNativeSelectionRecency) ResetForTest() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.entries = make(map[int64]time.Time)
	r.mu.Unlock()
}
