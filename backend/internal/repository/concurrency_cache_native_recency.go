// Package repository — Redis backend for NativeSelectionRecency.
//
// Split out from concurrency_cache.go to keep the ratified atomic
// short-TTL claim primitive isolated and reviewable. This is the
// production wire for the ConcurrencyService type assertion at
// service.(*ConcurrencyService).nativeSelectionRecency().
package repository

import (
	"context"
	"fmt"
	"time"
)

// nativeSelectionRecencyKeyPrefix identifies per-account atomic claim
// keys. TTL is caller-supplied (typically a couple of seconds) and the
// key is not otherwise referenced by the concurrency subsystem —
// losing the claim NEVER removes an account from the scheduler's
// candidate pool, it only deprioritizes it among equal-tier peers.
//
// Format: concurrency:native_select:{accountID}
const nativeSelectionRecencyKeyPrefix = "concurrency:native_select:"

// ReserveNativeAccountForSelection implements service.NativeSelectionRecency
// on the production Redis-backed concurrencyCache via atomic SET NX EX.
// Contract:
//   - (true, nil):  the caller won the claim within TTL; safe to prefer.
//   - (false, nil): another selector holds an active claim; prefer
//     alternatives if any exist, but the account remains
//     eligible as a fallback.
//   - (true, err):  fail-open on Redis error so a transient outage never
//     blocks selection; the wrapped error is returned for
//     diagnostics only.
func (c *concurrencyCache) ReserveNativeAccountForSelection(ctx context.Context, accountID int64, ttl time.Duration) (bool, error) {
	if ttl <= 0 || accountID <= 0 || c == nil || c.rdb == nil {
		return true, nil
	}
	key := fmt.Sprintf("%s%d", nativeSelectionRecencyKeyPrefix, accountID)
	ok, err := c.rdb.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return true, fmt.Errorf("native selection reserve: %w", err)
	}
	return ok, nil
}
