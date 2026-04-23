package service

import (
	"encoding/json"
	"sync"
	"time"
)

// Custom429Policy is a per-account override for 429 cooldown computation.
// Stored in account.Credentials under key "custom_429_policy".
//
// JSON layout:
//
//	{
//	  "enabled": true,
//	  "base_timeout_sec": 5,
//	  "backoff_increment_sec": 5,
//	  "max_attempts": 5
//	}
//
// Semantics:
//   - 1st 429 → cooldown = base_timeout_sec
//   - 2nd 429 → cooldown = base_timeout_sec + backoff_increment_sec
//   - Nth 429 → cooldown = base_timeout_sec + backoff_increment_sec * (N-1)
//   - When attempts exceed max_attempts → counter resets, caller falls back
//     to the platform's existing cooldown strategy (tier cooldown / PST midnight).
//   - Counter also resets if no 429 is seen for an account within
//     custom429IdleReset.
type Custom429Policy struct {
	Enabled             bool `json:"enabled"`
	BaseTimeoutSec      int  `json:"base_timeout_sec"`
	BackoffIncrementSec int  `json:"backoff_increment_sec"`
	MaxAttempts         int  `json:"max_attempts"`
}

type custom429State struct {
	mu       sync.Mutex
	attempts int
	lastHit  time.Time
}

var (
	custom429Counters sync.Map // key: account.ID (int64) → *custom429State
	custom429IdleReset = 5 * time.Minute
)

// ReadCustom429Policy extracts the policy from account credentials. Returns a
// zero value (Enabled=false) when the policy is missing or malformed.
func ReadCustom429Policy(a *Account) Custom429Policy {
	if a == nil || a.Credentials == nil {
		return Custom429Policy{}
	}
	raw, ok := a.Credentials["custom_429_policy"]
	if !ok || raw == nil {
		return Custom429Policy{}
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return Custom429Policy{}
	}
	p := Custom429Policy{}
	if v, ok := m["enabled"].(bool); ok {
		p.Enabled = v
	}
	p.BaseTimeoutSec = readJSONInt(m, "base_timeout_sec")
	p.BackoffIncrementSec = readJSONInt(m, "backoff_increment_sec")
	p.MaxAttempts = readJSONInt(m, "max_attempts")
	return p
}

// ApplyCustom429Policy consults the account's policy and returns the cooldown
// duration for this 429. The three booleans describe caller behavior:
//
//   - used == true → the returned duration replaces the default cooldown.
//   - exceeded == true → max_attempts was hit; caller MUST fall back to the
//     platform's existing strategy. The counter has already been reset.
//   - used == false && exceeded == false → policy disabled/invalid, caller
//     proceeds with default behavior; returned duration is ignored.
func ApplyCustom429Policy(account *Account) (cooldown time.Duration, used bool, exceeded bool) {
	if account == nil {
		return 0, false, false
	}
	p := ReadCustom429Policy(account)
	if !p.Enabled {
		return 0, false, false
	}
	if p.BaseTimeoutSec <= 0 || p.MaxAttempts <= 0 {
		return 0, false, false
	}

	v, _ := custom429Counters.LoadOrStore(account.ID, &custom429State{})
	st := v.(*custom429State)
	st.mu.Lock()
	defer st.mu.Unlock()

	now := time.Now()
	if !st.lastHit.IsZero() && now.Sub(st.lastHit) > custom429IdleReset {
		st.attempts = 0
	}
	st.lastHit = now
	st.attempts++

	if st.attempts > p.MaxAttempts {
		st.attempts = 0
		return 0, false, true
	}

	base := time.Duration(p.BaseTimeoutSec) * time.Second
	increment := time.Duration(p.BackoffIncrementSec) * time.Second
	if increment < 0 {
		increment = 0
	}
	cooldown = base + increment*time.Duration(st.attempts-1)
	if cooldown < time.Second {
		cooldown = time.Second
	}
	return cooldown, true, false
}

// ResetCustom429Counter clears any accumulated attempt state for an account.
// Callers may invoke this after a successful request to give the account a
// fresh backoff cycle on the next failure.
func ResetCustom429Counter(accountID int64) {
	custom429Counters.Delete(accountID)
}

func readJSONInt(m map[string]any, key string) int {
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0
		}
		return int(i)
	case string:
		// tolerate stringified numbers from settings UI
		var j json.Number = json.Number(n)
		i, err := j.Int64()
		if err != nil {
			return 0
		}
		return int(i)
	default:
		return 0
	}
}
