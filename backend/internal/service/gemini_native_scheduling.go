// Package service — Native Gemini scheduling helpers.
//
// Centralises the small pieces of state and derived predicates that the
// signature-aware scheduling in gateway_scheduling.go needs, in ONE place
// so consumers (handler → scheduler) share exactly the same contract.
//
// Design (as ratified by Main; see also memory://sched.native.sig):
//   - thoughtSignature detection lives in the handler (JSON-aware body inspection
//     is cheap and the handler already reads the body). The handler stashes
//     a single bool in ctx before calling SelectAccountWithLoadAwareness so
//     scheduling never re-parses the body.
//   - Native "very bad" / "very slow" gating is a hard threshold check
//     against the tracker's ratified min-sample-gated outputs
//     (NativeAccountQualityPenalty / NativeAccountLatencyEWMA). The tracker
//     returns 0 below its own sample floor, so a fresh account never
//     escapes sticky. Consumers must never disable/downgrade an account —
//     failing accounts remain fallback.
//   - Recent-selection avoidance is provided by an OPTIONAL NativeSelectionRecency
//     interface on the ConcurrencyCache (see native_selection_recency.go);
//     when absent, the scheduler silently falls back to plain shuffle
//     spread. The default reservation window is small (couple of seconds)
//     so this NEVER pins traffic to a subset of accounts under load.
package service

import (
	"context"
	"encoding/json"
	"math/rand"
	"sort"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

// NativeStickyEscapePenaltyThreshold — sticky binding is bypassed and the
// scheduler falls through to normal selection when the tracker's penalty is
// at least this value. Chosen to match the tracker owner's documented
// "very bad" characterisation (penalty range [0, 1], 0.5 == half-scale
// severity, min-sample-gated at the tracker so fresh accounts never trip
// this). Documented in code so operators can audit the escape threshold
// without spelunking through git history.
const NativeStickyEscapePenaltyThreshold = 0.5

// NativeStickyEscapeLatencyThreshold — sticky binding is bypassed when the
// tracker's end-to-end EWMA latency is at least this value. Matches the
// tracker owner's "very slow" characterisation.
const NativeStickyEscapeLatencyThreshold = 20 * time.Second

// DefaultNativeRecentSelectionWindow — default TTL for "recently selected"
// claims on Native accounts. Small enough that under sequential-new-session
// traffic every candidate rotates through, large enough to break stampedes
// from a single client burst. Overridable via config.
const DefaultNativeRecentSelectionWindow = 2 * time.Second

// thoughtSignatureCtxKey uniquely identifies the thoughtSignature-presence
// flag in a request context. Kept unexported and package-scoped so callers
// go through the setter/getter and can't collide by string key.
type thoughtSignatureCtxKey struct{}
type nativeQualityModelCtxKey struct{}

// WithNativeQualityModel preserves the client-requested model across channel
// mapping so scheduler reads and gateway writes use the same quality key.
func WithNativeQualityModel(ctx context.Context, model string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, nativeQualityModelCtxKey{}, model)
}

func NativeQualityModelFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	model, ok := ctx.Value(nativeQualityModelCtxKey{}).(string)
	return model, ok && model != ""
}

// WithThoughtSignaturePresent records whether the current inbound Gemini
// native request body carries a thoughtSignature field. Handler MUST call
// this before invoking SelectAccountWithLoadAwareness so the scheduler can
// distinguish "signature-bearing continuation" from "brand-new session"
// without re-parsing the body.
func WithThoughtSignaturePresent(ctx context.Context, present bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, thoughtSignatureCtxKey{}, present)
}

// ThoughtSignaturePresentFromContext returns (present, ok). ok is false
// when the flag was never set (handler path that never inspects it).
// Scheduler MUST treat ok=false as "unknown" and behave as if no signature
// were present — matches legacy behavior for non-Gemini paths.
func ThoughtSignaturePresentFromContext(ctx context.Context) (bool, bool) {
	if ctx == nil {
		return false, false
	}
	v, ok := ctx.Value(thoughtSignatureCtxKey{}).(bool)
	if !ok {
		return false, false
	}
	return v, true
}

// GeminiBodyContainsThoughtSignature parses Gemini content parts and detects
// non-empty thoughtSignature fields without matching tool arguments or text.
// Handler + tests share this exact predicate.
func GeminiBodyContainsThoughtSignature(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	hasSignaturePart := func(value any) bool {
		content, ok := value.(map[string]any)
		if !ok {
			return false
		}
		parts, ok := content["parts"].([]any)
		if !ok {
			return false
		}
		for _, rawPart := range parts {
			part, ok := rawPart.(map[string]any)
			if !ok {
				continue
			}
			if signature, ok := part["thoughtSignature"].(string); ok && signature != "" {
				return true
			}
		}
		return false
	}
	if contents, ok := payload["contents"].([]any); ok {
		for _, content := range contents {
			if hasSignaturePart(content) {
				return true
			}
		}
	}
	return hasSignaturePart(payload["cachedContent"])
}

// isNativePlatformAccount returns true only for platform=antigravity_native.
// Sig-aware scheduling helpers only apply on the native platform; other
// platforms keep their existing scheduling contract untouched.
func isNativePlatformAccount(account *Account) bool {
	if account == nil {
		return false
	}
	return account.Platform == domain.PlatformAntigravityNative
}

// isSchedulingForNativePlatform returns true when the scheduling call is
// selecting from the native platform bucket.
func isSchedulingForNativePlatform(platform string, useMixed bool) bool {
	// useMixed=true means anthropic/gemini group mixed with antigravity;
	// native platform is scheduled via ForcePlatform middleware, so useMixed
	// is always false for the native platform.
	if useMixed {
		return false
	}
	return platform == domain.PlatformAntigravityNative
}

// nativeStickyEscape returns true when the tracker signals that the given
// (account, model) pair has "very bad" quality or is "very slow" per the
// ratified thresholds above. The tracker itself gates on its internal
// min-sample floor and returns 0 for under-sampled or missing keys, so
// this predicate never fires for a fresh account. Consumers MUST NOT
// disable/downgrade the account on true — only escape sticky binding
// and re-select through the normal layered path.
func nativeStickyEscape(accountID int64, requestedModel string) (bool, string) {
	if accountID <= 0 {
		return false, ""
	}
	if p := NativeAccountQualityPenalty(accountID, requestedModel); p >= NativeStickyEscapePenaltyThreshold {
		return true, "very_bad_penalty"
	}
	if l := NativeAccountLatencyEWMA(accountID, requestedModel); l >= NativeStickyEscapeLatencyThreshold {
		return true, "very_slow_latency"
	}
	return false, ""
}

// nativeQualityOrderingScore returns a bounded ordering score used to
// TIE-BREAK equal-priority/equal-load Native candidates for NEW sessions.
// Range: [0, 1]. Lower is better. Composed from the tracker's penalty
// (dominant signal) and latency EWMA (secondary signal), each clamped so
// no candidate can be pushed outside the shuffled ordering group.
//
// This is a *ranking* helper only — never a filter. Even the worst-scored
// candidate remains eligible fallback.
func nativeQualityOrderingScore(accountID int64, requestedModel string) float64 {
	if accountID <= 0 {
		return 0
	}
	penalty := NativeAccountQualityPenalty(accountID, requestedModel)
	if penalty < 0 {
		penalty = 0
	}
	if penalty > 1 {
		penalty = 1
	}
	// Latency contribution: normalize by the escape threshold, cap at 1.
	// A latency of 0 (unknown) contributes 0 — no bias against untested
	// accounts.
	latency := NativeAccountLatencyEWMA(accountID, requestedModel)
	latencyScore := 0.0
	if latency > 0 && NativeStickyEscapeLatencyThreshold > 0 {
		latencyScore = float64(latency) / float64(NativeStickyEscapeLatencyThreshold)
		if latencyScore > 1 {
			latencyScore = 1
		}
	}
	// 70/30 weighted blend: penalty dominates because it reflects observed
	// bad outcomes (rate limits, semantic-empty responses), whereas latency
	// alone can bounce due to network variance.
	return 0.7*penalty + 0.3*latencyScore
}

// selectNativeFreshCandidate prefers a process-wide recency claim among
// healthy equal-priority/equal-load accounts. Poor accounts remain fallback,
// but never displace a healthy recently-used account.
func (s *GatewayService) selectNativeFreshCandidate(ctx context.Context, candidates []accountWithLoad, requestedModel string, recencyWindow time.Duration, preferOAuth bool) *accountWithLoad {
	if len(candidates) == 0 {
		return nil
	}
	ordered := append([]accountWithLoad(nil), candidates...)
	rand.Shuffle(len(ordered), func(i, j int) { ordered[i], ordered[j] = ordered[j], ordered[i] })
	sort.SliceStable(ordered, func(i, j int) bool {
		iPoor, _ := nativeStickyEscape(ordered[i].account.ID, requestedModel)
		jPoor, _ := nativeStickyEscape(ordered[j].account.ID, requestedModel)
		if iPoor != jPoor {
			return !iPoor
		}
		iScore := nativeQualityOrderingScore(ordered[i].account.ID, requestedModel)
		jScore := nativeQualityOrderingScore(ordered[j].account.ID, requestedModel)
		if iScore != jScore {
			return iScore < jScore
		}
		iLast, jLast := ordered[i].account.LastUsedAt, ordered[j].account.LastUsedAt
		if iLast == nil || jLast == nil {
			return iLast == nil && jLast != nil
		}
		if !iLast.Equal(*jLast) {
			return iLast.Before(*jLast)
		}
		if preferOAuth && ordered[i].account.Type != ordered[j].account.Type {
			return ordered[i].account.Type == AccountTypeOAuth
		}
		return false
	})

	healthyCount := 0
	for i := range ordered {
		poor, _ := nativeStickyEscape(ordered[i].account.ID, requestedModel)
		if poor {
			break
		}
		healthyCount++
	}
	claimLimit := healthyCount
	if claimLimit == 0 {
		claimLimit = len(ordered)
	}
	for i := 0; i < claimLimit; i++ {
		claimed, _ := s.concurrencyService.ReserveNativeAccountForSelection(ctx, ordered[i].account.ID, recencyWindow)
		if claimed {
			return &ordered[i]
		}
	}
	return &ordered[0]
}
