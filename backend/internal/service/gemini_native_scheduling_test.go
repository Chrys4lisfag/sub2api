package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type nativeSchedulingRecencyCache struct {
	schedulerTestConcurrencyCache
	recency *InMemoryNativeSelectionRecency
}

func (c *nativeSchedulingRecencyCache) ReserveNativeAccountForSelection(ctx context.Context, accountID int64, ttl time.Duration) (bool, error) {
	return c.recency.ReserveNativeAccountForSelection(ctx, accountID, ttl)
}

type blockingNativeSchedulingRecencyCache struct {
	schedulerTestConcurrencyCache
}

func (c *blockingNativeSchedulingRecencyCache) ReserveNativeAccountForSelection(ctx context.Context, _ int64, _ time.Duration) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

func TestReserveNativeAccountForSelection_BackendTimeoutFailsOpen(t *testing.T) {
	svc := NewConcurrencyService(&blockingNativeSchedulingRecencyCache{})
	started := time.Now()
	won, err := svc.ReserveNativeAccountForSelection(context.Background(), 1, time.Second)
	require.True(t, won)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
}

func TestInMemoryNativeSelectionRecency_InvalidAccountFailsOpen(t *testing.T) {
	recency := NewInMemoryNativeSelectionRecency()
	won, err := recency.ReserveNativeAccountForSelection(context.Background(), 0, time.Second)
	require.NoError(t, err)
	require.True(t, won)
	require.Empty(t, recency.entries)
}

func newNativeFreshSelectionService() *GatewayService {
	cache := &nativeSchedulingRecencyCache{recency: NewInMemoryNativeSelectionRecency()}
	return &GatewayService{concurrencyService: NewConcurrencyService(cache)}
}

func nativeSchedulingCandidates(count int) []accountWithLoad {
	candidates := make([]accountWithLoad, 0, count)
	for i := 1; i <= count; i++ {
		candidates = append(candidates, accountWithLoad{
			account:  &Account{ID: int64(i), Platform: PlatformAntigravityNative, Concurrency: 10},
			loadInfo: &AccountLoadInfo{AccountID: int64(i), LoadRate: 0},
		})
	}
	return candidates
}

func TestSelectNativeFreshCandidate_ParallelClaimsSpread(t *testing.T) {
	ResetNativeAccountQualityForTest()
	svc := newNativeFreshSelectionService()
	candidates := nativeSchedulingCandidates(12)
	start := make(chan struct{})
	selected := make(chan int64, len(candidates))
	var wg sync.WaitGroup
	wg.Add(len(candidates))
	for range candidates {
		go func() {
			defer wg.Done()
			<-start
			choice := svc.selectNativeFreshCandidate(context.Background(), candidates, "gemini-3.6-flash", time.Second, false)
			if choice == nil {
				selected <- 0
				return
			}
			selected <- choice.account.ID
		}()
	}
	close(start)
	wg.Wait()
	close(selected)

	seen := make(map[int64]struct{}, len(candidates))
	for accountID := range selected {
		seen[accountID] = struct{}{}
	}
	require.Len(t, seen, len(candidates), "atomic recency claims should spread a synchronized burst")
	for _, candidate := range candidates {
		require.Equal(t, 10, candidate.account.Concurrency, "selection must not change configured cap")
	}
}

func TestSelectNativeFreshCandidate_PreservesHealthyFallback(t *testing.T) {
	ResetNativeAccountQualityForTest()
	svc := newNativeFreshSelectionService()
	candidates := nativeSchedulingCandidates(2)
	for range qualityLowSampleFloor {
		RecordNativeAccountQuality(1, "gemini-3.6-flash", 30*time.Second, true)
		RecordNativeAccountQuality(2, "gemini-3.6-flash", time.Second, false)
	}

	first := svc.selectNativeFreshCandidate(context.Background(), candidates, "gemini-3.6-flash", time.Second, false)
	require.Equal(t, int64(2), first.account.ID)
	second := svc.selectNativeFreshCandidate(context.Background(), candidates, "gemini-3.6-flash", time.Second, false)
	require.Equal(t, int64(2), second.account.ID, "recent healthy account must remain preferable to poor fallback")
}

func TestNativeStickyEscape_IsModelScopedAndSampleGated(t *testing.T) {
	ResetNativeAccountQualityForTest()
	for range qualityLowSampleFloor - 1 {
		RecordNativeAccountQuality(7, "gemini-3.6-flash", 30*time.Second, true)
	}
	escape, _ := nativeStickyEscape(7, "gemini-3.6-flash")
	require.False(t, escape)

	RecordNativeAccountQuality(7, "gemini-3.6-flash", 30*time.Second, true)
	escape, reason := nativeStickyEscape(7, "gemini-3.6-flash")
	require.True(t, escape)
	require.NotEmpty(t, reason)

	escape, _ = nativeStickyEscape(7, "gemini-3.1-pro-low")
	require.False(t, escape, "quality must not bleed across model keys")
}

func TestThoughtSignatureContext_IsExplicit(t *testing.T) {
	_, ok := ThoughtSignaturePresentFromContext(context.Background())
	require.False(t, ok, "generic scheduling paths must preserve legacy behavior")

	ctx := WithThoughtSignaturePresent(context.Background(), true)
	present, ok := ThoughtSignaturePresentFromContext(ctx)
	require.True(t, ok)
	require.True(t, present)
	require.True(t, GeminiBodyContainsThoughtSignature([]byte(`{"contents":[{"parts":[{"thoughtSignature":"sig"}]}]}`)))
	require.True(t, GeminiBodyContainsThoughtSignature([]byte(`{"contents":[{"parts":[{"\u0074houghtSignature":"sig"}]}]}`)))
	require.False(t, GeminiBodyContainsThoughtSignature([]byte(`{"contents":[{"parts":[{"text":"literal \"thoughtSignature\" is not a key"}]}]}`)))
	require.False(t, GeminiBodyContainsThoughtSignature([]byte(`{"contents":[{"parts":[{"functionCall":{"name":"tool","args":{"thoughtSignature":"user-data"}}}]}]}`)))
	require.False(t, GeminiBodyContainsThoughtSignature([]byte(`{"contents":[{"parts":[{"text":"plain"}]}]}`)))
}
