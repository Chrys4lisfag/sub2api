package service

import (
	"sync"
	"testing"
	"time"
)

// helper: reset before each test to avoid cross-test contamination via the
// package-global tracker (the authoritative API is package-level; tests
// share state by design and must reset explicitly).
func resetQuality(t *testing.T) {
	t.Helper()
	ResetNativeAccountQualityForTest()
	t.Cleanup(func() { ResetNativeAccountQualityForTest() })
}

// TestNativeQuality_LowSampleFloor_Neutral verifies that below the low
// sample floor a fresh (accountID, model) pair returns 0 for both
// penalty and latency EWMA regardless of the sampled values. Fresh
// accounts must NOT be over-penalized on the first few requests.
func TestNativeQuality_LowSampleFloor_Neutral(t *testing.T) {
	resetQuality(t)

	// Feed 4 semantic-empty samples (below the floor of 5).
	for i := 0; i < qualityLowSampleFloor-1; i++ {
		RecordNativeAccountQuality(101, "gemini-3.6-flash", 500*time.Millisecond, true)
	}
	if got := NativeAccountQualityPenalty(101, "gemini-3.6-flash"); got != 0 {
		t.Fatalf("below floor penalty = %v, want 0 (neutral)", got)
	}
	if got := NativeAccountLatencyEWMA(101, "gemini-3.6-flash"); got != 0 {
		t.Fatalf("below floor latency = %v, want 0 (neutral)", got)
	}
	// One more sample crosses the floor — penalty should now surface.
	RecordNativeAccountQuality(101, "gemini-3.6-flash", 500*time.Millisecond, true)
	if got := NativeAccountQualityPenalty(101, "gemini-3.6-flash"); got <= 0 {
		t.Fatalf("at-floor penalty = %v, want > 0", got)
	}
	if got := NativeAccountLatencyEWMA(101, "gemini-3.6-flash"); got <= 0 {
		t.Fatalf("at-floor latency = %v, want > 0", got)
	}
}

// TestNativeQuality_PenaltyBounds verifies that no matter how many
// consecutive semantic-empty samples we feed, penalty never exceeds
// MaxNativeAccountPenalty (1.0). Also confirms it converges toward 1.
func TestNativeQuality_PenaltyBounds(t *testing.T) {
	resetQuality(t)

	for i := 0; i < 200; i++ {
		RecordNativeAccountQuality(202, "gemini-3.1-pro", time.Second, true)
	}
	p := NativeAccountQualityPenalty(202, "gemini-3.1-pro")
	if p < 0 || p > 1 {
		t.Fatalf("penalty out of [0,1]: %v", p)
	}
	if p < 0.9 {
		t.Fatalf("penalty did not converge toward 1 after 200 failures: %v", p)
	}
}

// TestNativeQuality_Recovery verifies that a burst of failures followed
// by a burst of successes reduces penalty back below the very-bad
// threshold — a bad account must not be sticky-poisoned forever.
func TestNativeQuality_Recovery(t *testing.T) {
	resetQuality(t)

	// Saturate with failures.
	for i := 0; i < 50; i++ {
		RecordNativeAccountQuality(303, "gemini-3.6-flash-high", time.Second, true)
	}
	before := NativeAccountQualityPenalty(303, "gemini-3.6-flash-high")
	if before < 0.5 {
		t.Fatalf("expected high penalty after failures, got %v", before)
	}
	// Feed successes; EWMA should decay penalty back down.
	for i := 0; i < 50; i++ {
		RecordNativeAccountQuality(303, "gemini-3.6-flash-high", 200*time.Millisecond, false)
	}
	after := NativeAccountQualityPenalty(303, "gemini-3.6-flash-high")
	if after >= before {
		t.Fatalf("penalty did not recover: before=%v after=%v", before, after)
	}
	if after > 0.1 {
		t.Fatalf("penalty did not decay near-zero after 50 successes: %v", after)
	}
}

// TestNativeQuality_LatencyClamping verifies that a pathological latency
// sample (> qualityMaxLatency) does not push the EWMA past the cap,
// and that a negative latency is treated as zero.
func TestNativeQuality_LatencyClamping(t *testing.T) {
	resetQuality(t)

	for i := 0; i < 20; i++ {
		RecordNativeAccountQuality(404, "gemini-3.1-pro", 2*qualityMaxLatency, false)
	}
	got := NativeAccountLatencyEWMA(404, "gemini-3.1-pro")
	if got > qualityMaxLatency {
		t.Fatalf("latency EWMA exceeded clamp: got=%v cap=%v", got, qualityMaxLatency)
	}

	resetQuality(t)
	for i := 0; i < 20; i++ {
		RecordNativeAccountQuality(405, "gemini-3.1-pro", -1*time.Second, false)
	}
	if got := NativeAccountLatencyEWMA(405, "gemini-3.1-pro"); got < 0 {
		t.Fatalf("negative sample produced negative EWMA: %v", got)
	}
}

// TestNativeQuality_IsolationAccountAndModel verifies that samples for
// (accountA, modelX) do not leak into (accountB, modelX) or
// (accountA, modelY). Distribution's scheduler depends on this to
// isolate account/model quality without cross-contamination.
func TestNativeQuality_IsolationAccountAndModel(t *testing.T) {
	resetQuality(t)

	for i := 0; i < 20; i++ {
		RecordNativeAccountQuality(500, "gemini-3.6-flash", time.Second, true)
	}
	// Different account, same model: must stay neutral.
	if got := NativeAccountQualityPenalty(600, "gemini-3.6-flash"); got != 0 {
		t.Fatalf("account isolation broken: acct 600 sees penalty %v", got)
	}
	// Same account, different model: must stay neutral.
	if got := NativeAccountQualityPenalty(500, "gemini-3.1-pro"); got != 0 {
		t.Fatalf("model isolation broken: acct 500 model 3.1-pro sees penalty %v", got)
	}
	// Same key: must reflect the bad EWMA.
	if got := NativeAccountQualityPenalty(500, "gemini-3.6-flash"); got <= 0 {
		t.Fatalf("original key lost its penalty: %v", got)
	}
}

// TestNativeQuality_PreservesRequestedModel_36_31 is the acceptance
// check: 3.6 and 3.1 wire-remaps must NOT collapse into one row.
// The tracker keys off the REQUESTED model name; wire model is metadata
// only. Verifying by feeding samples under "gemini-3.6-flash" vs
// "gemini-3.6-flash-high" (both map to the same wire model in the
// resolver) and asserting they read back independently.
func TestNativeQuality_PreservesRequestedModel_36_31(t *testing.T) {
	resetQuality(t)

	// 3.6 variants — same wire family, different REQUESTED names.
	for i := 0; i < 20; i++ {
		RecordNativeAccountQuality(700, "gemini-3.6-flash", time.Second, true)       // bad
		RecordNativeAccountQuality(700, "gemini-3.6-flash-high", time.Second, false) // good
	}
	badPen := NativeAccountQualityPenalty(700, "gemini-3.6-flash")
	goodPen := NativeAccountQualityPenalty(700, "gemini-3.6-flash-high")
	if badPen <= 0 || goodPen != 0 {
		t.Fatalf("3.6 requested-model isolation broken: bad=%v good=%v", badPen, goodPen)
	}

	// 3.1 variants — same account, different requested rows.
	for i := 0; i < 20; i++ {
		RecordNativeAccountQuality(701, "gemini-3.1-pro", time.Second, true)
		RecordNativeAccountQuality(701, "gemini-3.1-pro-low", time.Second, false)
	}
	if NativeAccountQualityPenalty(701, "gemini-3.1-pro") <= 0 {
		t.Fatal("3.1-pro penalty lost")
	}
	if NativeAccountQualityPenalty(701, "gemini-3.1-pro-low") != 0 {
		t.Fatal("3.1-pro-low leaked penalty from 3.1-pro")
	}
}

// TestNativeQuality_EmptyModelDropped verifies that a Record call with
// an empty model name is silently dropped rather than polluting a
// shared "" row that would mix across accounts.
func TestNativeQuality_EmptyModelDropped(t *testing.T) {
	resetQuality(t)

	RecordNativeAccountQuality(800, "", time.Second, true)
	if got := NativeAccountQualityPenalty(800, ""); got != 0 {
		t.Fatalf("empty-model row unexpectedly present: penalty %v", got)
	}
}

// TestNativeQuality_BoundedCapacity verifies LRU eviction: once the
// tracker fills, the oldest key gets dropped and its penalty resets to
// neutral. Uses a small tracker built via the internal constructor to
// keep the test fast without polluting the package-global.
func TestNativeQuality_BoundedCapacity(t *testing.T) {
	tr := newNativeQualityTracker(2, qualityAlpha, 1, qualityMaxPenalty, time.Now)

	// Fill: keys A, B → both present.
	tr.record(1, "A", time.Second, true)
	tr.record(2, "B", time.Second, true)
	if tr.penalty(1, "A") == 0 || tr.penalty(2, "B") == 0 {
		t.Fatal("both entries should be recorded pre-eviction")
	}
	// Add third: A (least recent) should evict — its state is now gone.
	tr.record(3, "C", time.Second, true)
	if tr.samplesFor(1, "A") != 0 {
		t.Fatalf("expected LRU eviction of (1,A); still present with samples=%d", tr.samplesFor(1, "A"))
	}
	if tr.penalty(2, "B") == 0 || tr.penalty(3, "C") == 0 {
		t.Fatal("B and C should still be present after evicting A")
	}
}

// TestNativeQuality_RaceSafeUnderConcurrency exercises the RWMutex-free
// single-mutex design under -race: 4 concurrent writers hammer the
// tracker while 4 concurrent readers stream penalty/latency reads.
// If any mutation races with a read, `go test -race` fails this test.
func TestNativeQuality_RaceSafeUnderConcurrency(t *testing.T) {
	resetQuality(t)

	const iters = 500
	var wg sync.WaitGroup
	writer := func(id int64, model string, empty bool) {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			RecordNativeAccountQuality(id, model, time.Duration(i)*time.Millisecond, empty)
		}
	}
	reader := func(id int64, model string) {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			_ = NativeAccountQualityPenalty(id, model)
			_ = NativeAccountLatencyEWMA(id, model)
		}
	}
	wg.Add(8)
	go writer(901, "gemini-3.6-flash", true)
	go writer(902, "gemini-3.6-flash", false)
	go writer(903, "gemini-3.1-pro", true)
	go writer(904, "gemini-3.1-pro", false)
	go reader(901, "gemini-3.6-flash")
	go reader(902, "gemini-3.6-flash")
	go reader(903, "gemini-3.1-pro")
	go reader(904, "gemini-3.1-pro")
	wg.Wait()

	// After the storm, sanity check: acct 901 (always-empty) has non-zero
	// penalty, acct 902 (always-good) has zero.
	if NativeAccountQualityPenalty(901, "gemini-3.6-flash") <= 0 {
		t.Fatal("always-empty account has zero penalty after storm")
	}
	if NativeAccountQualityPenalty(902, "gemini-3.6-flash") != 0 {
		t.Fatal("always-good account gained penalty after storm")
	}
}

// TestNativeQuality_ResetForTest verifies the reset helper drops every
// row so tests can be strung together without cross-contamination.
func TestNativeQuality_ResetForTest(t *testing.T) {
	resetQuality(t)

	for i := 0; i < 20; i++ {
		RecordNativeAccountQuality(1001, "gemini-3.6-flash", time.Second, true)
	}
	if NativeAccountQualityPenalty(1001, "gemini-3.6-flash") == 0 {
		t.Fatal("pre-reset penalty should be non-zero")
	}
	ResetNativeAccountQualityForTest()
	if got := NativeAccountQualityPenalty(1001, "gemini-3.6-flash"); got != 0 {
		t.Fatalf("post-reset penalty = %v, want 0", got)
	}
	if got := NativeAccountLatencyEWMA(1001, "gemini-3.6-flash"); got != 0 {
		t.Fatalf("post-reset latency = %v, want 0", got)
	}
}
