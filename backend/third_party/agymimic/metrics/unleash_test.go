package metrics

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	I "github.com/koval/agymimic/internal"
)

// fakeUnleash captures all requests an Unleash client sends and serves a
// minimal /features response with one boolean flag + one variant flag.
type fakeUnleash struct {
	mu         sync.Mutex
	register   atomic.Int32
	features   atomic.Int32
	metrics    atomic.Int32
	registerBody []map[string]any
	metricsBody  []map[string]any
	server     *httptest.Server
}

func newFakeUnleash(t *testing.T) *fakeUnleash {
	t.Helper()
	f := &fakeUnleash{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/client/register", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var b map[string]any
		_ = json.Unmarshal(raw, &b)
		f.mu.Lock()
		f.registerBody = append(f.registerBody, b)
		f.mu.Unlock()
		f.register.Add(1)
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/api/client/features", func(w http.ResponseWriter, r *http.Request) {
		f.features.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"features": [
				{"name":"enable-mendel","enabled":true,"type":"release"},
				{"name":"cascade-enable-messaging","enabled":true,"type":"release"},
				{"name":"browser-subagent-model","enabled":true,"type":"experiment",
				 "variants":[{"name":"fiercefalcon","weight":700},{"name":"disabled","weight":300}]}
			]
		}`))
	})
	mux.HandleFunc("/api/client/metrics", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var b map[string]any
		_ = json.Unmarshal(raw, &b)
		f.mu.Lock()
		f.metricsBody = append(f.metricsBody, b)
		f.mu.Unlock()
		f.metrics.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	f.server = httptest.NewServer(mux)
	return f
}

func (f *fakeUnleash) Close() { f.server.Close() }

// withHost swaps the production Unleash host out for our mock and returns a
// restore func that reverts to the original constant.
//
// We use a func var indirection trick: the production code reads
// `I.UnleashHost`, which is a const. We can't reassign it. Instead, we
// build a Client that points at the mock by passing an HTTP client whose
// Transport rewrites the Host header. Simpler: do the swap at request level
// by configuring an http.Client that redirects all traffic.
func patchedClient(target string) *http.Client {
	return &http.Client{
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			// rewrite Host → target
			if strings.HasPrefix(r.URL.String(), I.UnleashHost) {
				newURL := target + strings.TrimPrefix(r.URL.String(), I.UnleashHost)
				newReq, _ := http.NewRequestWithContext(r.Context(), r.Method, newURL, r.Body)
				newReq.Header = r.Header.Clone()
				return http.DefaultTransport.RoundTrip(newReq)
			}
			return http.DefaultTransport.RoundTrip(r)
		}),
		Timeout: 5 * time.Second,
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)
func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestUnleashLoop_WireShape(t *testing.T) {
	fake := newFakeUnleash(t)
	defer fake.Close()

	c := New(Options{
		HTTPClient:   patchedClient(fake.server.URL),
		InstanceID:   "DESKTOP-TESTXX\\testuser-DESKTOP-TESTXX",
		ConnectionID: "fixed-conn-uuid-1234",
		Interval:     500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	// Wait long enough for: initial register + features fetch + 2 metrics ticks
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fake.register.Load() >= 1 && fake.features.Load() >= 2 && fake.metrics.Load() >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	time.Sleep(100 * time.Millisecond)

	if fake.register.Load() < 1 {
		t.Fatalf("register never called")
	}
	if fake.features.Load() < 2 {
		t.Fatalf("features called %d times, expected >=2", fake.features.Load())
	}
	if fake.metrics.Load() < 1 {
		t.Fatalf("metrics never POSTed (got %d)", fake.metrics.Load())
	}

	// --- shape of register body ---
	fake.mu.Lock()
	rb := fake.registerBody[0]
	fake.mu.Unlock()
	for _, want := range []string{"appName", "instanceId", "connectionId", "sdkVersion", "strategies", "started", "interval", "platformName", "specVersion"} {
		if _, ok := rb[want]; !ok {
			t.Errorf("register body missing field %q", want)
		}
	}
	if got, _ := rb["instanceId"].(string); got != "DESKTOP-TESTXX\\testuser-DESKTOP-TESTXX" {
		t.Errorf("register.instanceId = %q, want fake one", got)
	}
	if got, _ := rb["connectionId"].(string); got != "fixed-conn-uuid-1234" {
		t.Errorf("register.connectionId = %q", got)
	}
	if got, _ := rb["appName"].(string); got != I.UnleashAppName {
		t.Errorf("register.appName = %q", got)
	}

	// --- shape of metrics body ---
	fake.mu.Lock()
	mb := fake.metricsBody[0]
	fake.mu.Unlock()
	bucket, _ := mb["bucket"].(map[string]any)
	if bucket == nil {
		t.Fatalf("metrics body missing bucket")
	}
	toggles, _ := bucket["toggles"].(map[string]any)
	if len(toggles) == 0 {
		t.Errorf("metrics body has no toggles — flag eval didn't run")
	}
	// Must have evaluated all 3 features we served
	for _, fl := range []string{"enable-mendel", "cascade-enable-messaging", "browser-subagent-model"} {
		if _, ok := toggles[fl]; !ok {
			t.Errorf("metrics toggles missing flag %q", fl)
		}
	}
	// Variant flag must have variant counters populated
	if bs, ok := toggles["browser-subagent-model"].(map[string]any); ok {
		vs, _ := bs["variants"].(map[string]any)
		if len(vs) == 0 {
			t.Errorf("browser-subagent-model variant counters empty")
		}
	}
	t.Logf("verified — register=%d features=%d metrics=%d  inst=%s conn=%s",
		fake.register.Load(), fake.features.Load(), fake.metrics.Load(),
		c.InstanceID(), c.ConnectionID())
}

func TestUnleashHeaders(t *testing.T) {
	fake := newFakeUnleash(t)
	defer fake.Close()

	var hdrCapture http.Header
	var captureOnce sync.Once
	intercept := &http.Client{
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if strings.HasPrefix(r.URL.String(), I.UnleashHost) && strings.HasSuffix(r.URL.Path, "/register") {
				captureOnce.Do(func() { hdrCapture = r.Header.Clone() })
				newURL := fake.server.URL + r.URL.Path
				newReq, _ := http.NewRequestWithContext(r.Context(), r.Method, newURL, r.Body)
				newReq.Header = r.Header.Clone()
				return http.DefaultTransport.RoundTrip(newReq)
			}
			newURL := fake.server.URL + r.URL.Path
			newReq, _ := http.NewRequestWithContext(r.Context(), r.Method, newURL, r.Body)
			newReq.Header = r.Header.Clone()
			return http.DefaultTransport.RoundTrip(newReq)
		}),
		Timeout: 3 * time.Second,
	}
	c := New(Options{
		HTTPClient:   intercept,
		InstanceID:   "HOST\\u-HOST",
		ConnectionID: "conn-id-x",
		Interval:     200 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	time.Sleep(800 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)

	for _, h := range []string{"Authorization", "unleash-appname", "unleash-instanceid", "unleash-connection-id", "unleash-sdk"} {
		if hdrCapture.Get(h) == "" {
			t.Errorf("missing header %q on register", h)
		}
	}
	if got := hdrCapture.Get("unleash-instanceid"); got != "HOST\\u-HOST" {
		t.Errorf("unleash-instanceid = %q", got)
	}
	if got := hdrCapture.Get("unleash-connection-id"); got != "conn-id-x" {
		t.Errorf("unleash-connection-id = %q", got)
	}
	if !strings.HasPrefix(hdrCapture.Get("Authorization"), "*:production.") {
		t.Errorf("Authorization wrong: %q", hdrCapture.Get("Authorization"))
	}
}
