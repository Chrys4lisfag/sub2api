// Package metrics implements the Unleash feature-flag client traffic that
// agy.exe sends every 60 seconds to antigravity-unleash.goog.
//
// Running this in the background mimics agy's organic traffic signal:
//   - POST /api/client/register on startup (or re-register on 401)
//   - GET  /api/client/features every 60s; parse + locally evaluate each flag
//   - POST /api/client/metrics every 60s with per-flag yes/no/variant counts
//
// Per-account separation: pass a stable Options.InstanceID + ConnectionID
// from auth.Tokens so every account on one server looks like its own host.
package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	I "github.com/koval/agymimic/internal"
)

// Client is one Unleash session for one Antigravity account.
type Client struct {
	httpc        *http.Client
	connectionID string
	instanceID   string
	registered   atomic.Bool

	mu      sync.Mutex
	flags   map[string]flagCounter
	knownFlags []featureFlag
	startedAt time.Time
	cancel  context.CancelFunc
	rng     *rand.Rand
	interval   time.Duration
}

type flagCounter struct {
	Yes      int            `json:"yes"`
	No       int            `json:"no"`
	Variants map[string]int `json:"variants"`
}

// featureFlag is the agy/Unleash-evaluated shape (one entry from
// `GET /api/client/features`). We only consume the fields we need.
type featureFlag struct {
	Name      string `json:"name"`
	Enabled   bool   `json:"enabled"`
	Type      string `json:"type,omitempty"`
	Variants  []struct {
		Name   string `json:"name"`
		Weight int    `json:"weight"`
	} `json:"variants,omitempty"`
}

type featuresPayload struct {
	Features []featureFlag `json:"features"`
}

// Options for New().
type Options struct {
	HTTPClient *http.Client

	// InstanceID — `unleash-instanceid` HTTP header + body field. Default
	// leaks "<HOSTNAME>\<USER>-<HOSTNAME>" (real machine, real user). On a
	// multi-account server, set this per account so each account looks
	// like a separate Windows host. Use auth.NewFakeInstanceLabel().
	InstanceID string

	// ConnectionID — per-session UUID for Unleash. Default = fresh UUID.
	// Persist it alongside Tokens so the account stays "the same daemon"
	// across restarts instead of looking new every time.
	ConnectionID string

	// Interval overrides the default 60s tick. Must match what agy uses
	// (Unleash interval field tells the server how often to expect us).
	Interval time.Duration
}

func New(opts Options) *Client {
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	inst := opts.InstanceID
	if inst == "" {
		host, _ := os.Hostname()
		user := os.Getenv("USERNAME")
		if user == "" {
			user = os.Getenv("USER")
		}
		inst = fmt.Sprintf("%s\\%s-%s", host, user, host)
	}
	connID := opts.ConnectionID
	if connID == "" {
		connID = uuid.NewString()
	}
	interval := opts.Interval
	if interval == 0 {
		interval = time.Duration(I.UnleashInterval) * time.Second
	}
	return &Client{
		httpc:        hc,
		connectionID: connID,
		instanceID:   inst,
		flags:        map[string]flagCounter{},
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
		interval:     interval,
	}
}

// Track records a flag evaluation manually. Usually you don't need this —
// the background loop tracks every flag returned by /features automatically.
func (c *Client) Track(flag string, yes bool, variant string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.flags[flag]
	if f.Variants == nil {
		f.Variants = map[string]int{}
	}
	if yes {
		f.Yes++
	} else {
		f.No++
	}
	if variant != "" {
		f.Variants[variant]++
	}
	c.flags[flag] = f
}

// Start kicks off the background loop. Call Cancel() to stop.
func (c *Client) Start(ctx context.Context) {
	ctx, c.cancel = context.WithCancel(ctx)
	c.startedAt = time.Now()
	go func() {
		// First tick: register, fetch features, fake-evaluate them.
		c.register(ctx)
		c.refreshFeatures(ctx)
		c.evaluateAll()

		t := time.NewTicker(c.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if !c.registered.Load() {
					c.register(ctx)
				}
				c.refreshFeatures(ctx)
				c.evaluateAll()
				c.postMetrics(ctx)
			}
		}
	}()
}

func (c *Client) Cancel() {
	if c.cancel != nil {
		c.cancel()
	}
}

// InstanceID returns the configured instance string (handy for logging).
func (c *Client) InstanceID() string { return c.instanceID }

// ConnectionID returns the persisted connection UUID.
func (c *Client) ConnectionID() string { return c.connectionID }

func (c *Client) commonHeaders(req *http.Request) {
	req.Header.Set("Authorization", I.UnleashAuthHeader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("User-Agent", "unleash-client-go-v4")
	req.Header.Set("unleash-appname", I.UnleashAppName)
	req.Header.Set("unleash-instanceid", c.instanceID)
	req.Header.Set("unleash-connection-id", c.connectionID)
	req.Header.Set("unleash-sdk", I.UnleashSDKVersion)
}

func (c *Client) register(ctx context.Context) {
	body := map[string]any{
		"appName":          I.UnleashAppName,
		"instanceId":       c.instanceID,
		"connectionId":     c.connectionID,
		"sdkVersion":       I.UnleashSDKVersion,
		"strategies":       I.UnleashStrategies,
		"started":          time.Now().Format(time.RFC3339Nano),
		"interval":         I.UnleashInterval,
		"platformVersion":  I.LatestGoVersion(),
		"platformName":     I.UnleashPlatformName,
		"yggdrasilVersion": nil,
		"specVersion":      I.UnleashSpecVersion,
	}
	buf, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, I.UnleashHost+"/api/client/register", bytes.NewReader(buf))
	c.commonHeaders(req)
	resp, err := c.httpc.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
		c.registered.Store(true)
	}
}

func (c *Client) refreshFeatures(ctx context.Context) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, I.UnleashHost+"/api/client/features", nil)
	c.commonHeaders(req)
	resp, err := c.httpc.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 401 {
		c.registered.Store(false)
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var p featuresPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return
	}
	c.mu.Lock()
	c.knownFlags = p.Features
	c.mu.Unlock()
}

// evaluateAll runs over every known feature flag once and "decides" its
// value the way an Unleash SDK would. We just need plausible counters so
// the metrics POST looks organic; precise gradualRollout math isn't required.
func (c *Client) evaluateAll() {
	c.mu.Lock()
	flags := c.knownFlags
	c.mu.Unlock()
	if len(flags) == 0 {
		return
	}
	for _, f := range flags {
		yes := f.Enabled
		// Pick a variant — weighted random or "disabled".
		variant := ""
		if yes && len(f.Variants) > 0 {
			total := 0
			for _, v := range f.Variants {
				if v.Weight > 0 {
					total += v.Weight
				}
			}
			if total > 0 {
				c.mu.Lock()
				pick := c.rng.Intn(total)
				c.mu.Unlock()
				acc := 0
				for _, v := range f.Variants {
					acc += v.Weight
					if pick < acc {
						variant = v.Name
						break
					}
				}
			}
		}
		c.Track(f.Name, yes, variant)
	}
}

func (c *Client) postMetrics(ctx context.Context) {
	c.mu.Lock()
	toggles := c.flags
	c.flags = map[string]flagCounter{}
	c.mu.Unlock()
	if len(toggles) == 0 {
		return
	}

	bucket := map[string]any{
		"start":   time.Now().Add(-time.Duration(I.UnleashInterval) * time.Second).Format(time.RFC3339Nano),
		"stop":    time.Now().Format(time.RFC3339Nano),
		"toggles": toggles,
	}
	body := map[string]any{
		"appName":          I.UnleashAppName,
		"instanceId":       c.instanceID,
		"connectionId":     c.connectionID,
		"bucket":           bucket,
		"platformVersion":  I.LatestGoVersion(),
		"platformName":     I.UnleashPlatformName,
		"yggdrasilVersion": nil,
		"sdkVersion":       I.UnleashSDKVersion,
		"specVersion":      I.UnleashSpecVersion,
	}
	buf, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, I.UnleashHost+"/api/client/metrics", bytes.NewReader(buf))
	c.commonHeaders(req)
	resp, err := c.httpc.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
	if resp.StatusCode == 401 {
		c.registered.Store(false)
	}
}
