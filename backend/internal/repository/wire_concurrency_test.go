package repository

import (
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestProvideConcurrencyCache_CeilsWaitTTLSeconds(t *testing.T) {
	tests := []struct {
		name     string
		sticky   time.Duration
		fallback time.Duration
		want     int
	}{
		{name: "sub-second", sticky: 500 * time.Millisecond, want: 1},
		{name: "fractional second", sticky: 1500 * time.Millisecond, want: 2},
		{name: "larger fallback", sticky: time.Second, fallback: 2500 * time.Millisecond, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.ConcurrencySlotTTLMinutes = 15
			cfg.Gateway.Scheduling.StickySessionWaitTimeout = tt.sticky
			cfg.Gateway.Scheduling.FallbackWaitTimeout = tt.fallback
			rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
			t.Cleanup(func() { _ = rdb.Close() })
			cache := ProvideConcurrencyCache(rdb, cfg).(*concurrencyCache)
			require.Equal(t, tt.want, cache.waitQueueTTLSeconds)
		})
	}
}
