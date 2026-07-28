package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/geminicli"
	"github.com/stretchr/testify/require"
)

type driveSuppressMockClient struct {
	getStorageQuotaFunc func(context.Context, string, string) (*geminicli.DriveStorageInfo, error)
}

func (m *driveSuppressMockClient) GetStorageQuota(ctx context.Context, accessToken, proxyURL string) (*geminicli.DriveStorageInfo, error) {
	return m.getStorageQuotaFunc(ctx, accessToken, proxyURL)
}

func TestFetchGoogleOneTier_DeduplicatesWarningsWithoutSkippingProbes(t *testing.T) {
	scopeErr := &geminicli.DriveAPIError{StatusCode: 403, Body: `{"error":{"message":"Request had insufficient authentication scopes."}}`}
	var calls atomic.Int32
	svc := &GeminiOAuthService{
		driveClient: &driveSuppressMockClient{getStorageQuotaFunc: func(context.Context, string, string) (*geminicli.DriveStorageInfo, error) {
			calls.Add(1)
			return nil, scopeErr
		}},
		driveScope403: newDriveScope403Suppressor(),
	}

	_, _, firstErr := svc.FetchGoogleOneTier(context.Background(), "token-a", "proxy-a")
	_, _, secondErr := svc.FetchGoogleOneTier(context.Background(), "token-a", "proxy-a")
	require.Same(t, scopeErr, firstErr)
	require.Same(t, scopeErr, secondErr)
	require.Equal(t, int32(2), calls.Load(), "warning dedupe must not replace upstream probes")

	fingerprint := driveScope403Fingerprint("token-a", "proxy-a")
	require.True(t, svc.driveScope403.suppressed(fingerprint))
	require.False(t, svc.driveScope403.record403(fingerprint), "repeat 403 warning must be suppressed")
}

func TestDriveScope403Suppressor_ConcurrentWarningAdmission(t *testing.T) {
	s := newDriveScope403Suppressor()
	fingerprint := driveScope403Fingerprint("token", "proxy")
	const workers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	var admitted atomic.Int32
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			<-start
			if s.record403(fingerprint) {
				admitted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	require.Equal(t, int32(1), admitted.Load())
}

func TestDriveScope403Suppressor_ExpiresAndIsolatesProxy(t *testing.T) {
	now := time.Unix(1000, 0)
	s := newDriveScope403Suppressor()
	s.ttl = time.Minute
	s.nowFn = func() time.Time { return now }
	proxyA := driveScope403Fingerprint("token", "proxy-a")
	proxyB := driveScope403Fingerprint("token", "proxy-b")

	require.True(t, s.record403(proxyA))
	require.True(t, s.suppressed(proxyA))
	require.False(t, s.suppressed(proxyB))

	now = now.Add(time.Minute)
	require.False(t, s.suppressed(proxyA))
	require.True(t, s.record403(proxyA))
}

func TestFetchGoogleOneTier_DoesNotCacheNon403(t *testing.T) {
	upstreamErr := errors.New("temporary network failure")
	var calls atomic.Int32
	svc := &GeminiOAuthService{
		driveClient: &driveSuppressMockClient{getStorageQuotaFunc: func(context.Context, string, string) (*geminicli.DriveStorageInfo, error) {
			calls.Add(1)
			return nil, upstreamErr
		}},
		driveScope403: newDriveScope403Suppressor(),
	}

	for range 2 {
		_, _, err := svc.FetchGoogleOneTier(context.Background(), "token-a", "proxy-a")
		require.Same(t, upstreamErr, err)
	}
	require.Equal(t, int32(2), calls.Load())
	require.False(t, svc.driveScope403.suppressed(driveScope403Fingerprint("token-a", "proxy-a")))
}

func TestFetchGoogleOneTier_UnlimitedQuotaIsUltra(t *testing.T) {
	svc := &GeminiOAuthService{
		driveClient: &driveSuppressMockClient{getStorageQuotaFunc: func(context.Context, string, string) (*geminicli.DriveStorageInfo, error) {
			return &geminicli.DriveStorageInfo{Usage: 42, Unlimited: true}, nil
		}},
		driveScope403: newDriveScope403Suppressor(),
	}
	tier, storage, err := svc.FetchGoogleOneTier(context.Background(), "token", "")
	require.NoError(t, err)
	require.Equal(t, GeminiTierGoogleAIUltra, tier)
	require.True(t, storage.Unlimited)
}
