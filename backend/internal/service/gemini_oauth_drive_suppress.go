package service

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

const (
	driveScope403DedupeTTL  = 30 * time.Minute
	driveScope403MaxEntries = 4096
)

type driveScope403Entry struct {
	firstSeenAt time.Time
}

// driveScope403Suppressor deduplicates confirmed Drive scope 403 warnings per
// access-token state. It never replaces or suppresses an upstream request or
// error; callers still observe each current probe result.
type driveScope403Suppressor struct {
	mu      sync.Mutex
	entries map[string]driveScope403Entry
	ttl     time.Duration
	maxSize int
	nowFn   func() time.Time
}

func newDriveScope403Suppressor() *driveScope403Suppressor {
	return &driveScope403Suppressor{
		entries: make(map[string]driveScope403Entry),
		ttl:     driveScope403DedupeTTL,
		maxSize: driveScope403MaxEntries,
		nowFn:   time.Now,
	}
}

func (s *driveScope403Suppressor) now() time.Time {
	if s == nil || s.nowFn == nil {
		return time.Now()
	}
	return s.nowFn()
}

// record403 reports whether a confirmed 403 should emit its warning.
func (s *driveScope403Suppressor) record403(fingerprint string) bool {
	if s == nil || fingerprint == "" {
		return true
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.entries[fingerprint]; ok && now.Sub(entry.firstSeenAt) < s.ttl {
		return false
	}
	s.evictLocked(now)
	s.entries[fingerprint] = driveScope403Entry{firstSeenAt: now}
	return true
}

func (s *driveScope403Suppressor) suppressed(fingerprint string) bool {
	if s == nil || fingerprint == "" {
		return false
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[fingerprint]
	if !ok {
		return false
	}
	if now.Sub(entry.firstSeenAt) >= s.ttl {
		delete(s.entries, fingerprint)
		return false
	}
	return true
}

func (s *driveScope403Suppressor) invalidate(fingerprint string) {
	if s == nil || fingerprint == "" {
		return
	}
	s.mu.Lock()
	delete(s.entries, fingerprint)
	s.mu.Unlock()
}

func (s *driveScope403Suppressor) evictLocked(now time.Time) {
	for key, entry := range s.entries {
		if now.Sub(entry.firstSeenAt) >= s.ttl {
			delete(s.entries, key)
		}
	}
	for len(s.entries) >= s.maxSize {
		for key := range s.entries {
			delete(s.entries, key)
			break
		}
	}
}

func driveScope403Fingerprint(accessToken, proxyURL string) string {
	if accessToken == "" {
		return ""
	}
	return driveTokenFingerprint(accessToken + "\x00" + proxyURL)
}

func driveTokenFingerprint(accessToken string) string {
	if accessToken == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(accessToken))
	return hex.EncodeToString(sum[:8])
}
