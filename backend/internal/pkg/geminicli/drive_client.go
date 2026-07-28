package geminicli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/httpclient"
)

// DriveStorageInfo represents Google Drive storage quota information
type DriveStorageInfo struct {
	Limit     int64 `json:"limit"`     // Storage limit in bytes; zero when Unlimited is true
	Usage     int64 `json:"usage"`     // Current usage in bytes
	Unlimited bool  `json:"unlimited"` // Drive omits limit for unlimited-storage accounts
}

// DriveAPIError preserves HTTP status and bounded Google error payload so
// callers can distinguish missing OAuth scope from unrelated 403 responses.
type DriveAPIError struct {
	StatusCode int
	Body       string
}

func (e *DriveAPIError) Error() string {
	return fmt.Sprintf("drive API error: status %d", e.StatusCode)
}

func (e *DriveAPIError) IsScopeUnavailable() bool {
	if e == nil || e.StatusCode != http.StatusForbidden {
		return false
	}
	body := strings.ToLower(e.Body)
	return strings.Contains(body, "access_token_scope_insufficient") ||
		strings.Contains(body, "insufficient authentication scopes") ||
		strings.Contains(body, "insufficientpermissions")
}

// DriveClient interface for Google Drive API operations
type DriveClient interface {
	GetStorageQuota(ctx context.Context, accessToken, proxyURL string) (*DriveStorageInfo, error)
}

type driveClient struct{}

// NewDriveClient creates a new Drive API client
func NewDriveClient() DriveClient {
	return &driveClient{}
}

// GetStorageQuota fetches storage quota from Google Drive API
func (c *driveClient) GetStorageQuota(ctx context.Context, accessToken, proxyURL string) (*DriveStorageInfo, error) {
	const driveAPIURL = "https://www.googleapis.com/drive/v3/about?fields=storageQuota"

	req, err := http.NewRequestWithContext(ctx, "GET", driveAPIURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+accessToken)

	// Get HTTP client with proxy support
	client, err := httpclient.GetClient(httpclient.Options{
		ProxyURL: proxyURL,
		Timeout:  10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	sleepWithContext := func(d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}

	// Retry logic with exponential backoff (+ jitter) for rate limits and transient failures
	var resp *http.Response
	maxRetries := 3
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for attempt := 0; attempt < maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("request cancelled: %w", ctx.Err())
		}

		resp, err = client.Do(req)
		if err != nil {
			// Network error retry
			if attempt < maxRetries-1 {
				backoff := time.Duration(1<<uint(attempt)) * time.Second
				jitter := time.Duration(rng.Intn(1000)) * time.Millisecond
				if err := sleepWithContext(backoff + jitter); err != nil {
					return nil, fmt.Errorf("request cancelled: %w", err)
				}
				continue
			}
			return nil, fmt.Errorf("network error after %d attempts: %w", maxRetries, err)
		}

		// Success
		if resp.StatusCode == http.StatusOK {
			break
		}

		// Retry 429, 500, 502, 503 with exponential backoff + jitter
		if (resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusInternalServerError ||
			resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusServiceUnavailable) && attempt < maxRetries-1 {
			if err := func() error {
				defer func() { _ = resp.Body.Close() }()
				backoff := time.Duration(1<<uint(attempt)) * time.Second
				jitter := time.Duration(rng.Intn(1000)) * time.Millisecond
				return sleepWithContext(backoff + jitter)
			}(); err != nil {
				return nil, fmt.Errorf("request cancelled: %w", err)
			}
			continue
		}

		break
	}

	if resp == nil {
		return nil, fmt.Errorf("request failed: no response received")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		_ = resp.Body.Close()
		statusText := http.StatusText(resp.StatusCode)
		if statusText == "" {
			statusText = resp.Status
		}
		fmt.Printf("[DriveClient] Drive API error: status=%d, msg=%s\n", resp.StatusCode, statusText)
		return nil, &DriveAPIError{StatusCode: resp.StatusCode, Body: string(body)}
	}

	defer func() { _ = resp.Body.Close() }()

	return decodeDriveStorageQuota(resp.Body)
}

func decodeDriveStorageQuota(r io.Reader) (*DriveStorageInfo, error) {
	type storageQuota struct {
		Limit json.RawMessage `json:"limit"`
		Usage json.RawMessage `json:"usage"`
	}
	var result struct {
		StorageQuota *storageQuota `json:"storageQuota"`
	}
	if err := json.NewDecoder(r).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode Drive storage quota: %w", err)
	}
	if result.StorageQuota == nil {
		return nil, fmt.Errorf("Drive response missing storageQuota object")
	}

	usage, err := parseDriveQuotaValue("usage", result.StorageQuota.Usage)
	if err != nil {
		return nil, err
	}
	if len(result.StorageQuota.Limit) == 0 {
		return &DriveStorageInfo{Usage: usage, Unlimited: true}, nil
	}
	limit, err := parseDriveQuotaValue("limit", result.StorageQuota.Limit)
	if err != nil {
		return nil, err
	}
	return &DriveStorageInfo{Limit: limit, Usage: usage}, nil
}

func parseDriveQuotaValue(field string, raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("Drive storage quota missing %s", field)
	}

	value := strings.TrimSpace(string(raw))
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		var quoted string
		if err := json.Unmarshal(raw, &quoted); err != nil {
			return 0, fmt.Errorf("Drive storage quota %s is malformed: %w", field, err)
		}
		value = quoted
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Drive storage quota %s is invalid: %w", field, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("Drive storage quota %s must be non-negative", field)
	}
	return parsed, nil
}
