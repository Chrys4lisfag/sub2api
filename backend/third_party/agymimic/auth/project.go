package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	I "github.com/koval/agymimic/internal"
)

// DiscoverProject walks the loadCodeAssist + onboardUser chain that agy.exe
// executes on first launch to resolve a cloudaicompanionProject for the user.
// Returns (projectID, tierID, error).
//
// preferProject — pass an existing managed project ID hint, or "" to let the
// backend pick.
func DiscoverProject(ctx context.Context, accessToken string, preferProject string) (string, string, error) {
	// 1) try loadCodeAssist on each fallback endpoint
	for _, base := range I.LoadEndpoints {
		payload, err := loadCodeAssist(ctx, base, accessToken, preferProject)
		if err != nil {
			continue
		}
		if pid := extractProjectID(payload); pid != "" {
			tier := ""
			if t, ok := payload["currentTier"].(map[string]any); ok {
				if id, _ := t["id"].(string); id != "" {
					tier = id
				}
			}
			return pid, tier, nil
		}

		// 2) loadCodeAssist returned no project — try onboardUser
		tierID := defaultTier(payload)
		if pid, err := onboardUser(ctx, base, accessToken, tierID, preferProject); err == nil && pid != "" {
			return pid, tierID, nil
		}
	}
	return "", "", errors.New("loadCodeAssist + onboardUser both failed across all endpoints")
}

func loadCodeAssist(ctx context.Context, base, accessToken, preferProject string) (map[string]any, error) {
	body := map[string]any{
		"metadata": metadataMap(preferProject),
	}
	buf, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+I.PathLoadCodeAssist, bytes.NewReader(buf))
	I.SetLoadCodeAssistHeaders(req, accessToken, "")

	c := &http.Client{Timeout: 15 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("loadCodeAssist %s: %d: %s", base, resp.StatusCode, string(raw))
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func onboardUser(ctx context.Context, base, accessToken, tierID, preferProject string) (string, error) {
	body := map[string]any{
		"tierId":   tierID,
		"metadata": metadataMap(preferProject),
	}
	buf, _ := json.Marshal(body)

	// onboardUser is a polling LRO — poll up to 10 times with 5s delays.
	for attempt := 0; attempt < 10; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+I.PathOnboardUser, bytes.NewReader(buf))
		I.SetLoadCodeAssistHeaders(req, accessToken, "")
		c := &http.Client{Timeout: 30 * time.Second}
		resp, err := c.Do(req)
		if err != nil {
			return "", err
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return "", fmt.Errorf("onboardUser: %d: %s", resp.StatusCode, string(raw))
		}
		var out struct {
			Done     bool `json:"done"`
			Response struct {
				CloudAICompanionProject struct {
					ID string `json:"id"`
				} `json:"cloudaicompanionProject"`
			} `json:"response"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return "", err
		}
		if out.Done {
			if id := out.Response.CloudAICompanionProject.ID; id != "" {
				return id, nil
			}
			if preferProject != "" {
				return preferProject, nil
			}
			return "", errors.New("onboardUser done with no project")
		}
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "", errors.New("onboardUser: gave up after 10 polls")
}

func metadataMap(projectID string) map[string]any {
	// agy.exe live capture: body is literally {"metadata":{"ideType":"ANTIGRAVITY"}}.
	// Backend's ClientMetadata.Platform enum rejects free-form "WINDOWS"/"MACOS"
	// strings — fields must be omitted or use the enum numeric form.
	m := map[string]any{"ideType": "ANTIGRAVITY"}
	if projectID != "" {
		m["duetProject"] = projectID
	}
	return m
}

func extractProjectID(payload map[string]any) string {
	if v, ok := payload["cloudaicompanionProject"].(string); ok && v != "" {
		return v
	}
	if v, ok := payload["cloudaicompanionProject"].(map[string]any); ok {
		if id, _ := v["id"].(string); id != "" {
			return id
		}
	}
	return ""
}

func defaultTier(payload map[string]any) string {
	tiers, ok := payload["allowedTiers"].([]any)
	if !ok || len(tiers) == 0 {
		return "FREE"
	}
	for _, t := range tiers {
		m, ok := t.(map[string]any)
		if !ok {
			continue
		}
		if def, _ := m["isDefault"].(bool); def {
			if id, _ := m["id"].(string); id != "" {
				return id
			}
		}
	}
	if m, ok := tiers[0].(map[string]any); ok {
		if id, _ := m["id"].(string); id != "" {
			return id
		}
	}
	return "FREE"
}
