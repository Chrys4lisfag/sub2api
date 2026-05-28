package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	I "github.com/koval/agymimic/internal"
)

// ListExperiments is the startup call agy.exe fires right after :loadCodeAssist.
// Body is empty JSON object `{}`. Response is the experiment list the user is
// enrolled in (returned as opaque map). Calling it improves request organicity
// — CLIProxyAPI skips it; agy.exe does it.
func (c *Client) ListExperiments(ctx context.Context) (map[string]any, error) {
	if err := c.ensureFresh(ctx); err != nil {
		return nil, err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+I.PathListExperiments, bytes.NewReader([]byte("{}")))
	c.tokensMu.RLock()
	I.SetAntigravityHeaders(req, c.tokens.AccessToken, c.version)
	c.tokensMu.RUnlock()
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("listExperiments: %d: %s", resp.StatusCode, string(raw))
	}
	var out map[string]any
	json.Unmarshal(raw, &out)
	return out, nil
}

// ModelInfo is one entry from fetchAvailableModels.
type ModelInfo struct {
	ID                string         `json:"-"`                  // map key
	DisplayName       string         `json:"displayName"`
	Model             string         `json:"model"`              // internal MODEL_PLACEHOLDER_M*
	APIProvider       string         `json:"apiProvider"`
	ModelProvider     string         `json:"modelProvider"`
	MaxTokens         int            `json:"maxTokens"`
	MaxOutputTokens   int            `json:"maxOutputTokens"`
	ThinkingBudget    int            `json:"thinkingBudget"`
	MinThinkingBudget int            `json:"minThinkingBudget"`
	SupportsImages    bool           `json:"supportsImages"`
	SupportsThinking  bool           `json:"supportsThinking"`
	SupportsVideo     bool           `json:"supportsVideo"`
	Recommended       bool           `json:"recommended"`
	TagTitle          string         `json:"tagTitle,omitempty"`
	TagDescription    string         `json:"tagDescription,omitempty"`
	QuotaInfo         map[string]any `json:"quotaInfo,omitempty"`
	ModelExperiments  map[string]any `json:"modelExperiments,omitempty"`
}

// FetchAvailableModels asks the backend which models this account can use.
// agy.exe calls it on first chat-screen render.
// Response shape: {"models": {"<id>": {ModelInfo}, ...}}
func (c *Client) FetchAvailableModels(ctx context.Context) ([]ModelInfo, error) {
	if err := c.ensureFresh(ctx); err != nil {
		return nil, err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+I.PathFetchAvailableModels, bytes.NewReader([]byte("{}")))
	c.tokensMu.RLock()
	I.SetAntigravityHeaders(req, c.tokens.AccessToken, c.version)
	c.tokensMu.RUnlock()
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("fetchAvailableModels: %d: %s", resp.StatusCode, string(raw))
	}
	var out struct {
		Models map[string]ModelInfo `json:"models"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	res := make([]ModelInfo, 0, len(out.Models))
	for id, m := range out.Models {
		m.ID = id
		res = append(res, m)
	}
	return res, nil
}
