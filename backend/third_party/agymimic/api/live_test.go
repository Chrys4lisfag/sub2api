package api_test

// Live integration tests against real daily-cloudcode-pa.sandbox.googleapis.com.
// Skipped unless AGYMIMIC_LIVE=1 and a tokens file exists at the default path.
//
// Run with:
//   AGYMIMIC_LIVE=1 go test ./api -run Live -v

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/koval/agymimic/api"
	"github.com/koval/agymimic/auth"
	I "github.com/koval/agymimic/internal"
	"github.com/koval/agymimic/types"
)

func loadLive(t *testing.T) *api.Client {
	t.Helper()
	if os.Getenv("AGYMIMIC_LIVE") == "" {
		t.Skip("AGYMIMIC_LIVE not set — skipping live test")
	}
	credsPath := filepath.Join(os.Getenv("USERPROFILE"), ".config", "agymimic", "tokens.json")
	if h, _ := os.UserHomeDir(); h != "" {
		credsPath = filepath.Join(h, ".config", "agymimic", "tokens.json")
	}
	raw, err := os.ReadFile(credsPath)
	if err != nil {
		t.Skipf("no tokens at %s — run `agycli login`", credsPath)
	}
	var tokens auth.Tokens
	if err := json.Unmarshal(raw, &tokens); err != nil {
		t.Fatalf("parse tokens: %v", err)
	}
	ctx := context.Background()
	if time.Now().After(tokens.ExpiresAt) {
		if err := auth.Refresh(ctx, &tokens); err != nil {
			t.Fatalf("refresh: %v", err)
		}
	}
	if tokens.ProjectID == "" {
		pid, tier, err := auth.DiscoverProject(ctx, tokens.AccessToken, "")
		if err != nil {
			t.Fatalf("project discovery: %v", err)
		}
		tokens.ProjectID = pid
		tokens.TierID = tier
	}
	_, _ = I.RefreshAntigravityVersion(ctx)
	return api.New(&tokens)
}

func collectStream(t *testing.T, ch <-chan api.StreamEvent) string {
	t.Helper()
	var sb strings.Builder
	for ev := range ch {
		if ev.Err != nil {
			t.Logf("stream-err: %v", ev.Err)
			continue
		}
		for _, c := range ev.Resp.Response.Candidates {
			for _, p := range c.Content.Parts {
				if p.Text != "" && !p.Thought {
					sb.WriteString(p.Text)
				}
			}
		}
	}
	return sb.String()
}

func TestLive_SingleTurn(t *testing.T) {
	cli := loadLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ch, err := cli.Stream(ctx, "gemini-3-flash", types.GenerateInner{
		Contents: []types.Content{
			{Role: "user", Parts: []types.Part{{Text: "Reply with exactly: SINGLE-TURN-OK"}}},
		},
		GenerationConfig: &types.GenerationConfig{MaxOutputTokens: 500},
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	out := collectStream(t, ch)
	t.Logf("response: %q", out)
	if !strings.Contains(out, "SINGLE-TURN-OK") {
		t.Errorf("expected SINGLE-TURN-OK in response, got: %q", out)
	}
}

func TestLive_MultiTurn(t *testing.T) {
	cli := loadLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Turn 1
	history := []types.Content{
		{Role: "user", Parts: []types.Part{{Text: "My favorite number is 42. Just acknowledge with: NOTED"}}},
	}
	ch, err := cli.Stream(ctx, "gemini-3-flash", types.GenerateInner{
		Contents:         history,
		GenerationConfig: &types.GenerationConfig{MaxOutputTokens: 500},
	})
	if err != nil {
		t.Fatalf("turn1 stream: %v", err)
	}
	turn1 := collectStream(t, ch)
	t.Logf("turn1: %q", turn1)

	// Add model's reply to history
	history = append(history, types.Content{Role: "model", Parts: []types.Part{{Text: turn1}}})
	// Turn 2 — reference earlier context
	history = append(history, types.Content{Role: "user", Parts: []types.Part{{Text: "What did I say was my favorite number? Reply with just the number."}}})

	ch2, err := cli.Stream(ctx, "gemini-3-flash", types.GenerateInner{
		Contents:         history,
		GenerationConfig: &types.GenerationConfig{MaxOutputTokens: 500},
	})
	if err != nil {
		t.Fatalf("turn2 stream: %v", err)
	}
	turn2 := collectStream(t, ch2)
	t.Logf("turn2: %q", turn2)
	if !strings.Contains(turn2, "42") {
		t.Errorf("model did not recall favorite number 42 — got: %q", turn2)
	}
}

func TestLive_CountTokens(t *testing.T) {
	cli := loadLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n, err := cli.CountTokens(ctx, "gemini-3-flash", types.GenerateInner{
		Contents: []types.Content{
			{Role: "user", Parts: []types.Part{{Text: "Hello world from agymimic, count my tokens please."}}},
		},
	})
	if err != nil {
		t.Fatalf("countTokens: %v", err)
	}
	t.Logf("prompt tokens: %d", n)
	if n <= 0 || n > 100 {
		t.Errorf("unexpected token count: %d", n)
	}
}

func TestLive_ListExperiments(t *testing.T) {
	cli := loadLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exps, err := cli.ListExperiments(ctx)
	if err != nil {
		t.Fatalf("listExperiments: %v", err)
	}
	if len(exps) == 0 {
		t.Error("listExperiments returned empty payload")
	}
	t.Logf("listExperiments keys: %v", keys(exps))
}

func TestLive_FetchModels(t *testing.T) {
	cli := loadLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	models, err := cli.FetchAvailableModels(ctx)
	if err != nil {
		t.Fatalf("fetchModels: %v", err)
	}
	if len(models) == 0 {
		t.Error("fetchAvailableModels returned empty")
	}
	for _, m := range models {
		if m.ID == "gemini-3-flash" {
			t.Logf("gemini-3-flash: display=%q recommended=%v thinking=%v budget=%d",
				m.DisplayName, m.Recommended, m.SupportsThinking, m.ThinkingBudget)
		}
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
