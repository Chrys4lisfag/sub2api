// agycli — reference CLI for the agymimic SDK. Subcommands:
//   login              — OAuth login, save tokens to ~/.config/agymimic/tokens.json
//   chat <prompt...>   — send one chat turn, stream the response to stdout
//   chat -m MODEL ...  — pick model (gemini-3-pro-high default)
//   probe              — show what we know about the account (project, models, experiments)
//
// Usage examples:
//   agycli login
//   agycli chat "what is 2+2?"
//   agycli -m claude-opus-4-6-thinking chat "explain quantum tunneling"
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/koval/agymimic/api"
	"github.com/koval/agymimic/auth"
	"github.com/koval/agymimic/metrics"
	I "github.com/koval/agymimic/internal"
	"github.com/koval/agymimic/types"
)

func main() {
	model := flag.String("m", "gemini-3-flash", "model id (gemini-3-flash | gemini-3.1-pro-low | claude-sonnet-4-6 | claude-opus-4-6-thinking | …)")
	tempF := flag.Float64("t", 0, "temperature (0 = default)")
	maxTok := flag.Int("max", 0, "max output tokens (0 = default; auto-bumped for thinking models)")
	think := flag.Int("think", 0, "thinking budget (0 = none)")
	tokensPath := flag.String("creds", defaultCredsPath(), "path to saved tokens.json")
	noMetrics := flag.Bool("no-metrics", false, "skip Unleash background metrics")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	cmd, rest := args[0], args[1:]

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if v, err := I.RefreshAntigravityVersion(ctx); err == nil {
		fmt.Fprintf(os.Stderr, "[agymimic] antigravity version = %s\n", v)
	}

	switch cmd {
	case "login":
		mustNoErr(cmdLogin(ctx, *tokensPath))
	case "chat":
		if len(rest) == 0 {
			fmt.Fprintln(os.Stderr, "chat: prompt required")
			os.Exit(2)
		}
		mustNoErr(cmdChat(ctx, *tokensPath, *model, *tempF, *maxTok, *think, !*noMetrics, strings.Join(rest, " ")))
	case "probe":
		mustNoErr(cmdProbe(ctx, *tokensPath))
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `agycli — Antigravity-mimic client

Subcommands:
  login                  one-time OAuth (opens a browser)
  chat <prompt...>       one chat turn, stream the response
  probe                  dump project ID + models + experiments

Flags:
  -m MODEL          model id (default: gemini-3-pro-high)
  -t FLOAT          temperature (0 = use API default)
  -max INT          max output tokens
  -think INT        thinking budget (for *-thinking models)
  -creds PATH       tokens file (default: ~/.config/agymimic/tokens.json)
  -no-metrics       skip Unleash background loop`)
}

func defaultCredsPath() string {
	if h, _ := os.UserHomeDir(); h != "" {
		return filepath.Join(h, ".config", "agymimic", "tokens.json")
	}
	return "tokens.json"
}

func loadTokens(p string) (*auth.Tokens, error) {
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var t auth.Tokens
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func saveTokens(p string, t *auth.Tokens) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(t, "", "  ")
	return os.WriteFile(p, raw, 0o600)
}

func cmdLogin(ctx context.Context, credsPath string) error {
	az, err := auth.StartAuthorize("")
	if err != nil {
		return err
	}
	fmt.Println("Open this URL in your browser to log in:")
	fmt.Println()
	fmt.Println("    " + az.URL)
	fmt.Println()
	fmt.Println("Waiting for redirect to localhost...")

	codeCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	code, err := az.Wait(codeCtx)
	if err != nil {
		return err
	}
	fmt.Println("Got authorization code. Exchanging for tokens + resolving project...")

	t, err := auth.ExchangeCode(ctx, code, az.Verifier)
	if err != nil {
		return err
	}
	if err := saveTokens(credsPath, t); err != nil {
		return err
	}
	fmt.Printf("OK. Saved tokens to %s\n", credsPath)
	fmt.Printf("  email:        %s\n", t.Email)
	fmt.Printf("  projectId:    %s\n", t.ProjectID)
	fmt.Printf("  tierId:       %s\n", t.TierID)
	fmt.Printf("  expires:      %s\n", t.ExpiresAt.Format(time.RFC3339))
	return nil
}

func newClient(ctx context.Context, credsPath string) (*api.Client, *auth.Tokens, error) {
	t, err := loadTokens(credsPath)
	if err != nil {
		return nil, nil, fmt.Errorf("load tokens: %w  (run `agycli login` first)", err)
	}
	if time.Now().After(t.ExpiresAt) {
		if err := auth.Refresh(ctx, t); err != nil {
			return nil, nil, fmt.Errorf("refresh: %w", err)
		}
	}
	if t.ProjectID == "" {
		if pid, tier, err := auth.DiscoverProject(ctx, t.AccessToken, ""); err == nil {
			t.ProjectID = pid
			t.TierID = tier
		} else {
			fmt.Fprintln(os.Stderr, "[agymimic] project discovery failed:", err)
		}
	}
	// Backfill identity fields for old tokens.json files — derived deterministically
	// from the email, so the same Google account always produces the same IDs even
	// after re-login or moving tokens.json to a new machine.
	auth.EnsureIdentity(t)
	_ = saveTokens(credsPath, t)
	return api.New(t), t, nil
}

func cmdChat(ctx context.Context, credsPath, model string, temp float64, maxTok, think int, withMetrics bool, prompt string) error {
	cli, t, err := newClient(ctx, credsPath)
	if err != nil {
		return err
	}

	if withMetrics {
		m := metrics.New(metrics.Options{
			InstanceID:   t.InstanceLabel,
			ConnectionID: t.ConnectionID,
		})
		m.Start(ctx)
		defer m.Cancel()
		fmt.Fprintf(os.Stderr, "[agymimic] unleash mimic active  instance=%s\n", m.InstanceID())
	}

	req := types.GenerateInner{
		Contents: []types.Content{
			{Role: "user", Parts: []types.Part{{Text: prompt}}},
		},
	}
	if temp > 0 || maxTok > 0 || think > 0 {
		gc := &types.GenerationConfig{}
		if temp > 0 {
			gc.Temperature = &temp
		}
		if think > 0 {
			// Claude requires max_output_tokens > thinking_budget.
			// Auto-bump max if the caller forgot.
			if maxTok <= think {
				maxTok = think + 1024
				fmt.Fprintf(os.Stderr, "[agymimic] auto-bumped max to %d (must exceed thinking budget %d)\n", maxTok, think)
			}
			gc.ThinkingConfig = &types.ThinkingConfig{ThinkingBudget: think, IncludeThoughts: true}
		}
		if maxTok > 0 {
			gc.MaxOutputTokens = maxTok
		}
		req.GenerationConfig = gc
	}

	ch, err := cli.Stream(ctx, model, req)
	if err != nil {
		return err
	}
	var (
		thinkingPrinted bool
		anyText         bool
	)
	for ev := range ch {
		if ev.Err != nil {
			fmt.Fprintln(os.Stderr, "\n[stream-err]", ev.Err)
			continue
		}
		for _, cand := range ev.Resp.Response.Candidates {
			for _, p := range cand.Content.Parts {
				if p.Thought && p.Text != "" {
					if !thinkingPrinted {
						fmt.Fprintln(os.Stderr, "\n--- thinking ---")
						thinkingPrinted = true
					}
					fmt.Fprint(os.Stderr, p.Text)
				} else if p.Text != "" {
					if thinkingPrinted && !anyText {
						fmt.Fprintln(os.Stderr, "\n--- answer ---")
					}
					fmt.Print(p.Text)
					anyText = true
				} else if p.FunctionCall != nil {
					b, _ := json.Marshal(p.FunctionCall)
					fmt.Fprintf(os.Stderr, "\n[functionCall] %s\n", b)
				}
			}
			if cand.FinishReason != "" {
				fmt.Fprintf(os.Stderr, "\n\n[finish=%s]\n", cand.FinishReason)
			}
		}
		if u := ev.Resp.Response.UsageMetadata; u != nil && u.TotalTokenCount > 0 {
			fmt.Fprintf(os.Stderr, "[tokens p=%d c=%d t=%d th=%d]\n",
				u.PromptTokenCount, u.CandidatesTokenCount, u.TotalTokenCount, u.ThoughtsTokenCount)
		}
	}
	return nil
}

func cmdProbe(ctx context.Context, credsPath string) error {
	cli, t, err := newClient(ctx, credsPath)
	if err != nil {
		return err
	}
	fmt.Printf("email      : %s\n", t.Email)
	fmt.Printf("project    : %s\n", t.ProjectID)
	fmt.Printf("tier       : %s\n", t.TierID)
	fmt.Printf("expires    : %s\n\n", t.ExpiresAt.Format(time.RFC3339))

	exps, err := cli.ListExperiments(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "listExperiments:", err)
	} else {
		raw, _ := json.MarshalIndent(exps, "", "  ")
		fmt.Println("--- experiments ---")
		fmt.Println(string(raw))
	}

	models, err := cli.FetchAvailableModels(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetchAvailableModels:", err)
	} else {
		fmt.Println("\n--- models ---")
		fmt.Printf("%-32s  %-30s  %-8s  %s\n", "id", "displayName", "thinking", "tagTitle")
		for _, m := range models {
			rec := ""
			if m.Recommended {
				rec = "[recommended]"
			}
			thinking := "no"
			if m.SupportsThinking {
				thinking = fmt.Sprintf("yes(%d)", m.ThinkingBudget)
			}
			fmt.Printf("%-32s  %-30s  %-8s  %s %s\n", m.ID, m.DisplayName, thinking, m.TagTitle, rec)
		}
	}
	return nil
}

func mustNoErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
