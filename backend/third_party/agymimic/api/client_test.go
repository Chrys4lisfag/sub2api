package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/koval/agymimic/auth"
	"github.com/koval/agymimic/types"
)

func TestBuildRequestEnvelope(t *testing.T) {
	c := New(&auth.Tokens{
		AccessToken: "ya29.x",
		ProjectID:   "test-project-123",
		ExpiresAt:   time.Now().Add(time.Hour),
	})
	req := c.buildRequest("gemini-3-pro-high", types.GenerateInner{
		Contents: []types.Content{{Role: "user", Parts: []types.Part{{Text: "hi"}}}},
	})
	if req.Project != "test-project-123" {
		t.Errorf("project = %q, want test-project-123", req.Project)
	}
	if req.Model != "gemini-3-pro-high" {
		t.Errorf("model = %q", req.Model)
	}
	if req.UserAgent != "antigravity" {
		t.Errorf("userAgent (body) = %q, want antigravity", req.UserAgent)
	}
	if !strings.HasPrefix(req.RequestID, "agent-") {
		t.Errorf("requestId = %q, want agent-<uuid>", req.RequestID)
	}
	// must serialize fields in expected order/casing
	raw, _ := json.Marshal(req)
	js := string(raw)
	for _, want := range []string{
		`"project":"test-project-123"`,
		`"model":"gemini-3-pro-high"`,
		`"request":{`,
		`"userAgent":"antigravity"`,
		`"requestId":"agent-`,
		`"contents":[{"role":"user","parts":[{"text":"hi"}]}]`,
	} {
		if !strings.Contains(js, want) {
			t.Errorf("envelope missing %q  full=%s", want, js)
		}
	}
}

func TestSystemInstructionIsObject(t *testing.T) {
	// Antigravity rejects plain-string systemInstruction with 400.
	si := &types.SystemInstruction{Parts: []types.Part{{Text: "you are helpful"}}}
	raw, _ := json.Marshal(si)
	if string(raw) != `{"parts":[{"text":"you are helpful"}]}` {
		t.Errorf("SystemInstruction shape: %s", raw)
	}
}

func TestStreamEventChannelTypes(t *testing.T) {
	// Just compile-time check.
	var _ <-chan StreamEvent
}
