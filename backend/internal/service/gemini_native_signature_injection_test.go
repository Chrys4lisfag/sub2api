package service

import (
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/tidwall/gjson"
)

func TestEnsureGeminiNativeFunctionCallSignatures(t *testing.T) {
	t.Run("stamps an unsigned call in a model turn", func(t *testing.T) {
		body := []byte(`{"contents":[
			{"role":"user","parts":[{"text":"read a"}]},
			{"role":"model","parts":[{"functionCall":{"name":"read","args":{"path":"a"}}}]},
			{"role":"user","parts":[{"functionResponse":{"name":"read","response":{"result":"x"}}}]},
			{"role":"user","parts":[{"text":"and now?"}]}
		]}`)
		out, n := EnsureGeminiNativeFunctionCallSignatures(body)
		if n != 1 {
			t.Fatalf("injected = %d, want 1", n)
		}
		got := gjson.GetBytes(out, "contents.1.parts.0.thoughtSignature").String()
		if got != antigravity.DummyThoughtSignature {
			t.Fatalf("signature = %q, want %q", got, antigravity.DummyThoughtSignature)
		}
		// Argument payloads must survive untouched.
		if p := gjson.GetBytes(out, "contents.1.parts.0.functionCall.args.path").String(); p != "a" {
			t.Fatalf("args mutated: %s", out)
		}
	})

	t.Run("never overwrites a real signature", func(t *testing.T) {
		body := []byte(`{"contents":[
			{"role":"model","parts":[{"functionCall":{"name":"read","args":{}},"thoughtSignature":"REAL_SIG"}]},
			{"role":"user","parts":[{"text":"go on"}]}
		]}`)
		out, n := EnsureGeminiNativeFunctionCallSignatures(body)
		if n != 0 {
			t.Fatalf("injected = %d, want 0", n)
		}
		if got := gjson.GetBytes(out, "contents.0.parts.0.thoughtSignature").String(); got != "REAL_SIG" {
			t.Fatalf("real signature clobbered: %q", got)
		}
	})

	t.Run("signed first call leaves bare siblings alone", func(t *testing.T) {
		// Real agy signs only the leading call of a parallel batch, and a live
		// probe confirmed upstream accepts signed-first + unsigned-secondary.
		body := []byte(`{"contents":[
			{"role":"model","parts":[
				{"functionCall":{"name":"read","args":{}},"thoughtSignature":"REAL_SIG"},
				{"functionCall":{"name":"grep","args":{}}}
			]},
			{"role":"user","parts":[{"text":"go on"}]}
		]}`)
		out, n := EnsureGeminiNativeFunctionCallSignatures(body)
		if n != 0 {
			t.Fatalf("injected = %d, want 0", n)
		}
		if gjson.GetBytes(out, "contents.0.parts.1.thoughtSignature").Exists() {
			t.Fatalf("sibling call was stamped: %s", out)
		}
	})

	t.Run("stamps only the leading call of an unsigned batch", func(t *testing.T) {
		body := []byte(`{"contents":[
			{"role":"model","parts":[
				{"functionCall":{"name":"read","args":{}}},
				{"functionCall":{"name":"grep","args":{}}}
			]},
			{"role":"user","parts":[{"text":"go on"}]}
		]}`)
		out, n := EnsureGeminiNativeFunctionCallSignatures(body)
		if n != 1 {
			t.Fatalf("injected = %d, want 1", n)
		}
		if gjson.GetBytes(out, "contents.0.parts.1.thoughtSignature").Exists() {
			t.Fatalf("sibling call was stamped: %s", out)
		}
	})

	t.Run("ignores user turns and tool arguments", func(t *testing.T) {
		// A user-authored argument may legally be named thoughtSignature or
		// hold a functionCall-shaped object; neither may be rewritten.
		body := []byte(`{"contents":[
			{"role":"user","parts":[{"functionResponse":{"name":"read","response":{"functionCall":{"name":"x"}}}}]},
			{"role":"user","parts":[{"text":"hi"}]}
		]}`)
		out, n := EnsureGeminiNativeFunctionCallSignatures(body)
		if n != 0 {
			t.Fatalf("injected = %d, want 0", n)
		}
		if string(out) != string(body) {
			t.Fatalf("body mutated:\n got %s\nwant %s", out, body)
		}
	})

	t.Run("handles the v1internal request envelope", func(t *testing.T) {
		body := []byte(`{"project":"p","request":{"contents":[
			{"role":"model","parts":[{"functionCall":{"name":"bash","args":{"command":"ls"}}}]},
			{"role":"user","parts":[{"text":"next"}]}
		]}}`)
		out, n := EnsureGeminiNativeFunctionCallSignatures(body)
		if n != 1 {
			t.Fatalf("injected = %d, want 1", n)
		}
		if got := gjson.GetBytes(out, "request.contents.0.parts.0.thoughtSignature").String(); got != antigravity.DummyThoughtSignature {
			t.Fatalf("signature = %q", got)
		}
	})

	t.Run("no-ops on empty and invalid bodies", func(t *testing.T) {
		for _, in := range [][]byte{nil, {}, []byte("not json"), []byte(`{"contents":"nope"}`)} {
			out, n := EnsureGeminiNativeFunctionCallSignatures(in)
			if n != 0 || string(out) != string(in) {
				t.Fatalf("input %q mutated to %q (n=%d)", in, out, n)
			}
		}
	})

	t.Run("preserves large integer literals in arguments", func(t *testing.T) {
		// json.Number decoding keeps 64-bit ids from degrading to float64.
		body := []byte(`{"contents":[
			{"role":"model","parts":[{"functionCall":{"name":"x","args":{"id":9007199254740993}}}]},
			{"role":"user","parts":[{"text":"go"}]}
		]}`)
		out, n := EnsureGeminiNativeFunctionCallSignatures(body)
		if n != 1 {
			t.Fatalf("injected = %d, want 1", n)
		}
		if got := gjson.GetBytes(out, "contents.0.parts.0.functionCall.args.id").Raw; got != "9007199254740993" {
			t.Fatalf("integer literal degraded to %s", got)
		}
	})
}

// The captured failure had 31 unsigned calls spread over ~1000 turns; make sure
// every one of them gets stamped in a single pass.
func TestEnsureGeminiNativeFunctionCallSignaturesManyTurns(t *testing.T) {
	contents := make([]any, 0, 64)
	wantInjected := 0
	for i := 0; i < 30; i++ {
		call := map[string]any{"functionCall": map[string]any{"name": "bash", "args": map[string]any{"command": "ls"}}}
		if i%3 == 0 {
			call["thoughtSignature"] = "REAL_SIG"
		} else {
			wantInjected++
		}
		contents = append(contents,
			map[string]any{"role": "model", "parts": []any{call}},
			map[string]any{"role": "user", "parts": []any{map[string]any{"functionResponse": map[string]any{"name": "bash", "response": map[string]any{"result": "ok"}}}}},
		)
	}
	contents = append(contents, map[string]any{"role": "user", "parts": []any{map[string]any{"text": "continue"}}})
	body, err := json.Marshal(map[string]any{"contents": contents})
	if err != nil {
		t.Fatal(err)
	}

	out, n := EnsureGeminiNativeFunctionCallSignatures(body)
	if n != wantInjected {
		t.Fatalf("injected = %d, want %d", n, wantInjected)
	}
	missing := 0
	gjson.GetBytes(out, "contents").ForEach(func(_, turn gjson.Result) bool {
		if turn.Get("role").String() != "model" {
			return true
		}
		turn.Get("parts").ForEach(func(_, part gjson.Result) bool {
			if part.Get("functionCall").Exists() && !part.Get("thoughtSignature").Exists() {
				missing++
			}
			return true
		})
		return true
	})
	if missing != 0 {
		t.Fatalf("%d function calls left unsigned", missing)
	}
}
