# agymimic — Go client that mimics Antigravity CLI's backend protocol

Pure-Go library + CLI that **byte-for-byte mimics agy.exe** when talking to Google's Cloud Code / Antigravity backend (`daily-cloudcode-pa.sandbox.googleapis.com`).

Everything here was derived from:
- Direct binary reverse of `agy.exe 1.0.2` (Antigravity CLI build, `go1.27-20260427-RC04 cl/906595525 +5fb2392a6f X:boringcrypto,simd`)
- Live in-process wire capture via Frida hook on `net_http.NewRequestWithContext` (no MITM, bypasses `DETECT_AND_USE_PROXY` and Throne VPN routing)
- Cross-reference with [`NoeFabris/opencode-antigravity-auth`](https://github.com/NoeFabris/opencode-antigravity-auth) (the spec doc + TS implementation) and [`ljw1004/antigravity-trace`](https://github.com/ljw1004/antigravity-trace) (IDE-level architecture trace)

Designed as a Go library for embedding (e.g. as a sub2api backend driver).

## Quick start

```bash
go build ./cmd/agycli
./agycli login                           # opens browser; tokens saved under ~/.config/agymimic
./agycli probe                           # dumps your project_id / tier / experiments / available models
./agycli chat "what is 2+2?"             # streaming chat with default gemini-3-pro-high
./agycli -m claude-opus-4-6-thinking -think 8000 chat "explain quantum tunneling"
```

## What's inside

| package | purpose |
|---|---|
| `internal/` | constants (OAuth client, endpoints, paths), header builders matching agy's exact `User-Agent`, `X-Goog-Api-Client`, `Client-Metadata` triple |
| `auth/` | PKCE+S256 OAuth flow, callback server on :51121, token exchange, refresh, and `loadCodeAssist`/`onboardUser` project discovery |
| `types/` | Gemini wire-format types (`Request`, `Content`, `Part`, `Tool`, `GenerationConfig`, `ThinkingConfig`, …) |
| `api/` | `Client` for `:generateContent` (unary), `:streamGenerateContent?alt=sse` (streaming), `:countTokens`, `:listExperiments`, `:fetchAvailableModels` |
| `metrics/` | Optional Unleash background loop — `POST /api/client/register` + `/metrics` every 60 s, exactly like agy. Buys you back the organic-traffic signal the abuse detector looks for |
| `cmd/agycli` | reference CLI (login / probe / chat) |

## Wire-protocol notes (matters when embedding)

1. **Endpoint defaults to `daily-cloudcode-pa.sandbox.googleapis.com`.** agy.exe targets the daily sandbox track, not prod. `internal.EndpointFallbacks` is the order agy retries on failure.
2. **HTTP/2 to cloudcode-pa.** Verified by capturing real agy.exe with `GODEBUG=http2debug=2` — agy negotiates ALPN `h2` and sends full HTTP/2 frames (SETTINGS, HEADERS, DATA, WINDOW_UPDATE, PING). The daily-fleet load balancer offers both `h2` and `http/1.1`; we pin `ForceAttemptHTTP2=true` to match. Earlier revisions of this README claimed HTTP/1.1 — that was wrong; the live capture is authoritative.
3. **`Client-Metadata: {"ideType":"ANTIGRAVITY","platform":"<OS>","pluginType":"GEMINI"}`** is required. Without it the backend returns 401/403.
4. **`X-Goog-Api-Client: google-cloud-sdk vscode_cloudshelleditor/0.1`** is the literal string the daily fleet expects from Antigravity. Don't change to `gl-go/…`.
5. **`User-Agent: antigravity/<ver> <os>/<arch>`** for the chat call; **`Mozilla/5.0 (…) Antigravity/<ver> Chrome/138.0.7204.235 Electron/37.3.1 …`** for control-plane (`loadCodeAssist`, `onboardUser`). The Mozilla form is what the IDE shell sends; mixing them works but pure-API-only style is more conspicuous.
6. **Request envelope:**
   ```json
   {
     "project": "<cloudaicompanion-id>",
     "model":   "gemini-3-pro-high",
     "request": { "contents":[…], "generationConfig":{…}, "systemInstruction":{"parts":[…]}, "tools":[…] },
     "userAgent": "antigravity",
     "requestId": "agent-<uuid>"
   }
   ```
   The outer `userAgent: "antigravity"` literal is **inside the body**, separate from the HTTP `User-Agent` header. Both are required.
7. **`systemInstruction` must be an object** `{ "parts": [{ "text": "…" }] }`, never a plain string. Plain string → 400.
8. **Tool names**: `[a-zA-Z_][a-zA-Z0-9_.:-]{0,63}`. Slashes forbidden. `mcp:foo.bar` is fine, `mcp/foo` is rejected.
9. **JSON-Schema in `tools.functionDeclarations[].parameters`** must NOT contain `$ref`, `$defs`, `const`, `default`, `examples`, `$schema`, `$id`. Inline everything and convert `const: x` to `enum: [x]`.

## Models the backend currently accepts on the daily endpoint

| ID | Backing model | Notes |
|---|---|---|
| `gemini-3-pro-high` | Gemini 3 Pro, thinking high | default in this CLI |
| `gemini-3-pro-low`  | Gemini 3 Pro, thinking low |  |
| `gemini-3-flash`    | Gemini 3 Flash |  |
| `claude-sonnet-4-6` | Claude Sonnet 4.6 (Vertex-routed) |  |
| `claude-opus-4-6-thinking` | Claude Opus 4.6 with extended thinking | pass `-think 8000` or more |
| `gpt-oss-120b-medium` | GPT-OSS 120B, Vertex |  |

(Use `agycli probe` to see what your tier is actually allowed.)

## Streaming response format

SSE — each `data:` line is one `Response` JSON. Stream ends on a `[DONE]` sentinel or final candidate with `finishReason: "STOP"`. We hand each frame to the caller as a `StreamEvent{Resp, Err}` on the channel returned by `Client.Stream`.

Token usage lands in `Resp.Response.UsageMetadata` on the final chunk.
Function calls land as `Part.FunctionCall` (not `Part.Text`).
Thinking deltas land with `Part.Thought=true` and have `Part.ThoughtSignature` for the cache key.

## OAuth flow

`agycli login` runs:

1. Generate PKCE pair (43-char verifier, S256 challenge).
2. Listen on `127.0.0.1:51121/oauth-callback` (the only port Google's installed-app client is allowed to redirect to for this `client_id`).
3. Open `https://accounts.google.com/o/oauth2/v2/auth` with `client_id=1071006060591-…`, `scope=cloud-platform userinfo.email userinfo.profile cclog experimentsandconfigs`, `access_type=offline`, `prompt=consent`.
4. User signs in → Google redirects with `code` + `state`.
5. POST `https://oauth2.googleapis.com/token` with `client_secret=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`, `code_verifier`, `grant_type=authorization_code` → access + refresh tokens.
6. POST `:loadCodeAssist` with `{"metadata":{"ideType":"ANTIGRAVITY",…}}` to discover the user's `cloudaicompanionProject`.
7. If `loadCodeAssist` returns nothing, POST `:onboardUser` (polling LRO) with the default tier to auto-provision one.
8. Persist tokens + projectID under `~/.config/agymimic/tokens.json`.

The `client_id` + `client_secret` are baked into `agy.exe`; they're not secret in any meaningful sense for an installed-app client.

## Embedding in your own code

```go
import (
    "github.com/koval/agymimic/api"
    "github.com/koval/agymimic/auth"
    "github.com/koval/agymimic/types"
)

// after `agycli login` saved a tokens.json:
tokens, _ := loadJSON("tokens.json")               // load auth.Tokens
client    := api.New(tokens,
    api.WithEndpoint("https://daily-cloudcode-pa.sandbox.googleapis.com"))

ch, err := client.Stream(ctx, "gemini-3-pro-high", types.GenerateInner{
    Contents: []types.Content{{Role:"user", Parts:[]types.Part{{Text:"hi"}}}},
})
for ev := range ch {
    // ev.Resp.Response.Candidates[0].Content.Parts[0].Text  → stream chunk
}
```

For multi-account, instantiate one `api.Client` per `auth.Tokens` and round-robin them yourself; each Client refreshes its own access token on the fly.

For organic-traffic mimicry on a long-running daemon, also spin a `metrics.Client` per account.

## What's NOT yet implemented

- **Auto-fallback across endpoints** in the API client (daily → autopush → prod). Auth side already does it; `api.Client` always hits whatever you passed to `WithEndpoint`.
- **`:tabChat?alt=sse`** (next-edit-prediction). agy fires this for inline completions; we don't need it for chat.
- **Vertex/Antigropic-style `messages` translation.** Caller has to build Gemini-style `Content` arrays. Open an adapter package if you want OpenAI shape in.
- **Conversation persistence.** Caller manages history; pass it back in `Contents` each turn.

## License

MIT.
