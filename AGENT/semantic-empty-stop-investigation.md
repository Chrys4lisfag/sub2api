# Antigravity Semantic-Empty STOP Investigation

## Status

Controlled tests established one actionable result; protocol-parity and root-cause explanations remain open.

For the captured request, `gemini-3.6-flash-high` with `thinkingLevel: HIGH` frequently returned HTTP 200 streams containing only thought text plus a thought signature, then `finishReason: STOP`, with no non-thought text and no function call. Lowering or disabling thinking made the same request usable in every controlled attempt. This proves the mitigation for this sample, not that HIGH thinking is the root cause.

On `2026-08-02`, a newly captured complete 189,612-byte request reproduced the
same semantic-empty signature in 1 of 5 bounded local HIGH-thinking attempts.
LOW and disabled-thinking controls were usable in 5 of 5 attempts each. The
reproduction preserved the exact request and captured OAuth account but used
direct local egress because the captured per-account proxy was not reachable
from the local control. This adds reproduction evidence, not protocol-parity or
root-cause proof.

sub2api's existing pre-commit semantic-empty guard is still required. It prevents false HTTP 200 empty responses by switching accounts before headers are committed. Exhaustion is surfaced as an explicit 502.

AGY wire evidence, external issue review, configuration-field matrix, and future local replay plan are maintained in [`agy-gemini-protocol-and-replay-plan.md`](agy-gemini-protocol-and-replay-plan.md).

## Definitions

A stream is **usable** when any chunk contains either:

- non-thought text; or
- a function call.

A stream is **semantic-empty** when it terminates without either usable form. Thought text and thought signatures do not count as an answer.

An empty terminal STOP after an earlier function call is valid protocol behavior. Classification must use accumulated stream state, not the terminal chunk alone.

## Captured production sample

Captured at `2026-07-29T21:44:19Z` by `backend/internal/handler/semantic_empty_capture.go`:

- Requested model: `gemini-3.6-flash`
- Wire model: `gemini-3.6-flash-high`
- Action: `streamGenerateContent`
- Request SHA-256: `f239f830bcea7d74fcf4b1be64aa561fc877095ffbbc5ccd4233755fb58fd07d`
- Request body: 74,647 bytes, complete and hash-verified
- Account record: 59, `antigravity_native`, OAuth, active and schedulable
- Per-account proxy: configured
- `maxOutputTokens`: 65,536
- Thinking: `includeThoughts: true`, `thinkingLevel: HIGH`
- Tools: one collection containing 13 function declarations
- `toolConfig`: omitted
- Captured upstream structure: one thought-text part, one thought signature, zero function calls, `finishReason: STOP`

The raw request, prompt, OAuth tokens, proxy credentials, and raw response are intentionally not committed.

## Production observations

### Fixed 30-minute window

`2026-07-29T15:22:27Z`–`15:52:27Z`:

- 140 Antigravity Native requests
- 134 2xx responses
- 6 explicit 502 responses
- 58 `stop_without_content` anomalies
- 52 recovered by account switching
- 6 exhausted semantic-empty failovers

### Fresh 30-minute window

`2026-07-29T20:41:43Z`–`21:11:43Z`:

- 87 semantic-empty STOP anomalies
- 76 recovered by account switching
- 11 exhausted semantic-empty failovers
- 11 explicit 502 responses
- no panic/fatal event

These windows establish that the upstream condition persists while sub2api's guard prevents observed anomalies from escaping as false-success HTTP 200 responses.

### Deployed effective-configuration observation

On `2026-07-30`, a temporary image containing only the bounded instrumentation was deployed by recreating the `sub2api` service. One hash-verified captured request was replayed once through the normal production endpoint and scheduler. The client request completed with HTTP 200 after failover.

The observed semantic-empty attempt was assigned to account record 62 and logged:

```text
model=gemini-3.6-flash
wire_model=gemini-3.6-flash-high
finish_reason=STOP
buffered_bytes=1202
failover_kind=semantic_empty
request_generation_config_parsed=true
request_generation_config_defaults_applied=true
request_generation_config_shape=object
request_max_output_tokens_numeric=true
request_max_output_tokens=65536
request_thinking_config_shape=object
request_include_thoughts_boolean=true
request_include_thoughts=true
request_thinking_budget_numeric=true
request_thinking_budget=10000
request_thinking_level=high
```

This independently confirms the same effective HIGH-thinking configuration on another live scheduler account. The warning contained no prompt, tools, schemas, credentials, request body, response body, or candidate text.

## Protocol findings

A separate hash-bound AGY runtime capture established this valid sequence:

1. function-call SSE event;
2. separate empty STOP SSE event on the same trace;
3. AGY executes the MCP call;
4. function-response follow-up;
5. final model response succeeds.

Therefore, `functionCall -> empty STOP` is not itself an anomaly. The stream-wide `sawFunctionCall` guard is correct.

## Controlled replay setup

CLIProxyAPI is pinned as `diagnostics/CLIProxyAPI` at PR #4598 commit `ca8d8c3c4696f30ee8669cfaaf340db8ddeda0ec`.

The disposable control used:

- one Antigravity auth file created from captured account record 59;
- that account's project ID and per-account proxy;
- one local API key;
- one matching auth entry loaded by CLIProxy;
- localhost-only listener on `127.0.0.1:18317`;
- no request debug logging;
- captured prompt/history/tools and request hash;
- exact wire model `gemini-3.6-flash-high`;
- sequential requests;
- structural output only.

This strongly isolates the same stored OAuth credential and proxy configuration. It is not cryptographic proof of Google's internal account identity or observed proxy egress; CLIProxy exposes no public per-request account pin.

## Test results

All live replays used the captured prompt/history/tools unless a single named field was changed.

### Generic Python SDK token-cap canary

Python `google-genai==2.12.1`, fixed simple prompt, unrelated normal scheduler account selection:

| Configuration | Attempts | Result |
|---|---:|---|
| `maxOutputTokens=32` | 5 | 5/5 no visible answer, `finishReason: MAX_TOKENS` |
| `maxOutputTokens=256` | 5 | 5/5 visible text, `finishReason: STOP` |

This proves that an undersized output cap can cause no visible answer, but its signature is `MAX_TOKENS`, not the production `STOP` anomaly. These canaries are not same-account evidence.

### CLIProxy routing sanity check

Using requested model alias `gemini-3.6-flash` against CLIProxy's local registry:

| Client | Attempts | Result |
|---|---:|---|
| Raw Gemini body | 5 | 5/5 HTTP 502 before upstream work |
| Python SDK | 5 | 5/5 `ServerError` |

The CLIProxy registry exposes the wire model name. All later tests used `gemini-3.6-flash-high`.

### CLIProxy current-main baseline

CLIProxy commit `2b63d6bcda136af1d3638be8e0038658911fb217`:

| Client | Attempts | Usable | Semantic-empty |
|---|---:|---:|---:|
| Raw captured Gemini body | 5 | 1 | 4 |
| Python `google-genai==2.12.1` via the same CLIProxy instance | 5 | 1 | 4 |

Every semantic-empty result was HTTP 200, thought-only, and terminated with STOP.

Current main removes non-Claude `request.generationConfig.maxOutputTokens` after capping, so this baseline does not test preserved max-output behavior.

### CLIProxy PR #4598

PR #4598 preserves capped Gemini `maxOutputTokens`. The captured value was 65,536, equal to the Flash output-token limit.

| Client | Attempts | Usable | Semantic-empty |
|---|---:|---:|---:|
| Raw captured Gemini body | 5 | 1 | 4 |
| Python `google-genai==2.12.1` via the same CLIProxy instance | 5 | 0 | 5 |

Preserving `maxOutputTokens=65536` did not eliminate or materially reduce the semantic-empty STOP condition.

### One-variable request matrix on PR #4598

Baseline fields were `thinkingLevel: HIGH`, `includeThoughts: true`, and omitted `toolConfig`.

| Variant | Only intentional change | Attempts | Usable | Outcome mix |
|---|---|---:|---:|---|
| `thinking_low` | thinking level HIGH -> LOW | 5 | 5 | 3 text, 2 function calls |
| `thinking_disabled` | `includeThoughts=false`, `thinkingBudget=0` | 5 | 5 | 1 text, 4 function calls |
| `tool_validated` | add function-calling mode VALIDATED | 5 | 1 | 1 text, 4 thought-only STOP |
| `tool_auto` | add function-calling mode AUTO | 5 | 1 | 1 text, 4 thought-only STOP |
| `thinking_low_tool_validated` | LOW plus VALIDATED | 5 | 5 | 3 text, 2 function calls |

Evidence:

- HIGH baseline: 80–100% semantic-empty across paired runs.
- LOW: 0/5 semantic-empty.
- Thinking disabled: 0/5 semantic-empty.
- Changing only tool mode: still 4/5 semantic-empty.
- LOW remained 5/5 usable even with VALIDATED tool mode.

Conclusion: the only causal result established by this matrix is that lowering or disabling thinking helped this captured account, prompt, model, and tool set. HIGH thinking remained associated with the failures, but protocol differences beyond the tested fields remain open.

### Exact official-envelope localhost control

Replay tooling now preserves a complete outer envelope byte-for-byte for the baseline. The accepted 94,771-byte AGY envelope, SHA-256 `49eba076d4dcac977fd8a745e5cc4576142074adc78508c94204e3c1f36c0ca2`, was sent once to a one-shot receiver bound only to `127.0.0.1`. Receiver byte count and SHA-256 matched exactly. A synthetic one-event SSE response containing only `finishReason: STOP` was classified as `semantic_empty_stop`.

This proves unchanged local transport and structural parser behavior only. The receiver had no upstream path; its synthetic STOP is not a genuine AGY/provider semantic-empty event and cannot confirm AGY retries.

A separate content-free typed round-trip originally found that official AGY `request.labels` were dropped by production `GeminiRequest`. That mismatch is now fixed with `map[string]string` plus absent-field preservation. Synthetic round-trip, native-wrapper, and private exact-envelope semantic probes pass.

### Current-binary time-tool recreation family

On `2026-08-01`, installed `agy.exe` SHA-256
`83fb6e9d80e751d174b3738c3eefb054e75e85e47b17d1e159fe4831adceadc8`
ran five sequential `agy_capture_get_time` tool sessions at each captured
thinking tier through a sanitized localhost MITM:

| Tier | Budget | Completed sessions | Target agent streams | Semantic-empty STOP |
|---|---:|---:|---:|---:|
| HIGH | 10,000 | 5/5 | 10 | 0 |
| MEDIUM | 4,000 | 5/5 | 11 | 0 |
| LOW | 1,000 | 5/5 | 11 | 0 |

Across 32 target agent streams, 17 contained a function call followed by a
separate empty `STOP`, and 15 contained final nonempty text. Stream-wide
classification correctly treated all function-call streams as usable. Every
initial request carried six labels, 22 tools, `VALIDATED`,
`maxOutputTokens=65536`, and its tier budget. Checkpoints carried explicit
`includeThoughts=false` and `thinkingBudget=0`.

This family is **not a successful identity-correlated reproduction**. The
strict runner was in preflight mode and reported
`preflight_complete_no_wire`, `request_call_identity_verified=false`, and
`accepted=false` for all 15 attempts. External MITM rows and MCP logs were
sequential and timestamp-adjacent but shared no verified request/call ID.
Therefore these runs cannot prove which observed wire call caused a fixture
call, cannot prove the path is fixed, and cannot confirm AGY retry behavior.

The old 74,647-byte problematic request was intentionally not retained after
the earlier controlled matrix, so this family did not replay it. Prompt,
history, tool count, labels, tool mode, and thinking representation differ.
Zero semantic-empty observations here do not contradict the earlier HIGH
reproduction and do not change the conclusion: lowering or disabling thinking
is proven only as a mitigation for the captured problematic sample.

Private structural evidence is under
`%LOCALAPPDATA%\agy-private-evidence\stop-matrix-current-20260801`; repository
contains hashes and counts only. Sanitized wire SHA-256 is
`5f305459ff57d69c78fd398a2c425e0e0faa6082119ee7c60ef043b9ef098d85`;
classification SHA-256 is
`a40ddec014d2bb803b9e823f707e4465a4c8982eff6acb5b9d89e17f856f3c3e`.
Settings were restored to SHA-256
`cc793e790f64fe46d4b53da2750a9fd90bbd9d455f59806b2251fff0f4d90dab`,
and the localhost proxy was stopped.

### Production scheduler replay

The single bounded replay through the deployed sub2api endpoint returned HTTP 200 after the semantic-empty attempt above triggered account failover. This exercised the production preprocessing, scheduler, upstream transport, semantic guard, and recovery path rather than only the disposable CLIProxy control.

## Code verification

### Temporary observation code

Before removal, the bounded effective-configuration instrumentation passed:

```text
go test -v ./internal/service -run "Test(ExtractNativeGenerationConfigLogFields|NativeGenerationConfigLogAttrsAreContentFree|LogNativeSemanticWarningIncludesEffectiveConfig|InspectStreamChunks_FunctionCallThenEmptyStopIsUsable)" -count=1
PASS
```

Those temporary tests covered effective defaults, Flash max-output clamping, wrapped-request passthrough, bounded content-free attributes, structured warning fields, and the accumulated `functionCall -> empty STOP` contract. The effective-configuration logger and its dedicated tests were removed after the deployed observation was captured.

### Final Go tree

The permanent cross-chunk protocol regression remains:

```text
go test -v ./internal/service -run TestInspectStreamChunks_FunctionCallThenEmptyStopIsUsable -count=1
PASS

go test ./internal/service -count=1
PASS
```

Current targeted validation after the payload-parity fixes:

```text
go test ./internal/pkg/antigravity -run "Test(V1InternalRequestLabelsRoundTrip|V1InternalRequestLabelsOmittedWhenAbsent|V1InternalRequestOptionalConfigFieldsRoundTrip|GeminiThinkingBudgetOmittedWhenAbsent|GeminiOptionalEmptyArraysRemainPresent)$" -count=1
PASS

go test ./internal/service -run "Test(InspectStreamChunks_FunctionCallThenEmptyStopIsUsable|NativeSemanticEmptyCompletion|NativeSemanticEmptyAnomaly)$" -count=1
PASS
```

### Python

```text
python -m unittest diagnostics.test_semantic_empty_replay diagnostics.test_genai_stream_probe
Ran 40 tests
OK
```

Tests cover exact-byte synthetic envelope baseline preservation, nested one-variable mutations, semantic no-op detection, hash/network gates, bounded SSE parsing, text, thought-only and empty STOP, valid/malformed function calls, hashed errors, and output-file behavior. They do not contact AGY or a provider.

## Temporary instrumentation used

During the bounded production observation, semantic-empty warnings included a fixed, content-free snapshot of the effective post-preprocessing generation configuration:

- parsed/default-applied flags;
- generation config shape;
- numeric `maxOutputTokens` presence and bounded value;
- thinking config shape;
- boolean `includeThoughts` presence and value;
- numeric thinking budget presence and value;
- bounded thinking-level classification.

Prompt text, system instructions, tools, schemas, stop sequences, candidate text, request bodies, and response bodies were excluded. The instrumentation did not change response or failover behavior. It was removed from the final source tree after the result was captured.

## Conclusions

1. Independent CLIProxy and Python SDK paths reproduced the same thought-only HTTP 200 STOP signature with the isolated captured credential configuration. This observation does not prove complete AGY protocol parity or root cause.
2. Preserving `maxOutputTokens=65536` did not help this sample; this is not a universal max-token conclusion.
3. Changing only tool-choice mode did not help this sample; tool schema/history parity remains open.
4. The only proven mitigation is that LOW or disabled thinking produced 15/15 usable responses across the three relevant variants. Do not call HIGH thinking the root cause without further protocol controls.
5. Hash-bound AGY static tracing maps CCPA `finishReason=STOP` to framework result kind `1`; the main core output-retry loop handles only kinds `3`, `5`, and `6`, so that loop does not output-retry STOP. Runtime confirmation with a genuine semantic-empty STOP and exact configured retry counts remain unproven.
6. Do not classify the final STOP chunk alone. Earlier function calls make the stream usable.
7. Keep synchronous pre-commit semantic-empty failover. Without it, downstream clients receive false-success HTTP 200 responses.
8. The current-binary 15-session time-tool family observed no semantic-empty target stream, but strict identity correlation failed for all 15. Treat it as an unsuccessful correlated reproduction, not evidence of a fix.
9. A post-deployment complete-request replay reproduced semantic-empty STOP in
   1/5 HIGH attempts over direct local egress; LOW and disabled thinking were
   each usable in 5/5. The egress difference prevents an exact-network-parity
   claim but does not weaken the bounded local reproduction itself.

## Production deployment validation, 2026-08-01

The payload-parity and stream-wide STOP changes were released as commit
`1b046492f1f6e754c09a76b9d306936c9a4ab1b5`. GitHub Actions
`docker-publish` run `30697157073` completed successfully before rollout.
Production operations used the approved Electerm MCP `usa` bookmark only.

Pre-deployment rollback identity:

- image ID: `sha256:48c6c56391f44288ea174d8e6e274e5162c8328ad0d723003e61fc62b079df56`;
- image revision label: `9669f73135b4603785e2a62acd95c4f042a49f86`;
- container health: healthy;
- local rollback tag retained as
  `ghcr.io/chrys4lisfag/sub2api:rollback-stop-20260801`.

The release command pulled `ghcr.io/chrys4lisfag/sub2api:latest`, verified its
revision label before recreation, and recreated only the `sub2api` service with
`--no-deps`. Deployed identity:

- image ID: `sha256:9050cf1831f4bd7658d4cb981282fb9e07ad0cee90f62bce8a5aa9dc5ee3f480`;
- image revision label:
  `1b046492f1f6e754c09a76b9d306936c9a4ab1b5`;
- health status: healthy;
- `GET http://127.0.0.1:8080/health`: HTTP 200;
- container start: `2026-08-01T11:16:12.016304022Z`.

A bounded canary observed logs from container start through
`2026-08-01T11:21:47Z`: 138 total lines, one readiness marker, and 49 request
markers. It found zero `stop_without_content` markers, zero
`failover_kind=semantic_empty` markers, zero detected 502 markers, and zero
panic/fatal markers. The preceding 15-minute baseline also had zero semantic
STOP, semantic failover, 502, or panic/fatal markers.

This proves exact-image deployment, service health, and absence of an immediate
traffic regression. No natural semantic-empty event occurred during the
canary, so the deployment did **not** exercise the live failover branch and is
not evidence that upstream semantic-empty generation is fixed. The permanent
regression tests remain the evidence for `functionCall -> empty STOP`
classification, and the earlier controlled matrix remains the evidence for the
thinking mitigation. No captured prompt, response body, credential, or tool
argument was transferred or logged during deployment validation.

## Post-deployment recurrence and local reproduction, 2026-08-02

The upstream condition continued after the release. A bounded rolling
60-minute production-log query against the deployed image returned 28
`stop_without_content` markers. Matching timestamps spanned
`2026-08-02T03:18:40.212+0800` through
`2026-08-02T03:49:17.522+0800`; sampled safe fields showed
`model=gemini-3.6-flash`, `wire_model=gemini-3.6-flash-high`,
`stream=true`, and multiple account records. No request or response body was
included in the structural count.

A later complete capture from the same active incident period was selected:

- captured at `2026-08-01T19:56:36.260612361Z`;
- action `streamGenerateContent`, requested model `gemini-3.6-flash`;
- complete inner request: 189,612 bytes, not truncated;
- request SHA-256:
  `5e08ffa7b4cf45a55f93b2f8edfd3376e314d4261902b59592cc982f68dff622`;
- gzip capture bundle: 107,328 bytes, SHA-256
  `50cc3a81b18e94a53b23748e0be9c321b04e3986aacd1d9df3a92937d59e5b65`;
- 133 content items, one tool collection, 13 declarations;
- `maxOutputTokens=65536`, `includeThoughts=true`,
  `thinkingLevel=HIGH`, omitted `thinkingBudget`, omitted `toolConfig`.

The bundle was transferred through Electerm SFTP into the ACL-private evidence
root and its hashes were verified locally. Prompt, history, tools, and schemas
remain only in private evidence; none is committed. Replay OAuth artifacts are
now permanent retained evidence by explicit user instruction: do not delete
them unless that instruction is explicitly reversed.

The local backend was CLIProxyAPI commit
`ca8d8c3c4696f30ee8669cfaaf340db8ddeda0ec`; the built binary SHA-256 was
`d569fbce6c6019d4a94319b099fdbbacd0aef33f25859df58ca869f6856fe124`.
It listened only on `127.0.0.1:18317`, loaded one retained direct-route auth
copy, used one retained file-backed local API key, disabled request/debug
logging, set upstream request retries to zero, and ran attempts sequentially.
The dry run preserved the baseline 189,612 bytes and exact request hash while
making zero network requests.

The captured per-account proxy was unusable from the local PC. Five initial
baseline attempts ended as HTTP errors during OAuth-token connection, before a
provider stream was available. After a bounded direct token refresh, one
fresh-token attempt through the captured proxy still ended as HTTP 500 before
provider SSE. These six transport failures are not semantic-empty results.

A direct-egress local fallback preserved the exact request bytes, model, and
captured OAuth account credential while changing only the network egress:

| Variant | Attempts | Semantic-empty STOP | Text | Function call | Usable |
|---|---:|---:|---:|---:|---:|
| unchanged HIGH baseline | 5 | 1 | 4 | 0 | 4 |
| `thinking_low` | 5 | 0 | 2 | 3 | 5 |
| `thinking_disabled` | 5 | 0 | 3 | 2 | 5 |

The reproduced failure was HTTP 200 event-stream output containing thought text
and a thought signature, then `finishReason=STOP`, with no non-thought text and
no function call. Classification used accumulated stream state and emitted no
content.

This verifies that a recent production-causing prompt can reproduce the same
upstream semantic-empty signature on the local PC. It does not establish exact
proxy-egress parity, cryptographically prove Google's internal account
identity, or prove HIGH thinking is the root cause. The only supported
intervention remains lowering or disabling thinking for this request family.

Private structural summary: 1,940 bytes, SHA-256
`4b72595709b5fe77910a4d6b7bf1ccbcffdf32e8be2a07db4bfd070b7259dc03`.
The hash-bound raw request, capture bundle, exact-proxy auth copy, refreshed
direct-route auth copy, local replay key, and server-side retained auth copy
remain in ACL-private storage. They are intentionally retained and must not be
deleted. Disposable runtime processes may be stopped without removing those
credentials or evidence files.

## Payload isolation, real-AGY injection, and recovery fix, 2026-08-02

The corrected direct-egress HIGH control used only provider-success responses
in its denominator. Three unchanged 10-attempt controls produced 5
semantic-empty STOPs, 20 text responses, and 5 function calls in 30 attempts.
Transport and startup failures were excluded.

Controlled payload reductions were non-monotonic:

| Variant | Attempts | Semantic-empty | Text | Function call |
|---|---:|---:|---:|---:|
| unchanged full HIGH | 30 | 5 | 20 | 5 |
| current user turn only | 10 | 7 | 0 | 3 |
| first/second system-prompt halves | 20 | 0 | 5 | 15 |
| even/odd system-prompt lines | 20 | 0 | 12 | 8 |
| no function-call/response history | 10 | 0 | 10 | 0 |
| remove early/late 19 atomic tool exchanges | 20 | 0 | 14 | 6 |
| omit all 38 thought signatures | 10 | 7 | 3 | 0 |
| replace all 38 with the supported dummy signature | 20 | 0 | 17 | 3 |
| replace only early or late 19 signatures | 20 | 4 | 13 | 3 |
| replace only the final signature | 10 | 1 | 8 | 1 |

Replacing every real signature with the dummy representation yielded 0/20
semantic-empty versus 5/30 for unchanged real signatures. The one-sided exact
table probability is approximately 0.067, so this is suggestive, not
root-cause proof. Replacing either half alone returned to 2/10; no individual
half or final signature was isolated. Omitting signatures greatly worsened the
failure. Do not strip signatures as a fix. Results instead support a
chain-wide interaction among system instruction, tool history, signature
representation, and HIGH thinking. Payload size alone is not monotonic.

Current `agy.exe` SHA-256
`83fb6e9d80e751d174b3738c3eefb054e75e85e47b17d1e159fe4831adceadc8`
was exercised through a localhost-only, permissioned MITM path. Observation
points are found by masked byte pattern with an exact-one-match guard rather
than fixed RVAs. Frida attachment caused the current process to exit before a
request, so Frida rows are negative instrumentation evidence only.

AGY session-loading paths were tested before wire injection. A temporary custom
agent successfully spoofed the complete captured system instruction. The CLI's
`--continue` and `--conversation` options can resume AGY-created conversations,
but no supported path imports an arbitrary external transcript with the exact
133-content history. The temporary agent was removed and the prior agent list
restored. Therefore system-prompt spoofing alone was not treated as full-payload
proof.

The first MITM family replaced the agent inner request exactly in ten runs, but
used AGY's unrelated current account/project. It produced eight initial text
responses and two function calls, no semantic-empty response. This remains a
different-account control, not evidence that AGY lacks the issue.

A second, operator-only gateway path fixed the locally controlled credential
and project gap. Real current `agy.exe` initiated each request and consumed each
returned SSE stream. The loopback addon loaded the retained credential file,
replaced the outer project with its hash-bound project, forwarded through the
same local direct egress as the account-28 controls, and never persisted headers
or tokens. Google's provider-internal account identity was not independently
exposed, so the claim is limited to the exact local credential/project inputs.
Agent request/response bodies remained only in ACL-private storage.
The stable project fingerprint was
`7bddb774000c7d4d8c9b797066a15a44bd8cf07ddc669c3187d508b2b4303bd7`.

An exact-source probe injected the complete 189,612-byte broken inner request
(SHA-256
`5e08ffa7b4cf45a55f93b2f8edfd3376e314d4261902b59592cc982f68dff622`).
Its agent responses were function call, genuine semantic-empty STOP, then text;
AGY also emitted its checkpoint request. The empty response was HTTP 200 with
thought-only content and STOP, and the following agent request retained the
same canonical inner hash. This directly proves current `agy.exe` is **not
absent** from the provider issue and proves AGY issues a subsequent model
request after receiving it.

Two ten-run forced-account batches then used AGY-native wire configurations
while preserving the exact prompt/history/tools invariant:

| AGY wire tier | CLI runs | Agent attempts | Text | Function call | Semantic-empty | Wire shape |
|---|---:|---:|---:|---:|---:|---|
| HIGH | 10 | 15 | 10 | 3 | 2 | `gemini-3.6-flash-high`, budget 10,000, no level |
| MEDIUM | 10 | 16 | 10 | 5 | 1 | `gemini-3.6-flash-medium`, budget 4,000, no level |

Every agent attempt returned HTTP 200, used one retained credential/project
fingerprint, and matched its expected inner canonical hash. Both HIGH empties
and the MEDIUM empty were followed by another exact agent attempt that recovered
to text or a function-call sequence ending in text. These samples prove
presence and recovery behavior; their sizes are too small to claim a precise
provider-wide rate or that MEDIUM eliminates the condition.

Direct same-account controls separated level and budget effects:

| Direct variant on HIGH alias | Attempts | Text | Function call | Semantic-empty |
|---|---:|---:|---:|---:|
| AGY-native HIGH, budget 10,000, no level | 20 | 12 | 2 | 6 |
| explicit HIGH plus budget 4,000 | 20 | 13 | 1 | 6 |
| budget 4,000 with level omitted | 20 | 12 | 5 | 3 |

Adding a 4,000 budget while retaining explicit HIGH did not help in this
sample. Omitting HIGH and using the moderate budget reduced but did not remove
failures. Thus budget-only mutation is not a reliable replacement for tier
progression. Structural answer-quality proxies did not show a candidate-token
collapse at MEDIUM; they do not measure semantic answer correctness.

Evidence-backed mechanism verdict remains narrow: the provider sometimes
returns HTTP 200 `STOP` after thought-only/no-usable output for this full
request family. No deterministic malformed field or single culprit block was
isolated, and HIGH is not established as the root cause. Current AGY encounters
the same response and automatically retries it.

Production recovery now follows the least-degrading observed order. The initial
request is untouched. The first semantic-empty retry also preserves HIGH,
matching AGY's demonstrated successful recovery. Only a second consecutive
semantic-empty lowers HIGH to MEDIUM and removes a stale budget so native wire
resolution emits the medium alias and budget 4,000. A further semantic-empty
lowers MEDIUM to LOW; LOW is never selected immediately. Non-semantic failovers,
LOW bodies, missing config, and malformed JSON remain byte-identical. Recognized
explicit `-high`/`-medium` model aliases and known AGY wire aliases are lowered
with the body, preventing a suffix from pinning retries to the old wire tier;
suffixless and unknown model IDs preserve their normal routing. The lowerer
supports bare `generationConfig`, OMP SDK `config`, and either form under a
wrapped `request`, including camelCase and snake_case level/budget keys. Prebuilt
v1internal envelopes synchronize their top-level model and tier budget before
send. Targeted JSON edits preserve unrelated large integers, tool arguments,
and history values.

Focused tests prove the retry sequence `HIGH -> HIGH -> MEDIUM -> LOW` and the
final serialized medium request shape (`gemini-3.6-flash-medium`, budget 4,000).
A five-run live forced-account check of that exact service-produced shape
completed seven agent attempts: five text, one function call, one
semantic-empty; all returned HTTP 200 and matched the expected request,
credential, and project hashes. The semantic-empty was recoverable. This proves
provider acceptance, not elimination.

Final review also corrected adjacent request-path gaps. Signature cleaning and
retry detection now cover escaped keys and `request.contents` in prebuilt
envelopes. Aggregated tool-call response rewriting preserves large JSON
integers. HTTP-200 non-stream bodies containing embedded quota errors are
classified as 429 before client write, and naked forwarding errors can no longer
fall through as implicit empty 200 responses. Decompression rejects decoded
bodies over 64 MiB with `http.MaxBytesError` instead of silently forwarding a
truncated prefix.

`go test ./internal/pkg/antigravity ./internal/pkg/httputil ./internal/service
./internal/handler -count=1` passed all four affected packages. Focused
retry/wire-shape/signature/decompression tests passed, and the 40-test Python
replay/stream suite passed. The disposable replay proxy was stopped and port
18317 was released. Retained auth files, refreshed copies, replay key, retention
policy, server-side retained copy, and raw private evidence were not deleted.

## Operational cleanup

Cleanup completed on `2026-07-30`:

- stopped the disposable CLIProxy process and confirmed no matching process remained;
- deleted `/tmp/sub2api-cli-replay` and independently confirmed the path was absent;
- restored the original capture override and recreated only the `sub2api` service with `--no-deps`;
- verified the restored image is `ghcr.io/chrys4lisfag/sub2api:latest`;
- verified Docker health is `healthy` and `GET /health` returns HTTP 200;
- verified `SEMANTIC_EMPTY_CAPTURE_DIR` is blank;
- removed the temporary observation image and confirmed its tag is absent;
- removed the remote observation source, compose override, restore backup, captured diagnostics, replay request, and temporary API-key file, then confirmed every path was absent;
- removed the local raw capture, temporary CLIProxy binaries/scripts, Python cache, and temporary observation override.
- removed the temporary Go effective-configuration logger and its dedicated tests; the service implementation has no remaining working-tree diff, while the permanent 25-line cross-chunk protocol regression remains;

That cleanup statement applies only to the older 2026-07-30 fixture. The newer
2026-08-02 raw request/response evidence and permanent replay credentials are
intentionally retained outside the repository under user-only ACLs.
