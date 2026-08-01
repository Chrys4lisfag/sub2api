# Antigravity Semantic-Empty STOP Investigation

## Status

Controlled tests established one actionable result; protocol-parity and root-cause explanations remain open.

For the captured request, `gemini-3.6-flash-high` with `thinkingLevel: HIGH` frequently returned HTTP 200 streams containing only thought text plus a thought signature, then `finishReason: STOP`, with no non-thought text and no function call. Lowering or disabling thinking made the same request usable in every controlled attempt. This proves the mitigation for this sample, not that HIGH thinking is the root cause.

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

No running diagnostic process, temporary credential, raw prompt, raw response, or generated capture artifact remains on the production host or in the repository.
