# AGY Gemini Protocol Evidence and Replay Plan

Date: 2026-08-14

## Scope and evidence rule

This is the sub2api investigation record. It intentionally lives in this repository's `AGENT/` directory, not in AGY's reversing folder.

Evidence labels:

- **Verified AGY runtime**: observed in a complete, hash-bound `agy.exe` transaction.
- **Verified AGY static**: recovered from the hash-bound IDB and a named application function.
- **Schema capability only**: generated protobuf type/accessor exists, but application population and wire emission are not proved.
- **External report**: useful test input from another project; never proof of AGY behavior.
- **Hypothesis / future test**: not yet established.

## Current verdict

The only causal result established by our controlled request matrix is:

> Lowering or disabling thinking made the captured problematic request usable.

This is a proven mitigation for that captured account, request, model, and tool set. It does **not** prove that HIGH thinking is the root cause. It does not prove that `maxOutputTokens`, tool choice, thought signatures, sampling fields, schema conversion, or response handling are correct. Those remain protocol-parity tests.

Observed matrix:

| Variant | Attempts | Usable |
|---|---:|---:|
| HIGH baseline | 5 | 0-1 |
| LOW | 5 | 5 |
| thinking disabled | 5 | 5 |
| HIGH + `VALIDATED` | 5 | 1 |
| HIGH + `AUTO` | 5 | 1 |
| LOW + `VALIDATED` | 5 | 5 |

## Gemini 3.7 Flash correction and real-AGY evidence, 2026-08-14

### Failure and corrected evidence boundary

The first implementation was wrong. It treated Google's public Gemini API ID `gemini-3.7-flash` as an Antigravity Cloud Code wire ID, added it without querying real AGY, and deployed commit `de7ac66b2f97f473ed3d2e9929f6c1d89fd53f10`. The production account test then returned upstream HTTP 404 `NOT_FOUND: Requested entity was not found.` This is direct proof that public Gemini API naming is not sufficient routing evidence for Antigravity Native.

Correct rule: model IDs, tier behavior, and thinking defaults for Antigravity must come from real `agy.exe` `models` output plus hash-bound successful wire captures. Public Google documentation remains product context only.

### Current real-AGY identity and model list

- path: `C:\Users\koval\AppData\Local\agy\bin\agy.exe`
- version: `1.1.13`
- size: `183,233,176` bytes
- SHA-256: `d628487eefa56b47fded7785125ab634de21e5cc92536f3cad0b56c1ad086eb2`
- `agy.exe models` exposed exactly three Gemini 3.7 Flash entries:
  - `gemini-3.7-flash-high` — `Gemini 3.7 Flash (High)`
  - `gemini-3.7-flash-medium` — `Gemini 3.7 Flash (Medium)`
  - `gemini-3.7-flash-low` — `Gemini 3.7 Flash (Low)`
- The suffixless `gemini-3.7-flash` did not appear in the real AGY model list.

Direct local print-mode controls used a non-sensitive exact-response prompt. All three exact IDs returned process status 0, AGY JSON status `SUCCESS`, and non-empty `OK` output. High additionally reported 61 thinking tokens in that run; the low and medium controls reported zero thinking tokens for this trivial prompt. Token counts are response observations, not tier-budget definitions.

### Hash-bound sanitized wire captures

Runner: external `AGENT/scripts/run_mitm_agy_capture.py`, pinned with `--expected-sha256 d628...6eb2`. Sanitizer SHA-256: `8e33577c7b92f7bf4f26b83fd33070fce1fd14e9a681c15961f859823c256d6c`. The sanitizer was narrowly extended to retain non-secret `model_enum` and `thinkingLevel` enum strings. Prompts, responses, OAuth values, projects, request/session identifiers, signatures, and arbitrary text remained redacted.

| Selected AGY ID | top-level `model` | `request.labels.model_enum` | emitted thinking config | provider result |
|---|---|---|---|---|
| `gemini-3.7-flash-low` | same | `MODEL_PLACEHOLDER_M300` | `includeThoughts: true`, `thinkingBudget: 1000` | HTTP 200, 3 SSE events, non-empty text |
| `gemini-3.7-flash-medium` | same | `MODEL_PLACEHOLDER_M299` | `includeThoughts: true`, `thinkingBudget: 4000` | HTTP 200, 2 SSE events, non-empty text |
| `gemini-3.7-flash-high` | same | `MODEL_PLACEHOLDER_M298` | `includeThoughts: true`, `thinkingBudget: -1` | HTTP 200, 2 SSE events, non-empty text |

`thinkingLevel` was absent from all three captured agent requests. Numeric `thinkingBudget` is therefore observed AGY wire behavior for this binary and these tiers. The `model_enum` values are agent-request label evidence; sub2api's synthesized native wrapper currently uses a checkpoint profile, and real checkpoint captures have labels absent, so these agent-only labels must not be invented on the checkpoint path without separate checkpoint evidence.

Sanitized artifacts remain outside the repository:

- `AGENT/logs/agy-37-low-20260814.jsonl` plus `-meta.json`
- `AGENT/logs/agy-37-medium-20260814.jsonl` plus `-meta.json`
- `AGENT/logs/agy-37-high-20260814.jsonl` plus `-meta.json`

Each metadata file records the exact target/addon hashes, return code 0, four captured rows, and no runner error. Each run contains one successful 3.7 agent request/response and one separate checkpoint request/response; only the agent pair supports the tier table above.

### Corrected implementation contract and release gate

- Expose only the three exact suffixed IDs; remove suffixless `gemini-3.7-flash` from panel, registry, candidates, defaults, and auto-filled mappings.
- Public-to-wire mapping is identity for each exact tier.
- When no caller thinking directive exists, synthesize the captured numeric budget: low `1000`, medium `4000`, high `-1`, with `includeThoughts: true`.
- Preserve explicit camelCase or snake_case thinking level/budget settings.
- Semantic retry lowering follows high -> medium -> low while keeping wire ID and default budget synchronized.
- Migration `176_add_gemini37_to_model_mapping.sql` is already deployed and remains immutable. Corrective migration `177_replace_gemini37_model_mapping.sql` adds the three tier identities and removes only the erroneous suffixless identity entry, preserving custom suffixless remaps and exact tier overrides.
- No corrected deployment is allowed until a locally built corrected sub2api revision sends all three exact IDs to the real provider, logs the selected wire IDs without secrets, and receives non-empty responses for every tier.

### Corrected local validation

The release gate passed before commit or deployment. `TestLiveGemini37Tiers` ran local working-tree code, not the deployed image. It exercised `ResolveWireFromBody`, `wrapNativeV1Internal`, and a real Cloud Code SSE POST using an ephemeral OAuth credential file stored only under the ACL-protected private evidence root. The test never logged the credential, project, prompt, or response content.

| local requested ID | selected wire ID | locally serialized budget | provider result |
|---|---|---:|---|
| `gemini-3.7-flash-low` | same | 1000 | HTTP 200, non-empty SSE text |
| `gemini-3.7-flash-medium` | same | 4000 | HTTP 200, non-empty SSE text |
| `gemini-3.7-flash-high` | same | -1 | HTTP 200, non-empty SSE text |

The gated test did not skip. All three named subtests ran and passed before review, then ran again after retry/privacy hardening; the final run completed in 3.38 seconds. Additional local validation passed:

```text
go test ./migrations ./internal/domain ./internal/pkg/antigravity ./internal/service ./internal/handler -count=1
pnpm test:run src/composables/__tests__/useModelWhitelist.spec.ts src/components/admin/account/__tests__/AccountTestModal.spec.ts
pnpm typecheck
```

Migration 177 was also executed twice after migration 176 against a disposable PostgreSQL 18 database. Assertions proved SQL execution, idempotency, exact-tier insertion, existing exact-tier precedence, removal of only the erroneous suffixless identity mapping, preservation of a custom suffixless remap, and no changes to unrelated/deleted accounts. The disposable container was removed.

This live test proves the corrected local serializer, exact tier routing, captured numeric defaults, provider acceptance, and non-empty output. It does not exercise production account scheduling or deployment; those remain post-release checks.

Pipeline:

```text
real agy.exe models + hash-bound successful captures
    -> exact public/wire tier IDs and numeric budgets
    -> frontend whitelist, priorities, labels, presets
    -> backend registry/default mapping + corrective migration
    -> ResolveWireFromBody / AntigravityWireModel identity routing
    -> native checkpoint wrapper with captured budget defaults
    -> local live provider probe for low, medium, high
    -> tests, commit, image publication, Electerm deployment
```

## Direct answers saved for payload-parity work

1. **Issue evidence:** external issues are test cases, not proof that sub2api is wrong. The direct official-AGY typed round-trip found one concrete payload mismatch: AGY emits `request.labels`, while sub2api dropped it. `GeminiRequest.Labels` now preserves the string map; synthetic round-trip, native-wrapper, and private exact-envelope semantic probes pass. Issue-derived risks still requiring parity tests include invented sampling defaults, thought-signature/history continuity, function-call IDs, schema casing, `systemInstruction`, stream metadata, and function-call precedence over terminal STOP.
2. **Complete field capture:** every serialized key is known for the five accepted raw bodies, six model-alias structural requests, and eight explicit-effort/default structural requests. HIGH, MEDIUM, LOW, and omitted-effort CLI controls are captured, but AGY-global coverage and 100% behavior parity are not proved. `thinkingLevel`, `temperature`, `topP`, `topK`, `candidateCount`, `stopSequences`, `responseModalities`, `imageConfig`, and allowed function names were absent from those bodies; other model families, tool choices, multimodal requests, providers, and retry-generated requests remain uncovered.
3. **STOP reproduction and current AGY replay:** the earlier 74,647-byte family reproduced semantic-empty STOP at HIGH and established LOW/disabled mitigation. The newer complete 189,612-byte account-28 request produced 5 semantic-empty STOPs in 30 unchanged HIGH provider-success controls; LOW and disabled controls remained usable. Current `agy.exe` then sent that exact newer inner request in ten hash-bound, raw-captured transactions: eight initial text responses and two function calls, no semantic-empty STOP. This proves exact current-binary replay but not an upstream fix because AGY used a different outer project/account and egress. HIGH remains a condition, not a proved root cause.

## Gemini 3.7 Flash virtual picker alias, superseding contract, 2026-08-14

The exact-tier-only exposure above was a safe corrective intermediate, not the final picker UX. Real AGY still exposes and accepts only the three suffixed wire IDs. Sub2api now exposes one suffixless **virtual** client alias, `gemini-3.7-flash`; that alias is resolved locally and is never sent to AGY.

| client model | client thinking level | real AGY wire ID | emitted thinking config |
|---|---|---|---|
| `gemini-3.7-flash` | `low` or `minimal` | `gemini-3.7-flash-low` | `thinkingBudget: 1000`, `includeThoughts: true` |
| `gemini-3.7-flash` | `medium`, absent, or unknown | `gemini-3.7-flash-medium` | `thinkingBudget: 4000`, `includeThoughts: true` |
| `gemini-3.7-flash` | `high` | `gemini-3.7-flash-high` | `thinkingBudget: -1`, `includeThoughts: true` |

Routing accepts camelCase or snake_case level fields in bare Gemini REST, Google SDK `config`, and wrapped `request` forms. Before provider dispatch it removes all level and stale budget aliases from those paths, writes the captured numeric budget in camelCase, and keeps `includeThoughts: true`. Explicit suffixed IDs remain accepted internally and stay pinned for backward compatibility and semantic-retry lowering, but registry, frontend whitelist, presets, priorities, and OMP picker expose only the virtual suffixless alias. Migration `178_add_gemini37_virtual_alias.sql` adds the identity alias to eligible existing mappings with existing custom mappings taking precedence.

`TestLiveGemini37VirtualAliasSlider` exercised the local working tree against the credential-derived real AGY endpoint. Low, medium, high, and absent-level/default-medium cases each selected the expected suffixed wire ID, serialized the exact captured numeric budget without `thinkingLevel`, received HTTP 200, and produced non-empty SSE text. This validates local alias resolution plus provider acceptance; production scheduling and deployed-image behavior remain separate release checks.

## Gemini 3.8 Flash support, real-AGY evidence, 2026-09-02

### Binary identity

- path: `C:\Users\koval\AppData\Local\agy\bin\agy.exe`
- version: `1.1.24`
- size: `187,601,560` bytes
- SHA-256: `7585871b1a34f9acc7f9c065f09e5bd1a7009519f0f219a4c43bb565c7880c95`
- previous 1.1.13 binary retained on disk as `agy.exe.<ns>.old` (183,233,176 bytes)

`agy.exe models` exposed exactly three Gemini 3.8 entries and no suffixless ID:

- `gemini-3.8-flash-high` — `Gemini 3.8 Flash (High)`
- `gemini-3.8-flash-medium` — `Gemini 3.8 Flash (Medium)`
- `gemini-3.8-flash-low` — `Gemini 3.8 Flash (Low)`

Unlike 3.7, there is no `gemini-3.8-flash-tiered` alias in the provider model list.

Direct print-mode controls with a non-sensitive prompt returned process status 0, AGY JSON
status `SUCCESS`, and non-empty `OK` output for all three exact IDs.

### Hash-bound sanitized wire captures

Runner `AGENT/scripts/run_mitm_agy_capture.py` pinned with `--expected-sha256 7585...80c95`,
existing sanitizer addon unchanged. Artifacts stay outside the repository at
`AGENT/logs/agy-38-{low,medium,high}-20260902.jsonl` plus `-meta.json`; each run recorded
return code 0 and four captured rows.

| Selected AGY ID | top-level `model` | `request.labels.model_enum` | emitted thinking config | provider result |
|---|---|---|---|---|
| `gemini-3.8-flash-low` | same | `MODEL_PLACEHOLDER_M320` | `includeThoughts: true`, `thinkingBudget: 1000` | HTTP 200, non-empty SSE text |
| `gemini-3.8-flash-medium` | same | `MODEL_PLACEHOLDER_M319` | `includeThoughts: true`, `thinkingBudget: 4000` | HTTP 200, non-empty SSE text |
| `gemini-3.8-flash-high` | same | `MODEL_PLACEHOLDER_M318` | `includeThoughts: true`, `thinkingBudget: -1` | HTTP 200, non-empty SSE text |

`thinkingLevel` was absent from every captured 3.8 agent request, exactly as with 3.7. Numeric
`thinkingBudget` is therefore the observed wire contract. Each run also contained one unrelated
`gemini-3.1-flash-lite` checkpoint request whose thinking config is `includeThoughts: false`,
`thinkingBudget: 0`; only the agent pair supports the table above. The `model_enum` labels are
agent-request evidence and must not be invented on sub2api's checkpoint path.

### Independent provider metadata confirmation

A disposable allowlisting addon captured `/v1internal:fetchAvailableModels` and persisted only
numeric and model-name fields. It reported, per 3.8 tier: `thinkingBudget` -1 / 4000 / 1000 for
high / medium / low, `minThinkingBudget: 32`, `maxOutputTokens: 65536`, `supportsImages: true`,
`supportsThinking: true`, `supportsVideo: true`. No input-token or context-window field exists in
that response. The addon and its run artifacts were deleted after reading; only the three
sanitized stream captures were retained.

### What `high` means

`high` is mapped, not unmapped: its captured budget is `-1`, which is AGY's dynamic
(model-chosen, uncapped) budget rather than a missing value. Measured with one identical
reasoning prompt through real AGY print mode:

| tier | wire budget | thinking tokens reported |
|---|---:|---:|
| low | 1000 | 0 |
| medium | 4000 | 139 |
| high | -1 | 142 |

`DefaultVariantThinkingLevel` intentionally has no 3.7 or 3.8 entries. Those families are routed
through the numeric-budget path because their real requests carry no `thinkingLevel` at all;
3.5 and 3.6 keep the level path because their own captures used levels.

### Implementation contract

Sub2api exposes one suffixless virtual client alias `gemini-3.8-flash`, resolved locally and
never sent to AGY, mirroring the 3.7 contract.

| client model | client thinking level | real AGY wire ID | emitted thinking config |
|---|---|---|---|
| `gemini-3.8-flash` | `low` or `minimal` | `gemini-3.8-flash-low` | `thinkingBudget: 1000`, `includeThoughts: true` |
| `gemini-3.8-flash` | `medium`, absent, or unknown | `gemini-3.8-flash-medium` | `thinkingBudget: 4000`, `includeThoughts: true` |
| `gemini-3.8-flash` | `high` | `gemini-3.8-flash-high` | `thinkingBudget: -1`, `includeThoughts: true` |

Shared helpers were generalized rather than duplicated: `DefaultVariantThinkingBudget`,
`LowerNumericBudgetTierOnce` (with `LowerGemini37TierOnce` kept as a thin alias),
`IsNumericBudgetVirtualAlias`, and `NormalizeNumericBudgetTierBody` now cover both 3.7 and 3.8;
`isGemini37FlashTier` and `thinkingBudgetForModel` gained the three 3.8 tiers. `maxOutputTokens`
needed no change: the existing flash cap of 65536 already matches the provider metadata.
Migration `179_add_gemini38_virtual_alias.sql` adds the identity alias to eligible existing
mappings with custom mappings taking precedence, using the same shape as migration 178.

### Local validation

`TestLiveGemini38VirtualAliasSlider` exercised local working-tree code against the
credential-derived real endpoint. Low, medium, high, and absent-level cases each selected the
expected exact wire ID, serialized the captured numeric budget with no `thinkingLevel`, received
HTTP 200, and produced non-empty SSE text. The ephemeral credential lived only in an
ACL-protected private directory and was deleted afterwards.

```text
go test ./internal/pkg/antigravity ./internal/domain ./internal/service ./internal/handler ./migrations -count=1
pnpm test:run src/composables/__tests__/useModelWhitelist.spec.ts src/components/admin/account/__tests__/AccountTestModal.spec.ts
pnpm typecheck
```

All passed before commit. OMP harness gained a single `gemini-3.8-flash` entry whose
`maxTokens: 65536` matches provider metadata; `contextWindow` and pricing are inherited family
values and are explicitly not AGY-reported.

## AGY artifact and evidence source

Hash-bound static-reversing artifact:

- binary: `C:\Users\koval\AppData\Local\agy\bin\agy_28_07.exe`
- SHA-256: `0da7e458b4daec92898d6d7f28d40e51d0f2b26fc20fb80a285d0a30bcde4519`
- active IDB observed in this pass: `agy_28_07.after_go_symbols.i64`
- image base: `0x140000000`
- recovered Go symbols: 154,150 valid records, zero parser errors

The CLI-distributed `agy.exe` later resolved to a different binary during runtime controls:

- launched path: `C:\Users\koval\AppData\Local\agy\bin\agy.exe`
- launch-time and post-capture SHA-256: `83fb6e9d80e751d174b3738c3eefb054e75e85e47b17d1e159fe4831adceadc8`
- size: 170,597,528 bytes
- an initial launch pinned to the older `0da7...4519` hash failed closed before capture

The two hashes are separate evidence identities. All IDB addresses and static findings in this document remain bound only to `0da7...4519`; the new tier captures below are bound only to `83fb...adc8`. The executable name is not a stable fixture identity.

Accepted raw runtime evidence remains access-controlled outside both repositories:

- `%LOCALAPPDATA%\agy-private-evidence\payload-accepted-postinit-20260729-211848-475517`
- sanitized index: AGY reversing folder `AGENT/baselines/agy_28_07_mcp_payload_capture.json`

No prompt, tool argument value, credential, project ID, session ID, request ID, response text, or thought signature is copied into this document.

## Protocol used by AGY

### Verified runtime transport

The accepted transaction emitted:

- method: `POST`
- scheme: HTTPS
- host: `daily-cloudcode-pa.googleapis.com`
- path: `/v1internal:streamGenerateContent`
- query field: `alt` (SSE mode)
- body: JSON `v1internal` envelope
- response: Server-Sent Events, one JSON object per `data:` scanner token

Observed outer request keys:

```text
model
project
request
requestId
requestType
userAgent
```

Observed inner `request` keys on the accepted agent request:

```text
contents
generationConfig
labels
sessionId
systemInstruction
toolConfig
tools
```

This proves the accepted Cloud Code path. It does not prove that every AGY provider, auth mode, request type, or model uses the same host or envelope.

### Complete generation configuration from real AGY requests

The raw bodies are complete JSON and hash-bound. For the accepted captured transactions, every serialized `generationConfig` key is known.

| Request class | `maxOutputTokens` | `includeThoughts` | `thinkingBudget` | `thinkingLevel` | `temperature` | `topP` | `topK` | `stopSequences` |
|---|---:|---:|---:|---|---|---|---|---|
| agent, initial | 65,536 | true | 10,000 | absent | absent | absent | absent | absent |
| agent, tool follow-up | 65,536 | true | 10,000 | absent | absent | absent | absent | absent |
| checkpoint | 16,384 | false | 0 | absent | absent | absent | absent | absent |

Four captured agent/follow-up bodies from two accepted transaction stages had the same generation-key set:

```text
generationConfig.maxOutputTokens
generationConfig.thinkingConfig.includeThoughts
generationConfig.thinkingConfig.thinkingBudget
```

No captured body contained `temperature`, `topP`, `topK`, `candidateCount`, `stopSequences`, or a serialized `thinkingLevel`.

A structural-only inventory was rerun over five complete private request bodies without emitting prompts, identifiers, schemas, tool names, argument values, or signatures:

| Stage | Bytes | Body SHA-256 | labels | contents | calls / responses / signatures |
|---|---:|---|---:|---:|---:|
| agent initial, transaction 1 | 90,143 | `cd079e52db9e75c7418022fe31450ef78d2ad1a23891efbe29cf9446230b86d3` | 6 | 1 | 0 / 0 / 0 |
| agent follow-up, transaction 1 | 93,738 | `597c87f1ada790b28d61d81b554028f79e45862ff47218954b2565b494b2c1ee` | 6 | 3 | 1 / 1 / 1 |
| checkpoint | 1,002 | `8eb70bc7e5082468c2fe9c62aae44353e2ad42b3b5ede41327b690e96f827321` | absent | 1 | 0 / 0 / 0 |
| agent initial, transaction 2 | 94,771 | `49eba076d4dcac977fd8a745e5cc4576142074adc78508c94204e3c1f36c0ca2` | 7 | 5 | 1 / 1 / 2 |
| agent follow-up, transaction 2 | 95,886 | `4d1350c01f9ae6831bc4f882b74f9d347a40d1b4b37d60c29c31c4a654c91f4f` | 7 | 7 | 2 / 2 / 3 |

All five had the six-key outer envelope listed above, a session ID, and a system instruction. Every agent body had labels, 22 tool items/declarations, and `VALIDATED`; checkpoint omitted labels, tools, and tool configuration. `imageConfig` and `responseModalities` were also absent from all five. Counts describe structure only; transaction-2 initial already contained prior history, so its call/response counts are not claims about the new turn.

Tool configuration:

| Request class | tools | `toolConfig` | mode | allowed names |
|---|---:|---|---|---|
| agent initial/follow-up | 22 collections/items in captured structure | present | `VALIDATED` | absent |
| checkpoint | absent | absent | absent | absent |

The accepted agent request used `requestType: agent`; the smaller disabled-thinking request used `requestType: checkpoint`.

### Current-binary sanitized HIGH/MEDIUM/LOW controls

Three fresh CLI runs captured one agent request and one checkpoint request each from the `83fb...adc8` executable. The sanitizer recursively retained every JSON dictionary key and list element while redacting sensitive values. Therefore these artifacts prove complete serialized key structure and allowlisted configuration values, but they are **not raw or byte-exact request bodies**. `body_length` is the observed raw byte count; artifact hashes below identify the sanitized JSONL and metadata files.

| Selected tier | Agent model | Agent bytes | `maxOutputTokens` | `includeThoughts` | `thinkingBudget` | labels | tools / mode |
|---|---|---:|---:|---:|---:|---:|---|
| HIGH | `gemini-3.6-flash-high` | 85,707 | 65,536 | true | 10,000 | 6 | 22 / `VALIDATED` |
| MEDIUM | `gemini-3.6-flash-medium` | 85,710 | 65,536 | true | 4,000 | 6 | 22 / `VALIDATED` |
| LOW | `gemini-3.6-flash-low` | 85,704 | 65,536 | true | 1,000 | 6 | 22 / `VALIDATED` |

Every agent request had the same outer six-key envelope and the same seven inner keys: `contents`, `generationConfig`, `labels`, `sessionId`, `systemInstruction`, `toolConfig`, and `tools`. Every run also emitted an 860-byte `checkpoint` request using `gemini-3.1-flash-lite`, `maxOutputTokens: 16384`, `includeThoughts: false`, and explicit `thinkingBudget: 0`; checkpoint omitted labels, tools, and tool configuration.

The current agent labels used six string-valued keys: `last_step_index`, `model_enum`, `trajectory_id`, `used_claude`, `used_claude_conservative`, and `used_non_gemini_model`. Older accepted follow-up bodies added string-valued `last_execution_id`, producing seven keys. Step index changed with history; trajectory/execution identifiers stayed stable over their observed scopes. Identifier values remain redacted. Checkpoint labels were absent, not an empty object.

Across all six sanitized requests, `thinkingLevel`, `temperature`, `topP`, `topK`, `candidateCount`, `stopSequences`, `responseModalities`, `imageConfig`, and `allowedFunctionNames` were absent. This absence is scoped to these current-binary CLI controls. The captures show AGY serializing numeric budgets for these three selected tiers; they do not prove that no AGY path can serialize `thinkingLevel`.

Private artifact identities:

| Tier | Sanitized JSONL SHA-256 | Metadata SHA-256 |
|---|---|---|
| HIGH | `8e39336d09dfadb71525d7805b0f5797b1c7bcbd641a2f48abdeae372a32f28d` | `9cc63bf350cfa2c5cdbd854fcbae24a1b900d46450cbe3838c46af46e987d7fd` |
| MEDIUM | `4dd759c8ecc2cc7087ed3b8a18638c6e3506393772845cf985df68f45d850652` | `ac1d5ca112fa4b31583c89bc454f678c291be39c9294d6ddd7dfc68da215167a` |
| LOW | `0bad073690bef2f35203eabd925e1074b0bd22f9c7bc0619dcbdbc960a0baa9e` | `8d0e62568bb0a25e92d8ac38c0bd1164063fdea0f45ddebf642e5455ec6af0c0` |

### Explicit `--effort` and omitted-default controls

Four more hash-pinned print-mode controls used these AGY arguments:

```text
--dangerously-skip-permissions [--effort high|medium|low] -p <redacted fixed prompt> --print-timeout 30s
```

All four exited successfully. Explicit effort selected the same wire model, budget, and key sets as the corresponding model-alias controls:

| CLI control | Agent model | Agent bytes | `maxOutputTokens` | `includeThoughts` | `thinkingBudget` | Agent keys/tools |
|---|---|---:|---:|---:|---:|---|
| `--effort high` | `gemini-3.6-flash-high` | 85,708 | 65,536 | true | 10,000 | same seven inner keys; 22 / `VALIDATED` |
| `--effort medium` | `gemini-3.6-flash-medium` | 85,711 | 65,536 | true | 4,000 | same seven inner keys; 22 / `VALIDATED` |
| `--effort low` | `gemini-3.6-flash-low` | 85,705 | 65,536 | true | 1,000 | same seven inner keys; 22 / `VALIDATED` |
| effort omitted | `claude-opus-4-6-thinking` | 79,695 | 64,000 | true | 1,024 | same seven inner keys; 22 / `VALIDATED` |

Each also emitted an 861-byte Gemini 3.1 Flash Lite checkpoint with `maxOutputTokens: 16384`, `includeThoughts: false`, explicit `thinkingBudget: 0`, four inner keys, and no labels/tools/tool config. The same optional generation/tool fields listed as absent above were absent here. The omitted-effort result is the installed CLI's current default selection, not a universal AGY default.

| Control | Sanitized wire JSONL SHA-256 | Hash-only session JSONL SHA-256 |
|---|---|---|
| HIGH | `e57b8fdf382f53ae52217b9d1ed595bef27f76f7606b6da2802dba358701de23` | `0c4163a17d37a57288cda37b194d6ccab53a4b25c6dd3dcea7b6a70e7e2bc6ea` |
| MEDIUM | `3827048378744dd3fa597f056c40de75ee2275ff53514b10d284543eb6e7cc26` | `2afbb81b5c4ddbc6c63fcdf7a267f5af286bd3b74cd3d345d3bf2ae515e62f49` |
| LOW | `bf286472516b053c34ea4e4729fa7776b075794e84fec9c5d2b8f80da654234d` | `b50a773f661cef4a1572851ee8510ba3dcaace072b5e34f827f0fb14e83751be` |
| omitted | `d8bda78e30223178459b570e03503a74de1815abfcfedec76ffab3de37b0e5c7` | `ac859e95fa871eff83155027583436d977ff7ff5ee5736a92a1cad67f736bfcc` |

Runner SHA-256 was `d91d36d13811d3d6342f0ed8af67cdc6487e3bcd2a966ee4a2d738d91f5c5713`; sanitizer SHA-256 was `0f47bdef53a66e3aed26277bb44507a10b32353212afa16b2e2d4a22bb313ffb`. Executable SHA-256 remained `83fb...adc8` before and after the family. A separate interactive HIGH control produced only hash-only terminal/session evidence (`4fc008a2bf3f3dcf4e2fef291650420935bda586af3d6c358b58f4cd4c7c5db2`) and no provider row; it is failure/control evidence, not payload evidence. All capture proxies are stopped, and private evidence inherits the user-only ACL.

### Full-field capture-to-type parity matrix

This matrix compares serialized JSON semantics, not byte ordering. “Absent” means the recursively key-preserving sanitizer did not find the key in any listed current control; it is not a claim about every AGY path.

| Surface | Real AGY evidence | sub2api representation after parity fix | Verdict |
|---|---|---|---|
| outer envelope | six keys on all raw and sanitized requests | `V1InternalRequest` preserves all six | match for captured bodies |
| `contents` / history | present; tool calls, responses, IDs, and signatures observed in raw follow-ups | typed parts preserve captured shapes | match for exact 94,771-byte semantic round-trip |
| `systemInstruction` | present on every listed request | typed pointer, omitted only when absent | match for captured bodies |
| `tools` | 22 on agent; absent on checkpoint | typed declarations preserve captured keys | match for captured bodies |
| `labels` | six or seven string pairs on agent; absent on checkpoint | `map[string]string`, `omitempty` | fixed; present and absent regressions pass |
| `sessionId` | present on every listed request | string field | match for captured bodies |
| `maxOutputTokens` | 65,536 agent; 16,384 checkpoint | integer field preserves observed nonzero values | match for captured bodies |
| `includeThoughts` | true agent; false checkpoint | pointer boolean preserves true, false, and absence | match plus absence semantics tested |
| `thinkingBudget` | 10,000/4,000/1,000 by tier; explicit 0 checkpoint | pointer integer preserves positive, zero, and absence | match |
| `thinkingLevel` | absent; static accessor capability only | optional pointer string | ready to preserve; no runtime parity claim |
| `temperature` / `topP` / `topK` | absent | optional numeric pointers | ready to preserve zero/populated/absent; no runtime parity claim |
| `candidateCount` | absent | optional integer pointer | ready to preserve zero/populated/absent; no runtime parity claim |
| `stopSequences` | absent | optional slice pointer | populated, explicit empty, and absent shapes preserved; no runtime parity claim |
| `responseModalities` | absent | optional slice pointer | populated, explicit empty, and absent shapes preserved; no runtime parity claim |
| `imageConfig` | absent | optional typed object | ready to preserve known fields; no runtime parity claim |
| tool mode | `VALIDATED` agent; tool config absent checkpoint | typed `mode` | match for captured bodies |
| `allowedFunctionNames` | absent | optional slice pointer | populated, explicit empty, and absent shapes preserved; no runtime parity claim |

Exact private-envelope semantic round-trip now matches every field that the 94,771-byte body emitted. This is 100% structural parity for that one captured body, not 100% AGY-global protocol or production-builder behavior parity.

### Have we dumped every configuration field?

**For the accepted captured bodies: yes.** The complete JSON bodies were parsed and their full key sets inspected.

**For AGY globally: no.** Missing coverage:

- model families other than the captured Gemini 3.6 Flash tiers, Gemini 3.1 Flash Lite checkpoint, and installed-default Claude Opus 4.6 Thinking path;
- no-tools ordinary chat and disabled-thinking agent requests;
- explicit required/none/function-name tool choices;
- image/multimodal generation;
- Business AI Code, Vertex AI, and production CCPA paths;
- retry-generated requests after a truly semantic-empty STOP;
- requests where model metadata supplies `thinkingLevel` instead of budget.

Generated protobuf accessors show schema capability for temperature, top-p, top-k, max output tokens, stop sequences, candidate count, and many newer fields. Accessor presence is not evidence that AGY populates or sends them.

### Verified static builder behavior

`codeassistclient.BuildGenerateContentRequest`:

- VA: `0x141df29e0`
- builds contents, system instruction, function declarations, tool config, session ID, generation config, and thinking config;
- writes `GenerationConfig.maxOutputTokens` through the protobuf field whose getter reads offset `+40`;
- constructs `ThinkingConfig.includeThoughts` and `thinkingBudget` through fields whose getters read offsets `+8` and `+16`;
- contains a conditional write to the field whose `GetThinkingLevel` accessor reads offset `+24` when an internal input value is nonzero.

Runtime capture proves max tokens, include-thoughts, and budget serialization. `thinkingLevel` is static capability only because it was absent from every captured body.

Generated accessors for temperature/top-p/top-k read protobuf fields at offsets `+8`, `+16`, and `+24` of `GenerationConfig`. The accepted application builder and runtime bodies did not establish population of those fields.

## Thinking lowering inside AGY

Verified runtime difference:

- ordinary captured agent requests: `includeThoughts=true`, `thinkingBudget=10000`;
- captured checkpoint request: `includeThoughts=false`, `thinkingBudget=0`.

Therefore AGY does use request-type-specific thinking suppression. This does **not** establish an error-triggered fallback. No captured sequence shows AGY receiving a semantic-empty STOP, lowering thinking, and retrying.

Future static target: trace inputs at `BuildGenerateContentRequest` offsets corresponding to budget, level, and suppression flags back to model metadata and request-type selection. Future runtime target: capture one genuine semantic-empty response and every subsequent provider request in the same trace.

## STOP and function-call handling

### Verified normal protocol sequence

One hash-bound AGY transaction proves:

1. initial provider request;
2. inbound SSE containing `functionCall`;
3. separate empty SSE with `finishReason: STOP` on the same trace;
4. local MCP `tools/call`;
5. successful MCP result;
6. follow-up provider request containing matching `functionCall` and `functionResponse`;
7. final user-visible completion.

Thus an empty terminal STOP event is not an error when an earlier event contained a function call. Any classifier must accumulate stream state.

### Current-binary time-tool trace and STOP recreation

A bounded current-binary control used installed `agy.exe` SHA-256
`83fb6e9d80e751d174b3738c3eefb054e75e85e47b17d1e159fe4831adceadc8`,
the access-controlled `agy_capture_get_time` MCP fixture, and a sanitized
localhost MITM. The first control externally observed this sequence:

1. HIGH agent request with six labels, 22 tools, `VALIDATED`,
   `maxOutputTokens=65536`, `includeThoughts=true`, and
   `thinkingBudget=10000`;
2. HTTP 200 stream containing a function call and then a separate empty
   `STOP`;
3. one fixture `tools/call`;
4. follow-up agent request containing a function call and function response;
5. final nonempty model response and terminal `STOP`.

This current-hash trace is **not accepted identity-correlated runtime
evidence**. The runner was intentionally in preflight mode, produced no Frida
wire rows, and reported `preflight_complete_no_wire`,
`request_call_identity_verified=false`, and `accepted=false`. The MITM rows
and MCP log are sequential and timestamp-adjacent, but share no verified
request or call identifier. They must not be used to claim that the observed
wire function call caused the observed MCP call or that this path is fixed.
The private first-trace identities are:

- `trace.json`: `d4d1ad429a3f2583015666b74cb44b38bdca7d2ece13ba32da8da9e8813c2720`;
- `meta.json`: `64471860280b70dfe8de9f995301bb41dc4472c69d2446c77c2a172664076b0e`;
- `mcp.jsonl`: `6936ff5bf2ecaa7cc3ee14ef194c1620f78115857cdb21443542498d2ed37d68`;
- sanitized `wire.jsonl`: `0ad9a8e5de4b7b290f3d329be7a8377649b0c4ae8de13799f81f7afff954039a`.

A second bounded family ran five sequential time-tool attempts per tier:

| Tier | Budget | Attempts completed | Target agent streams | Function-call streams | Final-text streams | Semantic-empty STOP |
|---|---:|---:|---:|---:|---:|---:|
| HIGH | 10,000 | 5/5 | 10 | 5 | 5 | 0 |
| MEDIUM | 4,000 | 5/5 | 11 | 6 | 5 | 0 |
| LOW | 1,000 | 5/5 | 11 | 6 | 5 | 0 |

All 17 function-call streams contained a later separate empty `STOP`; all
remain usable under stream-wide classification. Two sessions produced one
additional usable function-call stream. Each tier's sanitized MCP logs still
contained exactly five `tools/call` requests. No provider error, malformed
function call, `MAX_TOKENS`, safety finish, or semantic-empty target stream
was observed.

Every initial agent request in this family contained six labels, 22 tools,
`VALIDATED`, `maxOutputTokens=65536`, and the tier budget. Checkpoint requests
preserved explicit `includeThoughts=false` and `thinkingBudget=0`. Thus the
family exercises captured label, tool-mode, false/zero, and stream-wide STOP
handling shapes. Optional fields absent from current AGY requests remain
synthetic type-parity tests, not live runtime coverage.

The family is also **0/15 accepted** by the strict runner because preflight
mode cannot verify shared wire/MCP identity. Its result is a structural,
sequential external-wire observation, not a successful correlated
reproduction and not evidence that the semantic-empty bug is fixed. It also
does not replay the old 74,647-byte problematic request: that sensitive raw
payload was intentionally not retained after the earlier controlled matrix.
Prompt, history, tool count, labels, tool mode, and thinking representation
differ. Consequently, 0 observed semantic-empty streams here neither
contradicts the earlier HIGH failures nor changes the only proven mitigation:
lowering or disabling thinking helped that captured problematic sample.

Private family root:
`%LOCALAPPDATA%\agy-private-evidence\stop-matrix-current-20260801`.
Key hashes:

- sanitized `wire.jsonl`:
  `5f305459ff57d69c78fd398a2c425e0e0faa6082119ee7c60ef043b9ef098d85`;
- structural `classification.json`:
  `a40ddec014d2bb803b9e823f707e4465a4c8982eff6acb5b9d89e17f856f3c3e`;
- `runner-results.json`:
  `8fe112ef87e56bd74af76a45d7b7c11f3be23e83df8c1fe0c2bfa694501f2dab`;
- hash-only `manifest.json`:
  `d8dad8067db432c6a4b06fee9f9195ae433a8458862147231fa1b1eefbe362e5`.

Settings were restored after every attempt. Their final SHA-256 was
`cc793e790f64fe46d4b53da2750a9fd90bbd9d455f59806b2251fff0f4d90dab`.
The localhost proxy is stopped. No request/response body, credential, prompt,
tool value, project/session/request identifier, or thought signature was
copied into this repository.

### Verified static framing layer

`codeassistclient.ProcessStreamChunks` at `0x141e03080` scans complete SSE tokens and sends each converted event to its callback. `codeassistclient.toStreamResponseChunk` at `0x141df9f20` extracts `event:` and `data:` fields; it does not merge separate function-call and STOP events or classify `finishReason`.

The higher-level semantic STOP handler remains unresolved. Direct static evidence does not yet show whether AGY retries a response containing only thoughts/signatures and STOP.

## Retry and recovery status

### Hash-bound static call graph

All addresses below are from `agy_28_07.exe`, SHA-256
`0da7e458b4daec92898d6d7f28d40e51d0f2b26fc20fb80a285d0a30bcde4519`,
image base `0x140000000`, IDB `agy_28_07.after_go_symbols.i64`.

The normal framework execution path is:

```text
core.runExecutionLoop                         0x14142b1c0
  -> core.generateWithOutputRetry             0x14142b7e0
       -> core.generateWithAPIRetry            0x14142bec0
            -> core.processModelStream         0x14142e260
```

`generateWithOutputRetry` has one direct application caller, `runExecutionLoop`.
`generateWithAPIRetry` is called by `generateWithOutputRetry`.

A distinct planner stack is:

```text
generator.PlannerGenerator.Generate                    0x14189ab20
  -> PlannerGenerator.generateWithModelOutputRetry     0x14189ac80
       -> PlannerGenerator.generateWithAPIRetry         0x14189b560
       -> generator.generateModelOutputRetryPrompt      0x14189f720
```

`core.GenerateFullResponseWithRetry` at `0x14142a4a0` is referenced by the
user-intent and context-summary hooks. It is not evidence for main-turn
semantic-empty recovery.

### Core API retry

`core.generateWithAPIRetry` retries transport/API errors only when the model's
`ClassifyError` method marks the error retryable and the configured attempt
budget remains. It uses backoff, sleeps with context cancellation, records each
attempt, and appends an error step whose message format is
`API error (attempt %d): %v`.

For the CCPA client, `modelapiccpa.Client.ClassifyError` at `0x142071620`
delegates to `cortex/utils.IsRetryableAPIError` at `0x1413fdca0`. Directly
decoded predicates include:

- gRPC/status codes 4 (`DEADLINE_EXCEEDED`), 8 (`RESOURCE_EXHAUSTED`), and 14
  (`UNAVAILABLE`);
- code 13 (`INTERNAL`) only under the helper's caller-supplied mode flag;
- plain HTTP 429, 502, 503, and 504;
- HTTP 429/model-capacity retry delays only when the decoded delay is at most
  30 seconds;
- several typed/sentinel and message-substring cases not yet safely named.

The plain HTTP-code switch does not retry 500. This list is not a claim that
every listed error is reachable in every AGY request path.

### Core model-output retry

After each API attempt, `core.generateWithOutputRetry` accumulates response
metadata and usage. It retries only three nonzero internal output-result kinds:

| Numeric kind | Exact retry instruction |
|---:|---|
| `3` | `Your previous response was cut off because it exceeded the output token limit. Please continue from where you left off, keeping your response shorter.` |
| `5` | `Your previous response contained an improperly formatted function call. Please retry with a properly formatted function call.` |
| `6` | `Your previous response encountered a generation error. Please try again.` |

For other output-result kinds it returns without model-output retry. Retry
exhaustion produces `output retries exhausted after %d attempts`.

The retry mutation visible in this function is an appended error/instruction
step followed by a fresh generation call. The function does not write a lower
thinking budget/level, alter `maxOutputTokens`, change model, strip history, or
change tool mode. Therefore static evidence does **not** support a built-in
"lower thinking after STOP" fallback.

`core.processModelStream` accumulates text, thinking, signatures, tool calls,
usage, and nonzero result status across chunks. Tool calls are retained across
later chunks. Invalid tool-call validation sets internal result kind `7`;
terminal stream errors become kind `2` for one sentinel or kind `6` otherwise.

The CCPA STOP mapping is now statically complete:

1. `codeassistclient.CodeAssistClient_getStreamingTextCompletion_range1` at
   `0x141e0f6e0` converts Cloud `FinishReason` to the API-server `StopReason`.
   `FINISH_REASON_STOP` (`1`) becomes `STOP_REASON_STOP_PATTERN` (`2`).
   `MAX_TOKENS` (`2`) becomes stop reason `3`; safety/filter families become
   stop reason `11`; `OTHER` and unknown/default families become stop reason
   `15`; malformed/no-image families become stop reason `14`.
2. `core/integration.streamAdapter.Receive` at `0x14199c320` converts stop
   reason `2` to framework output-result kind `1`; stop reasons `3`, `11`, and
   `13` become kinds `3`, `4`, and `6`; every other stop reason becomes kind
   `0`.

Therefore CCPA `finishReason=STOP` reaches framework kind `1`. The output-retry
loop retries only kinds `3`, `5`, and `6`, so it does **not** retry STOP.
Conversion does not test whether visible content is empty: useful STOP and
semantic-empty STOP receive the same non-retried kind `1`.

Runtime evidence also proves `functionCall` followed by a separate empty STOP
is accumulated and dispatched correctly. Presence of an accumulated function
call takes effect before terminal STOP; that valid sequence is not
semantic-empty.

### Planner retry

The planner stack has separate API-error and model-output retry budgets.
Model-output retry depends on analyzed error type and configuration, including
an option equivalent to retry-only-on-invalid-arguments. It appends one of two
prompts:

```text
There was a problem parsing the tool call.
Error Message: %v
Guidance: You are trying to correct your previous tool call error, you must focus on fixing the failed tool call with sequential tool calls and try again. Do not do parallel tool calls and if you are fixing multiple tool calls, do them one at a time. Do not apologize.
Retries remaining: %d.
```

or:

```text
There was a problem.
Error Message: %v
Retries remaining: %d
```

No thinking-budget/level mutation was found in this planner retry function
either. Its direct caller is `PlannerGenerator.Generate`; no direct concrete
link from this distinct planner stack to the captured interactive CCPA turn has
been proven.

### Remaining retry-count limit

IDA MCP instance `ida-140a` was revalidated against the old hash above and
produced the complete STOP mapping, call graph, and predicates in this section.
The exact configured retry scalars are still not proved for the current runtime.

Core output retry consumes a caller-supplied `maxOutputRetry` value from core
state offset `+72` (`+0x48`). This is a consumption site, not evidence of a
constructor write or default. Core API retry consumes a separate caller-supplied
budget; its exact state value also remains unresolved. These execution settings
are not fields in the captured provider JSON.

Planner retry uses independent `ModelOutputRetryConfig.max_retries` and
`ModelAPIRetryConfig.max_retries` fields. Static
`PlannerGenerator.generateWithModelOutputRetry` at `0x14189ac80` reads
`CascadePlannerConfig.retryConfig` at `+152`, then
`PlannerRetryConfig.model_output_retry` at `+8`, then the optional
`max_retries` scalar at `+8`. Its attempt counter starts at zero and the loop
continues while `current_attempt <= max_retries`, so total possible model-output
attempts are `max_retries + 1`. `generateModelOutputRetryPrompt` at
`0x14189f720` renders `Retries remaining` as
`max_retries - current_attempt`.

The separate construction edge
`integration.retryConfigFromCortex` at `0x141999420` is called from
`integration.NewFromAgent` at `0x141998f49`. The caller selects
`CascadePlannerConfig.retryConfig` at `+152`, independently confirmed by
`CascadePlannerConfig.GetRetryConfig` at `0x140a1c520`. The helper follows
`PlannerRetryConfig.api_retry` at `+16`, reads
`ModelAPIRetryConfig.max_retries` through its optional scalar at `+8`, and
returns that value plus one. This proves the conversion from configured API
maximum retries to total API attempts, but not the configured scalar.

Historical local transcript scan is supporting evidence only. Among 64
`transcript.jsonl` files, two conversations from 2026-05-29 contained four
total occurrences of the planner prompt, all `Retries remaining: 4`. Under the
loop semantics above, those historical conversations used planner
model-output `max_retries=4`, or at most five attempts. The transcripts were
not hash-bound to the 2026-07-29 binary and contained no API-attempt or
exhaustion count, so they do not establish the current planner or core values.

A bounded 2026-08-01 runtime probe used the old binary image with SHA-256
`0da7e458b4daec92898d6d7f28d40e51d0f2b26fc20fb80a285d0a30bcde4519`.
x64dbg attached to exact-PID CLI sessions; live bytes matched disk at
`net/http.NewRequestWithContext` (`0x1404d4ca0`),
`integration.retryConfigFromCortex` (`0x141999420`), and
`core.Run` (`0x14142ab60`). A hardware execution breakpoint at
`NewRequestWithContext` hit, proving the breakpoint mechanism and live image
mapping. The exercised ordinary print, interactive, `--mode plan`, and
`code-verifier` CLI paths did not hit `retryConfigFromCortex`, `core.Run`,
`CoreExecutor.Execute`, or the planner retry getters before clean process
exit/fixture timeout. This is path non-traversal, not a retry-count result; it
suggests these CLI controls do not exercise the distinct Cortex planner/core
construction path.

The structural-only private manifest is
`%LOCALAPPDATA%\agy-private-evidence\retry-count-probe-manifest.json`
(2,478 bytes, SHA-256
`26475615a67cd0ce54e8af19b6181088af010ac67cf29aa0e641f183b35e03d7`);
it hash-binds four hash-only session logs and records null, rather than guessed,
values for all four configured counts.

One induced proxy failure stopped at the earlier `loadCodeAssist` eligibility
check. It never reached provider generation or either retry loop and therefore
supplies no retry-count evidence. During the work, the file at
`agy_28_07.exe` changed from the old hash to
`83fb6e9d80e751d174b3738c3eefb054e75e85e47b17d1e159fe4831adceadc8`
after being launched; cause was not proved. The active `agy.exe`, IDB, and
accepted runtime controls remain bound to the old hash and must not be treated
as interchangeable with that changed file.

Next proof target: exercise the IDE/Cortex construction path, stop at
`retryConfigFromCortex`, and read both optional scalars from the paused exact
thread. Independently capture core state `+0x48` and its API budget at the real
`core.Run` call. Do not infer current counts from loop shape, the historical
value, unrelated defaults, or CLI non-hits.

The other remaining runtime task is a genuine semantic-empty STOP trace proving
whether any subsequent provider request occurs, as predicted by the static
kind-`1` mapping.

## External issue and pull-request review

External reports supply test cases, not AGY facts.

| Reference | Protocol lesson | Relation to our failure |
|---|---|---|
| [OpenCode #38767](https://github.com/anomalyco/opencode/issues/38767) | Synthesized `temperature=1`, `top_p=.95`, `top_k=64` caused OpenRouter pre-inference 404 for newer Gemini IDs; merged fix omits unsupported defaults. | Test field omission/parity. Not semantic-empty STOP evidence. |
| [pi #6996](https://github.com/earendil-works/pi/issues/6996) | Gemini 3 tool continuation requires exact thought-signature/history preservation; missing signature yields HTTP 400. | Test signature continuity. Distinct from HTTP 200 empty STOP. |
| [polity4j #27](https://github.com/shiv15/polity4j/issues/27) | Generic FinishReason context only; no Gemini payload or fix. | No direct evidence. |
| [Jarvis #356](https://github.com/TYRMars/Jarvis/issues/356) | Raw STOP plus decoded function call was normalized as `stop`, so tool dispatch was skipped. | Confirms finish reason alone must not override function-call presence. |
| [sub2api #4812](https://github.com/Wei-Shaw/sub2api/issues/4812) | Reports 3.1 malformed/empty results and 3.6 HTTP 200 STOP with empty parts. Schema, routing, and response-loss cases must be separated. | Primary historical symptom; raw historical SSE unavailable. |
| [Gemini CLI #28351](https://github.com/google-gemini/gemini-cli/issues/28351) | After function response, Cloud Code returned HTTP 200 empty text + STOP; observed CLI returned control without useful output. | Strong matching external case; still not AGY proof. |
| [js-genai #1769](https://github.com/googleapis/js-genai/issues/1769) | Replayed thought signatures produced deterministic HTTP 200 STOP with empty part in reporter test; stripping signatures helped but dangling function-call signatures can instead cause HTTP 400. | High-priority payload-history A/B test. Do not blindly strip signatures. |
| [llm-rosetta #416](https://github.com/Oaklight/llm-rosetta/issues/416) | Audit found missing function-call ID, uppercase schema rejection, wrong error envelope, dropped camelCase `systemInstruction`, invalid token modalities, and missing streaming metadata. | Defines parity checklist; issue itself does not show semantic-empty STOP. |
| [LiteLLM #24442](https://github.com/BerriAI/litellm/issues/24442) | Gemini returned role-only candidate with STOP; proposed parser patches preserve an empty choice but do not fix upstream generation. | Detect and surface semantic empty; do not mistake parser tolerance for a model fix. |
| [Jarvis PR #372](https://github.com/TYRMars/Jarvis/pull/372) | Decoded function calls take precedence over raw STOP for blocking and streaming responses. | Directly reusable stream-state regression principle. |
| [OpenWhispr PR #1379](https://github.com/OpenWhispr/openwhispr/pull/1379) | Treats `MAX_TOKENS` as truncation, excludes thought parts from visible output, and applies model-aware thinking minimums. | Useful taxonomy; not a STOP fix and PR remains open. |
| [gemini-canvas-proxy PR #21](https://github.com/pranrichh/gemini-canvas-proxy/pull/21) | Adds finish-reason mapping and errors for no candidates/non-STOP empty candidates. Empty STOP remains accepted. | Useful error normalization, incomplete semantic-empty protection. |

Cross-issue protocol test set:

- omit unsupported sampling defaults instead of inventing top-k/top-p/temperature;
- preserve exact thought signatures and their adjacent function-call/function-response history;
- preserve function-call IDs where present;
- accept uppercase Gemini schema enum names at protocol boundaries;
- preserve camelCase `systemInstruction`;
- preserve `responseId`, `modelVersion`, and final `usageMetadata` in streams;
- distinguish STOP, MAX_TOKENS, safety blocks, malformed function calls, missing candidates, and semantic-empty candidates;
- prioritize accumulated function calls over terminal STOP classification;
- never call an empty-choice parser workaround an upstream-generation fix.

## Repeatable local replay pipeline

Implemented:

- `diagnostics/semantic_empty_replay.py`
- `diagnostics/test_semantic_empty_replay.py`

Capabilities:

- accepts a full external JSON or gzip payload from a no-follow regular file; never embeds captured prompts;
- verifies the exact decompressed source SHA-256 before planning;
- preserves complete inner or outer-envelope bytes unchanged for `baseline`, while deriving a parsed inner view only for structural analysis;
- applies mutations inside the nested `request` when an envelope is present, deterministically reserializes only changed variants, and returns original bytes for semantic no-ops;
- verifies exact post-mutation transmitted-byte hashes per variant;
- built-in allowlisted variants: baseline, LOW, disabled thinking, `VALIDATED`, `AUTO`, LOW plus `VALIDATED`;
- LOW switches the thinking oneof representation by removing `thinkingBudget`, setting `includeThoughts=true`, and setting `thinkingLevel=LOW`;
- sequential bounded attempts, response bytes, SSE events, line length, and socket deadline;
- loopback HTTP or HTTPS endpoint only, revalidated at the transport boundary;
- API key from environment or no-follow regular file, never command line;
- explicit `--allow-live-network` opt-in; no network is the default;
- structural JSONL only: hashes, status class, chunk/event/part counts, thought/text presence, valid versus malformed function calls, finish-reason enums, bounded usage counts, and semantic-empty classification;
- unwraps both canonical Gemini responses and CCPA `response` envelopes;
- never emits response text, prompt, schema, tool/function name, argument value, credential, account/project/session ID, URL, or raw body;
- mode-0600 exclusive output by default; overwrite requires explicit opt-in;
- no production deployment.

Dry-run:

```powershell
python diagnostics/semantic_empty_replay.py `
  --request-file C:\private\request.json `
  --expected-source-sha256 <source-sha256> `
  --body-shape envelope `
  --model gemini-3.6-flash-high `
  --variants baseline,thinking_low,thinking_disabled,tool_validated,tool_auto,thinking_low_tool_validated `
  --dry-run
```

Live local replay after reviewing dry-run request hashes:

```powershell
$env:SEMANTIC_REPLAY_API_KEY_FILE = "C:\private\local-proxy-key.txt"

python diagnostics/semantic_empty_replay.py `
  --request-file C:\private\request.json `
  --expected-source-sha256 <source-sha256> `
  --body-shape envelope `
  --model gemini-3.6-flash-high `
  --variants baseline,thinking_low,thinking_disabled,tool_validated,tool_auto,thinking_low_tool_validated `
  --attempts 5 `
  --base-url http://127.0.0.1:18317 `
  --allow-live-network `
  --output C:\private\semantic-replay.jsonl
```

For strongest live controls, also pass one `--expected-request-sha256 variant=hash` argument per planned variant using hashes from the reviewed dry run.

Validation:

```text
python -m unittest diagnostics.test_semantic_empty_replay diagnostics.test_genai_stream_probe
Ran 40 tests
OK
```

A six-variant dry run against the accepted 94,771-byte AGY envelope passed its source hash, preserved the exact baseline body/hash, found 22 declarations, emitted no content, and made zero network requests. Its hashes also exposed two semantic no-ops:

- baseline and `tool_validated` both remained the original 94,771 bytes with SHA-256 `49eba076d4dcac977fd8a745e5cc4576142074adc78508c94204e3c1f36c0ca2`, because real AGY already sent `VALIDATED`;
- `thinking_low` and LOW plus `VALIDATED` both produced SHA-256 `52715f29ff88f85c5808909af05c80020bd594916111693515622fe5966d0877`, because the tool mode was already `VALIDATED`.

No-op detection compares intended JSON path values before serialization. Raw-byte hashes remain a separate transport control.

### Exact-byte localhost baseline

The unchanged transaction-2 agent envelope was sent once to a strict one-shot receiver bound only to `127.0.0.1:18317`. The receiver had no proxy or upstream path and logged only byte count, SHA-256, path hash, and match status.

- planned bytes/hash: 94,771 / `49eba076d4dcac977fd8a745e5cc4576142074adc78508c94204e3c1f36c0ca2`;
- received bytes/hash: identical; `hash_match=true`;
- synthetic response: one SSE JSON event with `finishReason=STOP`, no text, and no function call;
- replay classification: `semantic_empty_stop`;
- network requests: one, localhost only.

This proves exact full-envelope local transport and parser classification. Synthetic STOP is not a genuine provider/AGY semantic-empty event and says nothing about AGY retry behavior.

### Official AGY versus sub2api structural comparison

A one-run local probe decoded the same official envelope through production types in `backend/internal/pkg/antigravity/gemini_types.go`, remarshaled it, and emitted only field names/counts. The temporary probe was removed immediately and is not part of the normal test suite.

| Surface | Official AGY capture | sub2api typed round-trip | Result |
|---|---|---|---|
| outer envelope | `model`, `project`, `request`, `requestId`, `requestType`, `userAgent` | same six keys | match |
| inner request | seven keys including `labels` | same seven keys after fix | match |
| generation config | `maxOutputTokens`, `thinkingConfig` | same | match for captured fields |
| thinking config | `includeThoughts`, `thinkingBudget` | same | match for captured fields |
| tool config | `functionCallingConfig.mode` | same | match for captured fields |
| tools/declarations | 22 / 22; declaration keys `description`, `name`, `parameters` | 22 / 22; same key union | match at measured level |
| contents | 5 items | 5 items | count match |
| semantic JSON | 94,771-byte official body | semantically equal after typed decode/remarshal | match; byte ordering/escaping identity not claimed |

The captured `labels` loss is fixed by `GeminiRequest.Labels map[string]string`. A private one-run probe decoded and remarshaled the exact 94,771-byte official body and confirmed semantic JSON equality without logging body content; the temporary probe was removed. Permanent tests cover labels present/absent, native-wrapper preservation of the six observed agent label keys, explicit-zero versus absent `thinkingBudget`, and optional `thinkingLevel`, `candidateCount`, `responseModalities`, and `allowedFunctionNames`. These fields were absent from the official body, so their type coverage is schema-preservation readiness, not proof that AGY emits them. This remains capture-to-typed-round-trip parity, not full live production-transform parity.

Local topology:

```text
external full request file
        |
        v
semantic_empty_replay.py --dry-run  (mutation/hash validation)
        |
        v
localhost CLIProxyAPI pinned backend
        |
        v
same upstream provider path
        |
        v
structural JSONL comparison
```

The local proxy controls request transformation and credential isolation. It does not cryptographically prove Google's internal account selection or proxy egress.

### Post-deployment complete-request replay

A new complete production-causing request was captured at
`2026-08-01T19:56:36.260612361Z`, transferred through Electerm SFTP, and
hash-verified in the ACL-private evidence root:

- inner request: 189,612 bytes;
- request SHA-256:
  `5e08ffa7b4cf45a55f93b2f8edfd3376e314d4261902b59592cc982f68dff622`;
- capture bundle SHA-256:
  `50cc3a81b18e94a53b23748e0be9c321b04e3986aacd1d9df3a92937d59e5b65`;
- root keys: `contents`, `generationConfig`, `systemInstruction`, `tools`;
- 133 content items and 13 function declarations;
- HIGH thinking with thoughts enabled, no numeric budget, and no `toolConfig`.

The pinned replay backend remained
`ca8d8c3c4696f30ee8669cfaaf340db8ddeda0ec`. Dry-run preserved the unchanged
baseline bytes/hash and made zero network requests. Attempts were sequential,
request retries were zero, the listener was loopback-only, and output was
structural JSONL.

The captured per-account proxy could not complete the local transport path:
five stale-token attempts failed during OAuth-token connection and one
fresh-token probe still returned HTTP 500 before provider SSE. Those attempts
cannot classify semantic generation.

A bounded direct-egress fallback kept the exact request and captured OAuth
account while changing egress:

| Variant | Attempts | Semantic-empty STOP | Text | Function call |
|---|---:|---:|---:|---:|
| unchanged HIGH baseline | 5 | 1 | 4 | 0 |
| LOW | 5 | 0 | 2 | 3 |
| disabled thinking | 5 | 0 | 3 | 2 |

The one HIGH failure matched the production signature: HTTP 200 stream,
thought-only content plus signature, terminal STOP, no non-thought text, and no
function call. Thus local reproduction succeeded, but exact-network-parity did
not. LOW and disabled thinking were usable in all ten mitigation attempts.
This supports the existing mitigation and still does not identify HIGH as root
cause.

Private structural summary SHA-256:
`4b72595709b5fe77910a4d6b7bf1ccbcffdf32e8be2a07db4bfd070b7259dc03`.
Raw prompt/history/tools and all replay credentials remain outside the
repository in ACL-private retained storage. Explicit user instruction forbids
deleting the auth artifacts unless that instruction is reversed.

### Current-binary exact inner-request injection and forced-account result

Current `agy.exe` SHA-256
`83fb6e9d80e751d174b3738c3eefb054e75e85e47b17d1e159fe4831adceadc8`
was exercised with permissioned print-mode prompts and localhost MITM. The
request hook is resolved by masked pattern with an exact-one-match guard; no
fixed RVA is trusted. Frida attachment exits before request emission on this
binary, so successful experiments use the existing proxy-enabled MITM path.

System-prompt spoofing was tested with a temporary custom agent carrying the
complete captured instruction. It does not load the external 133-content
history. `--continue` and `--conversation` resume AGY-owned sessions only; no
supported arbitrary session-import path was found. The temporary agent and its
acknowledgment were removed after the control. Full replay therefore uses exact
inner-request injection, not a claim that CLI resume imported the transcript.

The earlier ten-run exact injection proved byte/canonical forwarding but used
AGY's unrelated current account/project: eight initial text responses, two
function calls, zero semantic-empty. It remains a different-account control.

The corrected operator-only gateway loaded the retained credential file and
project, used the same direct local egress as the account-28 control, and
forwarded provider responses back into real current `agy.exe`. Google's
provider-internal account identity was not independently exposed; identity
claims are limited to the exact local credential/project inputs. Only
ACL-private artifacts contain bodies. Headers and credential values were never
persisted. All agent rows shared project fingerprint
`7bddb774000c7d4d8c9b797066a15a44bd8cf07ddc669c3187d508b2b4303bd7`.

The exact 189,612-byte broken source produced a function call, then a genuine
thought-only semantic-empty STOP, then text after AGY sent another exact agent
request. This proves current AGY has the same provider-visible issue and proves
its runtime recovery request, rather than proving absence.

Hash-bound AGY-native batches preserved the prompt/history/tools invariant and
varied only the actual wire tier representation:

| Tier | Runs | Agent attempts | Text | Function call | Semantic-empty | Serialized control |
|---|---:|---:|---:|---:|---:|---|
| HIGH | 10 | 15 | 10 | 3 | 2 | high alias; budget 10,000; level absent |
| MEDIUM | 10 | 16 | 10 | 5 | 1 | medium alias; budget 4,000; level absent |

All agent attempts were HTTP 200 and matched the expected inner canonical hash,
credential fingerprint, and project fingerprint. Every semantic-empty event
was followed by another exact agent attempt and recovered to text or a
function-call sequence ending in text. This is direct runtime confirmation of
a genuine semantic-empty STOP and subsequent AGY/provider request. It does not
prove a universal rate or that MEDIUM eliminates the stochastic condition.

A separate five-run validation sent the service's exact serialized MEDIUM retry
shape: medium alias, numeric budget 4,000, and explicit MEDIUM level. Seven agent
attempts yielded five text, one function call, and one recoverable
semantic-empty, all HTTP 200. This proves the provider accepts the final service
shape. The retry policy consequently preserves HIGH for the first retry,
changes to MEDIUM only after another semantic-empty, and reaches LOW only after
MEDIUM also fails. The implementation now lowers recognized explicit model-tier
suffixes and known AGY wire aliases together with `thinkingLevel`; otherwise
suffix precedence would keep the provider wire model HIGH. Unknown and
suffixless model IDs retain normal routing. Supported mutation paths cover bare
`generationConfig`, OMP SDK `config`, and both under a wrapped `request`, with
camelCase or snake_case level/budget keys. Prebuilt v1internal envelopes also
synchronize the top-level model and tier budget. Thinking mutation uses targeted
JSON paths so unrelated numeric literals and tool-argument values are preserved.

Final code review also hardened adjacent paths exercised by these request
shapes: escaped and prebuilt-envelope `thoughtSignature` keys now reach cleaning
and rectifier retry; aggregated tool-call rewrites retain large JSON integers;
HTTP-200 non-stream quota payloads become pre-write 429 failovers; naked
forwarding failures no longer become implicit empty 200 responses; and decoded
compressed request bodies over 64 MiB return an explicit `http.MaxBytesError`
rather than a truncated body. Checkpoint-envelope comments are scoped to that
captured request class rather than claiming universal AGY defaults.

Private evidence is retained under the ACL-only semantic AGY and thinking
ladder roots. Auth artifacts remain under the permanent retained-auth policy.
Disposable proxies were stopped; credentials and evidence were not deleted.

## Production deployment validation

Commit `1b046492f1f6e754c09a76b9d306936c9a4ab1b5` was published by successful
GitHub Actions run `30697157073` and deployed through the Electerm MCP
production bookmark. The pulled image was verified before recreation and the
running container afterward:

- deployed image ID:
  `sha256:9050cf1831f4bd7658d4cb981282fb9e07ad0cee90f62bce8a5aa9dc5ee3f480`;
- deployed revision label:
  `1b046492f1f6e754c09a76b9d306936c9a4ab1b5`;
- previous image ID retained under local rollback tag
  `ghcr.io/chrys4lisfag/sub2api:rollback-stop-20260801`;
- only `sub2api` was recreated with `--no-deps`;
- container became healthy and loopback `/health` returned HTTP 200.

The bounded canary from `2026-08-01T11:16:12Z` through `11:21:47Z` contained
49 request markers and no detected semantic-empty STOP, semantic failover, 502,
or panic/fatal marker. This is deployment and immediate-regression evidence
only. Because no natural semantic-empty event occurred, it does not validate
the live failover branch, prove an upstream fix, or resolve the correlation and
full-payload gaps above.

## Prioritized future plan

### P0: preserve and compare exact bodies

Status update: steps 1–3 and the five-attempt HIGH/LOW/disabled subset of step
5 are now complete for request SHA-256
`5e08ffa7b4cf45a55f93b2f8edfd3376e314d4261902b59592cc982f68dff622`.
Step 4 remains open, as does the rest of the one-variable protocol matrix.

1. Capture a new problematic full payload or accept one supplied by the client.
2. Store it only in the ACL-protected private evidence root.
3. Hash it and feed it to the local replay script by path.
4. Compare original CCPA body, locally transformed upstream body, and real AGY body by key presence/type only.
5. Run at least five sequential attempts per one-variable variant.

### P1: protocol-parity matrix

Test independently:

- budget versus `thinkingLevel` representation;
- thought-signature preservation, removal, and adjacency to function responses;
- omitted versus synthesized temperature/top-p/top-k;
- max-output preservation;
- tool mode omitted/AUTO/ANY/VALIDATED;
- allowed function names omitted/present;
- function-call IDs and signatures;
- uppercase versus lowercase schema types;
- advanced schema keyword sanitization;
- camelCase system instruction;
- request type, labels, session/request IDs, and user-agent envelope;
- streaming metadata and finish-reason taxonomy.

### P2: AGY runtime controls

Capture hash-bound requests for:

- plain text with no tools;
- one built-in tool;
- one MCP tool;
- LOW, medium/budget, HIGH, and disabled thinking;
- checkpoint versus agent request;
- function-call follow-up;
- a genuine semantic-empty STOP and any subsequent retry.

Every accepted run must correlate request, raw SSE token sequence, local tool action where applicable, and follow-up request.

### P3: recover configured retry counts

Exercise the IDE/Cortex construction path rather than repeating CLI controls
that did not traverse it. At `retryConfigFromCortex`, read planner output/API
optional scalars independently; at the real `core.Run` call, read core output
`+0x48` and the separately consumed API budget. Bind every value to exact
PID, disk/live pattern match, binary hash, and stopped-thread RIP. STOP mapping,
retry predicates, prompts, historical planner output `max_retries=4`, and
observed non-mutations are documented above; none supplies the current four
configured values. Confirm semantic-empty STOP non-retry with one separate
hash-bound runtime trace.

## Decision gates

Do not change production protocol behavior based on one external issue or a static field capability.

A candidate fix graduates only when:

1. exact full request is hash-bound;
2. only one field family changes;
3. at least five sequential attempts are classified structurally;
4. behavior reproduces through an independent client path where possible;
5. a valid function-call-plus-empty-STOP control still succeeds;
6. malformed/safety/MAX_TOKENS cases remain distinguishable;
7. disposable processes and overrides are removed afterward, while credentials or evidence under an explicit retention instruction remain ACL-private and are never deleted.
