# AGY Investigation User Plan

## Required startup rule

Read this file before continuing any AGY protocol, reversing, replay, or semantic-empty STOP work. Use it as the authoritative scope checklist. Read the reversing and testing guidance under `C:\Users\koval\AppData\Local\agy\bin\AGENT`, but record our verdicts and plans only under this repository's `AGENT` directory.

## Evidence boundary

The only proven functional mitigation is that changing/lowering thinking helps. Do not claim that HIGH thinking is the root cause. Static reversing results, retry mappings, and external reports are protocol observations unless runtime evidence proves a functional result.

## Working environment

- AGY binary and reversing workspace: `C:\Users\koval\AppData\Local\agy\bin`
- AGY reversing/testing documentation: `C:\Users\koval\AppData\Local\agy\bin\AGENT`
- Our documentation: `C:\Users\koval\Desktop\subtoapi\sub2api\AGENT`
- IDA is already open with restored Go symbols. Revalidate binary path, SHA-256, IDB path, and image base before trusting results.
- Use recall and reflect to recover prior reversing-agent knowledge. Use the documented AGY launch, IDA search, and runtime-testing pipelines when blocked.
- Ask the user for help when GUI/runtime interaction is needed.
- Keep documenting findings, limitations, future tests, and unresolved questions.

## Workstream 1: review Gemini protocol references

Read every issue and pull request below. Extract anything AGY or sub2api may implement incorrectly: request envelopes, sampling defaults, thinking configuration, tool schemas, thought signatures, function-call IDs, history ordering, streaming state, finish-reason handling, retry behavior, and parser behavior. Separate upstream/provider defects from client/proxy defects. Treat external reports as test cases, not AGY facts.

### Issues

1. https://github.com/anomalyco/opencode/issues/38767
2. https://github.com/earendil-works/pi/issues/6996
3. https://github.com/shiv15/polity4j/issues/27
4. https://github.com/TYRMars/Jarvis/issues/356
5. https://github.com/Wei-Shaw/sub2api/issues/4812
6. https://github.com/google-gemini/gemini-cli/issues/28351
7. https://github.com/googleapis/js-genai/issues/1769
8. https://github.com/Oaklight/llm-rosetta/issues/416
9. https://github.com/BerriAI/litellm/issues/24442

### Pull requests

1. https://github.com/TYRMars/Jarvis/pull/372
2. https://github.com/OpenWhispr/openwhispr/pull/1379
3. https://github.com/pranrichh/gemini-canvas-proxy/pull/21

### Deliverable

Maintain a reference-by-reference matrix containing the exact protocol lesson, evidence strength, relevance to semantic-empty STOP, and a reproducible test we can add locally.

## Workstream 2: explain and dump the real AGY protocol

Explain the protocol used by real `agy.exe`, based on hash-bound runtime or static evidence:

- transport, host, path, method, query parameters, and SSE framing;
- outer Cloud Code/v1internal envelope;
- inner request fields and request types;
- system instructions, contents/history, tools, tool configuration, function calls, function responses, IDs, and thought signatures;
- labels, project/session/request identifiers, model selection, and user agent;
- differences between agent, checkpoint, follow-up, Business AI Code, Vertex AI, and other paths.

Answer whether every configuration field has actually been dumped. Distinguish:

1. fields observed on the real AGY wire;
2. fields supported by generated protobuf accessors but not observed;
3. fields absent from a captured request;
4. request types/models/settings that have not yet been captured.

Configuration inventory must include every emitted field, including when present or absent:

- `thinkingConfig.thinkingBudget`;
- `thinkingConfig.thinkingLevel`;
- `thinkingConfig.includeThoughts`;
- `temperature`;
- `topP`;
- `topK`;
- `maxOutputTokens`;
- `candidateCount`;
- `stopSequences`;
- response modalities and other generation fields;
- tool/function-calling mode and allowed function names.

## Workstream 2.1: reverse thinking, STOP, and retry behavior

Search real AGY code and runtime behavior for:

- thinking lowering, disabling, request-type suppression, and model-metadata selection;
- any automatic thinking change after an error or empty STOP;
- SSE event parsing and accumulated stream state;
- `finishReason`/stop-reason conversion;
- valid `functionCall -> empty STOP -> tool execution -> functionResponse` handling;
- semantic-empty detection, if any;
- API retry classification, backoff, limits, and cancellation;
- output retry classification, prompts, limits, and exhaustion;
- planner retry behavior;
- mutations made during retries: thinking, model, max tokens, history, signatures, tools, or tool mode.

Document exact function names, addresses, binary hash, callers, predicates, and evidence limitations. Do not infer semantics from symbol names alone.

## Workstream 3: reproducible local full-payload replay

Create and maintain a repeatable pipeline that needs no production/server deployment. It must:

- load the same complete problematic payload or full AGY envelope;
- hash-bind and preserve the original input;
- support unchanged baseline replay;
- mutate one controlled field or protocol dimension at a time;
- test thinking budget/level/disabled settings;
- test temperature, top-p, top-k, max output tokens, and other generation fields;
- test VALIDATED/AUTO/omitted tool modes;
- test tools, schema normalization, function-call IDs, thought signatures, and history ordering;
- preserve and analyze SSE event boundaries and accumulated function-call state;
- compare structural hashes between official AGY and sub2api requests;
- run loopback-only by default, require explicit opt-in for live traffic, and never require production deployment;
- avoid recording prompts, response text, OAuth tokens, identifiers, tool arguments, or thought signatures;
- produce reproducible structural-only results and tests.

Priority runtime experiment: replay an unchanged official AGY payload, then run a one-variable matrix and observe whether a genuine semantic-empty STOP causes any subsequent AGY/provider request.

## Workstream 4: continuous documentation

Always update our `AGENT` documentation with:

- current verdict;
- direct evidence and hashes;
- protocol/configuration field inventory;
- issue/PR lessons;
- replay commands and test results;
- unresolved gaps;
- future static and runtime plan;
- cleanup and privacy boundaries.

Primary working documents:

- `AGENT/agy-user-plan.md` — authoritative user scope; reread first.
- `AGENT/agy-gemini-protocol-and-replay-plan.md` — protocol, reversing, external references, replay plan, and evidence limits.
- `AGENT/semantic-empty-stop-investigation.md` — controlled experiment results and narrow functional verdict.

## Completion boundary

Do not describe the whole plan as complete while any of these remain unresolved:

- full real-AGY configuration coverage across relevant request types/models/settings;
- unchanged problematic full-payload replay;
- official AGY versus sub2api structural parity matrix;
- runtime confirmation using a genuine semantic-empty STOP;
- exact configured retry counts;
- root-cause proof beyond the observed thinking mitigation.
