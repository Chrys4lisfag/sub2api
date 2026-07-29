#!/usr/bin/env python3
"""Bounded structural probe for google-genai streaming through a custom endpoint.

API keys are accepted only through environment variables or a file. Output never
contains prompts, response text, argument values, URLs, credentials, or IDs.
An internal hard deadline and the external runner both bound execution. Never
invoke this probe from a request handler.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import sys
import threading
import urllib.parse
from dataclasses import dataclass, field
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, TextIO

EXPECTED_SDK_VERSION = "2.12.1"
DEFAULT_MODEL = "gemini-3.6-flash-high"
DEFAULT_MAX_CHUNKS = 256
DEFAULT_DEADLINE_SECONDS = 180
MAX_INSPECTED_CANDIDATES = 4
MAX_INSPECTED_PARTS_PER_CANDIDATE = 64
MAX_RECORDED_FUNCTION_CALLS = 8
MAX_RECORDED_NAMES = 16
MAX_RECORDED_ARGUMENT_KEYS = 16
MAX_LABEL_BYTES = 128
KNOWN_FINISH_REASONS = {
    "BLOCKLIST",
    "FINISH_REASON_UNSPECIFIED",
    "IMAGE_PROHIBITED_CONTENT",
    "LANGUAGE",
    "MALFORMED_FUNCTION_CALL",
    "MAX_TOKENS",
    "OTHER",
    "PROHIBITED_CONTENT",
    "RECITATION",
    "SAFETY",
    "SPII",
    "STOP",
    "UNEXPECTED_TOOL_CALL",
}


@dataclass
class ProbeSummary:
    chunks: int = 0
    candidates: int = 0
    parts: int = 0
    saw_text: bool = False
    saw_non_thought_text: bool = False
    saw_function_call: bool = False
    saw_valid_function_call: bool = False
    function_names: set[str] = field(default_factory=set)
    finish_reasons: set[str] = field(default_factory=set)


def _safe_structural_label(value: Any, allowed: set[str]) -> str:
    if value is None:
        return ""
    raw = str(getattr(value, "value", value))
    encoded = raw.encode("utf-8", errors="replace")
    if raw in allowed and len(encoded) <= MAX_LABEL_BYTES:
        return raw
    return f"unexpected:{len(encoded)}:{hashlib.sha256(encoded).hexdigest()[:12]}"


def summarize_chunk(chunk: Any, index: int, total: ProbeSummary) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event": "chunk",
        "index": index,
        "candidate_count": 0,
        "candidate_count_inspected": 0,
        "part_count": 0,
        "part_count_inspected": 0,
        "text_parts": 0,
        "thought_text_parts": 0,
        "thought_signature_parts": 0,
        "function_calls": [],
        "function_calls_truncated": 0,
        "finish_reasons": [],
    }
    candidates = list(getattr(chunk, "candidates", None) or [])
    event["candidate_count"] = len(candidates)
    total.chunks += 1
    total.candidates += len(candidates)

    inspected_candidates = candidates[:MAX_INSPECTED_CANDIDATES]
    event["candidate_count_inspected"] = len(inspected_candidates)
    for candidate in inspected_candidates:
        reason = _safe_structural_label(
            getattr(candidate, "finish_reason", None),
            KNOWN_FINISH_REASONS,
        )
        if reason:
            event["finish_reasons"].append(reason)
            if len(total.finish_reasons) < MAX_RECORDED_NAMES:
                total.finish_reasons.add(reason)

        content = getattr(candidate, "content", None)
        parts = list(getattr(content, "parts", None) or []) if content is not None else []
        event["part_count"] += len(parts)
        total.parts += len(parts)
        inspected_parts = parts[:MAX_INSPECTED_PARTS_PER_CANDIDATE]
        event["part_count_inspected"] += len(inspected_parts)
        for part in inspected_parts:
            text = getattr(part, "text", None)
            thought = bool(getattr(part, "thought", False))
            if isinstance(text, str) and text:
                event["text_parts"] += 1
                total.saw_text = True
                if thought:
                    event["thought_text_parts"] += 1
                else:
                    total.saw_non_thought_text = True
            if getattr(part, "thought_signature", None) is not None:
                event["thought_signature_parts"] += 1

            call = getattr(part, "function_call", None)
            if call is None:
                continue
            total.saw_function_call = True
            name = _safe_structural_label(
                getattr(call, "name", None),
                {"get_weather"},
            )
            if name and len(total.function_names) < MAX_RECORDED_NAMES:
                total.function_names.add(name)
            if len(event["function_calls"]) >= MAX_RECORDED_FUNCTION_CALLS:
                event["function_calls_truncated"] += 1
                continue

            args = getattr(call, "args", None)
            raw_keys = (
                list(islice(args.keys(), MAX_RECORDED_ARGUMENT_KEYS))
                if isinstance(args, dict)
                else []
            )
            arg_keys = sorted(
                _safe_structural_label(key, {"city"})
                for key in raw_keys
            )
            city = args.get("city") if isinstance(args, dict) else None
            valid_contract = (
                getattr(call, "name", None) == "get_weather"
                and isinstance(city, str)
                and bool(city.strip())
            )
            total.saw_valid_function_call = total.saw_valid_function_call or valid_contract
            event["function_calls"].append({
                "name": name,
                "argument_keys": arg_keys,
                "argument_keys_truncated": max(0, len(args) - len(arg_keys)) if isinstance(args, dict) else 0,
                "arguments_present": args is not None,
                "valid_contract": valid_contract,
            })

    return event


def final_event(summary: ProbeSummary) -> dict[str, Any]:
    if summary.saw_valid_function_call:
        outcome = "function_call"
    elif summary.saw_function_call:
        outcome = "malformed_function_call"
    elif summary.saw_non_thought_text:
        outcome = "text"
    elif summary.saw_text:
        outcome = "thought_only"
    elif "STOP" in summary.finish_reasons:
        outcome = "empty_stop"
    else:
        outcome = "empty_stream"
    return {
        "event": "summary",
        "outcome": outcome,
        "chunks": summary.chunks,
        "candidates": summary.candidates,
        "parts": summary.parts,
        "saw_text": summary.saw_text,
        "saw_non_thought_text": summary.saw_non_thought_text,
        "saw_function_call": summary.saw_function_call,
        "saw_valid_function_call": summary.saw_valid_function_call,
        "function_names": sorted(summary.function_names),
        "finish_reasons": sorted(summary.finish_reasons),
    }


def _safe_error(exc: BaseException) -> dict[str, Any]:
    raw = str(exc).encode("utf-8", errors="replace")
    status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    return {
        "event": "error",
        "error_type": type(exc).__name__,
        "message_bytes": len(raw),
        "message_sha256": hashlib.sha256(raw).hexdigest(),
        "status": status if isinstance(status, int) else None,
    }


def _read_api_key() -> str:
    direct = os.environ.get("GENAI_PROBE_API_KEY", "").strip()
    key_file = os.environ.get("GENAI_PROBE_API_KEY_FILE", "").strip()
    if direct and key_file:
        raise ValueError("set only one of GENAI_PROBE_API_KEY or GENAI_PROBE_API_KEY_FILE")
    if key_file:
        direct = Path(key_file).read_text(encoding="utf-8").strip()
    if not direct:
        raise ValueError("GENAI_PROBE_API_KEY or GENAI_PROBE_API_KEY_FILE is required")
    return direct


def _normalize_base_url(value: str) -> str:
    base_url = value.strip().rstrip("/")
    if not base_url:
        raise ValueError("--base-url or GENAI_PROBE_BASE_URL is required")
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("base URL must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("base URL must not contain credentials, query, or fragment")
    if parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "::1", "localhost"}:
        raise ValueError("plain HTTP is allowed only for a loopback endpoint")
    if parsed.path.endswith("/v1beta") or parsed.path.endswith("/v1"):
        raise ValueError("base URL must omit API version; google-genai appends /v1beta")
    return base_url


def _start_hard_deadline(seconds: int, output: TextIO) -> threading.Timer:
    if seconds < 1 or seconds > 3600:
        raise ValueError("--deadline-seconds must be between 1 and 3600")

    def force_exit() -> None:
        os._exit(124)

    def best_effort_nonblocking_write(fd: int, payload: bytes) -> bool:
        try:
            os.set_blocking(fd, False)
            os.write(fd, payload)
            return True
        except BaseException:
            return False

    def expire() -> None:
        payload = (
            json.dumps(
                {"event": "timeout", "deadline_seconds": seconds},
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        kill_timer = threading.Timer(0.1, force_exit)
        kill_timer.daemon = True
        kill_timer.start()
        try:
            if not best_effort_nonblocking_write(output.fileno(), payload):
                best_effort_nonblocking_write(2, payload)
        finally:
            force_exit()

    timer = threading.Timer(seconds, expire)
    timer.daemon = True
    timer.start()
    return timer


def _open_output(path: str | None) -> tuple[TextIO, bool]:
    if not path:
        return sys.stdout, False
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600)
    return os.fdopen(fd, "w", encoding="utf-8"), True


def _emit(stream: TextIO, event: dict[str, Any]) -> None:
    stream.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")
    stream.flush()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Detached structural google-genai streaming probe")
    parser.add_argument("--base-url", default=os.environ.get("GENAI_PROBE_BASE_URL", ""))
    parser.add_argument("--model", default=os.environ.get("GENAI_PROBE_MODEL", DEFAULT_MODEL))
    parser.add_argument("--output")
    parser.add_argument("--max-chunks", type=int, default=DEFAULT_MAX_CHUNKS)
    parser.add_argument("--deadline-seconds", type=int, default=DEFAULT_DEADLINE_SECONDS)
    parser.add_argument("--expected-sdk-version", default=EXPECTED_SDK_VERSION)
    return parser


def run_probe(args: argparse.Namespace, output: TextIO) -> int:
    base_url = _normalize_base_url(args.base_url)
    if args.max_chunks < 1 or args.max_chunks > 4096:
        raise ValueError("--max-chunks must be between 1 and 4096")

    installed = importlib.metadata.version("google-genai")
    if installed != args.expected_sdk_version:
        raise RuntimeError(
            f"google-genai version mismatch: expected {args.expected_sdk_version}, got {installed}"
        )

    from google import genai
    from google.genai import types

    api_key = _read_api_key()
    client = genai.Client(
        enterprise=False,
        api_key=api_key,
        http_options=types.HttpOptions(base_url=base_url),
    )
    config = types.GenerateContentConfig(
        temperature=0,
        max_output_tokens=256,
        tools=[
            types.Tool(
                function_declarations=[
                    types.FunctionDeclaration(
                        name="get_weather",
                        description="Query weather for a city",
                        parameters={
                            "type": "object",
                            "properties": {"city": {"type": "string"}},
                            "required": ["city"],
                        },
                    )
                ]
            )
        ],
        tool_config=types.ToolConfig(
            function_calling_config=types.FunctionCallingConfig(
                mode="ANY",
                allowed_function_names=["get_weather"],
            )
        ),
    )
    summary = ProbeSummary()
    _emit(output, {
        "event": "start",
        "sdk": "google-genai",
        "sdk_version": installed,
        "model": _safe_structural_label(args.model, {DEFAULT_MODEL}),
        "stream": True,
        "function_calling_mode": "ANY",
        "allowed_function_names": ["get_weather"],
    })
    try:
        responses: Iterable[Any] = client.models.generate_content_stream(
            model=args.model,
            contents="Call get_weather for London. Return only the function call.",
            config=config,
        )
        for index, chunk in enumerate(islice(responses, args.max_chunks), start=1):
            _emit(output, summarize_chunk(chunk, index, summary))
        result = final_event(summary)
        _emit(output, result)
        return 0 if result["outcome"] == "function_call" else 2
    except BaseException as exc:
        _emit(output, _safe_error(exc))
        return 3
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def main() -> int:
    os.umask(0o077)
    args = _build_parser().parse_args()
    try:
        output, should_close = _open_output(args.output)
    except BaseException as exc:
        _emit(sys.stdout, _safe_error(exc))
        return 3

    deadline = None
    try:
        deadline = _start_hard_deadline(args.deadline_seconds, output)
        return run_probe(args, output)
    except BaseException as exc:
        _emit(output, _safe_error(exc))
        return 3
    finally:
        if deadline is not None:
            deadline.cancel()
        if should_close:
            output.close()


if __name__ == "__main__":
    raise SystemExit(main())
