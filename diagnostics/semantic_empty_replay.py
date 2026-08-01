#!/usr/bin/env python3
"""Bounded, content-free replay of a semantic-empty Gemini request.

Dry-run is default. Network I/O requires --allow-live-network. Request bodies
and response chunks are never written to output; output contains only bounded
structural facts, enums, counts, booleans, and SHA-256 digests.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import hmac
import http.client
import io
import ipaddress
import json
import os
import re
import stat
import sys
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence, TextIO

DEFAULT_BASE_URL = "http://127.0.0.1:18317"
DEFAULT_VARIANTS = ("baseline", "thinking_low")
DEFAULT_ATTEMPTS = 1
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_RESPONSE_BYTES = 1 << 20
DEFAULT_MAX_EVENTS = 4096
DEFAULT_MAX_LINE_BYTES = 256 << 10
MAX_REQUEST_FILE_BYTES = 32 << 20
REQUEST_CHUNK_BYTES = 64 << 10
RESPONSE_CHUNK_BYTES = 16 << 10
MAX_ATTEMPTS = 20
MAX_RESPONSE_BYTES = 16 << 20
MAX_EVENTS = 16384
MAX_API_KEY_BYTES = 4096
MAX_USAGE_COUNT = 1_000_000_000
MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
SHA256_RE = re.compile(r"[0-9a-fA-F]{64}\Z")

VARIANTS = (
    "baseline",
    "thinking_low",
    "thinking_disabled",
    "tool_validated",
    "tool_auto",
    "thinking_low_tool_validated",
)
KNOWN_FINISH_REASONS = {
    "BLOCKLIST",
    "FINISH_REASON_UNSPECIFIED",
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


@dataclass(frozen=True)
class RequestPlan:
    variant: str
    body: bytes
    sha256: str
    model: str


@dataclass
class ResponseSummary:
    status_class: str
    content_type: str
    response_bytes: int = 0
    response_chunks: int = 0
    lines: int = 0
    events: int = 0
    malformed_events: int = 0
    oversized_lines: int = 0
    candidates: int = 0
    parts: int = 0
    text_parts: int = 0
    thought_text_parts: int = 0
    thought_signature_parts: int = 0
    function_calls: int = 0
    valid_function_calls: int = 0
    malformed_function_calls: int = 0
    usage_metadata_events: int = 0
    prompt_token_count: int = 0
    candidate_token_count: int = 0
    total_token_count: int = 0
    thought_token_count: int = 0
    saw_non_thought_text: bool = False
    saw_function_call: bool = False
    truncated: bool = False
    events_truncated: bool = False
    finish_reasons: set[str] = field(default_factory=set)
    unexpected_finish_hashes: set[str] = field(default_factory=set)
    response_prefix_sha256: str = ""

    def observe_json(self, value: Any) -> None:
        self.events += 1
        if not isinstance(value, dict):
            return
        payload = value.get("response")
        if not isinstance(payload, dict):
            payload = value
        usage = payload.get("usageMetadata")
        if isinstance(usage, dict):
            self.usage_metadata_events += 1
            usage_fields = (
                ("promptTokenCount", "prompt_token_count"),
                ("candidatesTokenCount", "candidate_token_count"),
                ("totalTokenCount", "total_token_count"),
                ("thoughtsTokenCount", "thought_token_count"),
            )
            for source_name, field_name in usage_fields:
                count = usage.get(source_name)
                if isinstance(count, int) and not isinstance(count, bool):
                    setattr(self, field_name, max(0, min(count, MAX_USAGE_COUNT)))
        candidates = payload.get("candidates")
        if not isinstance(candidates, list):
            return
        self.candidates += len(candidates)
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            reason = candidate.get("finishReason", candidate.get("finish_reason"))
            if reason is not None:
                normalized = str(reason).upper()
                if normalized in KNOWN_FINISH_REASONS:
                    self.finish_reasons.add(normalized)
                elif len(self.unexpected_finish_hashes) < 8:
                    self.finish_reasons.add("UNEXPECTED")
                    self.unexpected_finish_hashes.add(_sha256_text(str(reason)))
            content = candidate.get("content")
            parts = content.get("parts") if isinstance(content, dict) else None
            if not isinstance(parts, list):
                continue
            self.parts += len(parts)
            for part in parts:
                if not isinstance(part, dict):
                    continue
                text = part.get("text")
                thought = part.get("thought") is True
                if isinstance(text, str) and text:
                    if thought:
                        self.thought_text_parts += 1
                    else:
                        self.text_parts += 1
                        self.saw_non_thought_text = True
                signature = part.get("thoughtSignature", part.get("thought_signature"))
                if signature is not None:
                    self.thought_signature_parts += 1
                call_present = "functionCall" in part or "function_call" in part
                call = part.get("functionCall", part.get("function_call"))
                if not call_present or call is None:
                    continue
                self.function_calls += 1
                valid = (
                    isinstance(call, dict)
                    and isinstance(call.get("name"), str)
                    and bool(call["name"].strip())
                    and isinstance(call.get("args"), dict)
                )
                if valid:
                    self.valid_function_calls += 1
                    self.saw_function_call = True
                else:
                    self.malformed_function_calls += 1

    def outcome(self) -> str:
        if self.status_class != "success":
            return "http_error"
        if self.truncated or self.events_truncated:
            return "bounded_incomplete"
        if self.saw_function_call:
            return "function_call"
        if self.saw_non_thought_text:
            return "text"
        if self.malformed_function_calls:
            return "malformed_function_call"
        if "STOP" in self.finish_reasons:
            return "semantic_empty_stop"
        if self.malformed_events:
            return "malformed_stream"
        return "empty_stream"

    def event(self, variant: str, attempt: int) -> dict[str, Any]:
        return {
            "event": "result",
            "variant": variant,
            "attempt": attempt,
            "status_class": self.status_class,
            "content_type": self.content_type,
            "outcome": self.outcome(),
            "response_bytes": self.response_bytes,
            "response_chunks": self.response_chunks,
            "lines": self.lines,
            "events": self.events,
            "malformed_events": self.malformed_events,
            "oversized_lines": self.oversized_lines,
            "candidates": self.candidates,
            "parts": self.parts,
            "text_parts": self.text_parts,
            "thought_text_parts": self.thought_text_parts,
            "thought_signature_parts": self.thought_signature_parts,
            "function_calls": self.function_calls,
            "valid_function_calls": self.valid_function_calls,
            "malformed_function_calls": self.malformed_function_calls,
            "usage_metadata_events": self.usage_metadata_events,
            "prompt_token_count": self.prompt_token_count,
            "candidate_token_count": self.candidate_token_count,
            "total_token_count": self.total_token_count,
            "thought_token_count": self.thought_token_count,
            "saw_non_thought_text": self.saw_non_thought_text,
            "saw_function_call": self.saw_function_call,
            "truncated": self.truncated,
            "events_truncated": self.events_truncated,
            "finish_reasons": sorted(self.finish_reasons),
            "unexpected_finish_reason_sha256": sorted(self.unexpected_finish_hashes),
            "response_prefix_sha256": self.response_prefix_sha256,
        }


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def _hash_chunks(chunks: Iterable[bytes | memoryview]) -> str:
    digest = hashlib.sha256()
    for chunk in chunks:
        digest.update(chunk)
    return digest.hexdigest()


def iter_bytes(value: bytes, chunk_size: int = REQUEST_CHUNK_BYTES) -> Iterator[memoryview]:
    """Yield bounded views without constructing a second request-body copy."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")
    view = memoryview(value)
    for offset in range(0, len(view), chunk_size):
        yield view[offset : offset + chunk_size]


def request_sha256(body: bytes) -> str:
    """Incrementally hash exact bytes sent by _perform_request."""
    return _hash_chunks(iter_bytes(body))


def _read_bounded(path: Path, maximum: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("input must be a regular file")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(maximum + 1)
    finally:
        if fd >= 0:
            os.close(fd)
    if len(data) > maximum:
        raise ValueError("input exceeds byte limit")
    return data


def load_request_file(
    path: str,
    body_shape: str = "auto",
    expected_source_sha256: str | None = None,
) -> bytes:
    """Load and validate JSON while preserving exact decompressed source bytes."""
    raw = _read_bounded(Path(path), MAX_REQUEST_FILE_BYTES)
    if raw.startswith(b"\x1f\x8b"):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
                raw = stream.read(MAX_REQUEST_FILE_BYTES + 1)
        except (OSError, EOFError) as exc:
            raise ValueError("invalid gzip request file") from exc
        if len(raw) > MAX_REQUEST_FILE_BYTES:
            raise ValueError("decompressed request exceeds byte limit")
    if expected_source_sha256 is not None:
        if not SHA256_RE.fullmatch(expected_source_sha256):
            raise ValueError("expected source SHA-256 is invalid")
        source_hash = request_sha256(raw)
        if not hmac.compare_digest(expected_source_sha256.lower(), source_hash):
            raise ValueError("expected source SHA-256 mismatch")
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("request file is not valid JSON") from exc
    if not isinstance(document, dict):
        raise ValueError("request file must contain a JSON object")
    if body_shape not in {"auto", "inner", "envelope"}:
        raise ValueError("body shape is invalid")
    if body_shape == "envelope" and not isinstance(document.get("request"), dict):
        raise ValueError("envelope body shape requires a request object")
    return raw


def _generation_config(document: dict[str, Any]) -> dict[str, Any]:
    value = document.get("generationConfig")
    if not isinstance(value, dict):
        raise ValueError("request must contain generationConfig object")
    return value


def _request_document(document: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Return inner Gemini request for analysis/mutation without altering source bytes."""
    if isinstance(document.get("generationConfig"), dict):
        return document, False
    request = document.get("request")
    if isinstance(request, dict):
        _generation_config(request)
        return request, True
    raise ValueError("request must contain generationConfig object")


def apply_variant(body: bytes, variant: str) -> bytes:
    """Apply one allowlisted semantic replay mutation deterministically."""
    if variant not in VARIANTS:
        raise ValueError("variant is not allowlisted")
    if variant == "baseline":
        return body
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("request body is not valid JSON") from exc
    if not isinstance(document, dict):
        raise ValueError("request body must be an object")
    request, _ = _request_document(document)
    config = _generation_config(request)
    thinking = config.get("thinkingConfig")
    if not isinstance(thinking, dict):
        raise ValueError("mutation requires thinkingConfig object")
    changed = False

    if variant in {"thinking_low", "thinking_low_tool_validated"}:
        if "thinkingBudget" in thinking:
            thinking.pop("thinkingBudget")
            changed = True
        if thinking.get("includeThoughts") is not True:
            thinking["includeThoughts"] = True
            changed = True
        if thinking.get("thinkingLevel") != "LOW":
            thinking["thinkingLevel"] = "LOW"
            changed = True
    elif variant == "thinking_disabled":
        if "thinkingLevel" in thinking:
            thinking.pop("thinkingLevel")
            changed = True
        if thinking.get("includeThoughts") is not False:
            thinking["includeThoughts"] = False
            changed = True
        if thinking.get("thinkingBudget") != 0:
            thinking["thinkingBudget"] = 0
            changed = True

    if variant in {"tool_validated", "tool_auto", "thinking_low_tool_validated"}:
        mode = "AUTO" if variant == "tool_auto" else "VALIDATED"
        tool_config = request.get("toolConfig")
        if tool_config is None:
            tool_config = {}
            request["toolConfig"] = tool_config
        if not isinstance(tool_config, dict):
            raise ValueError("mutation requires toolConfig object or omission")
        calling = tool_config.get("functionCallingConfig")
        if calling is None:
            calling = {}
            tool_config["functionCallingConfig"] = calling
            changed = True
        if not isinstance(calling, dict):
            raise ValueError("mutation requires functionCallingConfig object or omission")
        if calling.get("mode") != mode:
            calling["mode"] = mode
            changed = True

    if not changed:
        return body
    return json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def inspect_request(body: bytes) -> dict[str, Any]:
    document = json.loads(body)
    if not isinstance(document, dict):
        raise ValueError("request body must be an object")
    request, envelope_present = _request_document(document)
    config = _generation_config(request)
    thinking = config.get("thinkingConfig")
    tools = request.get("tools")
    if not isinstance(tools, list):
        tools = []
    declarations = 0
    for tool in tools:
        if isinstance(tool, dict) and isinstance(tool.get("functionDeclarations"), list):
            declarations += len(tool["functionDeclarations"])
    level = thinking.get("thinkingLevel") if isinstance(thinking, dict) else None
    if level not in {"HIGH", "MEDIUM", "LOW", "MINIMAL"}:
        level = "UNSET" if level is None else "UNEXPECTED"
    include = thinking.get("includeThoughts") if isinstance(thinking, dict) else None
    maximum = config.get("maxOutputTokens")
    budget = thinking.get("thinkingBudget") if isinstance(thinking, dict) else None
    return {
        "request_object_keys": len(request),
        "outer_object_keys": len(document),
        "envelope_present": envelope_present,
        "generation_config_present": True,
        "max_output_tokens_present": "maxOutputTokens" in config,
        "max_output_tokens_numeric": isinstance(maximum, (int, float)) and not isinstance(maximum, bool),
        "thinking_config_present": isinstance(thinking, dict),
        "thinking_level": level,
        "include_thoughts_present": isinstance(thinking, dict) and "includeThoughts" in thinking,
        "include_thoughts_boolean": isinstance(include, bool),
        "include_thoughts": include if isinstance(include, bool) else None,
        "thinking_budget_present": isinstance(thinking, dict) and "thinkingBudget" in thinking,
        "thinking_budget_numeric": isinstance(budget, (int, float)) and not isinstance(budget, bool),
        "tool_collections": len(tools),
        "function_declarations": declarations,
        "tool_config_present": isinstance(request.get("toolConfig"), dict),
    }


def normalize_base_url(value: str) -> str:
    """Validate versionless loopback-only HTTP(S) base URL."""
    value = value.strip().rstrip("/")
    if not value or any(ord(char) < 32 for char in value):
        raise ValueError("base URL is required and must not contain controls")
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("base URL is invalid") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("base URL must be absolute HTTP(S)")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("base URL must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("base URL must not contain query or fragment")
    if not _is_loopback_host(parsed.hostname):
        raise ValueError("replay endpoint must be loopback")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("base URL port is invalid")
    if parsed.path.rstrip("/").endswith(("/v1", "/v1beta")):
        raise ValueError("base URL must omit API version")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def read_api_key(environ: Mapping[str, str] | None = None) -> str:
    """Read API key from exactly one environment source; never from argv."""
    environ = os.environ if environ is None else environ
    direct = environ.get("SEMANTIC_REPLAY_API_KEY", "").strip()
    filename = environ.get("SEMANTIC_REPLAY_API_KEY_FILE", "").strip()
    if direct and filename:
        raise ValueError("set only one API key source")
    if filename:
        raw = _read_bounded(Path(filename), MAX_API_KEY_BYTES)
        try:
            direct = raw.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise ValueError("API key file is not UTF-8") from exc
    if not direct:
        raise ValueError("API key environment source is required for live replay")
    if len(direct.encode("utf-8")) > MAX_API_KEY_BYTES or "\n" in direct or "\r" in direct:
        raise ValueError("API key is invalid")
    return direct


def open_output(path: str | None, overwrite: bool = False) -> tuple[TextIO, bool]:
    """Open output privately; existing files require explicit overwrite."""
    if not path:
        return sys.stdout, False
    flags = os.O_WRONLY | os.O_CREAT
    flags |= os.O_TRUNC if overwrite else os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        mode = os.fstat(fd).st_mode
        if not stat.S_ISREG(mode):
            raise ValueError("output must be a regular file")
        os.fchmod(fd, 0o600)
        return os.fdopen(fd, "w", encoding="utf-8"), True
    except BaseException:
        os.close(fd)
        raise


def emit(stream: TextIO, event: dict[str, Any]) -> None:
    stream.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")
    stream.flush()


def safe_error(exc: BaseException) -> dict[str, Any]:
    message = str(exc).encode("utf-8", errors="replace")
    if isinstance(exc, (ValueError, argparse.ArgumentError)):
        category = "validation"
    elif isinstance(exc, http.client.HTTPException):
        category = "protocol"
    elif isinstance(exc, OSError):
        category = "io"
    else:
        category = "internal"
    return {
        "event": "error",
        "category": category,
        "message_bytes": len(message),
        "message_sha256": hashlib.sha256(message).hexdigest(),
    }


def _status_class(status: int) -> str:
    if 200 <= status < 300:
        return "success"
    if 400 <= status < 500:
        return "client_error"
    if 500 <= status < 600:
        return "server_error"
    return "other"


def _content_type(value: str | None) -> str:
    media = (value or "").split(";", 1)[0].strip().lower()
    if media == "text/event-stream":
        return "event_stream"
    if media in {"application/json", "application/problem+json"} or media.endswith("+json"):
        return "json"
    return "missing" if not media else "other"


def _observe_line(summary: ResponseSummary, line: bytes, max_events: int) -> None:
    summary.lines += 1
    line = line.rstrip(b"\r")
    if not line or line.startswith(b":") or line.startswith(b"event:"):
        return
    if line.startswith(b"data:"):
        line = line[5:].lstrip()
    if line == b"[DONE]":
        return
    if summary.events >= max_events:
        summary.events_truncated = True
        return
    try:
        value = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError):
        summary.malformed_events += 1
        return
    summary.observe_json(value)


def summarize_http_response(
    response: Any,
    *,
    max_response_bytes: int,
    max_events: int,
    max_line_bytes: int = DEFAULT_MAX_LINE_BYTES,
) -> ResponseSummary:
    """Consume response incrementally and retain only bounded structural state."""
    if not 1 <= max_response_bytes <= MAX_RESPONSE_BYTES:
        raise ValueError("max response bytes is out of range")
    if not 1 <= max_events <= MAX_EVENTS:
        raise ValueError("max events is out of range")
    if max_line_bytes < 1:
        raise ValueError("max line bytes must be positive")
    summary = ResponseSummary(
        status_class=_status_class(int(response.status)),
        content_type=_content_type(response.getheader("Content-Type")),
    )
    digest = hashlib.sha256()
    pending = bytearray()
    discarding_oversized_line = False
    remaining = max_response_bytes
    while remaining > 0:
        chunk = response.read(min(RESPONSE_CHUNK_BYTES, remaining + 1))
        if not chunk:
            break
        summary.response_chunks += 1
        accepted = chunk[:remaining]
        if len(chunk) > remaining:
            summary.truncated = True
        digest.update(accepted)
        summary.response_bytes += len(accepted)
        remaining -= len(accepted)
        for byte in accepted:
            if byte == 10:
                if discarding_oversized_line:
                    discarding_oversized_line = False
                    summary.lines += 1
                else:
                    _observe_line(summary, bytes(pending), max_events)
                pending.clear()
            elif not discarding_oversized_line:
                pending.append(byte)
                if len(pending) > max_line_bytes:
                    pending.clear()
                    discarding_oversized_line = True
                    summary.oversized_lines += 1
        if summary.truncated:
            break
    if remaining == 0 and not summary.truncated and response.read(1):
        summary.truncated = True
    if discarding_oversized_line:
        summary.lines += 1
    elif pending:
        _observe_line(summary, bytes(pending), max_events)
    summary.response_prefix_sha256 = digest.hexdigest()
    return summary


def _request_target(parsed: urllib.parse.SplitResult, model: str) -> str:
    if not MODEL_RE.fullmatch(model):
        raise ValueError("model is invalid")
    prefix = parsed.path.rstrip("/")
    encoded_model = urllib.parse.quote(model, safe="-._")
    return f"{prefix}/v1beta/models/{encoded_model}:streamGenerateContent?alt=sse"


def _open_connection(parsed: urllib.parse.SplitResult, timeout: float) -> Any:
    connection_type = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    return connection_type(parsed.hostname, parsed.port, timeout=timeout)


def _perform_request(
    base_url: str,
    plan: RequestPlan,
    api_key: str,
    *,
    allow_live_network: bool,
    timeout_seconds: float,
    max_response_bytes: int,
    max_events: int,
    connection_factory: Callable[[urllib.parse.SplitResult, float], Any] = _open_connection,
) -> ResponseSummary:
    """Send one plan. Network gate and exact-byte hash check fail before socket creation."""
    if not allow_live_network:
        raise ValueError("live network was not explicitly allowed")
    if not hmac.compare_digest(request_sha256(plan.body), plan.sha256):
        raise ValueError("request plan SHA-256 mismatch")
    parsed = urllib.parse.urlsplit(normalize_base_url(base_url))
    connection = connection_factory(parsed, timeout_seconds)
    try:
        connection.putrequest("POST", _request_target(parsed, plan.model))
        connection.putheader("Authorization", "Bearer " + api_key)
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Accept", "text/event-stream")
        connection.putheader("Content-Length", str(len(plan.body)))
        connection.endheaders()
        for chunk in iter_bytes(plan.body):
            connection.send(chunk)
        response = connection.getresponse()
        return summarize_http_response(
            response,
            max_response_bytes=max_response_bytes,
            max_events=max_events,
        )
    finally:
        connection.close()


def parse_variants(values: Sequence[str] | None) -> tuple[str, ...]:
    if not values:
        return DEFAULT_VARIANTS
    variants = tuple(part.strip() for value in values for part in value.split(",") if part.strip())
    if not variants or len(set(variants)) != len(variants):
        raise ValueError("variants must be nonempty and unique")
    if any(variant not in VARIANTS for variant in variants):
        raise ValueError("variant is not allowlisted")
    return variants


def _parse_expected_hashes(values: Sequence[str], variants: Sequence[str]) -> dict[str, str]:
    expected: dict[str, str] = {}
    for raw in values:
        if "=" in raw:
            variant, digest = raw.split("=", 1)
            if variant not in variants:
                raise ValueError("expected hash names an unplanned variant")
        else:
            if len(variants) != 1:
                raise ValueError("unqualified expected hash requires exactly one variant")
            variant, digest = variants[0], raw
        if variant in expected:
            raise ValueError("duplicate expected hash")
        if not SHA256_RE.fullmatch(digest):
            raise ValueError("expected request SHA-256 is invalid")
        expected[variant] = digest.lower()
    if values and set(expected) != set(variants):
        raise ValueError("expected hash is required for every planned variant")
    return expected


def build_plans(
    source_body: bytes,
    variants: Sequence[str],
    model: str,
    expected_values: Sequence[str] = (),
) -> list[RequestPlan]:
    """Build and hash all requests before any caller may open a socket."""
    if not MODEL_RE.fullmatch(model):
        raise ValueError("model is invalid")
    expected = _parse_expected_hashes(expected_values, variants)
    plans = []
    for variant in variants:
        body = apply_variant(source_body, variant)
        digest = request_sha256(body)
        wanted = expected.get(variant)
        if wanted is not None and not hmac.compare_digest(wanted, digest):
            raise ValueError("expected request SHA-256 mismatch")
        plans.append(RequestPlan(variant, body, digest, model))
    return plans


def run_replay(args: argparse.Namespace, output: TextIO) -> int:
    """Validate and plan, then dry-run or perform bounded sequential replay."""
    if args.dry_run and args.allow_live_network:
        raise ValueError("--dry-run conflicts with --allow-live-network")
    if not 1 <= args.attempts <= MAX_ATTEMPTS:
        raise ValueError("--attempts is out of range")
    if not 0.1 <= args.timeout_seconds <= 300:
        raise ValueError("--timeout-seconds is out of range")
    if not 1 <= args.max_response_bytes <= MAX_RESPONSE_BYTES:
        raise ValueError("--max-response-bytes is out of range")
    if not 1 <= args.max_events <= MAX_EVENTS:
        raise ValueError("--max-events is out of range")

    variants = parse_variants(args.variants)
    source_body = load_request_file(
        args.request_file,
        args.body_shape,
        args.expected_source_sha256,
    )
    plans = build_plans(
        source_body,
        variants,
        args.model,
        args.expected_request_sha256 or (),
    )
    live = bool(args.allow_live_network)
    base_url = normalize_base_url(args.base_url)

    for plan in plans:
        emit(output, {
            "event": "plan",
            "mode": "live" if live else "dry_run",
            "variant": plan.variant,
            "request_bytes": len(plan.body),
            "request_sha256": plan.sha256,
            "model_sha256": _sha256_text(plan.model),
            **inspect_request(plan.body),
        })
    if not live:
        emit(output, {
            "event": "summary",
            "mode": "dry_run",
            "variants": len(plans),
            "attempts_per_variant": 0,
            "network_requests": 0,
        })
        return 0

    # Read secret only after every post-mutation request hash passes verification.
    api_key = read_api_key()
    outcomes: dict[str, int] = {}
    total = 0
    for plan in plans:
        for attempt in range(1, args.attempts + 1):
            summary = _perform_request(
                base_url,
                plan,
                api_key,
                allow_live_network=True,
                timeout_seconds=args.timeout_seconds,
                max_response_bytes=args.max_response_bytes,
                max_events=args.max_events,
            )
            emit(output, summary.event(plan.variant, attempt))
            outcomes[summary.outcome()] = outcomes.get(summary.outcome(), 0) + 1
            total += 1
    emit(output, {
        "event": "summary",
        "mode": "live",
        "variants": len(plans),
        "attempts_per_variant": args.attempts,
        "network_requests": total,
        "outcome_counts": {key: outcomes[key] for key in sorted(outcomes)},
    })
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Hash-verified A/B replay of a raw Gemini request. Defaults to a "
            "no-network baseline-versus-LOW dry run."
        ),
        epilog=(
            "For live mode set SEMANTIC_REPLAY_API_KEY or "
            "SEMANTIC_REPLAY_API_KEY_FILE. API keys are never accepted on argv."
        ),
    )
    parser.add_argument(
        "--request-file",
        required=True,
        metavar="PATH",
        help="raw request JSON (.json, or gzip-compressed JSON)",
    )
    parser.add_argument(
        "--body-shape",
        choices=("auto", "inner", "envelope"),
        default="auto",
        help="validate shape but preserve exact decompressed bytes (default: %(default)s)",
    )
    parser.add_argument("--model", required=True, help="Gemini endpoint model name")
    parser.add_argument(
        "--variants",
        action="append",
        metavar="NAME[,NAME...]",
        help=(
            "allowlisted variants; comma-separate or repeat in desired A/B order "
            "(default: baseline,thinking_low)"
        ),
    )
    parser.add_argument(
        "--expected-source-sha256",
        metavar="HEX",
        help=(
            "verify decompressed file bytes before auto/envelope extraction; "
            "distinct from post-mutation request hashes"
        ),
    )
    parser.add_argument(
        "--expected-request-sha256",
        action="append",
        metavar="[VARIANT=]HEX",
        help=(
            "verify exact post-mutation bytes before networking; repeat with "
            "VARIANT=HEX for multiple variants"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="explicit no-network mode (also the default without --allow-live-network)",
    )
    parser.add_argument(
        "--allow-live-network",
        action="store_true",
        help="explicitly authorize bounded live network requests",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        metavar="URL",
        help=(
            f"versionless loopback API base (default: {DEFAULT_BASE_URL}); "
            "credentials, query, and fragment are rejected"
        ),
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=DEFAULT_ATTEMPTS,
        help=f"sequential attempts per variant, 1-{MAX_ATTEMPTS} (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="socket timeout per attempt, 0.1-300 (default: %(default)s)",
    )
    parser.add_argument(
        "--max-response-bytes",
        type=int,
        default=DEFAULT_MAX_RESPONSE_BYTES,
        help=f"response-prefix cap, 1-{MAX_RESPONSE_BYTES} bytes (default: %(default)s)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=DEFAULT_MAX_EVENTS,
        help=f"JSON/SSE event cap per attempt, 1-{MAX_EVENTS} (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        metavar="PATH",
        help="write JSONL structural output to a mode-0600 file (default: stdout)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="allow truncating an existing regular output file",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    os.umask(0o077)
    args = build_parser().parse_args(argv)
    try:
        output, should_close = open_output(args.output, args.overwrite)
    except BaseException as exc:
        emit(sys.stdout, safe_error(exc))
        return 3
    try:
        try:
            return run_replay(args, output)
        except BaseException as exc:
            emit(output, safe_error(exc))
            return 3
    finally:
        if should_close:
            output.close()


if __name__ == "__main__":
    raise SystemExit(main())
