import gzip
import importlib.util
import io
import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("semantic_empty_replay.py")
SPEC = importlib.util.spec_from_file_location("semantic_empty_replay", MODULE_PATH)
replay = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = replay
SPEC.loader.exec_module(replay)

SECRET_PROMPT = "private prompt must never reach structural output"
BASE_DOCUMENT = {
    "contents": [{"role": "user", "parts": [{"text": SECRET_PROMPT}]}],
    "generationConfig": {
        "maxOutputTokens": 65536,
        "thinkingConfig": {
            "includeThoughts": True,
            "thinkingLevel": "HIGH",
            "thinkingBudget": 10000,
        },
    },
    "tools": [{"functionDeclarations": [{
        "name": "private_tool_name",
        "description": "private schema description",
        "parameters": {"type": "object"},
    }]}],
}
BASE_BODY = json.dumps(BASE_DOCUMENT, separators=(",", ":")).encode()


class FakeResponse:
    def __init__(self, body, status=200, content_type="text/event-stream", read_size=7):
        self.status = status
        self._body = io.BytesIO(body)
        self._content_type = content_type
        self._read_size = read_size
        self.read_calls = 0

    def getheader(self, name):
        return self._content_type if name.lower() == "content-type" else None

    def read(self, amount=-1):
        self.read_calls += 1
        if amount < 0:
            amount = self._read_size
        return self._body.read(min(amount, self._read_size))


class FakeConnection:
    def __init__(self, response):
        self.response = response
        self.method = None
        self.target = None
        self.headers = []
        self.sent = []
        self.closed = False

    def putrequest(self, method, target):
        self.method = method
        self.target = target

    def putheader(self, name, value):
        self.headers.append((name, value))

    def endheaders(self):
        pass

    def send(self, chunk):
        self.sent.append(bytes(chunk))

    def getresponse(self):
        return self.response

    def close(self):
        self.closed = True


class SemanticEmptyReplayTests(unittest.TestCase):
    def write_request(self, directory, body=BASE_BODY, gzip_file=False):
        suffix = ".json.gz" if gzip_file else ".json"
        path = Path(directory) / ("request" + suffix)
        path.write_bytes(gzip.compress(body) if gzip_file else body)
        return path

    def parse_args(self, request_file, *extra):
        return replay.build_parser().parse_args([
            "--request-file", str(request_file),
            "--model", "gemini-3.6-flash-high",
            *extra,
        ])

    def test_parser_has_clear_safe_defaults_and_no_api_key_argument(self):
        parser = replay.build_parser()
        help_text = parser.format_help()
        self.assertIn("no-network", help_text)
        self.assertIn("SEMANTIC_REPLAY_API_KEY", help_text)
        self.assertIn("--allow-live-network", help_text)
        self.assertIn("--expected-request-sha256", help_text)
        self.assertIn("--expected-source-sha256", help_text)
        self.assertNotIn("--api-key", help_text)
        with tempfile.TemporaryDirectory() as directory:
            args = self.parse_args(self.write_request(directory))
        self.assertFalse(args.allow_live_network)
        self.assertFalse(args.dry_run)
        self.assertEqual(args.attempts, 1)
        self.assertIsNone(args.variants)

    def test_loads_raw_and_gzip_request_without_changing_inner_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_path = self.write_request(directory)
            gzip_path = self.write_request(directory, gzip_file=True)
            self.assertEqual(replay.load_request_file(str(raw_path)), BASE_BODY)
            self.assertEqual(replay.load_request_file(str(gzip_path)), BASE_BODY)

    def test_body_shape_validation_preserves_envelope_bytes(self):
        envelope = json.dumps({
            "model": "outer-private-model",
            "request": BASE_DOCUMENT,
            "unrelated": "outer-private-value",
        }).encode()
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_request(directory, envelope)
            auto = replay.load_request_file(str(path), "auto")
            explicit = replay.load_request_file(str(path), "envelope")
            whole = replay.load_request_file(str(path), "inner")
        self.assertEqual(auto, envelope)
        self.assertEqual(explicit, envelope)
        self.assertEqual(whole, envelope)
        self.assertEqual(replay.request_sha256(auto), replay.request_sha256(envelope))
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_request(directory)
            with self.assertRaises(ValueError):
                replay.load_request_file(str(path), "envelope")

    def test_source_hash_covers_exact_decompressed_envelope_bytes(self):
        envelope = json.dumps({"request": BASE_DOCUMENT, "private": "outer"}).encode()
        source_hash = replay.request_sha256(envelope)
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_request(directory, envelope, gzip_file=True)
            preserved = replay.load_request_file(
                str(path),
                "auto",
                expected_source_sha256=source_hash,
            )
            self.assertEqual(preserved, envelope)
            with self.assertRaises(ValueError):
                replay.load_request_file(
                    str(path),
                    "auto",
                    expected_source_sha256="0" * 64,
                )

    def test_request_file_rejects_non_object_invalid_and_oversized_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "request.json"
            for raw in (b"not-json", b"[]"):
                path.write_bytes(raw)
                with self.subTest(raw=raw), self.assertRaises(ValueError):
                    replay.load_request_file(str(path))
            path.write_bytes(b"x" * (replay.MAX_REQUEST_FILE_BYTES + 1))
            with self.assertRaises(ValueError):
                replay.load_request_file(str(path))

    def test_input_reads_require_regular_files_and_reject_supported_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises((ValueError, OSError)):
                replay.load_request_file(directory)
            if getattr(os, "O_NOFOLLOW", 0) and hasattr(os, "symlink"):
                target = self.write_request(directory)
                link = Path(directory) / "request-link.json"
                os.symlink(target, link)
                with self.assertRaises(OSError):
                    replay.load_request_file(str(link))

    def test_variants_are_allowlisted_and_deterministic(self):
        self.assertEqual(replay.apply_variant(BASE_BODY, "baseline"), BASE_BODY)
        expected = {
            "thinking_low": ("LOW", True, None),
            "thinking_disabled": (None, False, None),
            "tool_validated": ("HIGH", True, "VALIDATED"),
            "tool_auto": ("HIGH", True, "AUTO"),
            "thinking_low_tool_validated": ("LOW", True, "VALIDATED"),
        }
        for variant, (level, include, mode) in expected.items():
            with self.subTest(variant=variant):
                first = replay.apply_variant(BASE_BODY, variant)
                self.assertEqual(first, replay.apply_variant(BASE_BODY, variant))
                document = json.loads(first)
                thinking = document["generationConfig"]["thinkingConfig"]
                self.assertEqual(thinking.get("thinkingLevel"), level)
                self.assertEqual(thinking["includeThoughts"], include)
                if variant in {"thinking_low", "thinking_low_tool_validated"}:
                    self.assertNotIn("thinkingBudget", thinking)
                if variant == "thinking_disabled":
                    self.assertEqual(thinking["thinkingBudget"], 0)
                if mode is not None:
                    self.assertEqual(document["toolConfig"]["functionCallingConfig"]["mode"], mode)
        with self.assertRaises(ValueError):
            replay.apply_variant(BASE_BODY, "set_arbitrary_private_field")
        with self.assertRaises(ValueError):
            replay.parse_variants(["baseline,arbitrary"])

    def test_envelope_baseline_is_exact_and_mutations_target_inner_request(self):
        envelope_document = {
            "model": "outer-private-model",
            "request": BASE_DOCUMENT,
            "requestType": "agent",
        }
        envelope = json.dumps(envelope_document, indent=2).encode()
        self.assertEqual(replay.apply_variant(envelope, "baseline"), envelope)
        inspected = replay.inspect_request(envelope)
        self.assertTrue(inspected["envelope_present"])
        self.assertEqual(inspected["request_object_keys"], len(BASE_DOCUMENT))
        mutated = json.loads(replay.apply_variant(envelope, "thinking_low"))
        thinking = mutated["request"]["generationConfig"]["thinkingConfig"]
        self.assertEqual(thinking["thinkingLevel"], "LOW")
        self.assertNotIn("thinkingBudget", thinking)
        self.assertEqual(mutated["model"], envelope_document["model"])
        self.assertEqual(mutated["requestType"], "agent")
        validated_document = json.loads(envelope)
        validated_document["request"]["toolConfig"] = {
            "functionCallingConfig": {"mode": "VALIDATED"}
        }
        validated = json.dumps(validated_document, indent=3).encode()
        self.assertEqual(replay.apply_variant(validated, "tool_validated"), validated)

    def test_variant_parsing_supports_comma_and_repeated_values(self):
        self.assertEqual(
            replay.parse_variants(["baseline,thinking_low", "tool_auto"]),
            ("baseline", "thinking_low", "tool_auto"),
        )
        self.assertEqual(replay.parse_variants(None), replay.DEFAULT_VARIANTS)
        with self.assertRaises(ValueError):
            replay.parse_variants(["baseline", "baseline"])

    def test_request_hash_is_exact_incremental_hash(self):
        expected = __import__("hashlib").sha256(BASE_BODY).hexdigest()
        self.assertEqual(replay.request_sha256(BASE_BODY), expected)
        self.assertGreater(len(list(replay.iter_bytes(BASE_BODY, 5))), 1)

    def test_expected_hash_supports_single_and_qualified_ab(self):
        baseline_hash = replay.request_sha256(BASE_BODY)
        low_hash = replay.request_sha256(replay.apply_variant(BASE_BODY, "thinking_low"))
        one = replay.build_plans(BASE_BODY, ("baseline",), "gemini-3.6-flash-high", (baseline_hash,))
        pair = replay.build_plans(
            BASE_BODY,
            ("baseline", "thinking_low"),
            "gemini-3.6-flash-high",
            (f"baseline={baseline_hash}", f"thinking_low={low_hash}"),
        )
        self.assertEqual(one[0].sha256, baseline_hash)
        self.assertEqual([plan.sha256 for plan in pair], [baseline_hash, low_hash])
        with self.assertRaises(ValueError):
            replay.build_plans(BASE_BODY, ("baseline",), "gemini-3.6-flash-high", ("0" * 64,))
        with self.assertRaises(ValueError):
            replay.build_plans(
                BASE_BODY,
                ("baseline", "thinking_low"),
                "gemini-3.6-flash-high",
                (baseline_hash,),
            )

    def test_hash_mismatch_fails_before_network_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.parse_args(
                self.write_request(directory),
                "--variants", "baseline",
                "--allow-live-network",
                "--expected-request-sha256", "0" * 64,
            )
            with (
                mock.patch.object(replay, "_perform_request") as perform,
                mock.patch.object(replay, "read_api_key") as read_key,
                self.assertRaises(ValueError),
            ):
                replay.run_replay(args, io.StringIO())
        perform.assert_not_called()
        read_key.assert_not_called()

    def test_missing_live_opt_in_cannot_create_connection(self):
        plan = replay.RequestPlan(
            "baseline", BASE_BODY, replay.request_sha256(BASE_BODY), "gemini-3.6-flash-high"
        )
        factory = mock.Mock(side_effect=AssertionError("network attempted"))
        with self.assertRaises(ValueError):
            replay._perform_request(
                replay.DEFAULT_BASE_URL,
                plan,
                "fixture-key",
                allow_live_network=False,
                timeout_seconds=1,
                max_response_bytes=1024,
                max_events=10,
                connection_factory=factory,
            )
        factory.assert_not_called()

    def test_transport_rejects_remote_http_and_https_before_connection(self):
        plan = replay.RequestPlan(
            "baseline", BASE_BODY, replay.request_sha256(BASE_BODY), "gemini-3.6-flash-high"
        )
        for base_url in ("http://gateway.example", "https://gateway.example"):
            with self.subTest(base_url=base_url):
                factory = mock.Mock(side_effect=AssertionError("network attempted"))
                with self.assertRaises(ValueError):
                    replay._perform_request(
                        base_url,
                        plan,
                        "fixture-key",
                        allow_live_network=True,
                        timeout_seconds=1,
                        max_response_bytes=1024,
                        max_events=10,
                        connection_factory=factory,
                    )
                factory.assert_not_called()

    def test_default_dry_run_is_reproducible_redacted_and_network_free(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.parse_args(self.write_request(directory))
            first = io.StringIO()
            second = io.StringIO()
            with mock.patch.object(replay, "_perform_request") as perform:
                self.assertEqual(replay.run_replay(args, first), 0)
                self.assertEqual(replay.run_replay(args, second), 0)
            perform.assert_not_called()
        self.assertEqual(first.getvalue(), second.getvalue())
        encoded = first.getvalue()
        events = [json.loads(line) for line in encoded.splitlines()]
        self.assertEqual([event["variant"] for event in events[:2]], ["baseline", "thinking_low"])
        self.assertEqual(events[-1]["network_requests"], 0)
        self.assertNotIn(SECRET_PROMPT, encoded)
        self.assertNotIn("private_tool_name", encoded)
        self.assertNotIn("gemini-3.6-flash-high", encoded)
        self.assertNotIn(replay.DEFAULT_BASE_URL, encoded)

    def test_dry_run_flag_conflicts_with_live_authorization(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.parse_args(
                self.write_request(directory), "--dry-run", "--allow-live-network"
            )
            with self.assertRaises(ValueError):
                replay.run_replay(args, io.StringIO())

    def test_base_url_validation_and_versionless_target(self):
        accepted = (
            "http://localhost:18317/",
            "http://127.9.8.7:18317/prefix/",
            "http://[::1]:18317",
            "https://localhost:18317/prefix",
        )
        for value in accepted:
            with self.subTest(value=value):
                self.assertTrue(replay.normalize_base_url(value))
        rejected = (
            "http://gateway.example",
            "https://gateway.example",
            "https://user:password@gateway.example",
            "https://gateway.example?private=value",
            "https://gateway.example#private",
            "https://gateway.example/v1",
            "https://gateway.example/v1beta/",
            "ftp://127.0.0.1",
            "relative/path",
        )
        for value in rejected:
            with self.subTest(value=value), self.assertRaises(ValueError):
                replay.normalize_base_url(value)
        parsed = __import__("urllib.parse", fromlist=["urlsplit"]).urlsplit(
            "http://127.0.0.1:18317/prefix"
        )
        self.assertEqual(
            replay._request_target(parsed, "gemini-3.6-flash-high"),
            "/prefix/v1beta/models/gemini-3.6-flash-high:streamGenerateContent?alt=sse",
        )

    def test_api_key_uses_environment_or_file_and_rejects_conflict(self):
        self.assertEqual(
            replay.read_api_key({"SEMANTIC_REPLAY_API_KEY": " fixture-key "}),
            "fixture-key",
        )
        with tempfile.TemporaryDirectory() as directory:
            key_path = Path(directory) / "key"
            key_path.write_text("file-key\n", encoding="utf-8")
            self.assertEqual(
                replay.read_api_key({"SEMANTIC_REPLAY_API_KEY_FILE": str(key_path)}),
                "file-key",
            )
            with self.assertRaises(ValueError):
                replay.read_api_key({
                    "SEMANTIC_REPLAY_API_KEY": "direct",
                    "SEMANTIC_REPLAY_API_KEY_FILE": str(key_path),
                })
        with self.assertRaises(ValueError):
            replay.read_api_key({})
        with self.assertRaises(ValueError):
            replay.read_api_key({"SEMANTIC_REPLAY_API_KEY": "line1\nline2"})
        if getattr(os, "O_NOFOLLOW", 0) and hasattr(os, "symlink"):
            with tempfile.TemporaryDirectory() as directory:
                key_path = Path(directory) / "key"
                key_path.write_text("file-key", encoding="utf-8")
                link = Path(directory) / "key-link"
                os.symlink(key_path, link)
                with self.assertRaises(OSError):
                    replay.read_api_key({"SEMANTIC_REPLAY_API_KEY_FILE": str(link)})

    def test_output_is_0600_exclusive_and_overwrite_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.jsonl"
            stream, should_close = replay.open_output(str(path))
            self.assertTrue(should_close)
            stream.write("old")
            stream.close()
            if os.name != "nt":
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                replay.open_output(str(path))
            os.chmod(path, 0o666)
            stream, should_close = replay.open_output(str(path), overwrite=True)
            self.assertTrue(should_close)
            stream.write("new")
            stream.close()
            self.assertEqual(path.read_text(), "new")
            if os.name != "nt":
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_stream_summary_accumulates_function_call_before_empty_stop(self):
        body = (
            "data: " + json.dumps({
                "candidates": [{"content": {"parts": [{
                    "functionCall": {"name": "private_tool", "args": {"secret": "value"}}
                }]}}]
            }) + "\n\n" +
            "data: " + json.dumps({
                "candidates": [{"content": {"parts": []}, "finishReason": "STOP"}]
            }) + "\n"
        ).encode()
        response = FakeResponse(body, read_size=3)
        summary = replay.summarize_http_response(
            response, max_response_bytes=4096, max_events=20
        )
        encoded = json.dumps(summary.event("baseline", 1))
        self.assertEqual(summary.outcome(), "function_call")
        self.assertTrue(summary.saw_function_call)
        self.assertIn("STOP", summary.finish_reasons)
        self.assertNotIn("private_tool", encoded)
        self.assertNotIn("secret", encoded)
        self.assertGreater(response.read_calls, 1)

    def test_response_envelope_usage_and_function_call_validity_are_structural(self):
        private_name = "private-function"
        wrapped = {
            "response": {
                "candidates": [{
                    "content": {"parts": [
                        {"functionCall": {"name": private_name, "args": {}}},
                        {"functionCall": {"name": "", "args": {}}},
                    ]},
                    "finishReason": "STOP",
                }],
                "usageMetadata": {
                    "promptTokenCount": 12,
                    "candidatesTokenCount": -7,
                    "totalTokenCount": replay.MAX_USAGE_COUNT + 99,
                    "thoughtsTokenCount": True,
                },
            }
        }
        body = (f"data: {json.dumps(wrapped)}\n").encode()
        summary = replay.summarize_http_response(
            FakeResponse(body),
            max_response_bytes=4096,
            max_events=20,
        )
        encoded = json.dumps(summary.event("baseline", 1))
        self.assertEqual(summary.outcome(), "function_call")
        self.assertEqual(summary.function_calls, 2)
        self.assertEqual(summary.valid_function_calls, 1)
        self.assertEqual(summary.malformed_function_calls, 1)
        self.assertEqual(summary.usage_metadata_events, 1)
        self.assertEqual(summary.prompt_token_count, 12)
        self.assertEqual(summary.candidate_token_count, 0)
        self.assertEqual(summary.total_token_count, replay.MAX_USAGE_COUNT)
        self.assertEqual(summary.thought_token_count, 0)
        self.assertNotIn(private_name, encoded)

    def test_only_malformed_function_call_is_not_usable(self):
        body = ("data: " + json.dumps({
            "response": {
                "candidates": [{
                    "content": {"parts": [{"functionCall": {"name": "run"}}]},
                    "finishReason": "STOP",
                }]
            }
        }) + "\n").encode()
        summary = replay.summarize_http_response(
            FakeResponse(body),
            max_response_bytes=4096,
            max_events=20,
        )
        self.assertFalse(summary.saw_function_call)
        self.assertEqual(summary.valid_function_calls, 0)
        self.assertEqual(summary.malformed_function_calls, 1)
        self.assertEqual(summary.outcome(), "malformed_function_call")

    def test_stream_summary_classifies_thought_only_stop_and_redacts_content(self):
        secret = "private chain of thought"
        body = ("data: " + json.dumps({
            "candidates": [{
                "content": {"parts": [{
                    "text": secret,
                    "thought": True,
                    "thoughtSignature": "private-signature",
                }]},
                "finishReason": "STOP",
            }]
        }) + "\n").encode()
        summary = replay.summarize_http_response(
            FakeResponse(body), max_response_bytes=4096, max_events=20
        )
        encoded = json.dumps(summary.event("baseline", 1))
        self.assertEqual(summary.outcome(), "semantic_empty_stop")
        self.assertEqual(summary.thought_text_parts, 1)
        self.assertEqual(summary.thought_signature_parts, 1)
        self.assertNotIn(secret, encoded)
        self.assertNotIn("private-signature", encoded)

    def test_stream_summary_bounds_bytes_events_and_untrusted_enums(self):
        unknown = "private-finish-reason"
        event = ("data: " + json.dumps({
            "candidates": [{"finishReason": unknown, "content": {"parts": []}}]
        }) + "\n").encode()
        summary = replay.summarize_http_response(
            FakeResponse(event * 10, read_size=19),
            max_response_bytes=len(event) * 3,
            max_events=1,
        )
        encoded = json.dumps(summary.event("baseline", 1))
        self.assertLessEqual(summary.response_bytes, len(event) * 3)
        self.assertTrue(summary.truncated)
        self.assertTrue(summary.events_truncated)
        self.assertEqual(summary.finish_reasons, {"UNEXPECTED"})
        self.assertEqual(len(summary.unexpected_finish_hashes), 1)
        self.assertNotIn(unknown, encoded)

    def test_perform_request_streams_exact_bytes_to_fake_connection(self):
        response_body = b'data: {"candidates":[{"finishReason":"STOP","content":{"parts":[]}}]}\n'
        connection = FakeConnection(FakeResponse(response_body))
        factory = mock.Mock(return_value=connection)
        plan = replay.RequestPlan(
            "baseline", BASE_BODY, replay.request_sha256(BASE_BODY), "gemini-3.6-flash-high"
        )
        summary = replay._perform_request(
            replay.DEFAULT_BASE_URL,
            plan,
            "fixture-key",
            allow_live_network=True,
            timeout_seconds=1,
            max_response_bytes=4096,
            max_events=20,
            connection_factory=factory,
        )
        self.assertEqual(connection.method, "POST")
        self.assertEqual(b"".join(connection.sent), BASE_BODY)
        self.assertIn("?alt=sse", connection.target)
        self.assertIn(("Authorization", "Bearer fixture-key"), connection.headers)
        self.assertTrue(connection.closed)
        self.assertEqual(summary.outcome(), "semantic_empty_stop")

    def test_live_loop_is_bounded_sequential_and_structural(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.parse_args(
                self.write_request(directory),
                "--variants", "baseline,thinking_low",
                "--attempts", "2",
                "--allow-live-network",
            )
            output = io.StringIO()
            fake_summary = replay.ResponseSummary("success", "event_stream")
            fake_summary.finish_reasons.add("STOP")
            with (
                mock.patch.object(replay, "read_api_key", return_value="fixture-key"),
                mock.patch.object(replay, "_perform_request", return_value=fake_summary) as perform,
            ):
                result = replay.run_replay(args, output)
        self.assertEqual(result, 0)
        self.assertEqual(perform.call_count, 4)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(events[-1]["network_requests"], 4)
        self.assertEqual(events[-1]["outcome_counts"], {"semantic_empty_stop": 4})
        self.assertNotIn(SECRET_PROMPT, output.getvalue())
        for call in perform.call_args_list:
            self.assertTrue(call.kwargs["allow_live_network"])

    def test_safe_error_hashes_message_without_leaking_it(self):
        event = replay.safe_error(ValueError("private prompt https://private.example/path"))
        encoded = json.dumps(event)
        self.assertEqual(event["category"], "validation")
        self.assertEqual(len(event["message_sha256"]), 64)
        self.assertNotIn("private prompt", encoded)
        self.assertNotIn("private.example", encoded)


if __name__ == "__main__":
    unittest.main()
