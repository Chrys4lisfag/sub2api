import importlib.util
import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock
from types import SimpleNamespace


MODULE_PATH = Path(__file__).with_name("genai_stream_probe.py")
SPEC = importlib.util.spec_from_file_location("genai_stream_probe", MODULE_PATH)
probe = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = probe
SPEC.loader.exec_module(probe)


class GenAIStreamProbeTests(unittest.TestCase):
    def test_summarize_function_call_redacts_argument_values(self):
        chunk = SimpleNamespace(candidates=[SimpleNamespace(
            finish_reason="STOP",
            content=SimpleNamespace(parts=[SimpleNamespace(
                text=None,
                thought=False,
                function_call=SimpleNamespace(name="get_weather", args={"city": "private-city"}),
            )]),
        )])
        summary = probe.ProbeSummary()

        event = probe.summarize_chunk(chunk, 1, summary)
        encoded = json.dumps(event)

        self.assertTrue(summary.saw_function_call)
        self.assertEqual(event["function_calls"], [{
            "name": "get_weather",
            "argument_keys": ["city"],
            "arguments_present": True,
            "argument_keys_truncated": 0,
            "valid_contract": True,
        }])
        self.assertNotIn("private-city", encoded)
        self.assertEqual(probe.final_event(summary)["outcome"], "function_call")

    def test_thought_only_stop_is_distinct_from_empty_stop(self):
        thought = SimpleNamespace(candidates=[SimpleNamespace(
            finish_reason="STOP",
            content=SimpleNamespace(parts=[SimpleNamespace(
                text="private reasoning",
                thought=True,
                thought_signature=b"private-signature",
                function_call=None,
            )]),
        )])
        summary = probe.ProbeSummary()
        event = probe.summarize_chunk(thought, 1, summary)

        self.assertEqual(event["thought_text_parts"], 1)
        self.assertEqual(event["thought_signature_parts"], 1)
        self.assertNotIn("private reasoning", json.dumps(event))
        self.assertEqual(probe.final_event(summary)["outcome"], "thought_only")

        empty = probe.ProbeSummary()
        probe.summarize_chunk(SimpleNamespace(candidates=[SimpleNamespace(
            finish_reason="STOP",
            content=SimpleNamespace(parts=[]),
        )]), 1, empty)
        self.assertEqual(probe.final_event(empty)["outcome"], "empty_stop")

    def test_malformed_function_call_is_not_success(self):
        chunk = SimpleNamespace(candidates=[SimpleNamespace(
            finish_reason="STOP",
            content=SimpleNamespace(parts=[SimpleNamespace(
                text=None,
                thought=False,
                thought_signature=None,
                function_call=SimpleNamespace(name=None, args=None),
            )]),
        )])
        summary = probe.ProbeSummary()

        probe.summarize_chunk(chunk, 1, summary)

        self.assertTrue(summary.saw_function_call)
        self.assertFalse(summary.saw_valid_function_call)
        self.assertEqual(probe.final_event(summary)["outcome"], "malformed_function_call")

    def test_wrong_type_or_empty_city_is_not_valid_function_call(self):
        for city in (None, 123, "", "   "):
            with self.subTest(city=city):
                chunk = SimpleNamespace(candidates=[SimpleNamespace(
                    finish_reason="STOP",
                    content=SimpleNamespace(parts=[SimpleNamespace(
                        text=None,
                        thought=False,
                        function_call=SimpleNamespace(
                            name="get_weather",
                            args={"city": city},
                        ),
                    )]),
                )])
                summary = probe.ProbeSummary()

                event = probe.summarize_chunk(chunk, 1, summary)

                self.assertFalse(event["function_calls"][0]["valid_contract"])
                self.assertFalse(summary.saw_valid_function_call)
                self.assertEqual(
                    probe.final_event(summary)["outcome"],
                    "malformed_function_call",
                )

    def test_adversarial_chunk_output_is_bounded_and_redacted(self):
        private_name = "secret-token-" + ("x" * 10000)
        private_key = "https://private.example/" + ("y" * 10000)
        args = {f"{private_key}-{index}": "private-value" for index in range(200)}
        parts = [
            SimpleNamespace(
                text=None,
                thought=False,
                thought_signature=b"private-signature",
                function_call=SimpleNamespace(name=private_name, args=args),
            )
            for _ in range(300)
        ]
        candidate = SimpleNamespace(
            finish_reason="UNTRUSTED-" + ("z" * 10000),
            content=SimpleNamespace(parts=parts),
        )
        summary = probe.ProbeSummary()

        event = probe.summarize_chunk(
            SimpleNamespace(candidates=[candidate] * 20),
            1,
            summary,
        )
        encoded = json.dumps(event)

        self.assertLess(len(encoded), 20000)
        self.assertEqual(len(event["function_calls"]), probe.MAX_RECORDED_FUNCTION_CALLS)
        self.assertGreater(event["function_calls_truncated"], 0)
        self.assertEqual(event["candidate_count_inspected"], probe.MAX_INSPECTED_CANDIDATES)
        self.assertNotIn("secret-token", encoded)
        self.assertNotIn("private.example", encoded)
        self.assertNotIn("private-value", encoded)

    def test_run_probe_does_not_pull_past_chunk_limit_and_forces_developer_mode(self):
        valid_chunk = SimpleNamespace(candidates=[SimpleNamespace(
            finish_reason="STOP",
            content=SimpleNamespace(parts=[SimpleNamespace(
                text=None,
                thought=False,
                thought_signature=None,
                function_call=SimpleNamespace(name="get_weather", args={"city": "private-city"}),
            )]),
        )])

        class OneChunkThenFail:
            def __init__(self):
                self.pulls = 0

            def __iter__(self):
                return self

            def __next__(self):
                self.pulls += 1
                if self.pulls == 1:
                    return valid_chunk
                raise AssertionError("probe pulled beyond max_chunks")

        iterator = OneChunkThenFail()
        created = {}
        google_module = ModuleType("google")
        genai_module = ModuleType("google.genai")
        types_module = ModuleType("google.genai.types")

        def factory(**kwargs):
            return SimpleNamespace(**kwargs)

        for name in (
            "HttpOptions",
            "GenerateContentConfig",
            "Tool",
            "FunctionDeclaration",
            "ToolConfig",
            "FunctionCallingConfig",
        ):
            setattr(types_module, name, factory)

        class FakeClient:
            def __init__(self, **kwargs):
                created.update(kwargs)
                self.models = SimpleNamespace(
                    generate_content_stream=lambda **_kwargs: iterator
                )

            def close(self):
                created["closed"] = True

        genai_module.Client = FakeClient
        genai_module.types = types_module
        google_module.genai = genai_module
        args = SimpleNamespace(
            base_url="http://127.0.0.1:8080/antigravity-native",
            max_chunks=1,
            expected_sdk_version=probe.EXPECTED_SDK_VERSION,
            model=probe.DEFAULT_MODEL,
        )
        output = io.StringIO()
        modules = {
            "google": google_module,
            "google.genai": genai_module,
            "google.genai.types": types_module,
        }
        with (
            mock.patch.dict(sys.modules, modules),
            mock.patch.object(
                probe.importlib.metadata,
                "version",
                return_value=probe.EXPECTED_SDK_VERSION,
            ),
            mock.patch.object(probe, "_read_api_key", return_value="fixture-key"),
            mock.patch.dict(
                os.environ,
                {
                    "GOOGLE_GENAI_USE_ENTERPRISE": "true",
                    "GOOGLE_GENAI_USE_VERTEXAI": "true",
                },
            ),
        ):
            result = probe.run_probe(args, output)

        self.assertEqual(result, 0)
        self.assertEqual(iterator.pulls, 1)
        self.assertIs(created["enterprise"], False)
        self.assertTrue(created["closed"])
        self.assertNotIn("private-city", output.getvalue())

    def test_safe_error_emits_hash_not_message(self):
        event = probe._safe_error(RuntimeError("secret-token https://private.example/path"))
        encoded = json.dumps(event)

        self.assertEqual(event["error_type"], "RuntimeError")
        self.assertEqual(len(event["message_sha256"]), 64)
        self.assertNotIn("secret-token", encoded)
        self.assertNotIn("private.example", encoded)

    def test_hard_deadline_emits_bounded_timeout_and_exits_124(self):
        timers = []

        class FakeTimer:
            def __init__(self, seconds, callback):
                self.seconds = seconds
                self.callback = callback
                self.daemon = False
                self.started = False
                timers.append(self)

            def start(self):
                self.started = True

        read_fd, write_fd = os.pipe()
        try:
            with os.fdopen(write_fd, "w") as output:
                with mock.patch.object(probe.threading, "Timer", FakeTimer):
                    timer = probe._start_hard_deadline(7, output)

                    self.assertEqual(timer.seconds, 7)
                    self.assertTrue(timer.started)
                    self.assertTrue(timer.daemon)

                    with (
                        mock.patch.object(
                            probe.os,
                            "_exit",
                            side_effect=SystemExit(124),
                        ),
                        self.assertRaisesRegex(SystemExit, "124"),
                    ):
                        timer.callback()

                payload = os.read(read_fd, 4096)
                self.assertEqual(
                    json.loads(payload),
                    {"event": "timeout", "deadline_seconds": 7},
                )
                self.assertEqual(len(timers), 2)
                self.assertEqual(timers[1].seconds, 0.1)
                self.assertTrue(timers[1].started)
                self.assertTrue(timers[1].daemon)
        finally:
            os.close(read_fd)

    def test_hard_deadline_exits_when_output_pipe_is_full(self):
        child = f"""
import importlib.util
import os
import sys

path = {str(MODULE_PATH)!r}
spec = importlib.util.spec_from_file_location("genai_stream_probe_child", path)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
read_fd, write_fd = os.pipe()
os.set_blocking(write_fd, False)
try:
    while True:
        os.write(write_fd, b"x" * 65536)
except BlockingIOError:
    pass
os.set_blocking(write_fd, True)
output = os.fdopen(write_fd, "w")
module._start_hard_deadline(1, output)
os.write(write_fd, b"x")
"""
        completed = subprocess.run(
            [sys.executable, "-c", child],
            check=False,
            timeout=5,
        )

        self.assertEqual(completed.returncode, 124)

    def test_main_reports_output_open_failure_without_path(self):
        private_path = "/private/secret/probe.jsonl"
        args = SimpleNamespace(
            output=private_path,
            deadline_seconds=7,
        )
        stdout = io.StringIO()

        with (
            mock.patch.object(
                probe,
                "_build_parser",
                return_value=SimpleNamespace(parse_args=lambda: args),
            ),
            mock.patch.object(
                probe,
                "_open_output",
                side_effect=OSError(private_path),
            ),
            mock.patch.object(probe.sys, "stdout", stdout),
        ):
            result = probe.main()

        event = json.loads(stdout.getvalue())
        self.assertEqual(result, 3)
        self.assertEqual(event["event"], "error")
        self.assertEqual(event["error_type"], "OSError")
        self.assertNotIn(private_path, stdout.getvalue())

    def test_output_is_exclusive_and_private(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.jsonl"
            stream, should_close = probe._open_output(str(path))
            self.assertTrue(should_close)
            stream.write("{}\n")
            stream.close()
            if os.name != "nt":
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                probe._open_output(str(path))

    def test_base_url_omits_sdk_api_version_and_rejects_unsafe_http(self):
        self.assertEqual(
            probe._normalize_base_url("http://127.0.0.1:8080/antigravity-native/"),
            "http://127.0.0.1:8080/antigravity-native",
        )
        self.assertEqual(
            probe._normalize_base_url("https://gateway.example/antigravity-native"),
            "https://gateway.example/antigravity-native",
        )
        for value in (
            "http://127.0.0.1:8080/antigravity-native/v1beta",
            "http://127.0.0.1:8080/antigravity-native/v1",
            "http://gateway.example/antigravity-native",
            "https://user:password@gateway.example/antigravity-native",
            "https://gateway.example/antigravity-native?secret=value",
            "https://gateway.example/antigravity-native#fragment",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                probe._normalize_base_url(value)

    def test_api_key_file_and_conflicting_sources(self):
        old_direct = os.environ.get("GENAI_PROBE_API_KEY")
        old_file = os.environ.get("GENAI_PROBE_API_KEY_FILE")
        try:
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "key"
                path.write_text("fixture-key\n", encoding="utf-8")
                os.environ.pop("GENAI_PROBE_API_KEY", None)
                os.environ["GENAI_PROBE_API_KEY_FILE"] = str(path)
                self.assertEqual(probe._read_api_key(), "fixture-key")
                os.environ["GENAI_PROBE_API_KEY"] = "other"
                with self.assertRaises(ValueError):
                    probe._read_api_key()
        finally:
            if old_direct is None:
                os.environ.pop("GENAI_PROBE_API_KEY", None)
            else:
                os.environ["GENAI_PROBE_API_KEY"] = old_direct
            if old_file is None:
                os.environ.pop("GENAI_PROBE_API_KEY_FILE", None)
            else:
                os.environ["GENAI_PROBE_API_KEY_FILE"] = old_file


if __name__ == "__main__":
    unittest.main()
