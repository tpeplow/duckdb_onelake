import json
from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen
import uuid

from benchmark_parallel_reads import decode_section
from benchmark_process import BenchmarkFailure, MeasuredChild
from benchmark_stream_server import load_manifest, stream_delay
from benchmark_streams import check_result, expected_checksum, parse_result, run_client, summaries


class FixtureResultTest(unittest.TestCase):
    def test_client_waits_for_final_live_resource_snapshot(self):
        result = {
            "seconds": 0.1,
            "rows": 1048576,
            "checksum": expected_checksum(1048576),
            "metrics": {"stream_requests": 8},
            "shutdown_seconds": 0.01,
        }
        script = (
            "import os,sys; assert os.environ['ONELAKE_BENCH_WAIT_FOR_EXIT']=='1'; "
            f"print({'BENCH_RESULT ' + json.dumps(result)!r}, flush=True); input()"
        )

        def client(command, **kwargs):
            return MeasuredChild([sys.executable, "-u", "-c", script], **kwargs)

        with patch("benchmark_streams.MeasuredChild", side_effect=client):
            measured = run_client(Path("unused-client.exe"), "http://127.0.0.1:1", "fast", 4, timeout=5, interval=0.005)
        self.assertTrue(measured["normal_child_exit"])
        if sys.platform == "win32":
            self.assertIsNotNone(measured["final_alive_process_counters"])
            self.assertGreater(measured["resources"]["process_peak_working_set_bytes"], 0)
            self.assertGreaterEqual(measured["process_cpu_seconds"], 0)

    def test_result_validation_and_full_checksum_equivalence(self):
        value = {
            "seconds": 0.1,
            "rows": 1048576,
            "checksum": expected_checksum(1048576),
            "metrics": {"stream_requests": 8},
            "shutdown_seconds": 0.01,
        }
        result = parse_result("test tests::benchmark_client ... BENCH_RESULT " + json.dumps(value) + "\nok")
        references = {}
        check_result(result, "balanced", references)
        with self.assertRaises(BenchmarkFailure):
            check_result({**result, "checksum": 456}, "balanced", references)
        with self.assertRaises(BenchmarkFailure):
            check_result({**result, "rows": 0}, "balanced", references)
        check_result({**result, "rows": 4096}, "early", references)
        check_result({**result, "rows": 4096, "checksum": 999}, "early", references)
        with self.assertRaises(BenchmarkFailure):
            parse_result("BENCH_RESULT " + json.dumps({**value, "headers": "not allowed"}))
        with self.assertRaises(BenchmarkFailure):
            parse_result("BENCH_RESULT {}\nBENCH_RESULT {}")

    def test_known_fixture_checksum_formula_matches_small_oracle(self):
        for rows in (1, 7, 8, 15, 4096):
            self.assertEqual(expected_checksum(rows), sum(i for i in range(rows) if i % 7))

    def test_summary_excludes_warmup_and_uses_ranges(self):
        runs = [
            {
                "scenario": "fast",
                "cap": cap,
                "warmup": iteration == 0,
                "status": "ok",
                "seconds": 100 if iteration == 0 else 8 / cap,
                "process_cpu_seconds": 1,
                "shutdown_seconds": 0.01,
                "resources": {"process_peak_working_set_bytes": 1000 * cap},
            }
            for cap in (1, 2, 4, 8)
            for iteration in range(6)
        ]
        result = summaries(runs)
        self.assertEqual(result["fast"]["1"]["seconds"]["median"], 8)
        self.assertEqual(result["fast"]["8"]["seconds"]["over_cap1"], 0.125)
        self.assertNotIn("p95", json.dumps(result))

    def test_delays_match_fixed_scenarios(self):
        self.assertEqual(stream_delay("balanced", "3"), 0.002)
        self.assertEqual(stream_delay("one", "0"), 0.002)
        self.assertEqual(stream_delay("skewed", "0"), 0.01)
        self.assertEqual(stream_delay("skewed", "1"), 0)
        self.assertEqual(stream_delay("stalled", "0"), 0)


class FixtureServerTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(__file__).parent / (".benchmark-fixture-test-" + uuid.uuid4().hex)
        self.directory.mkdir()
        self.addCleanup(shutil.rmtree, self.directory)
        streams = []
        for index in range(8):
            name = f"stream{index}.arrow"
            (self.directory / name).write_bytes(bytes([index]) * 128)
            streams.append({"id": str(index), "file": name, "batch_ends": [32, 96]})
        self.manifest = {"streams": streams}
        (self.directory / "manifest.json").write_text(json.dumps(self.manifest), encoding="utf-8")

    def test_server_is_separate_process_and_serves_stream_boundaries(self):
        script = Path(__file__).with_name("benchmark_stream_server.py")
        with MeasuredChild(
            [
                sys.executable,
                "-u",
                str(script.resolve()),
                "--fixtures",
                str(self.directory.resolve()),
                "--scenario",
                "fast",
            ],
            timeout=10,
        ) as server:
            server.wait_marker("BENCH_READY")
            url = decode_section(server.stdout, "BENCH_SERVER")["url"]
            with urlopen(url + "/health", timeout=5) as response:
                self.assertEqual(response.status, 200)
            with urlopen(Request(url + "/read", data=b"{}"), timeout=5) as response:
                self.assertEqual(len(json.load(response)["streams"]), 8)
            with urlopen(url + "/readStream/3", timeout=5) as response:
                self.assertEqual(response.headers["Content-Type"], "application/vnd.apache.arrow.stream")
                self.assertEqual(int(response.headers["Content-Length"]), 128)
                self.assertEqual(response.read(), bytes([3]) * 128)
        self.assertIsNotNone(server.process.poll())

    def test_manifest_rejects_nonmonotonic_boundaries(self):
        self.manifest["streams"][0]["batch_ends"] = [96, 32]
        (self.directory / "manifest.json").write_text(json.dumps(self.manifest), encoding="utf-8")
        with self.assertRaises(ValueError):
            load_manifest(self.directory)

    def test_one_stream_allocation_control(self):
        script = Path(__file__).with_name("benchmark_stream_server.py")
        with MeasuredChild(
            [
                sys.executable,
                "-u",
                str(script.resolve()),
                "--fixtures",
                str(self.directory.resolve()),
                "--scenario",
                "one",
            ],
            timeout=10,
        ) as server:
            server.wait_marker("BENCH_READY")
            url = decode_section(server.stdout, "BENCH_SERVER")["url"]
            with urlopen(Request(url + "/read", data=b"{}"), timeout=5) as response:
                self.assertEqual(json.load(response), {"streams": [{"streamId": "0"}]})


if __name__ == "__main__":
    unittest.main()
