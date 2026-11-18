import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from benchmark_reads import comparable_result, difference, failure_reason, main, parse_metrics, parse_runs


class BenchmarkMetricsTest(unittest.TestCase):
    def test_failure_diagnostics_do_not_copy_service_details(self):
        self.assertEqual(
            failure_reason("IO Error: OneLake read stream returned HTTP 429; sensitive service body"),
            "http_429",
        )
        self.assertEqual(failure_reason("IO Error: OneLake transport failed"), "transport_failure")
        self.assertEqual(failure_reason("sensitive unknown error body"), "unclassified_child_failure")

    def test_threads_are_applied_to_sql_and_recorded_without_live_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            shell = Path(directory) / "shell.exe"
            shell.write_bytes(b"offline fixture")
            output = Path(directory) / "report.json"
            arguments = [
                "benchmark_reads.py",
                "--approved-live-read",
                "--shell",
                str(shell),
                "--workspace",
                "00000000-0000-0000-0000-000000000001",
                "--item",
                "00000000-0000-0000-0000-000000000002",
                "--schema",
                "dbo",
                "--table",
                "fixture",
                "--cases",
                "count",
                "--modes",
                "TABLE_API",
                "--repeats",
                "1",
                "--output",
                str(output),
            ]
            result = SimpleNamespace(
                returncode=0,
                stdout='BENCH_QUERY_0\n[{"n":3}]\nRun Time (s): real 0.25 user 0.1 sys 0.0\nBENCH_DONE\n',
            )
            for threads in (None, 1, 8):
                with self.subTest(threads=threads):
                    argv = arguments + ([] if threads is None else ["--threads", str(threads)])
                    with patch("sys.argv", argv), patch(
                        "benchmark_reads.subprocess.check_output", side_effect=["fixture-commit", ""]
                    ), patch("benchmark_reads.subprocess.run", return_value=result) as run, contextlib.redirect_stdout(
                        io.StringIO()
                    ):
                        main()
                    expected = 2 if threads is None else threads
                    self.assertIn(f"SET threads={expected};", run.call_args.kwargs["input"])
                    self.assertEqual(json.loads(output.read_text())["threads"], expected)
                    output.unlink()
            failed = SimpleNamespace(
                returncode=1,
                stdout="BENCH_QUERY_0\n",
                stderr="OneLake read stream returned HTTP 429; sensitive service body",
            )
            with patch("sys.argv", arguments + ["--threads", "8"]), patch(
                "benchmark_reads.subprocess.check_output", side_effect=["fixture-commit", ""]
            ), patch("benchmark_reads.subprocess.run", return_value=failed), contextlib.redirect_stdout(
                io.StringIO()
            ), self.assertRaises(
                RuntimeError
            ):
                main()
            failure = json.loads(output.read_text())
            self.assertFalse(failure["equivalent"])
            self.assertEqual(failure["failure"]["reason"], "http_429")
            self.assertEqual(failure["failure"]["query_markers"], 1)
            self.assertNotIn("sensitive", output.read_text())
            output.unlink()
            for threads in (0, -1):
                with self.subTest(threads=threads), patch("sys.argv", arguments + ["--threads", str(threads)]), patch(
                    "benchmark_reads.subprocess.run"
                ) as run, contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    main()
                run.assert_not_called()

    def test_peak_stream_metric_is_a_high_water_snapshot(self):
        self.assertEqual(
            difference(
                {"execution.allocated_streams": 16, "execution.peak_active_streams": 4},
                {"execution.allocated_streams": 8, "execution.peak_active_streams": 4},
            ),
            {"execution.allocated_streams": 8, "execution.peak_active_streams": 4},
        )

    def test_metrics_and_profile_control_do_not_replace_query_timing(self):
        stdout = (
            'BENCH_QUERY_0\n[{"n":10}]\nRun Time (s): real 1.25 user 0.1 sys 0.0\n'
            "Run Time (s): real 0.001 user 0.0 sys 0.0\n"
            'BENCH_METRICS_0\n[{"phase":"execution","metric":"rows","value":2048}]\n'
            "Run Time (s): real 0.002 user 0.0 sys 0.0\n"
            'BENCH_QUERY_1\n[{"n":10}]\nRun Time (s): real 0.75 user 0.1 sys 0.0\n'
            'BENCH_METRICS_1\n[{"phase":"execution","metric":"rows","value":4096}]\n'
            "BENCH_DONE\n"
        )
        runs = parse_runs(stdout)
        self.assertEqual([run["seconds"] for run in runs], [1.25, 0.75])
        self.assertEqual([run["result"] for run in runs], [[{"n": 10}], [{"n": 10}]])
        first = parse_metrics(stdout, "BENCH_METRICS_0")
        second = parse_metrics(stdout, "BENCH_METRICS_1")
        self.assertEqual(difference(second, first), {"execution.rows": 2048})

    def test_missing_query_timer_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_runs('BENCH_QUERY_0\n[{"n":10}]\nBENCH_DONE\n')

    def test_unordered_value_sample_checks_counts_not_selected_values(self):
        first = [{"n": 10, "nonnull": 10, "total": 123}]
        second = [{"n": 10, "nonnull": 9, "total": 456}]
        self.assertEqual(
            comparable_result("value_sample", first),
            comparable_result("value_sample", second),
        )
        self.assertNotEqual(
            comparable_result("aggregate", first),
            comparable_result("aggregate", second),
        )
        with self.assertRaises(ValueError):
            comparable_result("value_sample", [{"n": 10, "nonnull": 11}])


if __name__ == "__main__":
    unittest.main()
