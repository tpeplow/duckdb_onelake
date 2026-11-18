import json
from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch
import uuid
from decimal import Decimal

from benchmark_parallel_reads import (
    BenchmarkFailure,
    Ledger,
    build_parser,
    check_equivalent,
    decode_section,
    execute,
    extension_provenance,
    metric_delta,
    parse_timer,
    schedule,
    validate_metrics,
    workload_queries,
)
from benchmark_process import MeasuredChild


class BenchmarkParsingTest(unittest.TestCase):
    def parser_args(self):
        return [
            "--approved-live-read",
            "--serial-shell",
            "serial.exe",
            "--workspace",
            str(uuid.uuid4()),
            "--item",
            str(uuid.uuid4()),
            "--candidate-source",
            "source+working-tree",
            "--host-conditions",
            "idle",
            "--output",
            "new-report.json",
        ]

    def test_target_is_required_and_guid_validated(self):
        parser = build_parser()
        args = self.parser_args()
        parsed = parser.parse_args(args)
        self.assertIsInstance(parsed.workspace, uuid.UUID)
        self.assertIsInstance(parsed.item, uuid.UUID)
        self.assertEqual((parsed.schema, parsed.table), ("dbo", "PricePaid"))
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(args + ["--workspace", "not-a-guid"])
        index = args.index("--workspace")
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(args[:index] + args[index + 2 :])
        index = args.index("--item")
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(args[:index] + args[index + 2 :])

    def test_identifiers_are_quoted_and_control_characters_rejected(self):
        queries = workload_queries('custom"schema', "Price;Paid")
        for query in queries.values():
            self.assertIn('benchmark."custom""schema"."Price;Paid"', query)
        parser = build_parser()
        for value in ("", "dbo\n.print injected", "dbo\r", "dbo\0", "dbo\t"):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser.parse_args(self.parser_args() + ["--schema", value])

    def test_cpu_uses_first_query_timer_and_exact_decimal(self):
        output = (
            'BENCH_QUERY\r\n[{"total":123456789012345678901234567890.12}]\r\n'
            "Run Time (s): real 2.0 user 3.2 sys 0.8\n"
            "Run Time (s): real 999 user 999 sys 999\nBENCH_QUERY_DONE\n"
        )
        self.assertEqual(parse_timer(output, "BENCH_QUERY")["cpu_seconds"], 4)
        self.assertEqual(parse_timer(output, "BENCH_QUERY")["average_cpu_cores"], 2)
        self.assertEqual(
            decode_section(output, "BENCH_QUERY")[0]["total"], Decimal("123456789012345678901234567890.12")
        )
        with self.assertRaises(BenchmarkFailure):
            parse_timer("BENCH_QUERY\n[]\nBENCH_DONE\n", "BENCH_QUERY")

    def test_peak_is_not_differenced(self):
        before = {"execution.allocated_streams": 8, "execution.peak_active_streams": 4}
        after = {"execution.allocated_streams": 16, "execution.peak_active_streams": 4}
        self.assertEqual(
            metric_delta(after, before), {"execution.allocated_streams": 8, "execution.peak_active_streams": 4}
        )

    def test_static_reader_provenance_is_required(self):
        prefix = "BENCH_EXTENSIONS\n"
        row = {"extension_name": "onelake", "loaded": True, "install_mode": "STATICALLY_LINKED"}
        self.assertEqual(extension_provenance(prefix + json.dumps([row])), [row])
        with self.assertRaises(BenchmarkFailure):
            extension_provenance(prefix + json.dumps([{**row, "install_mode": "REPOSITORY"}]))

    def test_baseline_unavailable_and_schema_allocation_validation(self):
        before = {"schema.allocation_requests": 0, "schema.stream_requests": 0, "execution.allocation_requests": 0}
        after = {
            **before,
            "execution.allocation_requests": 1,
            "execution.errors": 0,
            "execution.completions": 1,
            "execution.stream_requests": 8,
            "execution.stream_bytes": 1000,
            "execution.rows": 100,
            "execution.batches": 5,
        }
        _, diagnostics = validate_metrics(before, after, candidate=False, complete=True)
        self.assertEqual(diagnostics["allocated_streams"], 8)
        self.assertIsNone(diagnostics["peak_active_streams"])
        _, diagnostics = validate_metrics(before, after, candidate=False, complete=False)
        self.assertIsNone(diagnostics["allocated_streams"])
        with self.assertRaises(BenchmarkFailure):
            validate_metrics(before, after, candidate=True, complete=True)
        with self.assertRaises(BenchmarkFailure):
            validate_metrics(before, {**after, "schema.allocation_requests": 1}, candidate=False, complete=True)
        with self.assertRaises(BenchmarkFailure):
            validate_metrics(before, {**after, "execution.allocation_requests": 2}, candidate=False, complete=True)

    def test_correctness_and_arbitrary_limit(self):
        references = {}
        narrow = [
            {
                "row_count": 10,
                "nonnull_price": 9,
                "min_price": Decimal("1.2"),
                "max_price": Decimal("99"),
                "total_price_2dp": Decimal("100.01"),
            }
        ]
        check_equivalent("narrow", narrow, references)
        with self.assertRaises(BenchmarkFailure):
            check_equivalent("narrow", [{**narrow[0], "total_price_2dp": Decimal("100.02")}], references)
        check_equivalent("limit", [{"row_count": 10, "nonnull_price": 5}], references)
        check_equivalent("limit", [{"row_count": 9, "nonnull_price": 9}], references)
        with self.assertRaises(BenchmarkFailure):
            check_equivalent("limit", [{"row_count": 10, "nonnull_price": 11}], references)

    def test_wide_cross_check_preserves_decimal_precision(self):
        value = Decimal("123456789012345678901234567890.12")
        references = {}
        check_equivalent(
            "narrow",
            [
                {
                    "row_count": 2,
                    "nonnull_price": 2,
                    "min_price": 0,
                    "max_price": value,
                    "total_price_2dp": Decimal("123456789012345678901234567891.12"),
                }
            ],
            references,
        )
        check_equivalent(
            "wide",
            [
                {
                    "County": str(i),
                    "row_count": 1,
                    "nonnull_price": 1,
                    "total_price_2dp": amount,
                    "postcode_chars": 0,
                    "town_chars": 0,
                    "street_chars": 0,
                }
                for i, amount in enumerate((value, Decimal(1)))
            ],
            references,
        )


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(__file__).parent / (".benchmark-test-" + uuid.uuid4().hex)
        self.directory.mkdir()
        self.addCleanup(shutil.rmtree, self.directory)
        self.path = self.directory / "report.json"

    def test_schedule_and_hard_cap_include_failed_attempts(self):
        specs = list(schedule())
        self.assertEqual(len(specs), 20)
        self.assertEqual(sum(spec["case"] != "limit" for spec in specs), 14)
        self.assertEqual(
            [s["build"] for s in specs[2:8]], ["serial", "candidate", "candidate", "serial", "serial", "candidate"]
        )
        ledger = Ledger(self.path, {})
        for spec in specs:
            ledger.reserve(spec)["status"] = "failed"
        with self.assertRaises(BenchmarkFailure):
            ledger.reserve(specs[-1])
        with self.assertRaises(FileExistsError):
            Ledger(self.path, {})
        self.assertEqual(len(json.loads(self.path.read_text())["attempts"]), 20)

    def test_reservation_precedes_failure_and_never_retries(self):
        ledger = Ledger(self.path, {})
        calls = []

        def fail(*args, **kwargs):
            saved = json.loads(self.path.read_text())
            self.assertEqual(saved["attempts"][0]["status"], "reserved")
            calls.append(1)
            raise RuntimeError("SECRET service error body")

        self.assertFalse(execute(ledger, {"serial": "unused"}, runner=fail))
        self.assertEqual(len(calls), 1)
        saved = self.path.read_text()
        self.assertNotIn("SECRET", saved)
        self.assertEqual(json.loads(saved)["attempts"][0]["status"], "failed")

    def test_setup_failure_records_no_execute_and_uses_cli_credentials(self):
        ledger = Ledger(self.path, {})
        with patch("benchmark_parallel_reads.MeasuredChild") as process:
            child = process.return_value.__enter__.return_value
            child.wait_marker.side_effect = BenchmarkFailure("child_exited_before_marker")
            self.assertFalse(
                execute(
                    ledger,
                    {"serial": Path("unused.exe")},
                    target=f"{uuid.uuid4()}/{uuid.uuid4()}",
                    queries=workload_queries(),
                )
            )
        sent = "".join(call.args[0] for call in child.send.call_args_list)
        self.assertIn("CHAIN 'cli'", sent)
        self.assertNotIn("CHAIN 'env'", sent)
        self.assertNotIn("EXECUTE bench_query", sent)
        attempt = json.loads(self.path.read_text())["attempts"][0]
        self.assertEqual(attempt["stage"], "setup_before_ready")
        self.assertFalse(attempt["read_started"])
        self.assertEqual(attempt["status"], "failed")
        self.assertEqual(len(ledger.report["attempts"]), 1)

    def test_execute_intent_is_persisted_before_submission_and_failure(self):
        ledger = Ledger(self.path, {})
        before = [
            {"phase": "schema", "metric": "allocation_requests", "value": 0},
            {"phase": "schema", "metric": "stream_requests", "value": 0},
            {"phase": "execution", "metric": "allocation_requests", "value": 0},
        ]
        stdout = (
            'BENCH_EXTENSIONS\n[{"extension_name":"onelake","loaded":true,"install_mode":"STATICALLY_LINKED"}]\n'
            "BENCH_PREPARE\nRun Time (s): real 0.1 user 0.0 sys 0.0\n"
            "BENCH_METRICS_BEFORE\n" + json.dumps(before) + "\nBENCH_READY\n"
        )
        submitted = []

        def send(commands):
            if "EXECUTE bench_query" in commands:
                saved = json.loads(self.path.read_text())["attempts"][0]
                self.assertEqual(saved["stage"], "execute")
                self.assertTrue(saved["read_started"])
                submitted.append(True)

        with patch("benchmark_parallel_reads.MeasuredChild") as process:
            child = process.return_value.__enter__.return_value
            child.started = 0
            child.stdout = stdout
            child.send.side_effect = send
            child.wait_marker.side_effect = [None, BenchmarkFailure("child_timeout")]
            self.assertFalse(
                execute(
                    ledger,
                    {"serial": Path("unused.exe")},
                    target=f"{uuid.uuid4()}/{uuid.uuid4()}",
                    queries=workload_queries(),
                )
            )
        self.assertEqual(submitted, [True])
        attempt = json.loads(self.path.read_text())["attempts"][0]
        self.assertEqual(attempt["stage"], "execute")
        self.assertTrue(attempt["read_started"])
        self.assertEqual(attempt["failure"], "child_timeout")

    def test_full_and_partial_subcaps(self):
        ledger = Ledger(self.path, {})
        for _ in range(6):
            ledger.reserve({"build": "serial", "case": "limit"})
        with self.assertRaises(BenchmarkFailure):
            ledger.reserve({"build": "serial", "case": "limit"})
        for _ in range(14):
            ledger.reserve({"build": "serial", "case": "narrow"})
        with self.assertRaises(BenchmarkFailure):
            ledger.reserve({"build": "serial", "case": "wide"})

    def test_single_stream_pilot_stops_immediately(self):
        ledger = Ledger(self.path, {})

        def run(*args, **kwargs):
            return {
                "result": [
                    {"row_count": 0, "nonnull_price": 0, "min_price": None, "max_price": None, "total_price_2dp": None}
                ],
                "diagnostics": {"allocated_streams": 1, "peak_active_streams": None},
            }

        self.assertFalse(execute(ledger, {"serial": "unused"}, runner=run))
        self.assertEqual(len(ledger.report["attempts"]), 1)
        self.assertEqual(ledger.report["failure"], "pilot_single_stream")

    def test_complete_run_summarizes_three_pairs_not_pilots(self):
        ledger = Ledger(self.path, {})

        def run(shell, case, *, candidate, **kwargs):
            rows = {
                "narrow": [
                    {
                        "row_count": 10,
                        "nonnull_price": 10,
                        "min_price": 1,
                        "max_price": 10,
                        "total_price_2dp": Decimal("55.00"),
                    }
                ],
                "wide": [
                    {
                        "County": "test",
                        "row_count": 10,
                        "nonnull_price": 10,
                        "total_price_2dp": Decimal("55.00"),
                        "postcode_chars": 0,
                        "town_chars": 0,
                        "street_chars": 0,
                    }
                ],
                "limit": [{"row_count": 10, "nonnull_price": 10}],
            }
            return {
                "result": rows[case],
                "diagnostics": {
                    "allocated_streams": 8,
                    "peak_active_streams": 4 if candidate else None,
                },
                "query_timing": {"seconds": 1 if candidate else 2, "cpu_seconds": 1},
                "resources": {
                    "process_peak_working_set_bytes": 1000,
                    "query_sampled_peak_working_set_bytes": 900,
                    "query_sampled_peak_private_bytes": 800,
                },
                "rates": {
                    "consumed_mib_per_second": 10,
                    "delivered_source_rows_per_second": 10,
                    "cpu_seconds_per_million_source_rows": 100000,
                },
            }

        self.assertTrue(execute(ledger, {"serial": "unused", "candidate": "unused"}, runner=run))
        self.assertEqual(len(ledger.report["attempts"]), 20)
        self.assertEqual(ledger.report["summaries"]["narrow"]["wall_seconds"]["serial_over_candidate"], 2)
        self.assertIn("CPU candidate/serial", ledger.report["comparison_table"])
        self.assertEqual(json.loads(self.path.read_text())["attempts"][0]["result"][0]["total_price_2dp"], "55.00")


class ChildLifecycleTest(unittest.TestCase):
    def test_nonfinite_timeout_rejected_before_launch(self):
        for value in (float("nan"), float("inf"), 0):
            with self.assertRaisesRegex(BenchmarkFailure, "invalid_measurement_interval"):
                MeasuredChild(["must-not-launch"], timeout=value)

    def test_drains_verbose_stderr_and_takes_live_final_sample(self):
        script = (
            "import sys; sys.stderr.write('x'*200000); sys.stderr.flush(); "
            "sys.stdout.write('x'*200000+'\\n'); sys.stdout.flush(); "
            "print('BENCH_READY', flush=True); input(); "
            "print('BENCH_DONE', flush=True); input()"
        )
        with MeasuredChild([sys.executable, "-u", "-c", script], timeout=10) as child:
            child.wait_marker("BENCH_READY")
            child.start_measurement()
            child.send("\n")
            child.wait_marker("BENCH_DONE", measure=True)
            child.sample()
            self.assertIsNone(child.process.poll())
            if sys.platform == "win32":
                resources = child.resources()
                self.assertGreater(resources["process_peak_working_set_bytes"], 0)
                self.assertIsNotNone(resources["query_final_alive_sample"])
            child.send("\n")
            child.wait_exit()
        self.assertEqual(child.process.returncode, 0)
        self.assertTrue(all(not thread.is_alive() for thread in child.threads))

    def test_timeout_kills_and_joins_child(self):
        with self.assertRaisesRegex(BenchmarkFailure, "child_timeout"):
            with MeasuredChild([sys.executable, "-u", "-c", "import time; time.sleep(20)"], timeout=0.1) as child:
                child.wait_marker("BENCH_NEVER", measure=True)
        self.assertIsNotNone(child.process.poll())
        self.assertTrue(all(not thread.is_alive() for thread in child.threads))

    def test_early_exit_is_sanitized(self):
        with self.assertRaisesRegex(BenchmarkFailure, "child_exited_before_marker"):
            with MeasuredChild(
                [sys.executable, "-u", "-c", "import sys; sys.stderr.write('TOKEN'); sys.exit(3)"],
                timeout=5,
            ) as child:
                child.wait_marker("BENCH_DONE")
        self.assertEqual(child.process.returncode, 3)

    def test_output_limit_still_drains_and_cleans_up(self):
        script = "import sys,time; sys.stdout.write('x'*(9*1024*1024)); " "sys.stdout.flush(); time.sleep(20)"
        with self.assertRaisesRegex(BenchmarkFailure, "child_output_limit"):
            with MeasuredChild([sys.executable, "-u", "-c", script], timeout=10) as child:
                child.wait_marker("BENCH_NEVER")
        self.assertIsNotNone(child.process.poll())
        self.assertTrue(all(not thread.is_alive() for thread in child.threads))


if __name__ == "__main__":
    unittest.main()
from contextlib import redirect_stderr
import io
