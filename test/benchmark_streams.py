"""Opt-in deterministic reader benchmark; server CPU is outside the measured client.

The supplied Rust unit-test executable must contain the ignored fixture/client
tests. This command does not compile Rust, contact OneLake, or retry failed runs.
One warm-up and five measured runs are performed for each scenario/cap.
The client must implement ONELAKE_BENCH_WAIT_FOR_EXIT=1: flush BENCH_RESULT,
then await a stdin line while this harness takes the final live resource sample.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import time
from urllib.request import urlopen

from benchmark_parallel_reads import decode_section, persist
from benchmark_process import BenchmarkFailure, MeasuredChild
from benchmark_provenance import checkout_provenance
from benchmark_stream_server import SCENARIOS, load_manifest


def parse_result(stdout):
    matches = re.findall(r"BENCH_RESULT (\{[^\r\n]*\})", stdout)
    if len(matches) != 1:
        raise BenchmarkFailure("missing_or_duplicate_client_result")
    try:
        result = json.loads(matches[0])
    except ValueError:
        raise BenchmarkFailure("invalid_client_result") from None
    if not isinstance(result, dict) or set(result) != {"seconds", "rows", "checksum", "metrics", "shutdown_seconds"}:
        raise BenchmarkFailure("invalid_client_result")
    for key in ("seconds", "shutdown_seconds"):
        if type(result[key]) not in (int, float) or not 0 <= result[key] < float("inf"):
            raise BenchmarkFailure("invalid_client_timing")
    if type(result["rows"]) is not int or result["rows"] < 0:
        raise BenchmarkFailure("invalid_client_rows")
    if type(result["checksum"]) is not int or result["checksum"] < 0:
        raise BenchmarkFailure("invalid_client_checksum")
    if not isinstance(result["metrics"], dict) or any(
        not re.fullmatch(r"[a-z_]+", key) or type(value) not in (int, float) or not 0 <= value < float("inf")
        for key, value in result["metrics"].items()
    ):
        raise BenchmarkFailure("invalid_client_metrics")
    return result


def expected_checksum(rows):
    multiple_count = (rows - 1) // 7
    return rows * (rows - 1) // 2 - 7 * multiple_count * (multiple_count + 1) // 2


def check_result(result, scenario, references):
    expected_rows = 4096 if scenario == "early" else 32 * 4096 * (1 if scenario == "one" else 8)
    if result["rows"] != expected_rows:
        raise BenchmarkFailure("fixture_row_count_mismatch")
    if scenario != "early":
        if result["checksum"] != expected_checksum(expected_rows):
            raise BenchmarkFailure("known_fixture_checksum_mismatch")
        actual = (result["rows"], result["checksum"])
        if scenario in references and actual != references[scenario]:
            raise BenchmarkFailure("fixture_checksum_mismatch")
        references.setdefault(scenario, actual)


def run_client(executable, url, scenario, cap, timeout, interval):
    env = {
        **os.environ,
        "ONELAKE_BENCH_URL": url,
        "ONELAKE_BENCH_CAP": str(cap),
        "ONELAKE_BENCH_SCENARIO": scenario,
        "ONELAKE_BENCH_WAIT_FOR_EXIT": "1",
    }
    with MeasuredChild(
        [str(executable.resolve()), "--exact", "tests::benchmark_client", "--ignored", "--nocapture"],
        env=env,
        timeout=timeout,
        interval=interval,
    ) as child:
        child.start_measurement()
        child.wait_output_line("BENCH_RESULT ", measure=True)
        result = parse_result(child.stdout)
        if child.process.poll() is not None:
            raise BenchmarkFailure("client_exit_handshake_missing")
        final_alive = child.sample()
        if child.process.poll() is not None:
            raise BenchmarkFailure("client_exited_during_final_snapshot")
        result["final_alive_process_counters"] = final_alive
        result["resources"] = child.resources()
        child.send("\n")
        child.wait_exit()
        final = child.counters.sample()
        result["final_process_counters_after_exit"] = final
        if final is not None:
            result["resources"]["process_peak_working_set_bytes"] = max(
                result["resources"]["process_peak_working_set_bytes"] or 0,
                final["process_peak_working_set_bytes"],
            )
        result["resources"]["measurement_scope"] = (
            "whole test process including startup, not query-only; "
            "final counters sampled after BENCH_RESULT while child waits on stdin; "
            "OS high-water also checked after normal exit"
        )
        result["process_cpu_seconds"] = child.counters.cpu_seconds()
        result["process_seconds"] = time.perf_counter() - child.started
        result["normal_child_exit"] = True
    return result


def summaries(runs):
    summary = {}
    for scenario in SCENARIOS:
        selected = [run for run in runs if run["scenario"] == scenario and not run["warmup"] and run["status"] == "ok"]
        if not selected:
            continue
        summary[scenario] = {}
        for cap in (1, 2, 4, 8):
            group = [run for run in selected if run["cap"] == cap]
            if len(group) != 5:
                continue
            summary[scenario][str(cap)] = {}
            for name, select in {
                "seconds": lambda run: run["seconds"],
                "process_cpu_seconds": lambda run: run["process_cpu_seconds"],
                "process_peak_working_set_bytes": lambda run: run["resources"]["process_peak_working_set_bytes"],
                "shutdown_seconds": lambda run: run["shutdown_seconds"],
            }.items():
                values = [select(run) for run in group]
                summary[scenario][str(cap)][name] = (
                    {"median": statistics.median(values), "min": min(values), "max": max(values)}
                    if all(value is not None for value in values)
                    else None
                )
        serial = summary[scenario].get("1")
        for stats in summary[scenario].values():
            for name, value in stats.items():
                if value is not None and serial and serial[name] is not None:
                    reference = serial[name]["median"]
                    value["over_cap1"] = value["median"] / reference if reference else None
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-benchmark", action="store_true", required=True)
    parser.add_argument("--test-exe", type=Path, required=True)
    parser.add_argument("--fixture-dir", type=Path, required=True, help="New directory for generated fixtures")
    parser.add_argument("--source", required=True, help="Source revision used to compile the test executable")
    parser.add_argument("--host-conditions", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenarios", choices=SCENARIOS, nargs="+", default=list(SCENARIOS))
    parser.add_argument("--caps", choices=(1, 2, 4, 8), type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--sample-interval", type=float, default=0.005)
    args = parser.parse_args()
    if not all(math.isfinite(value) and value > 0 for value in (args.timeout, args.sample_interval)):
        parser.error("Timeout and sample interval must be positive")
    if args.output.exists() or args.output.with_name(args.output.name + ".pending").exists():
        parser.error("Refusing to overwrite an existing report")
    if args.fixture_dir.exists():
        parser.error("Refusing to overwrite existing fixtures")
    executable_hash = hashlib.sha256(args.test_exe.read_bytes()).hexdigest()
    provenance = checkout_provenance(Path(__file__).resolve().parents[1])
    args.fixture_dir.mkdir()
    with args.output.open("x", encoding="utf-8"):
        pass
    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "running",
        "source": args.source,
        "test_exe_sha256": executable_hash,
        "checkout_provenance": provenance,
        "host": {"os": platform.platform(), "cpu_count": os.cpu_count(), "conditions": args.host_conditions},
        "fixture": {"streams": 8, "batches_per_stream": 32, "rows_per_batch": 4096},
        "warmups_per_configuration": 1,
        "measured_per_configuration": 5,
        "method": [
            "Separate loopback HTTP server process; server CPU/memory excluded.",
            "Balanced/early/one delay 2ms per manifest chunk; skew stream0 10ms, others zero; fast/stalled zero.",
            "Process CPU GetProcessTimes includes Rust test harness startup and all reader worker threads.",
            "Process peak includes Rust test harness startup; sampled memory may miss short-lived peaks.",
            "ONELAKE_BENCH_WAIT_FOR_EXIT=1 holds client after flushed BENCH_RESULT for final live sampling; newline releases it.",
            "Rust client checks known fixture checksum; Python cross-checks full checksums across caps/repeats.",
            "Stalled consumer pauses 100ms after first batch; early consumer stops after first batch.",
            "No timing correctness thresholds or p95 estimates; no retries; normal exit joins client workers.",
        ],
        "runs": [],
    }
    persist(args.output, report)
    references = {}
    try:
        env = {**os.environ, "ONELAKE_BENCH_FIXTURE_DIR": str(args.fixture_dir.resolve())}
        generated = subprocess.run(
            [str(args.test_exe.resolve()), "--exact", "tests::benchmark_fixture", "--ignored", "--nocapture"],
            env=env,
            capture_output=True,
            timeout=args.timeout,
        )
        if generated.returncode:
            raise BenchmarkFailure("fixture_generation_failed")
        streams = load_manifest(args.fixture_dir)
        report["fixture_sha256"] = {
            stream_id: hashlib.sha256(entry["path"].read_bytes()).hexdigest() for stream_id, entry in streams.items()
        }
        server_script = Path(__file__).with_name("benchmark_stream_server.py")
        for scenario in dict.fromkeys(args.scenarios):
            with MeasuredChild(
                [
                    sys.executable,
                    "-u",
                    str(server_script),
                    "--fixtures",
                    str(args.fixture_dir.resolve()),
                    "--scenario",
                    scenario,
                ],
                timeout=args.timeout,
            ) as server:
                server.wait_marker("BENCH_READY")
                url = decode_section(server.stdout, "BENCH_SERVER")["url"]
                with urlopen(url + "/health", timeout=5) as response:
                    if response.status != 200:
                        raise BenchmarkFailure("fixture_server_not_ready")
                for cap in dict.fromkeys(args.caps):
                    for iteration in range(6):
                        run = {
                            "scenario": scenario,
                            "cap": cap,
                            "warmup": iteration == 0,
                            "iteration": iteration,
                            "status": "reserved",
                        }
                        report["runs"].append(run)
                        persist(args.output, report)
                        try:
                            run.update(
                                run_client(args.test_exe, url, scenario, cap, args.timeout, args.sample_interval)
                            )
                            check_result(run, scenario, references)
                            run["status"] = "ok"
                        except (Exception, KeyboardInterrupt):
                            run["status"] = "failed"
                            raise
                        finally:
                            persist(args.output, report)
        report["status"] = "complete"
    except (Exception, KeyboardInterrupt) as error:
        report["status"] = "failed"
        report["failure"] = str(error) if isinstance(error, BenchmarkFailure) else "benchmark_client_failure"
    finally:
        report["summaries"] = summaries(report["runs"])
        persist(args.output, report)
    print("scenario | cap | wall median [min,max] s | cap1/current speedup")
    for scenario, caps in report["summaries"].items():
        for cap, stats in caps.items():
            wall = stats["seconds"]
            ratio = wall.get("over_cap1")
            speedup = f"{1 / ratio:.3f}" if ratio else "unavailable"
            print(f"{scenario} | {cap} | {wall['median']:.4f} " f"[{wall['min']:.4f},{wall['max']:.4f}] | {speedup}")
    return 0 if report["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
