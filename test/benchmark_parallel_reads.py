"""Opt-in fixed-budget serial/candidate benchmark (no automatic retries).

Run --help for usage. The output is also the durable attempted-allocation ledger.
Decimal JSON values are persisted as exact decimal strings, never binary floats.
Supply the approved --workspace UUID and --item UUID; schema/table default to
dbo/PricePaid. Target approval is external to this reusable harness.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import time
import uuid

from benchmark_process import BenchmarkFailure, MeasuredChild
from benchmark_provenance import checkout_provenance
from benchmark_reads import identifier

BASELINE_SHA256 = "7f67797049e02f0d88cef79c4d4d92dd71f97e7c5ab696ba6a14a34e7e05c7b3"
WORKLOADS = ("narrow", "wide", "limit")


def identifier_argument(value):
    if not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise argparse.ArgumentTypeError("Identifiers must be nonempty and contain no control characters")
    return value


def workload_queries(schema="dbo", table="PricePaid"):
    table_path = f"benchmark.{identifier(identifier_argument(schema))}.{identifier(identifier_argument(table))}"
    return {
        "narrow": f"""SELECT COUNT(*) AS row_count, COUNT(Price) AS nonnull_price,
MIN(Price) AS min_price, MAX(Price) AS max_price,
SUM(CAST(Price AS DECIMAL(38, 2))) AS total_price_2dp
FROM {table_path}""",
        "wide": f"""SELECT County, COUNT(*) AS row_count, COUNT(Price) AS nonnull_price,
SUM(CAST(Price AS DECIMAL(38, 2))) AS total_price_2dp,
SUM(length(coalesce(Postcode, ''))) AS postcode_chars,
SUM(length(coalesce(TownCity, ''))) AS town_chars,
SUM(length(coalesce(Street, ''))) AS street_chars
FROM {table_path} GROUP BY County ORDER BY County NULLS FIRST""",
        "limit": f"""SELECT COUNT(*) AS row_count, COUNT(Price) AS nonnull_price
FROM (SELECT Price FROM {table_path} LIMIT 10)""",
    }


SETTINGS = {
    "threads": 4,
    "read_mode": "TABLE_API",
    "read_cache": "OFF",
    "enable_external_file_cache": False,
    "memory_limit": "4GB",
    "read_metrics": True,
    "prepared": True,
    "profile": False,
    "credential_chain": "cli",
}
METRIC_NAMES = set(
    (
        "token_calls token_ns metadata_requests metadata_bytes metadata_ns bind_ns "
        "reader_opens reader_open_ns allocation_requests allocation_bytes allocation_ns "
        "stream_requests stream_open_ns stream_bytes stream_read_ns ipc_processing_ns "
        "batches rows first_batches first_batch_ns completions errors arrow_fetch_ns "
        "arrow_operator_ns output_rows allocated_streams peak_active_streams"
    ).split()
)
TIMER = re.compile(r"Run Time \(s\): real (?P<wall>[0-9.]+) user (?P<user>[0-9.]+) sys (?P<sys>[0-9.]+)")


def decode_section(stdout, marker):
    normalized = stdout.replace("\r\n", "\n")
    try:
        body = normalized.split(marker + "\n", 1)[1].lstrip()
        return json.JSONDecoder(parse_float=Decimal).raw_decode(body)[0]
    except (IndexError, ValueError):
        raise BenchmarkFailure("invalid_json_section") from None


def parse_timer(stdout, marker):
    body = stdout.replace("\r\n", "\n").split(marker + "\n", 1)
    if len(body) != 2:
        raise BenchmarkFailure("missing_timer")
    match = TIMER.search(body[1].split("BENCH_", 1)[0])
    if not match:
        raise BenchmarkFailure("missing_timer")
    wall, user, system = (float(match[name]) for name in ("wall", "user", "sys"))
    return {
        "seconds": wall,
        "user_seconds": user,
        "system_seconds": system,
        "cpu_seconds": user + system,
        "average_cpu_cores": (user + system) / wall if wall else None,
    }


def metrics_section(stdout, marker):
    rows = decode_section(stdout, marker)
    if not isinstance(rows, list):
        raise BenchmarkFailure("invalid_metrics")
    result = {}
    for row in rows:
        if not isinstance(row, dict):
            raise BenchmarkFailure("invalid_metrics")
        phase, metric, value = (row.get(key) for key in ("phase", "metric", "value"))
        if phase not in ("shared", "schema", "execution") or metric not in METRIC_NAMES:
            raise BenchmarkFailure("invalid_metrics")
        if type(value) is not int or value < 0:
            raise BenchmarkFailure("invalid_metrics")
        key = f"{phase}.{metric}"
        if key in result:
            raise BenchmarkFailure("duplicate_metric")
        result[key] = value
    return result


def extension_provenance(stdout):
    rows = decode_section(stdout, "BENCH_EXTENSIONS")
    if not isinstance(rows, list) or not any(
        row == {"extension_name": "onelake", "loaded": True, "install_mode": "STATICALLY_LINKED"} for row in rows
    ):
        raise BenchmarkFailure("onelake_not_statically_linked")
    # Persist only selected, non-sensitive extension metadata.
    return [
        {"extension_name": row["extension_name"], "loaded": row["loaded"], "install_mode": row["install_mode"]}
        for row in rows
        if row.get("extension_name") in ("onelake", "azure")
        and type(row.get("loaded")) is bool
        and row.get("install_mode") in ("STATICALLY_LINKED", "NOT_INSTALLED", "CUSTOM_PATH", "REPOSITORY")
    ]


def metric_delta(after, before):
    """Peak is a snapshot high-water mark, not a cumulative delta."""
    result = {}
    for key, value in after.items():
        if key.endswith(".peak_active_streams"):
            result[key] = value
        else:
            result[key] = value - before.get(key, 0)
            if result[key] < 0:
                raise BenchmarkFailure("decreasing_counter")
    return result


def validate_metrics(before, after, *, candidate, complete):
    for snapshot in (before, after):
        for name in ("allocation_requests", "stream_requests"):
            if snapshot.get("schema." + name) != 0:
                raise BenchmarkFailure("schema_read_allocation")
    if before.get("execution.allocation_requests") != 0:
        raise BenchmarkFailure("prepare_read_allocation")
    delta = metric_delta(after, before)
    if delta.get("execution.allocation_requests") != 1:
        raise BenchmarkFailure("execution_allocation_count")
    if delta.get("execution.errors") != 0:
        raise BenchmarkFailure("reader_error")
    if complete and delta.get("execution.completions") != 1:
        raise BenchmarkFailure("incomplete_full_scan")
    for name in ("stream_requests", "stream_bytes", "rows", "batches"):
        if "execution." + name not in delta:
            raise BenchmarkFailure("missing_execution_metrics")
    allocated = delta.get("execution.allocated_streams")
    peak = delta.get("execution.peak_active_streams")
    if candidate:
        if allocated is None or peak is None:
            raise BenchmarkFailure("missing_candidate_diagnostics")
        if not 1 <= peak <= min(4, allocated):
            raise BenchmarkFailure("invalid_candidate_concurrency")
        if delta["execution.stream_requests"] > allocated:
            raise BenchmarkFailure("duplicate_stream_request")
        if complete and delta["execution.stream_requests"] != allocated:
            raise BenchmarkFailure("incomplete_stream_requests")
    else:
        allocated = delta["execution.stream_requests"] if complete else None
    return delta, {
        "allocated_streams": allocated,
        "allocated_streams_source": (
            "diagnostic" if candidate else ("inferred_from_complete_scan_gets" if complete else "unavailable")
        ),
        "peak_active_streams": peak if candidate else None,
    }


def validate_result(case, rows):
    fields = {
        "narrow": {"row_count", "nonnull_price", "min_price", "max_price", "total_price_2dp"},
        "wide": {
            "County",
            "row_count",
            "nonnull_price",
            "total_price_2dp",
            "postcode_chars",
            "town_chars",
            "street_chars",
        },
        "limit": {"row_count", "nonnull_price"},
    }[case]
    if not isinstance(rows, list) or (case != "wide" and len(rows) != 1):
        raise BenchmarkFailure("invalid_aggregate")
    for row in rows:
        if not isinstance(row, dict) or set(row) != fields:
            raise BenchmarkFailure("invalid_aggregate")
        n, nonnull = row["row_count"], row["nonnull_price"]
        if type(n) is not int or type(nonnull) is not int or not 0 <= nonnull <= n:
            raise BenchmarkFailure("invalid_aggregate_counts")
        if case == "limit" and n > 10:
            raise BenchmarkFailure("invalid_limit_bounds")
        for key, value in row.items():
            if key == "County":
                if value is not None and not isinstance(value, str):
                    raise BenchmarkFailure("invalid_aggregate")
            elif value is not None and (
                not isinstance(value, (int, Decimal))
                or isinstance(value, bool)
                or (isinstance(value, Decimal) and not value.is_finite())
            ):
                raise BenchmarkFailure("invalid_aggregate")
    return rows


def check_equivalent(case, rows, references):
    validate_result(case, rows)
    if case == "limit":
        return  # Unordered samples are constrained by bounds, not equality.
    if case in references and references[case] != rows:
        raise BenchmarkFailure("inconsistent_source_aggregates")
    references.setdefault(case, rows)
    if "wide" in references and "narrow" in references:
        narrow = references["narrow"][0]
        wide = references["wide"]
        for key in ("row_count", "nonnull_price", "total_price_2dp"):
            values = [row[key] for row in wide if row[key] is not None]
            with localcontext() as context:
                context.prec = 100
                total = sum(values) if values else (0 if key != "total_price_2dp" else None)
            if total != narrow[key]:
                raise BenchmarkFailure("inconsistent_source_workloads")


def schedule():
    yield {"build": "serial", "case": "narrow", "pilot": True, "pair": 0}
    yield {"build": "candidate", "case": "narrow", "pilot": True, "pair": 0}
    for case in WORKLOADS:
        for pair in range(1, 4):
            order = ("serial", "candidate") if pair % 2 else ("candidate", "serial")
            for build in order:
                yield {"build": build, "case": case, "pilot": False, "pair": pair}


def exact_json_value(value):
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError("Unsupported report value")


def persist(path, report):
    pending = path.with_name(path.name + ".pending")
    with pending.open("x", encoding="utf-8") as file:
        json.dump(report, file, indent=2, default=exact_json_value)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    os.replace(pending, path)


class Ledger:
    def __init__(self, path, report):
        self.path, self.report = path, report
        if path.with_name(path.name + ".pending").exists():
            raise BenchmarkFailure("existing_pending_report")
        # Exclusive reservation prevents overwrite and makes every invocation non-resumable.
        with path.open("x", encoding="utf-8"):
            pass
        report["attempts"] = []
        persist(path, report)

    def reserve(self, spec):
        attempts = self.report["attempts"]
        full = sum(item["case"] != "limit" for item in attempts)
        partial = len(attempts) - full
        if (
            len(attempts) >= 20
            or (spec["case"] != "limit" and full >= 14)
            or (spec["case"] == "limit" and partial >= 6)
        ):
            raise BenchmarkFailure("allocation_budget_exhausted")
        attempt = {
            **spec,
            "attempt": len(attempts) + 1,
            "status": "reserved",
            "stage": "reserved",
            "read_started": False,
            "reserved_at_utc": datetime.now(timezone.utc).isoformat(),
            "provenance": self.report.get("builds", {}).get(spec["build"]),
        }
        attempts.append(attempt)
        self.save()  # Must complete before Popen; failed launches consume the reservation.
        return attempt

    def save(self):
        persist(self.path, self.report)


@contextmanager
def isolated_extensions():
    directory = Path(__file__).resolve().parent / (".benchmark-extensions-" + uuid.uuid4().hex)
    directory.mkdir()
    try:
        yield directory
    finally:
        # Do not silently delete unexpected files downloaded by an extension.
        if not any(directory.iterdir()):
            directory.rmdir()


def run_query(shell, case, *, candidate, target, queries, timeout=900, interval=0.01, on_progress=None):
    state = {"stage": "target_validation", "read_started": False}

    def progress(stage, *, read_started=None):
        state["stage"] = stage
        if read_started is not None:
            state["read_started"] = read_started
        if on_progress:
            on_progress(**state)

    try:
        progress("target_validation")
        try:
            workspace, item = target.split("/")
            target = f"{uuid.UUID(workspace)}/{uuid.UUID(item)}"
        except (AttributeError, ValueError):
            raise BenchmarkFailure("invalid_target") from None
        progress("extension_directory")
        with isolated_extensions() as extension_directory:
            return _run_query(
                shell,
                case,
                candidate=candidate,
                timeout=timeout,
                interval=interval,
                extension_directory=extension_directory,
                target=target,
                queries=queries,
                progress=progress,
            )
    except (Exception, KeyboardInterrupt) as error:
        error.benchmark_stage = state["stage"]
        error.read_started = state["read_started"]
        raise


def _run_query(shell, case, *, candidate, timeout, interval, extension_directory, target, queries, progress):
    root = Path(__file__).resolve().parents[1]
    setup = "\n".join(
        [
            ".bail on",
            ".mode json",
            ".timer on",
            "SET threads=4;",
            "SET memory_limit='4GB';",
            "SET enable_external_file_cache=false;",
            # Azure is a shared credential dependency, not the OneLake reader under test.
            # Load the installed dependency before hiding the extension cache.
            "LOAD azure;",
            "SET extension_directory='" + str(extension_directory).replace("'", "''") + "';",
            "LOAD onelake;",
            "SET onelake_enable_read_metrics=true;",
            ".print BENCH_EXTENSIONS",
            "SELECT extension_name, loaded, install_mode FROM duckdb_extensions() "
            "WHERE extension_name IN ('onelake', 'azure') ORDER BY extension_name;",
            "CREATE SECRET bench (TYPE ONELAKE, PROVIDER credential_chain, CHAIN 'cli');",
            f"ATTACH '{target}' AS benchmark " "(TYPE ONELAKE, READ_MODE 'TABLE_API', READ_CACHE 'OFF', READ_ONLY);",
            ".print BENCH_PREPARE",
            "PREPARE bench_query AS " + queries[case] + ";",
            ".print BENCH_METRICS_BEFORE",
            "SELECT * FROM onelake_read_metrics();",
            ".print BENCH_READY",
            "",
        ]
    )
    progress("child_launch")
    with MeasuredChild(
        [str(shell.resolve()), "-init", os.devnull, "-unsigned", "-batch"],
        cwd=root,
        timeout=timeout,
        interval=interval,
    ) as child:
        progress("setup_before_ready")
        child.send(setup)
        child.wait_marker("BENCH_READY")
        progress("prepare_validation")
        setup_seconds = time.perf_counter() - child.started
        extensions = extension_provenance(child.stdout)
        before = metrics_section(child.stdout, "BENCH_METRICS_BEFORE")
        # Reject any unexpected binding allocation before authorizing EXECUTE.
        if any(
            before.get(key) != 0
            for key in (
                "schema.allocation_requests",
                "schema.stream_requests",
                "execution.allocation_requests",
            )
        ):
            raise BenchmarkFailure("prepare_read_allocation")
        if candidate and any(
            before.get("execution." + name) != 0
            for name in (
                "allocated_streams",
                "peak_active_streams",
            )
        ):
            raise BenchmarkFailure("missing_or_nonzero_prepare_diagnostics")
        prepare = parse_timer(child.stdout, "BENCH_PREPARE")
        child.start_measurement()
        # Persist intent before submitting SQL: a failed write may have sent partial input.
        progress("execute", read_started=True)
        child.send(".print BENCH_QUERY\nEXECUTE bench_query;\n.print BENCH_QUERY_DONE\n")
        child.wait_marker("BENCH_QUERY_DONE", measure=True)
        progress("query_result")
        child.sample()
        resources = child.resources()
        query = parse_timer(child.stdout, "BENCH_QUERY")
        rows = validate_result(case, decode_section(child.stdout, "BENCH_QUERY"))
        cleanup_started = time.perf_counter()
        progress("final_metrics")
        child.send(".print BENCH_METRICS_AFTER\nSELECT * FROM onelake_read_metrics();\n" ".print BENCH_FINAL\n")
        child.wait_marker("BENCH_FINAL")
        if child.process.poll() is not None:
            raise BenchmarkFailure("child_exited_before_final_snapshot")
        final_alive = child.counters.sample()
        if child.process.poll() is not None:
            raise BenchmarkFailure("child_exited_during_final_snapshot")
        if final_alive is not None:
            resources["process_peak_working_set_bytes"] = max(
                resources["process_peak_working_set_bytes"] or 0,
                final_alive["process_peak_working_set_bytes"],
            )
        after = metrics_section(child.stdout, "BENCH_METRICS_AFTER")
        delta, diagnostics = validate_metrics(before, after, candidate=candidate, complete=case != "limit")
        progress("process_exit")
        child.send(".quit\n")
        child.wait_exit()
        elapsed = time.perf_counter() - child.started
        cleanup = time.perf_counter() - cleanup_started
    seconds, cpu = query["seconds"], query["cpu_seconds"]
    source_rows = delta["execution.rows"]
    return {
        "settings": {
            **SETTINGS,
            "startup_init": os.devnull,
            "extension_directory": str(extension_directory),
            "extension_directory_isolated": True,
        },
        "query": queries[case],
        "result": rows,
        "extensions": extensions,
        "stage": "client_completed",
        "read_started": True,
        "query_timing": query,
        "prepare_timing": prepare,
        "process_seconds": elapsed,
        "setup_seconds": setup_seconds,
        "post_query_metrics_and_exit_seconds": cleanup,
        "normal_child_exit": True,
        "resources": resources,
        "final_alive_process_counters": final_alive,
        "metrics_before": before,
        "metrics_after": after,
        "metrics": delta,
        "diagnostics": diagnostics,
        "rates": {
            "consumed_mib_per_second": delta["execution.stream_bytes"] / 1048576 / seconds if seconds else None,
            "delivered_source_rows_per_second": source_rows / seconds if seconds else None,
            "cpu_seconds_per_million_source_rows": cpu * 1_000_000 / source_rows if source_rows else None,
            "limit_delivered_rows_per_sql_row": (
                source_rows / rows[0]["row_count"] if case == "limit" and rows[0]["row_count"] else None
            ),
        },
    }


def summarize(attempts):
    result = {}
    selectors = {
        "wall_seconds": lambda run: run["query_timing"]["seconds"],
        "cpu_seconds": lambda run: run["query_timing"]["cpu_seconds"],
        "process_peak_working_set_bytes": lambda run: run["resources"]["process_peak_working_set_bytes"],
        "query_sampled_peak_working_set_bytes": lambda run: run["resources"]["query_sampled_peak_working_set_bytes"],
        "query_sampled_peak_private_bytes": lambda run: run["resources"]["query_sampled_peak_private_bytes"],
        "consumed_mib_per_second": lambda run: run["rates"]["consumed_mib_per_second"],
        "source_rows_per_second": lambda run: run["rates"]["delivered_source_rows_per_second"],
        "cpu_seconds_per_million_source_rows": lambda run: run["rates"]["cpu_seconds_per_million_source_rows"],
    }
    for case in WORKLOADS:
        groups = {
            build: [
                a
                for a in attempts
                if not a["pilot"] and a["case"] == case and a["build"] == build and a["status"] == "ok"
            ]
            for build in ("serial", "candidate")
        }
        if any(len(group) != 3 for group in groups.values()):
            continue
        result[case] = {}
        for name, select in selectors.items():
            values = {build: [select(run) for run in group] for build, group in groups.items()}
            if any(value is None for group in values.values() for value in group):
                result[case][name] = None
                continue
            stats = {
                build: {"median": statistics.median(group), "min": min(group), "max": max(group)}
                for build, group in values.items()
            }
            serial, candidate = (stats[build]["median"] for build in ("serial", "candidate"))
            stats.update(
                {
                    "candidate_minus_serial": candidate - serial,
                    "candidate_over_serial": candidate / serial if serial else None,
                    "serial_over_candidate": serial / candidate if candidate else None,
                }
            )
            result[case][name] = stats
    return result


def execute(ledger, shells, *, target=None, queries=None, runner=run_query, timeout=900, interval=0.01):
    references = {}
    try:
        for spec in schedule():
            attempt = ledger.reserve(spec)

            def on_progress(stage, read_started):
                attempt.update(stage=stage, read_started=read_started)
                ledger.save()

            try:
                run = runner(
                    shells[spec["build"]],
                    spec["case"],
                    candidate=spec["build"] == "candidate",
                    timeout=timeout,
                    interval=interval,
                    target=target,
                    queries=queries,
                    on_progress=on_progress,
                )
                attempt.update(run)
                attempt["stage"] = "result_validation"
                check_equivalent(spec["case"], run["result"], references)
                if spec["pilot"]:
                    if run["diagnostics"]["allocated_streams"] <= 1:
                        raise BenchmarkFailure("pilot_single_stream")
                    if spec["build"] == "candidate" and run["diagnostics"]["peak_active_streams"] <= 1:
                        raise BenchmarkFailure("pilot_no_observed_concurrency")
                attempt["status"] = "ok"
                attempt["stage"] = "complete"
            except (Exception, KeyboardInterrupt) as error:
                attempt["status"] = "failed"
                attempt["stage"] = getattr(error, "benchmark_stage", attempt["stage"])
                attempt["read_started"] = getattr(error, "read_started", attempt["read_started"])
                attempt["failure"] = (
                    str(error)
                    if isinstance(error, BenchmarkFailure)
                    else "interrupted" if isinstance(error, KeyboardInterrupt) else "client_failure"
                )
                raise
            finally:
                ledger.save()
        ledger.report["status"] = "complete"
    except (Exception, KeyboardInterrupt) as error:
        ledger.report["status"] = "inconclusive"
        ledger.report["failure"] = (
            str(error)
            if isinstance(error, BenchmarkFailure)
            else "interrupted" if isinstance(error, KeyboardInterrupt) else "client_failure"
        )
    finally:
        ledger.report["summaries"] = summarize(ledger.report["attempts"])
        ledger.report["comparison_table"] = comparison_table(ledger.report["summaries"])
        ledger.save()
    return ledger.report["status"] == "complete"


def comparison_table(summaries):
    lines = [
        "workload | serial median [min,max] s | candidate median [min,max] s | "
        "wall speedup | CPU candidate/serial | process peak candidate/serial"
    ]
    for case, summary in summaries.items():
        wall = summary["wall_seconds"]
        cells = [
            f"{wall[b]['median']:.4f} [{wall[b]['min']:.4f},{wall[b]['max']:.4f}]" for b in ("serial", "candidate")
        ]
        ratios = [wall["serial_over_candidate"], summary["cpu_seconds"]["candidate_over_serial"]]
        memory = summary["process_peak_working_set_bytes"]
        ratios.append(memory["candidate_over_serial"] if memory else None)
        cells += [f"{value:.3f}" if value is not None else "unavailable" for value in ratios]
        lines.append(f"{case} | " + " | ".join(cells))
    return "\n".join(lines)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approved-live-read", action="store_true", required=True)
    parser.add_argument("--workspace", type=uuid.UUID, required=True)
    parser.add_argument("--item", type=uuid.UUID, required=True)
    parser.add_argument("--schema", type=identifier_argument, default="dbo")
    parser.add_argument("--table", type=identifier_argument, default="PricePaid")
    parser.add_argument("--serial-shell", type=Path, required=True)
    parser.add_argument("--candidate-shell", type=Path, default=Path("build") / "release" / "duckdb.exe")
    parser.add_argument(
        "--candidate-source",
        required=True,
        help="Caller-supplied build revision/dirty label; checkout diff hashes are recorded separately",
    )
    parser.add_argument(
        "--host-conditions", required=True, help="Observed idle/load conditions; do not include secrets"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--timeout",
        type=float,
        default=900,
        help="Whole child wall limit including setup (default 900s); does not extend the reader's HTTP timeout",
    )
    parser.add_argument("--sample-interval", type=float, default=0.01)
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if not all(math.isfinite(value) and value > 0 for value in (args.timeout, args.sample_interval)):
        parser.error("Timeout and sampling interval must be positive")
    shells = {"serial": args.serial_shell, "candidate": args.candidate_shell}
    hashes = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in shells.items()}
    if hashes["serial"] != BASELINE_SHA256:
        parser.error("Serial shell does not match the approved archived SHA256")
    if hashes["candidate"] == hashes["serial"]:
        parser.error("Candidate shell is identical to the serial baseline")
    root = Path(__file__).resolve().parents[1]
    provenance = checkout_provenance(root)
    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "running",
        "target": {
            "workspace": str(args.workspace),
            "item": str(args.item),
            "schema": args.schema,
            "table": args.table,
        },
        "settings": SETTINGS,
        "source_commit": provenance["head"],
        "source_dirty": provenance["dirty"],
        "checkout_provenance": provenance,
        "builds": {
            name: {
                "shell": str(path.resolve()),
                "sha256": hashes[name],
                "source": "2c7de78" if name == "serial" else args.candidate_source,
            }
            for name, path in shells.items()
        },
        "host": {"os": platform.platform(), "cpu_count": os.cpu_count(), "conditions": args.host_conditions},
        "budget": {"attempted_allocations": 20, "full_scans": 14, "partial_reads": 6},
        "client_timeout_seconds": args.timeout,
        "transport_timeout_note": (
            "Outer per-child timeout includes startup, PREPARE, EXECUTE and metrics. "
            "It does not extend the reader's existing 15s connect/60s HTTP request timeout "
            "or one-hour reader deadline; timeouts consume attempts and are not retried."
        ),
        "method": [
            "Fresh client per query; PREPARE before timed EXECUTE; EXECUTE rebinding included.",
            "Two excluded pilots, then three alternating-order pairs per workload. No retries/resumption.",
            "read_started means EXECUTE submission began, not proof of an allocation. Stage/read intent is durably saved before SQL submission; setup failures still consume reservations.",
            "No cache flush or common source snapshot guarantee; mismatches are inconclusive.",
            "Decimal JSON numbers stored as exact strings; sums round each source DOUBLE to two decimals.",
            "CPU is shell GetProcessTimes user+system, including worker threads, not machine utilization.",
            "Process peak includes startup/setup; query memory peaks sampled, not exact maxima.",
            "Final counters sampled with client alive; normal process exit leaves no client workers.",
            "Consumed body bytes are not wire/TLS traffic; summed worker durations overlap.",
            "LIMIT counts checked only for bounds; results need not be equal across arbitrary samples.",
            "Reader shutdown is included in query timing; post-query time includes metrics and process exit.",
            "Baseline new diagnostics unavailable; complete baseline GET count infers allocation streams.",
            "Azure credential dependency loaded before isolating extension_directory; onelake must report STATICALLY_LINKED.",
        ],
    }
    try:
        ledger = Ledger(args.output, report)
    except (FileExistsError, BenchmarkFailure):
        parser.error("Refusing to overwrite an existing report or pending ledger")
    success = execute(
        ledger,
        shells,
        target=f"{args.workspace}/{args.item}",
        queries=workload_queries(args.schema, args.table),
        timeout=args.timeout,
        interval=args.sample_interval,
    )
    print(report["comparison_table"])
    print("complete" if success else "inconclusive; sanitized partial report saved")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
