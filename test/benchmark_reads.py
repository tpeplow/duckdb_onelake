"""Opt-in, read-only native-shell benchmark. Credentials stay in the environment."""

import argparse
import hashlib
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import time
import uuid


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def parse_runs(stdout):
    runs = []
    for match in re.finditer(
        r"BENCH_QUERY_(\d+)\r?\n(.*?)(?=BENCH_QUERY_|BENCH_DONE|\Z)",
        stdout,
        re.S,
    ):
        body = match.group(2).strip()
        result, _ = json.JSONDecoder().raw_decode(body)
        timer = re.search(r"Run Time \(s\): real ([0-9.]+)", body)
        if not timer:
            raise ValueError("Missing query timing")
        runs.append(
            {
                "iteration": int(match.group(1)),
                "seconds": float(timer[1]),
                "result": result,
            }
        )
    return runs


def parse_metrics(stdout, marker):
    body = stdout.split(marker + "\n", 1)[1].lstrip()
    rows, _ = json.JSONDecoder().raw_decode(body)
    return {f"{row['phase']}.{row['metric']}": row["value"] for row in rows}


def difference(after, before):
    return {
        key: value if key.endswith(".peak_active_streams") else value - before.get(key, 0)
        for key, value in after.items()
    }


def comparable_result(case, rows):
    if case == "value_sample":
        row = rows[0]
        if not (0 <= row["nonnull"] <= row["n"] <= 10):
            raise ValueError("Invalid LIMIT sample counts")
        # Unordered LIMIT can select different rows; values force the projection,
        # but are not an equality oracle across independent reads.
        return [{"n": row["n"]}]
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approved-live-read", action="store_true", required=True)
    parser.add_argument("--shell", type=Path, required=True)
    parser.add_argument("--workspace", type=uuid.UUID, required=True)
    parser.add_argument("--item", type=uuid.UUID, required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--column", default="Price")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--read-metrics", action="store_true")
    parser.add_argument("--prepared", action="store_true")
    parser.add_argument("--profile", action="store_true")
    parser.add_argument(
        "--cases",
        nargs="+",
        choices=[
            "sample",
            "value_sample",
            "count",
            "aggregate",
            "filter",
            "impossible",
        ],
    )
    parser.add_argument(
        "--binary-source",
        help="Known source revision for an archived shell; otherwise current checkout",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=["DIRECT_FILES", "TABLE_API"],
        default=["DIRECT_FILES", "TABLE_API"],
    )
    parser.add_argument("--external-file-cache", choices=["true", "false"], default="false")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    if args.output.exists():
        parser.error("Refusing to overwrite an existing report")
    if any("\n" in v or "\r" in v for v in (args.schema, args.table, args.column)):
        parser.error("Names must not contain shell command delimiters")
    if any(ch in str(args.output) for ch in ("\n", "\r")):
        parser.error("Output path must not contain shell command delimiters")
    root = Path(__file__).resolve().parents[1]
    table = f"lake.{identifier(args.schema)}.{identifier(args.table)}"
    column = identifier(args.column)
    queries = {
        "sample": f"SELECT count(*) AS n FROM (SELECT {column} FROM {table} LIMIT 10)",
        "value_sample": (
            f"SELECT count(*) AS n, count({column}) AS nonnull, "
            f"min({column}) AS minimum, max({column}) AS maximum, sum({column}) AS total "
            f"FROM (SELECT {column} FROM {table} LIMIT 10)"
        ),
        "count": f"SELECT count(*) AS n FROM {table}",
        "aggregate": (
            f"SELECT count(*) AS n, count({column}) AS nonnull, "
            f"min({column}) AS minimum, max({column}) AS maximum, "
            f"sum({column}) AS total FROM {table}"
        ),
        "filter": (f"SELECT count(*) AS n, sum({column}) AS total FROM {table} " f"WHERE {column} >= 10000000"),
        "impossible": (f"SELECT count(*) AS n FROM {table} WHERE {column} > 1000000000"),
    }
    if args.cases:
        queries = {case: queries[case] for case in args.cases}
    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_commit": subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip(),
        "source_dirty": bool(
            subprocess.check_output(["git", "-C", str(root), "status", "--porcelain"], text=True).strip()
        ),
        "binary_source": args.binary_source,
        "shell_sha256": hashlib.sha256(args.shell.read_bytes()).hexdigest(),
        "target": f"{args.workspace}/{args.item}/{args.schema}/{args.table}",
        "external_file_cache": args.external_file_cache == "true",
        "threads": 2,
        "memory_limit": "4GB",
        "read_metrics": args.read_metrics,
        "prepared": args.prepared,
        "profile": args.profile,
        "method": (
            "Fresh process per mode/workload; first query and repeated queries. "
            "SQL timings include binding; --prepared measures explicit PREPARE separately, "
            "but EXECUTE can still rebind an unversioned catalog. "
            "process timings include setup and metrics/profile collection. "
            "No OS/service cache flush. Metrics are client-consumed response bytes, not wire bytes; "
            "direct-file HTTP outside DuckDB filesystem accounting is not measured. "
            "Count-only samples may optimize away the named column and use an authorized count carrier. "
            "value_sample forces a value projection; unordered sample values are not compared for equality. "
            "Independent sessions do not guarantee a common source snapshot."
        ),
        "experiments": [],
        "equivalent": False,
    }
    references = {}
    for case_index, (case, query) in enumerate(queries.items()):
        modes = args.modes.copy()
        if case_index % 2:
            modes.reverse()
        for mode in modes:
            commands = [
                ".bail on",
                ".mode json",
                ".timer on",
                "SET threads=2;",
                "SET memory_limit='4GB';",
                f"SET enable_external_file_cache={args.external_file_cache};",
                "LOAD onelake;",
                f"SET onelake_enable_read_metrics={'true' if args.read_metrics else 'false'};",
                "LOAD azure;",
                "CREATE SECRET bench (TYPE ONELAKE, PROVIDER credential_chain, CHAIN 'env');",
                f"ATTACH '{args.workspace}/{args.item}' AS lake " f"(TYPE ONELAKE, READ_MODE '{mode}', READ_ONLY);",
            ]
            if args.read_metrics:
                commands += [
                    ".print BENCH_METRICS_SETUP",
                    "SELECT * FROM onelake_read_metrics();",
                ]
            if args.prepared:
                commands += [
                    ".print BENCH_PREPARE",
                    "PREPARE bench_query AS " + query + ";",
                ]
                if args.read_metrics:
                    commands += [
                        ".print BENCH_METRICS_PREPARE",
                        "SELECT * FROM onelake_read_metrics();",
                    ]
            profiles = []
            for index in range(args.repeats):
                if args.profile:
                    path = args.output.resolve().with_name(f"{args.output.stem}-{mode}-{case}-{index}.profile.json")
                    if path.exists():
                        raise RuntimeError("Refusing to overwrite a profile")
                    profiles.append(path)
                    commands += [
                        "PRAGMA enable_profiling='json';",
                        "SET profiling_output='" + str(path).replace("'", "''") + "';",
                    ]
                commands += [
                    f".print BENCH_QUERY_{index}",
                    "EXECUTE bench_query;" if args.prepared else query + ";",
                ]
                if args.profile:
                    commands += ["PRAGMA disable_profiling;"]
                if args.read_metrics:
                    commands += [
                        f".print BENCH_METRICS_{index}",
                        "SELECT * FROM onelake_read_metrics();",
                    ]
            commands += [".print BENCH_DONE", ".quit"]
            print(f"{mode}: {case}", flush=True)
            started = time.perf_counter()
            result = subprocess.run(
                [str(args.shell.resolve()), "-unsigned", "-batch"],
                cwd=root,
                input="\n".join(commands) + "\n",
                capture_output=True,
                text=True,
                timeout=900,
            )
            # Do not persist service errors, tokens, raw output or environment variables.
            if result.returncode:
                raise RuntimeError(f"{mode}/{case} failed with exit code {result.returncode}")
            runs = parse_runs(result.stdout)
            if len(runs) != args.repeats:
                raise RuntimeError("Incomplete benchmark output")
            setup_metrics = parse_metrics(result.stdout, "BENCH_METRICS_SETUP") if args.read_metrics else {}
            previous = setup_metrics
            prepare_seconds = None
            prepare_metrics = {}
            if args.prepared:
                prepare_seconds = float(
                    re.search(
                        r"BENCH_PREPARE\r?\nRun Time \(s\): real ([0-9.]+)",
                        result.stdout,
                    )[1]
                )
                if args.read_metrics:
                    previous = parse_metrics(result.stdout, "BENCH_METRICS_PREPARE")
                    prepare_metrics = difference(previous, setup_metrics)
            for run in runs:
                comparable = comparable_result(case, run["result"])
                reference = references.setdefault(case, comparable)
                if comparable != reference:
                    raise RuntimeError(f"Result mismatch for {mode}/{case}; source may have changed")
                if args.read_metrics:
                    current = parse_metrics(result.stdout, f"BENCH_METRICS_{run['iteration']}")
                    run["metrics"] = difference(current, previous)
                    previous = current
                if args.profile:
                    profile = json.loads(profiles[run["iteration"]].read_text(encoding="utf-8"))
                    run["profile"] = profile
                    profiles[run["iteration"]].unlink()
            report["experiments"].append(
                {
                    "mode": mode,
                    "case": case,
                    "query": query,
                    "process_seconds": time.perf_counter() - started,
                    "setup_metrics": setup_metrics,
                    "prepare_seconds": prepare_seconds,
                    "prepare_metrics": prepare_metrics,
                    "runs": runs,
                }
            )
            args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
            print([run["seconds"] for run in runs], flush=True)
    report["equivalent"] = True
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
