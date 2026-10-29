"""Opt-in, read-only native-shell benchmark. Credentials stay in the environment."""

import argparse
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
    parser.add_argument(
        "--external-file-cache", choices=["true", "false"], default="false"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    if args.output.exists():
        parser.error("Refusing to overwrite an existing report")
    if any("\n" in v or "\r" in v for v in (args.schema, args.table, args.column)):
        parser.error("Names must not contain shell command delimiters")
    root = Path(__file__).resolve().parents[1]
    table = f"lake.{identifier(args.schema)}.{identifier(args.table)}"
    column = identifier(args.column)
    queries = {
        "sample": f"SELECT count(*) AS n FROM (SELECT {column} FROM {table} LIMIT 10)",
        "count": f"SELECT count(*) AS n FROM {table}",
        "aggregate": (
            f"SELECT count(*) AS n, count({column}) AS nonnull, "
            f"min({column}) AS minimum, max({column}) AS maximum, "
            f"sum({column}) AS total FROM {table}"
        ),
        "filter": (
            f"SELECT count(*) AS n, sum({column}) AS total FROM {table} "
            f"WHERE {column} >= 10000000"
        ),
        "impossible": (
            f"SELECT count(*) AS n FROM {table} WHERE {column} > 1000000000"
        ),
    }
    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_commit": subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
        ).strip(),
        "target": f"{args.workspace}/{args.item}/{args.schema}/{args.table}",
        "external_file_cache": args.external_file_cache == "true",
        "threads": 2,
        "memory_limit": "4GB",
        "method": (
            "Fresh process per mode/workload; first query and repeated queries. "
            "SQL timings include binding/execution; process timings include setup. "
            "No OS/service cache flush. Rust HTTP bytes and memory are not measured. "
            "Independent sessions do not guarantee a common source snapshot."
        ),
        "experiments": [],
        "equivalent": False,
    }
    references = {}
    for case_index, (case, query) in enumerate(queries.items()):
        modes = ["DIRECT_FILES", "TABLE_API"]
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
                "LOAD azure;",
                "CREATE SECRET bench (TYPE ONELAKE, PROVIDER credential_chain, CHAIN 'env');",
                f"ATTACH '{args.workspace}/{args.item}' AS lake "
                f"(TYPE ONELAKE, READ_MODE '{mode}', READ_ONLY);",
            ]
            for index in range(args.repeats):
                commands += [f".print BENCH_QUERY_{index}", query + ";"]
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
                raise RuntimeError(
                    f"{mode}/{case} failed with exit code {result.returncode}"
                )
            runs = parse_runs(result.stdout)
            if len(runs) != args.repeats:
                raise RuntimeError("Incomplete benchmark output")
            for run in runs:
                reference = references.setdefault(case, run["result"])
                if run["result"] != reference:
                    raise RuntimeError(
                        f"Result mismatch for {mode}/{case}; source may have changed"
                    )
            report["experiments"].append(
                {
                    "mode": mode,
                    "case": case,
                    "query": query,
                    "process_seconds": time.perf_counter() - started,
                    "runs": runs,
                }
            )
            args.output.write_text(
                json.dumps(report, indent=2) + "\n", encoding="utf-8"
            )
            print([run["seconds"] for run in runs], flush=True)
    report["equivalent"] = True
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
