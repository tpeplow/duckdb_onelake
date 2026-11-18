# Direct Parquet versus Table API: thread-count comparison

Date: 2026-11-18.

**No stream-level parallelism was observed.** Every successful Table API execution
received exactly one stream, with peak active streams equal to one, including at
eight configured DuckDB threads. The parallel-capable reader therefore used its
serial fast path. These measurements do not establish parallel-stream speedup.

## Results

Dataset: PricePaid, **30,535,540 rows**. Values below are elapsed seconds: the
median of two repeat executions, excluding the first execution in each process.
The [sanitized JSON evidence](read-modes-2026-11-18.json) preserves all three timing
samples, aggregate results, query text, settings, source-report hashes and numeric
reader counters for every workload/configuration.

| Workload | Direct Parquet (2 threads) | Table API (1 thread) | Table API (2 threads) | Table API (8 threads) |
| --- | ---: | ---: | ---: | ---: |
| Count over LIMIT 10 | 8.1930 | 3.2750 | 3.4440 | 3.0185 |
| COUNT(*) | 4.8975 | 8.2975 | 7.4645 | 7.7735 |
| Full numeric aggregate | 8.9100 | 10.6465 | 12.0055 | 11.5225 |
| Price >= 10 million | 9.0130 | 11.9930 | 12.4160 | 11.5280 |
| No-match filter | 7.3605 | 11.7875 | 11.8695 | 10.4850 |

The numeric aggregate computes count, non-null count, min, max and sum of Price.
The selective filter computes count and sum where Price >= 10,000,000.
The no-match filter counts rows where Price > 1,000,000,000; the measured maximum
was 900,000,000. The counted LIMIT query can optimize away the named Price column;
it is not a forced Price-value transfer test.

All reported results matched across modes, repetitions and thread counts.
The successful measurements contain **60 query executions**, including **45 Table
API execution allocations**, with **zero schema read allocations**. Direct-file
HTTP requests are not measured by the Table API counters.

## Method and provenance

- Same native shell for all configurations; reader source commit:
  `f93d9d6` (`Process OneLake read streams concurrently`).
- Shell SHA-256:
  `7f020e3e4720729fe95276ecb051868e4ee8d339fade06bfa6843622c05494fc`.
  The executable was built from those reader changes before their commit.
- `memory_limit='4GB'`, `enable_external_file_cache=false`, Table API read cache
  OFF (default), and `onelake_enable_read_metrics=true`.
- Fresh DuckDB process per mode/workload, with three query executions in that
  process. SQL timing includes binding and execution. Token acquisition is
  outside the SQL timer. Explicit PREPARE and profiling are disabled.
- Direct-file/two-thread and Table API/two-thread measurements were reused from
  the preceding comparison on the same day. One-thread and eight-thread Table
  API measurements followed. Thread configurations were not interleaved or
  randomized; time-dependent network/service/host effects remain possible.
- OS and service caches were not flushed. "First execution" does not mean a
  genuinely cold-cache run. Independent allocations have no common snapshot
  guarantee, although the observed aggregate results agreed.
- Each summary uses only two repeat samples. No statistical significance or p95
  claim is made. Full-scan, count and LIMIT workloads have different opportunities
  for metadata shortcuts, projection and early termination.
- Reader duration counters ending in `_ns` are elapsed times, not process CPU.
  `peak_active_streams` is a connection high-water snapshot, not an additive
  per-query counter. These runs do not report client CPU or peak process memory.

Increasing DuckDB's thread setting does not request more streams from OneLake.
The client can only parallelize independent streams actually returned by the
service. Here the effective stream-worker count stayed at one in every column.

## Failed attempt and continuation

The initial eight-thread sweep completed the LIMIT and count workloads, then its
aggregate child process exited with code 1. At that point the runner did not retain
a sanitized error category or partial query timings. Its exact read-allocation
count is unknown; that child was configured for three query executions.

This failure is recorded in the JSON's `excluded_attempts`, not counted as a
zero-time or successful measurement. The aggregate workload was rerun once in a
new process with fresh credentials. The remaining filter and no-match workloads
then ran separately. All three completed successfully; the cause of the original
failure was not established. The runner now preserves sanitized failure
categories and query-marker counts without retaining raw service errors.

## Rerunning

Use an explicitly approved Lakehouse target. The runner expects in-memory
`AZURE_STORAGE_TOKEN` and `FABRIC_API_TOKEN` for the same caller; do not put tokens
in command lines or repository files. These queries perform full reads and can
incur service charges.

```powershell
python test\benchmark_reads.py --approved-live-read `
  --shell build\release\duckdb.exe `
  --workspace <approved-workspace-guid> --item <approved-item-guid> `
  --schema HousePrices --table PricePaid --column Price `
  --modes TABLE_API --threads 1 --repeats 3 `
  --cases sample count aggregate filter impossible `
  --external-file-cache false --read-metrics `
  --binary-source <known-reader-build-revision> --output <new-report.json>
```

Repeat with `--threads 2` and `--threads 8`. For the direct-file baseline, use
`--modes DIRECT_FILES --threads 2`. The runner refuses to overwrite an existing
report and does not automatically retry failed queries.

The committed evidence deliberately excludes credentials, identities, target
GUIDs, headers, raw service output and absolute local paths. Original session
reports remain separate; their filenames and SHA-256 hashes identify the inputs
used to generate the sanitized evidence.
