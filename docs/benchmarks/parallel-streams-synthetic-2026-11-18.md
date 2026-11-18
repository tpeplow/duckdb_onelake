# Local parallel-stream benchmark

The local, balanced eight-stream test was **5.96x faster with eight workers**
than with one: median elapsed time fell from **701.5 ms to 117.6 ms**.
Four workers achieved **3.54x** speedup. This demonstrates overlap of independent
stream reads under controlled loopback conditions, **not a measured live OneLake
speedup**.

The [JSON evidence](parallel-streams-synthetic-2026-11-18.json) contains all 144
successful runs, including warm-ups, numeric stream counters, and summaries.
These are the existing 2026-11-18 measurements; no benchmark was rerun for this
write-up.

## Balanced-stream result

Eight streams, each containing 32 batches of 4,096 rows: **1,048,576 rows total**.
The fixture includes nullable numeric values, 64-character strings, and dictionary
values. Each configuration has one excluded warm-up and five measured runs.

| Worker cap | Median elapsed (ms) | Measured range (ms) | Speedup vs. cap 1 | Median client CPU (s) | Median process peak (MiB) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 701.5 | 696.1-728.7 | 1.00x | 0.265625 | 11.36 |
| 2 | 373.5 | 367.6-385.1 | 1.88x | 0.187500 | 12.46 |
| 4 | 197.9 | 196.9-221.3 | 3.54x | 0.250000 | 14.02 |
| 8 | 117.6 | 113.2-120.8 | 5.96x | 0.218750 | 15.49 |

Speedup is `median elapsed at cap 1 / median elapsed at selected cap`.
Process peak working set increased **23.5% at cap 4** and **36.4% at cap 8**.
CPU measurements are coarse and noisy, include test-process startup, and are
**not evidence of a CPU optimization**.

## Controls and trade-offs

Median elapsed milliseconds for all six scenarios:

| Scenario | Cap 1 | Cap 2 | Cap 4 | Cap 8 |
| --- | ---: | ---: | ---: | ---: |
| Balanced streams | 701.5 | 373.5 | 197.9 | 117.6 |
| One delayed stream | 672.9 | 370.5 | 372.4 | 391.7 |
| No injected server delay | 328.9 | 129.9 | 99.3 | 73.5 |
| Consumer pauses after first batch | 406.8 | 240.4 | 205.8 | 190.5 |
| Stop after first batch | 9.4 | 11.0 | 12.5 | 13.9 |
| Single-stream control | 95.4 | 100.7 | 94.1 | 94.9 |

More workers are not universally better:

- The delayed-stream case improved **1.81x at cap 4**, but cap 8 was slower than
  cap 4. Cap-4 client CPU rose from 0.203125s to 0.265625s.
- Early-stop elapsed time increased from 9.4 ms to 13.9 ms. GET attempts grew
  from **1 to 2, 4 and 8** with the corresponding worker caps, despite the consumer
  accepting only the first 4,096-row batch. Prefetched data may be discarded.
- The single-stream control remained approximately 95-101 ms, with one GET and
  peak one open stream at every configured cap. It retains the serial fast path.
- Memory is bounded by the worker cap and per-stream limits, not by DuckDB's
  `memory_limit`. Each worker can retain decoder, frame and dictionary state in
  addition to its one outstanding batch credit.

## Method and interpretation

The harness is `test/benchmark_streams.py`, using the ignored Rust
`tests::benchmark_fixture` and `tests::benchmark_client` tests and
`test/benchmark_stream_server.py`.

The HTTP fixture server runs in a **separate process**, so its CPU and memory are
excluded from client resource measurements. All scenarios use the same generated
data, except the single-stream control uses only the first stream.

| Scenario | Controlled behavior |
| --- | --- |
| Balanced, early-stop, single-stream | 2 ms server delay per manifest chunk |
| Delayed stream | Stream 0 has 10 ms delay per chunk; other streams have none |
| Fast | No injected server delay |
| Stalled consumer | No server delay; consumer pauses 100 ms after the first batch |
| Early-stop | Consumer drops the reader after its first batch, without reaching EOF |

Six scenarios x four caps x six runs = **144 successful runs**: 24 excluded
warm-ups and 120 measured samples. Complete reads check expected row counts and a
known numeric checksum; the harness cross-checks checksums across caps and repeats.
Early-stop checks the first-batch row count rather than requiring identical
unordered values. Normal client exit joins the workers.

Reader elapsed time starts immediately before opening the reader and ends after
consumption and reader teardown. It includes local allocation, stream opens,
decoding, checksum work, intentional consumer pauses, and shutdown; shutdown time
is also recorded separately. It excludes process startup and fixture generation.
This is a **Rust reader-only** benchmark: it does not measure native DuckDB SQL
conversion/aggregation, cache encoding, cloud authentication, or Fabric service
execution.

Client CPU uses Windows `GetProcessTimes`; memory uses
`GetProcessMemoryInfo`. These process-level measurements include Rust test-harness
startup and worker threads. Nominal memory sampling interval is 5 ms, with a final
live sample and OS high-water check at process exit. Sampled peaks can miss short
spikes; process peak working set is not DuckDB-managed memory.

The host was Windows 11 with 14 logical processors. No concurrent agent builds or
tests ran, but other users' host activity was not controlled. Results use medians
and observed ranges, not p95 estimates or confidence intervals. Timing is not a
correctness threshold. The no-delay scenario still includes real loopback HTTP,
OS scheduling, IPC and checksum work; it is not an isolated decoder CPU test.

The client cap limits consumption of streams already allocated by the service;
it does not request that stream count from OneLake. Real performance depends on
the number of returned streams, their balance, network conditions, and consumer
work. The [live read-mode report](read-modes-2026-11-18.md) returned one stream per
successful allocation and therefore cannot validate this synthetic speedup.

## Provenance

- Measured source label: `2c7de78+working-tree`; changes were subsequently committed
  as `f93d9d6` (`Process OneLake read streams concurrently`).
- Release Rust test executable SHA-256:
  `c07eb1b505c56508beb371ccc06d0a2edf2e251e158fa0f546035098fa356b56`.
- Build-time tracked diff SHA-256:
  `bb06d22bbd5c85aa31c43d120fdc24495cc02aa7161a02c4a87e8c2fd5a886b3`.
- JSON retains build-time checkout provenance, fixture hashes and the original
  report digest. This executable is the Rust test client, not the DuckDB shell.

## Reproduce locally

No Fabric credentials or live reads are needed. From the repository root:

```powershell
cargo test --release --no-run --locked `
  --manifest-path rust\onelake_table_reader\Cargo.toml

python test\benchmark_streams.py --run-benchmark `
  --test-exe <release-Rust-unit-test-executable> `
  --fixture-dir <new-fixture-directory> `
  --source <revision-and-worktree-state> `
  --host-conditions <observed-load> `
  --output <new-report.json>
```

Use the unit-test executable path emitted by Cargo, a new fixture directory and
an unused output filename. Defaults reproduce all six scenarios and caps 1/2/4/8.
The harness refuses overwrites and does not retry failed runs.
