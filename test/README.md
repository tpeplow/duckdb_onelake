# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

## Compatibility and CI

The native extension is currently built and tested locally against **DuckDB
v1.4.2** on Windows x64 MSVC. Load it with that DuckDB version; a successful build
does not establish compatibility with another version's native extension ABI.

The distribution and code-quality workflow uses v1.4.2 for DuckDB and its CI
tools. Its configured native targets are Linux x64/ARM64, macOS x64/ARM64 and
Windows x64 MSVC. Reduced-matrix mode is disabled. WebAssembly, Windows ARM64,
MinGW and Linux musl are excluded from this initial matrix.

An independent offline job runs the Rust reader's locked tests, formatting and
strict Clippy checks, plus the Python benchmark tests, on Linux, Windows and
macOS runners. These checks use no Fabric credentials. The ignored live reader
test is not enabled. The native cache suite remains an explicit test target;
run it using the instructions below.

**Configured CI is not completed platform validation.** Builds and tests must
pass on a platform before claiming support for it; they may run locally.
Local Windows results do not prove POSIX cache admission or other platforms'
behavior. The local unsigned experimental package is Windows x64 only and uses
no GitHub Actions. The wider matrix above is not a support claim, and the
workflow configuration does not publish a release.

The required live integration scope is a read-only smoke check: authenticate,
discover an approved table through `READ_MODE 'TABLE_API'`, and successfully
consume its returned schema and data with caching OFF. Existing approved live
reader tests cover this path. OneLake RLS/CLS policy matrices, permission changes
and revocation timing are not acceptance requirements for this extension.
The opt-in restricted fixtures below remain optional additional coverage, not
a requirement to provision or change security policies.

## Read-mode benchmark

`benchmark_reads.py` compares TABLE_API and DIRECT_FILES using the native shell.
It requires Python's standard library, an explicitly approved Lakehouse table,
and in-memory `AZURE_STORAGE_TOKEN` and `FABRIC_API_TOKEN` for the same caller.
The default workloads expect a numeric Price column; `--column` selects another.
They include full scans and allocate billed read sessions. Do not run against
an unapproved target. No data or permissions are changed.

```powershell
python test\benchmark_reads.py --approved-live-read `
  --shell build\release\duckdb.exe --workspace <workspace-guid> --item <item-guid> `
  --schema HousePrices --table PricePaid --external-file-cache false `
  --output <new-report-path.json>
```

The default disables DuckDB's external-file cache in both modes. Each workload
uses a fresh process, alternating mode order, with three queries per process.
The harness checks aggregate result equality across repetitions and modes and
refuses to overwrite reports. It records source revision, individual timings
and configuration, not tokens or raw service output. First-use is not genuinely
cold: OS/service caches are not flushed. The `impossible` workload's threshold
is only impossible when the selected data's maximum is at most one billion.
Wall time includes SQL binding/execution; no Rust wire-byte or per-query memory
measurement is implied. Keep generated reports outside the repository.

## Read metrics (experimental)

Enable numeric, connection-local diagnostics before binding a TABLE_API query:

```sql
SET onelake_enable_read_metrics = true;
ATTACH '<workspace-guid>/<item-guid>' AS lake
    (TYPE ONELAKE, READ_MODE 'TABLE_API', READ_ONLY);
PREPARE sample AS SELECT Price FROM lake.dbo.example LIMIT 10;
SELECT * FROM onelake_read_metrics() WHERE value > 0;
EXECUTE sample;
SELECT * FROM onelake_read_metrics() WHERE value > 0;
SET onelake_enable_read_metrics = false;
```

Use the authentication setup in the README and substitute your schema, table and
column. The setting defaults to false. Counters accumulate until the connection
closes; subtract cumulative snapshots to measure a statement. The
`peak_active_streams` high-water mark aggregates with max, not addition or subtraction;
use a fresh connection for an isolated per-query peak. Disabling collection preserves
existing counters. Metrics queries do not reset them or contact OneLake. No query
text, identities, tokens, URLs, stream IDs, headers or rows are included.

The function returns `phase`, `metric`, and an unsigned integer `value`. Phases are
`shared` (Table API token/metadata work), `schema` (metadata-only binding), and
`execution` (projected reads and the native Arrow operator). All durations ending
in `_ns` are wall-clock nanoseconds, **not CPU time**.

| Metrics | Meaning |
| --- | --- |
| `token_calls`, `token_ns` | Token-provider and identity-validation calls, including cache hits; not token-service HTTP request counts. |
| `metadata_requests`, `metadata_bytes`, `metadata_ns` | Table API discovery/catalog/Get table HTTP attempts, response bytes retained by the bounded receiver, and request time. |
| `bind_ns`, `reader_opens`, `reader_open_ns` | Metadata binding adapter time and execution stream-construction attempts/time. Binding does not include all earlier catalog discovery. |
| `allocation_requests`, `allocation_bytes`, `allocation_ns` | Rust POST attempts, consumed allocation-response bytes, and allocation/parse time. Attempts are not a billing ledger. |
| `allocated_streams`, `peak_active_streams` | Total streams returned by allocations, and the maximum simultaneously open stream requests/bodies per reader (connection aggregate: max). Includes backpressure waits, not a measure of simultaneous CPU work or wire transfer. |
| `stream_requests`, `stream_open_ns` | Rust GET attempts and time until response headers are available. |
| `stream_bytes`, `stream_read_ns` | Bytes returned by successful response-body reads and time spent in those reads. Includes IPC metadata/dictionaries, not just values. |
| `ipc_processing_ns` | Sum of worker-local framing, validation, decoding and projection elapsed time minus that worker's observed body-read time. Excludes stream-open and ready-queue waits; includes allocation/copies/scheduling and instrumentation overhead. |
| `batches`, `rows` | Arrow batches/rows delivered by Rust, including surplus rows received for LIMIT. |
| `first_batches`, `first_batch_ns` | Number of readers reaching a first batch and sum of allocation-start-to-first-batch spans. Includes intervening consumer delay; divide by first_batches for a mean. |
| `completions`, `errors` | Readers reaching EOF and Rust open/iteration errors. Early LIMIT termination is not completion; native token/binding errors are not Rust errors. |
| `arrow_fetch_ns`, `arrow_operator_ns`, `output_rows` | Native C-stream fetch time, inclusive Arrow local-init/scan time, and rows emitted by that operator before residual SQL filtering. Parquet cache hits are not counted as Arrow output. |

Schema-phase reader, allocation and stream counters remain zero: binding uses
Get table, counted in shared metadata metrics. PREPARE, DESCRIBE, ordinary EXPLAIN
and optimized-away LIMIT 0 must not allocate reads. EXECUTE may still rebind and
fetch metadata; no schema cache is introduced.

Times overlap, including between parallel workers: reader-open includes allocation/initial-stream work; binding
includes Get table work; first-batch spans include several stages.
`arrow_operator_ns - arrow_fetch_ns` estimates remaining native conversion/scan
overhead, not isolated conversion CPU. Do not sum all durations. Server execution,
DNS/TLS, network buffering and scheduling are not independently observable.
Body bytes are client-consumed bytes, **not wire traffic**: TLS/HTTP overhead,
transport-internal prefetch not consumed by Rust is excluded. Bytes consumed by
workers but abandoned after LIMIT are included, even if their batches were never
delivered. Native teardown publishes the final counters after joining workers.

These counters instrument the TABLE_API path, not DIRECT_FILES' catalog or Delta
kernel HTTP. Zero counters in direct-file mode do not mean zero traffic.
Use DuckDB JSON profiles for native file bytes, Parquet operators, filters and
local SQL; those counters also do not include every HTTP client.

The benchmark accepts `--read-metrics`, `--prepared`, `--profile`, `--cases` and
`--modes`. For an approved target, add these to the earlier benchmark command:

```text
--read-metrics --prepared --profile --cases sample value_sample aggregate filter
```

It records setup/PREPARE metrics separately, per-execution deltas and optional
DuckDB profiles. EXECUTE is not necessarily binding-free: the current unversioned
OneLake catalog causes DuckDB to rebind prepared statements. Catalog lookup hands
its freshly fetched metadata bind to the immediately following scan bind once,
within the same statement. A simple uncached table scan therefore needs a
metadata GET and one execution allocation, not an additional schema allocation.
No schema bind is reused across statements. Unsupported metadata fails without
falling back to a read stream.

The count-only `sample` workload can optimize away the named column. The Arrow
reader chooses a narrow, plain primitive from the authorized schema as its count
carrier, falling back to the first visible column when no supported primitive is
available. NULL carrier values still contribute rows to COUNT(*). This heuristic
does not guarantee minimum transfer size and does not avoid full-batch reads.
Use `value_sample` to force the named column through a LIMIT aggregate, or
`aggregate` for its full transfer. Unordered LIMIT samples compare row counts
and check non-null-count bounds, not equality of selected values.

Reports include the shell SHA-256 and checkout revision/dirty state. When using
an archived shell, supply `--binary-source <revision>` to distinguish its source
from the checkout containing the benchmark harness.

Run the same workloads without `--read-metrics` to assess
instrumentation overhead; keep profile settings equal. Mode order alternates
between workloads. Network/service variability can outweigh instrumentation cost.
No automatic full-table benchmark is run by loading the extension.

For a controlled Rust-only overhead comparison without network traffic:

```sh
cargo test --release --locked --manifest-path rust/onelake_table_reader/Cargo.toml metrics_overhead -- --ignored --nocapture
python -m unittest discover -s test -p test_benchmark_reads.py
```

The local benchmark warms up, then alternates enabled/disabled order over seven
rounds of 1,000 two-batch reads. It measures Rust framing/decoding overhead only,
not the C++ counters or live transport.

## Guarded Arrow-to-Parquet cache (experimental)

The experimental cache reuses previously authorized Arrow results as local
Parquet fragments. `READ_CACHE 'OFF'` is the default and disables both fill and
reuse. Keep it OFF when every query must obtain fresh service-authorized data:

```sql
ATTACH '<workspace-guid>/<item-guid>' AS fresh
    (TYPE ONELAKE, READ_MODE 'TABLE_API', READ_CACHE 'OFF', READ_ONLY);
```

To accept bounded-staleness reuse, supply an explicit positive integer
time-to-live (TTL) in seconds:

```sql
ATTACH '<workspace-guid>/<item-guid>' AS lake
    (TYPE ONELAKE, READ_MODE 'TABLE_API', READ_CACHE 'PARQUET',
     READ_CACHE_TTL_SECONDS 300, READ_ONLY);
```

300 seconds is an example, not a default. PARQUET requires TABLE_API and an
explicit TTL; older PARQUET attachments without one now fail. Zero, negative,
fractional, nonnumeric, NULL and overflowing values are rejected, as is a TTL
with OFF. The TTL is fixed for the attachment. Setting
`enable_external_file_cache=false` bypasses both fill and reuse.

**TTL does not validate current data or effective permissions.** Previously
authorized rows can remain readable after data, RLS or other access changes.
Identity checks and fresh schema binding remain, but an unchanged schema cannot
detect every policy change. The administrative dataAccessRoles API is not called;
TTL caching needs no additional administrative permission beyond normal read
authorization. Leave caching OFF if this staleness is unacceptable. Fresh reads
still follow the service's own authorization and propagation semantics.
Permission changes do not automatically invalidate or refresh this cache.
A low positive `READ_CACHE_TTL_SECONDS` reduces the local reuse window, but does
not guarantee immediate revocation or bypass service-side propagation delays.

The monotonic deadline starts immediately before opening the execution read.
Allocation, transfer and encoding consume the TTL; hits and metadata GETs never
renew it. Before each hit, an expired entry is removed and a fresh Arrow read
starts. A fill that expires stops retaining data and continues its original
stream without replay. A cached query admitted before expiry may finish afterward.
TTL therefore bounds admission age, not completion time or absolute source-data
age. Idle expired storage may be reclaimed lazily on access or normal cleanup;
there is no background polling or secure-erasure guarantee.

Configure with `-DONELAKE_BUILD_CACHE_PROTOTYPE=ON` and build the
`onelake_cache_storage_test` target. Run it with an existing, empty, private
temporary-directory path as its sole argument. On Windows, the helper below
creates a fresh fixture with a protected ACL and also tests junction rejection:

```powershell
pwsh -NoProfile -File test\cache\run.ps1 -TempDirectory <new-absolute-fixture-path>
```

On POSIX, create the fixture with mode 0700 and use `umask 077` so native spill
files also remain private. The helper never changes an existing directory and
never recursively deletes one. The tests use synthetic data only: no Fabric access,
tokens or external services are required. An in-process source supplies an
explicit synthetic snapshot/policy validator; this does not prove live safety.
The executable exercises native
Parquet COPY/read and incremental Arrow-batch encoding over buffer-managed
virtual files, preserving the caller's batch ownership and surplus rows.
It covers cross-connection isolation, external-file range invalidation without
flushing unrelated entries, spill limits and cleanup. Native file validity is
not a source-completeness or authorization proof. SQL integration cases cover
complete and empty hits, prepared authorization checks, changed snapshots and
policies, projection/filter mapping, partial coverage, cancellation, pressure,
and failures during initialization and before/after output. They print controlled
uncached/fill/hit timings and execution-stream/batch counts. These synthetic
timings include no network latency, are not live-cache speedups, and should be
repeated without concurrent builds for comparisons.

The adapter encodes each received batch through DuckDB's registered Parquet
writer, retaining surplus rows without downloading subsequent batches for LIMIT.
It marks a source complete only at EOF. An incomplete entry can answer only a
proven simple unordered column projection with a finite constant LIMIT and
sufficient rows; unsupported shapes or disabled optimization miss.
Every hit checks identity, attachment lifetime and expiry before native Parquet initialization,
including metadata-only COUNT and impossible predicates. Simple copied predicates
enable native filtering while the original SQL predicates remain residual.
Guarded files can be opened only inside that scan's scoped native callbacks;
direct `read_parquet()` calls cannot use a discovered private path to skip
these checks. The current adapter is single-threaded; future asynchronous child
readers must propagate this permission scope explicitly, not weaken it.
Cache-local fill errors discard the entry and continue the same Arrow stream.
Cache-read errors can fall back to a fresh authorized stream only before any
output; after output they fail the query, never replaying it.

Files and manifests are connection-private. Attachment retirement removes its
entries across owning connections, invalidates their byte ranges, and rejects
old prepared scans. A new attachment gets a new namespace. Already-running
readers can retain their backing objects until they release their handles.

Data blocks, file objects/control blocks, block indexes, manifest paths/keys,
registry nodes, and lifetime tracking use DuckDB's buffer allocator. Growth is
charged before allocation; metadata-only exhaustion and exact cleanup are tested.
Native Parquet still follows DuckDB's own allocation/accounting behavior; this
does not impose a process-wide RSS bound or account every engine/OS allocation.
Normal cleanup does not promise secure erasure. Unread files do not create
external-file-cache metadata merely to invalidate them.

Before fill or reuse, admission checks the configured spill directory. A disk
directory must already exist at an absolute normalized local path. Windows
checks owner/DACL, rejects reparse points, remote drives and unsafe ancestors;
POSIX checks ownership/modes and rejects links and unsafe ancestors. Existing
children must also pass checks. Unverifiable admission is a miss, not an error
or a permission change. An empty `temp_directory` permits memory-only caching,
with DuckDB allocation failures still causing fill abandonment.
OS owners/administrators and host configuration are trusted: do not change
directory permissions or spill configuration while retained data exists.

The current geometry (one fragment per received batch, 2,048-row groups, Snappy)
is untuned. Encoding performs an additional Arrow-to-DuckDB conversion.
The local production gates are implemented and tested on Windows; POSIX runtime
validation remains part of the cross-platform release work. Deterministic tests
cover TTL boundaries, non-sliding hits, allocation/fill expiry, already-admitted
readers, OFF and disabled external caching. The separate synthetic authoritative
validator tests remain; no equivalent live snapshot/effective-permission validator
has been established. TTL is an explicitly weaker alternative, not that validator.

## Rust table reader

### Restricted public-holidays fixture

`sql/onelake_restricted_read_live.test` is a separate, opt-in security fixture.
Set `ONELAKE_LIVE_RESTRICTED_READ_APPROVED=true`, `ONELAKE_TEST_WORKSPACE`,
`ONELAKE_TEST_ITEM`, `ONELAKE_TEST_SCHEMA`, `ONELAKE_TEST_TABLE`, and
`ONELAKE_TEST_COUNTRY`. Supply `AZURE_STORAGE_TOKEN` and `FABRIC_API_TOKEN` only
in memory for the same restricted caller; this fixture uses the `env` provider.
The country and identifier substitutions are trusted test configuration, not
untrusted input; do not include SQL quotes or shell delimiters in those values.

The target must have the public-holidays columns `countryOrRegion`, `holidayName`,
`normalizeHolidayName`, `isPaidTimeOff`, `countryRegionCode`, and `date`, with
10-100,000 visible rows restricted to the configured country. Direct-file reads
must be denied. Configure the policies separately; the test never changes them.

```powershell
# Set the approved target and in-memory credentials first.
$env:ONELAKE_LIVE_RESTRICTED_READ_APPROVED = 'true'
$env:ONELAKE_TEST_COUNTRY = 'Australia'
$env:UNITTEST_ROOT_DIRECTORY = '.'
& .\build\release\test\unittest.exe 'test/sql/onelake_restricted_read_live.test'
```

The fixture keeps an authorized reference in process memory, verifies completion
below its row cap, and compares counts, NULL-aware aggregates, reordered
projections, local predicates, a self-join, and repeated prepared execution.
Both caches are disabled; metric assertions require fresh service allocations.
No exact row count is hardcoded. Keep the source and policy stable during the
test: independent allocations do not share a guaranteed snapshot. The reference
is caller-visible data, not an unrestricted physical-table baseline.

The direct-file assertion accepts only HTTP 401/403, not arbitrary SQL failures.
It can fail at catalog discovery, which is not evidence that a storage read was
attempted. Independently test a known table file through its discovered DFS route
when reporting raw-file denial. Successful secured reads alongside storage denial
demonstrate the supported restricted path; these checks do not simulate a Table
API failure or establish revocation timing, CLS enforcement, or write permission.
Reads allocate billed sessions. Reports should retain assertions and counters,
not individual rows or credentials.

`sql/onelake_restricted_cache_live.test` uses the same approval, credentials and
public-holidays fixture. Run it separately or select both files with a
comma-separated SQLLogicTest filter:

```powershell
& .\build\release\test\unittest.exe 'test/sql/onelake_restricted_cache_live.test'
```

It uses memory-only managed caching (`temp_directory=''`, `memory_limit='1GB'`),
without creating spill directories. It checks OFF reads, a 300-second TTL
projection fill/hit, missing-column misses, complete six-column fills/hits,
prepared execution, zero execution-body bytes on hits, zero schema allocations
and removal of owned external-file-cache ranges on DETACH. Full results are
compared as multisets with fresh authorized data, including duplicate/NULL/string
values. Samples use simple unordered projections so partial reuse is eligible.

A separate 20-second TTL fixture requires a genuine hit before waiting 21 seconds,
then verifies a fresh read and reuse of the replacement generation. Slow service
allocation/binding can consume that TTL and fail the live test; a miss must not be
misreported as an expiry test of a retained entry. Exact TTL boundaries and
non-sliding behavior also have deterministic native coverage.

These are **static-policy RLS cache tests**, not a revocation test or proof of
current authorization on a hit. They do not change policies or force token
refresh. Keep caching OFF for fresh service-authorized reads; the documented
TTL staleness window and service propagation caveats still apply. Neither
fixture enables live access in CI.

### General reader checks

ATTACH defaults to the existing `DIRECT_FILES` reader. Secured Arrow tests must
explicitly select `READ_MODE 'TABLE_API'`; neither mode falls back to the other.
`sql/onelake_read_modes.test` covers mode validation, READ_ONLY enforcement,
default-mode cache rejection and lazy attachment without contacting Fabric.
The native cache suite checks the catalog selected by omitted/explicit modes.
DIRECT_FILES retains existing write behavior, even when selected by omission;
TABLE_API writes remain blocked until independently authorized writes are implemented.

The opt-in SQL test `sql/onelake_table_api_live.test` requires
`ONELAKE_LIVE_READ_APPROVED=true`, `ONELAKE_TEST_WORKSPACE`, `ONELAKE_TEST_ITEM`,
`ONELAKE_TEST_SCHEMA`, `ONELAKE_TEST_TABLE`, and `ONELAKE_TEST_PROVIDER` (`env` or
`cli`). It expects a PricePaid-compatible table with at least ten positive-price
rows and the columns Price, PropertyType and DateOfTransfer. It covers prepared
re-execution, COUNT without selected columns, projected types, local filtering
and joins. All reads are limited samples, not full-table/security-policy tests.
When using `env`, supply the Storage token only in memory. Running the test
allocates billed read sessions. Do not enable it against an unapproved target.

`sql/onelake_direct_files_live.test` separately requires
`ONELAKE_LIVE_DIRECT_READ_APPROVED=true` and the same workspace/item/schema/table
variables, plus in-memory `FABRIC_API_TOKEN` and `AZURE_STORAGE_TOKEN`. Use a
Lakehouse with a PricePaid-compatible Delta table containing ten non-null prices.
Its catalog must return an HTTPS OneLake DFS location to exercise conversion to
the equivalent ABFSS URI, which lets Delta use the Azure access-token secret.
The host and target path are preserved. Legacy discovery does not enumerate
MirroredDatabase items; use an authorized Lakehouse table for this test.
This is read-only and does not change permissions or retry with another identity.

Run its deterministic tests and checks from the repository root:

```sh
cargo test --locked --manifest-path rust/onelake_table_reader/Cargo.toml
cargo fmt --check --manifest-path rust/onelake_table_reader/Cargo.toml
cargo clippy --locked --manifest-path rust/onelake_table_reader/Cargo.toml --all-targets -- -D warnings
```

These tests use in-memory Arrow streams and loopback HTTP, not Fabric credentials.
They cover gated overlapping reads, as-ready delivery, worker caps, per-worker
batch credits, early-drop/join, worker panic/start failure, multiple streams,
empty tables, independent dictionary/null preservation, truncated
transfers, schema changes, cancellation, expiry, message limits, HTTP failures,
redirect rejection, and Arrow C stream ownership.

An ignored `approved_live_first_batch` test reads only the first batch from an
explicitly approved target. Set `ONELAKE_TABLE_READER_REQUEST` to a JSON object
containing `baseUrl` (the discovered HTTPS OneLake table origin), `workspaceId`,
`itemId`, `schemaName`, `tableName`, and a nonempty `columns` array. Omit `columns`
to request the caller-visible schema/all authorized columns; do not send `["*"]`.
Set
`AZURE_STORAGE_TOKEN` in memory to the caller's token for the exact audience
`https://storage.azure.com` (no trailing slash). Never store tokens in files or
logs. Then run:

```sh
cargo test --locked --manifest-path rust/onelake_table_reader/Cargo.toml approved_live_first_batch -- --ignored --nocapture
```

Clear the environment variables afterwards. This test is deliberately partial:
it does not establish full-table completeness, RLS/CLS enforcement, or write
permissions. Do not run write tests against that read target.

The native adapter passes the current DuckDB thread count, capped to the allocated
stream count, per allocation. Direct Rust/FFI callers may set the client-only
positive integer `maxParallelStreams`; omission defaults to 1. This field is never
sent to the service. Single-stream allocations and threads=1 use the serial path.
Parallel workers yield batches as ready, not in allocation order. Each worker
holds at most one in-flight/queued batch credit and one IPC message internally.
The reader rejects metadata over 1 MiB, bodies over 256 MiB,
cumulative dictionary payload over 256 MiB per stream, and compressed IPC. Arrow
conversion, consumer-held batches and cache encoding require additional memory.
Worker frame/dictionary/batch memory scales with concurrency independently of
DuckDB memory_limit; there is no query-wide pool or shared byte budget.
It uses a 15-second connection timeout, a 60-second per-request timeout, and a
60-minute local session deadline. Cancellation is cooperative; a blocked network
read can take up to its request timeout to return. No retries, redirects,
automatic token refresh, session reallocation, or direct-file fallback occur.
Acquire a fresh caller token before opening a reader; mid-session authentication
failure ends the scan.

### Parallel-stream client impact

The normal Rust tests prove overlap and backpressure with gates and exact counters,
not elapsed-time thresholds. Two opt-in benchmark CLIs quantify trade-offs:

```powershell
python test\benchmark_streams.py --run-benchmark `
  --test-exe <release-Rust-unit-test-executable> --fixture-dir <new-fixture-directory> `
  --source <revision-and-worktree-state> --host-conditions <observed-load> --output <new-report.json>

python test\benchmark_parallel_reads.py --approved-live-read `
  --serial-shell <archived-serial-duckdb.exe> --candidate-shell build\release\duckdb.exe `
  --workspace <approved-workspace-id> --item <approved-item-id> `
  --candidate-source <revision-and-worktree-state> --host-conditions <observed-load> `
  --output <new-report.json>
```

Build the Rust unit-test executable with `cargo test --release --no-run --locked
--manifest-path rust\onelake_table_reader\Cargo.toml`. The deterministic benchmark
uses a separate loopback server and eight streams of 32 batches x 4,096 rows,
including strings, NULLs and dictionaries. It compares caps 1/2/4/8 with balanced,
skewed, fast, stalled-consumer, early-stop and single-stream controls. Each
configuration has one excluded warm-up and five measured runs.

The live benchmark requires an explicitly approved PricePaid-compatible table.
It compares an archived serial build with the candidate at the **same threads=4**
setting, with TABLE_API, Azure CLI credentials and both read caches off. Sign in
to the approved tenant with Azure CLI before running it. It reserves attempts durably
before launching a fresh process, refuses report overwrite and never retries.
Two narrow-aggregate pilots gate the remaining three paired repetitions of
narrow aggregation, wider County grouping and LIMIT 10. The hard ceiling is
20 attempts, including 14 full scans; failures consume the budget. A single-stream
allocation or inconsistent pilot result stops the comparison.

Reports include wall time, process CPU, rows/bytes per second, first-batch latency,
stream diagnostics and LIMIT amplification. On Windows, OS counters supply process
peak working set (including setup) and sampled query working set/private bytes.
Sampled peaks can miss short spikes; process memory is not DuckDB-managed memory.
Resource collection is explicitly unavailable where unsupported. Decimal sums
compare Price rounded to two decimal places to avoid floating-point reduction-order
differences. Full-result comparisons assume a stable source; unordered LIMIT
values need not match. Reports retain per-run data and median/ranges, not p95 claims
from three pairs. No arbitrary speedup threshold replaces correctness checks.

Run Python regressions with `python -m unittest discover -s test -p 'test_benchmark*.py'`.
Set `ONELAKE_BENCH_SMOKE_SHELL` to a built shell to include the offline prepared-SELECT
resource-collection smoke test. These tests do not contact OneLake.