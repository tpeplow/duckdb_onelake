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

## Guarded Arrow-to-Parquet cache (experimental)

`READ_CACHE 'OFF'` is the default. Request the experimental path with:

```sql
ATTACH '<workspace-guid>/<item-guid>' AS lake
    (TYPE ONELAKE, READ_MODE 'TABLE_API', READ_CACHE 'PARQUET', READ_ONLY);
```

`PARQUET` is rejected with `DIRECT_FILES`. Setting
`enable_external_file_cache=false` bypasses this cache too.
**Live OneLake reads currently always miss and do not fill.** The deployed
read-allocation contract provides no supported validator for both the exact
source snapshot and effective authorization. The live adapter supplies none;
a TTL, matching token, schema or file version is not a substitute. There is
no SQL override. Normal secured Arrow reads continue without encoding overhead.

Configure with `-DONELAKE_BUILD_CACHE_PROTOTYPE=ON` and build the
`onelake_cache_storage_test` target. Run it with a new private temporary-directory
path as its sole argument. It uses synthetic data only: no Fabric access,
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
Every hit checks identity and revalidates before native Parquet initialization,
including metadata-only COUNT and impossible predicates. Simple copied predicates
enable native filtering while the original SQL predicates remain residual.
Guarded files can be opened only inside that scan's scoped native callbacks;
direct `read_parquet()` calls cannot use a discovered private path to skip
revalidation. The current adapter is single-threaded; future asynchronous child
readers must propagate this permission scope explicitly, not weaken it.
Cache-local fill errors discard the entry and continue the same Arrow stream.
Cache-read errors can fall back to a fresh authorized stream only before any
output; after output they fail the query, never replaying it.

Files and manifests are connection-private. Data blocks and the block index use
DuckDB memory/spill accounting; normal cleanup does not promise secure erasure.
The current geometry (one fragment per received batch, 2,048-row groups, Snappy)
is untuned. Encoding performs an additional Arrow-to-DuckDB conversion. Metadata
and allocator overhead are not a strict process-RSS bound. Before enabling live
retention, establish the service validator, private spill-directory admission,
complete accounting, and attachment-retirement handling. These remain production
gates, not capabilities established by synthetic tests.

## Rust table reader

ATTACH defaults to the secured reader. The explicit `DIRECT_FILES` mode retains
the legacy scan path. `sql/onelake_read_modes.test` covers mode validation and
lazy attachment without contacting Fabric. Existing remote-write examples must
explicitly select DIRECT_FILES until independently authorized writes are implemented.

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
They cover multiple streams, empty tables, dictionary/null preservation, truncated
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

The initial reader consumes independent streams sequentially, buffers at most one
IPC message internally, and rejects metadata over 1 MiB, bodies over 256 MiB,
cumulative dictionary payload over 256 MiB per stream, and compressed IPC. Arrow
conversion and batches retained by the consumer require additional memory.
It uses a 15-second connection timeout, a 60-second per-request timeout, and a
60-minute local session deadline. Cancellation is cooperative; a blocked network
read can take up to its request timeout to return. No retries, redirects,
automatic token refresh, session reallocation, or direct-file fallback occur.
Acquire a fresh caller token before opening a reader; mid-session authentication
failure ends the scan.