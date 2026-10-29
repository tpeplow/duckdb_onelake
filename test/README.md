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