#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "onelake_cached_scan.hpp"
#include "onelake_cache_storage.hpp"

#include <iostream>
#include <chrono>

using namespace duckdb;

namespace {
struct Fixture {
	idx_t opens = 0;
	idx_t batches = 0;
	idx_t version = 0;
	idx_t policy = 0;
	idx_t rows = 8192;
	bool granted = true;
	bool validation = true;
	bool fail = false;
	bool cancel = false;
};

class Stream : public ArrowArrayStreamWrapper {
public:
	Stream(ClientContext &context, shared_ptr<Fixture> fixture, vector<string> names)
	    : context(context), fixture(std::move(fixture)), names(std::move(names)),
	      properties(context.GetClientProperties()) {
		for (auto &name : this->names) {
			types.push_back(name == "Price" ? LogicalType::DOUBLE : LogicalType::BIGINT);
		}
		arrow_array_stream = {};
		arrow_array_stream.private_data = this;
		arrow_array_stream.get_schema = [](ArrowArrayStream *stream, ArrowSchema *out) {
			auto &self = *static_cast<Stream *>(stream->private_data);
			ArrowConverter::ToArrowSchema(out, self.types, self.names, self.properties);
			return 0;
		};
		arrow_array_stream.get_last_error = [](ArrowArrayStream *) {
			return "synthetic fixture";
		};
		arrow_array_stream.release = [](ArrowArrayStream *stream) {
			stream->release = nullptr;
		};
	}
	shared_ptr<ArrowArrayWrapper> GetNextChunk() override {
		if (fixture->cancel && offset) {
			context.interrupted = true;
			throw InterruptException();
		}
		if (fixture->fail && offset) {
			throw IOException("Synthetic truncated stream");
		}
		auto result = make_shared_ptr<ArrowArrayWrapper>();
		if (offset == fixture->rows) {
			return result;
		}
		DataChunk chunk;
		chunk.Initialize(context, types);
		chunk.SetCardinality(MinValue<idx_t>(STANDARD_VECTOR_SIZE, fixture->rows - offset));
		for (idx_t c = 0; c < names.size(); ++c) {
			for (idx_t i = 0; i < chunk.size(); ++i) {
				auto value = offset + i + fixture->version * 100000;
				if (names[c] == "Price") {
					FlatVector::GetData<double>(chunk.data[c])[i] = double(value);
				} else {
					FlatVector::GetData<int64_t>(chunk.data[c])[i] = int64_t(value);
				}
			}
		}
		offset += chunk.size();
		++fixture->batches;
		ArrowConverter::ToArrowArray(chunk, &result->arrow_array, properties, {});
		return result;
	}

private:
	ClientContext &context;
	shared_ptr<Fixture> fixture;
	vector<string> names;
	vector<LogicalType> types;
	ClientProperties properties;
	idx_t offset = 0;
};

class Source : public OneLakeScanSource {
public:
	explicit Source(shared_ptr<Fixture> fixture) : fixture(std::move(fixture)) {
	}
	string CacheKey() const override {
		return "synthetic-test-source";
	}
	void CheckIdentity(ClientContext &) override {
		if (!fixture->granted) {
			throw IOException("Synthetic access denied");
		}
	}
	OneLakeOpenedRead Open(ClientContext &context, const vector<string> &columns) override {
		CheckIdentity(context);
		if (!columns.empty()) {
			++fixture->opens;
		}
		OneLakeOpenedRead opened;
		opened.stream = make_uniq<Stream>(context, fixture, columns.empty() ? vector<string> {"Price", "id"} : columns);
		if (fixture->validation) {
			auto version = fixture->version;
			auto policy = fixture->policy;
			opened.revalidate = [fixture = fixture, version, policy](ClientContext &) {
				return fixture->validation && fixture->granted && fixture->version == version &&
				       fixture->policy == policy;
			};
		}
		return opened;
	}
	shared_ptr<Fixture> fixture;
};

struct Info : TableFunctionInfo {
	explicit Info(shared_ptr<Source> source) : source(std::move(source)) {
	}
	shared_ptr<Source> source;
};

unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &types,
                              vector<string> &names) {
	return BindOneLakeCachedScan(context, static_cast<Info &>(*input.info).source, true, names, types);
}

unique_ptr<MaterializedQueryResult> Query(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error(sql + ": " + result->GetError());
	}
	return result;
}

void Check(bool value, const char *message) {
	if (!value) {
		throw std::runtime_error(message);
	}
}

int64_t Scalar(Connection &connection, const string &sql) {
	return Query(connection, sql)->GetValue(0, 0).GetValue<int64_t>();
}

// Test-only fault injection into the native child; no production override exists.
table_function_t parquet_scan;
int fail_after = -1;
idx_t parquet_rows = 0;
void InstrumentedParquet(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	if (fail_after == 0) {
		throw IOException("Synthetic cached read failure");
	}
	parquet_scan(context, input, output);
	parquet_rows += output.size();
	if (output.size() && fail_after > 0) {
		--fail_after;
	}
}

void Benchmark(DuckDB &db, Fixture &fixture) {
	fixture.rows = 262144;
	++fixture.version;
	const vector<string> queries {"SELECT sum(Price)::BIGINT FROM cache_fixture()",
	                              "SELECT count(*) FROM cache_fixture()",
	                              "SELECT sum(Price)::BIGINT FROM cache_fixture() WHERE Price > 462000"};
	for (auto &query : queries) {
		Connection connection(db);
		int64_t expected = 0;
		for (auto &mode : {"uncached", "fill", "hit"}) {
			Query(connection,
			      string("SET enable_external_file_cache=") + (string(mode) == "uncached" ? "false" : "true"));
			// An unavailable validator forces the same original Arrow path without retention.
			fixture.validation = string(mode) != "uncached";
			auto opens = fixture.opens;
			auto batches = fixture.batches;
			auto start = std::chrono::steady_clock::now();
			auto value = Scalar(connection, query);
			auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
			if (string(mode) == "uncached") {
				expected = value;
			}
			Check(value == expected, "Benchmark result mismatch");
			Check(fixture.opens - opens == (string(mode) == "hit" ? 0 : 1), "Benchmark cache path mismatch");
			std::cout << "BENCH " << mode << " ms=" << elapsed << " opens=" << fixture.opens - opens
			          << " batches=" << fixture.batches - batches << " result=" << value << " sql=" << query << '\n';
		}
	}
	Connection connection(db);
	for (auto &mode : {"uncached", "fill", "hit"}) {
		Query(connection, string("SET enable_external_file_cache=") + (string(mode) == "uncached" ? "false" : "true"));
		auto opens = fixture.opens;
		auto batches = fixture.batches;
		auto start = std::chrono::steady_clock::now();
		auto result = Query(connection, "SELECT Price FROM cache_fixture() LIMIT 10");
		auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
		Check(result->RowCount() == 10, "Partial benchmark result mismatch");
		Check(fixture.opens - opens == (string(mode) == "hit" ? 0 : 1), "Partial benchmark cache path mismatch");
		std::cout << "BENCH " << mode << " ms=" << elapsed << " opens=" << fixture.opens - opens
		          << " batches=" << fixture.batches - batches
		          << " result=10 sql=SELECT Price FROM cache_fixture() LIMIT 10\n";
	}
	fixture.rows = 8192;
}
} // namespace

void TestCachedScan(DuckDB &db) {
	auto fixture = make_shared_ptr<Fixture>();
	auto function = OneLakeCachedScanFunction();
	function.name = "cache_fixture";
	function.bind = Bind;
	function.function_info = make_shared_ptr<Info>(make_shared_ptr<Source>(fixture));
	ExtensionLoader loader(*db.instance, "onelake_cache_tests");
	loader.RegisterFunction(std::move(function));
	RegisterOneLakeCacheOptimizer(*db.instance);
	Connection connection(db);
	Query(connection, "SET threads=2");
	const string sum = "SELECT sum(Price)::BIGINT FROM cache_fixture()";
	Check(Scalar(connection, sum) == 33550336, "Initial Arrow aggregate mismatch");
	auto opens = fixture->opens;
	Check(Scalar(connection, sum) == 33550336 && fixture->opens == opens, "Complete cache hit missed");
	Check(Scalar(connection, "SELECT count(*) FROM cache_fixture()") == 8192 && fixture->opens == opens,
	      "Metadata-only cached count failed");
	auto path =
	    Query(connection, "SELECT path FROM duckdb_external_file_cache() WHERE path LIKE 'onelake-cache://%' LIMIT 1")
	        ->GetValue(0, 0)
	        .GetValue<string>();
	Check(connection.Query("SELECT count(*) FROM read_parquet('" + path + "')")->HasError(),
	      "Raw cache path bypassed snapshot/authorization validation");
	Query(connection, sum);
	opens = fixture->opens;
	Query(connection, "BEGIN");
	auto &parquet = Catalog::GetEntry<TableFunctionCatalogEntry>(*connection.context, SYSTEM_CATALOG, DEFAULT_SCHEMA,
	                                                             "parquet_scan")
	                    .functions.GetFunctionReferenceByOffset(0);
	Query(connection, "COMMIT");
	parquet_scan = parquet.function;
	parquet.function = InstrumentedParquet;
	parquet_rows = 0;
	Check(Scalar(connection, "SELECT sum(Price)::BIGINT FROM cache_fixture() WHERE Price >= 8000") == 1554336,
	      "Cached filter result mismatch");
	Check(parquet_rows == 192 && fixture->opens == opens, "Copied predicates did not reach native Parquet");
	parquet.function = parquet_scan;
	Query(connection, "PREPARE cached_guard AS SELECT count(*) FROM cache_fixture() WHERE Price > 1000000");
	fixture->granted = false;
	Check(connection.Query("EXECUTE cached_guard")->HasError(), "Optimizer bypassed execution authorization");
	fixture->granted = true;
	++fixture->version;
	Check(Scalar(connection, sum) == 852750336, "Stale snapshot was reused");
	Check(fixture->opens == opens + 1, "Stale entry did not cause one fresh read");
	fixture->rows = 4000;
	++fixture->policy;
	Check(Scalar(connection, "SELECT count(*) FROM cache_fixture()") == 4000, "Stale effective policy reused");
	fixture->rows = 8192;
	++fixture->policy;
	Query(connection, sum);
	opens = fixture->opens;
	fixture->validation = false;
	Query(connection, sum);
	Query(connection, sum);
	Check(fixture->opens == opens + 2, "Unavailable validation reused or filled cache");
	fixture->validation = true;
	Query(connection, "SET enable_external_file_cache=false");
	opens = fixture->opens;
	Query(connection, sum);
	Query(connection, sum);
	Check(fixture->opens == opens + 2, "Cache-disable setting was ignored");
	Query(connection, "SET enable_external_file_cache=true");

	Connection partial(db);
	opens = fixture->opens;
	Query(partial, "SELECT Price FROM cache_fixture() LIMIT 10");
	Check(fixture->opens == opens + 1, "Partial fill did not read once");
	auto batches = fixture->batches;
	Query(partial, "SELECT Price FROM cache_fixture() LIMIT 10");
	Check(fixture->opens == opens + 1 && fixture->batches == batches, "Safe partial LIMIT was not reused");
	Check(Scalar(partial, sum) == 852750336 && fixture->opens == opens + 2, "Partial entry answered a full aggregate");
	for (auto &query :
	     {"SELECT Price FROM cache_fixture() LIMIT 10 OFFSET 0", "SELECT Price FROM cache_fixture() LIMIT 10 OFFSET 1",
	      "SELECT Price FROM cache_fixture() WHERE Price > 108000 LIMIT 10",
	      "SELECT Price FROM cache_fixture() ORDER BY Price DESC LIMIT 10",
	      "SELECT DISTINCT Price FROM cache_fixture() LIMIT 10", "SELECT Price+1 FROM cache_fixture() LIMIT 10",
	      "SELECT sum(Price) OVER () FROM cache_fixture() LIMIT 10", "SELECT sum(Price) FROM cache_fixture() LIMIT 10",
	      "SELECT Price FROM cache_fixture() USING SAMPLE 10 ROWS LIMIT 10"}) {
		Connection unsupported(db);
		Query(unsupported, "SELECT Price FROM cache_fixture() LIMIT 10");
		opens = fixture->opens;
		Query(unsupported, query);
		Check(fixture->opens == opens + 1, "Unsupported partial shape reused incomplete data");
	}
	Connection unoptimized(db);
	Query(unoptimized, "SELECT Price FROM cache_fixture() LIMIT 10");
	Query(unoptimized, "PRAGMA disable_optimizer");
	opens = fixture->opens;
	Query(unoptimized, "SELECT Price FROM cache_fixture() LIMIT 10");
	Check(fixture->opens == opens + 1, "Disabled optimizer reused partial entry");
	Connection insufficient(db);
	Query(insufficient, "SELECT Price FROM cache_fixture() LIMIT 10");
	opens = fixture->opens;
	Check(Query(insufficient, "SELECT Price FROM cache_fixture() LIMIT 8192")->RowCount() == 8192 &&
	          fixture->opens == opens + 1,
	      "Insufficient partial coverage was reused");

	Connection projection(db);
	auto reordered = Query(projection, "SELECT sum(id)::BIGINT, sum(Price)::BIGINT FROM cache_fixture()");
	Check(reordered->GetValue(0, 0) == reordered->GetValue(1, 0), "Arrow projection reorder failed");
	opens = fixture->opens;
	reordered = Query(projection, "SELECT sum(id)::BIGINT, sum(Price)::BIGINT FROM cache_fixture()");
	Check(reordered->GetValue(0, 0).GetValue<int64_t>() == 852750336 && fixture->opens == opens,
	      "Parquet projection reorder failed");
	Query(projection, "SELECT sum(Price)::BIGINT, sum(id)::BIGINT FROM cache_fixture()");
	Check(Scalar(projection, "SELECT sum(id)::BIGINT FROM cache_fixture() WHERE Price >= 108000") == 20754336,
	      "Filter-only column mapping failed");

	Connection empty(db);
	fixture->rows = 0;
	++fixture->version;
	Check(Scalar(empty, "SELECT count(*) FROM cache_fixture()") == 0, "Empty Arrow source failed");
	opens = fixture->opens;
	Check(Scalar(empty, "SELECT count(*) FROM cache_fixture()") == 0 && fixture->opens == opens,
	      "Empty complete cache failed");
	fixture->rows = 8192;
	--fixture->version;

	Connection faults(db);
	Query(faults, sum);
	auto original_init = parquet.init_global;
	parquet.init_global = [](ClientContext &, TableFunctionInitInput &) -> unique_ptr<GlobalTableFunctionState> {
		throw IOException("Synthetic cached global-init failure");
	};
	opens = fixture->opens;
	Check(Scalar(faults, sum) == 852750336 && fixture->opens == opens + 1,
	      "Cache global initialization failed to fall back once");
	parquet.init_global = original_init;
	Query(faults, sum);
	auto original_local = parquet.init_local;
	parquet.init_local = [](ExecutionContext &, TableFunctionInitInput &,
	                        GlobalTableFunctionState *) -> unique_ptr<LocalTableFunctionState> {
		throw IOException("Synthetic cached local-init failure");
	};
	opens = fixture->opens;
	Check(Scalar(faults, sum) == 852750336 && fixture->opens == opens + 1,
	      "Cache local initialization failed to fall back once");
	parquet.init_local = original_local;
	Query(faults, sum);
	parquet.function = InstrumentedParquet;
	fail_after = 0;
	opens = fixture->opens;
	Check(Scalar(faults, sum) == 852750336 && fixture->opens == opens + 1,
	      "Before-output cache failure did not fall back exactly once");
	parquet.function = parquet_scan;
	Query(faults, sum);
	parquet.function = InstrumentedParquet;
	fail_after = 1;
	opens = fixture->opens;
	Check(faults.Query(sum)->HasError() && fixture->opens == opens,
	      "After-output cache failure replayed source or returned incomplete result");
	parquet.function = parquet_scan;
	fail_after = -1;
	Check(Scalar(faults, sum) == 852750336 && fixture->opens == opens + 1,
	      "Failed cached read retained stale generation");

	Connection failed(db);
	fixture->fail = true;
	opens = fixture->opens;
	Check(failed.Query(sum)->HasError(), "Truncated stream succeeded");
	Check(fixture->opens == opens + 1, "Failed stream was replayed");
	fixture->fail = false;
	Check(Scalar(failed, sum) == 852750336 && fixture->opens == opens + 2, "Failed generation was reused");
	Connection cancelled(db);
	fixture->cancel = true;
	opens = fixture->opens;
	Check(cancelled.Query(sum)->HasError() && fixture->opens == opens + 1, "Cancellation replayed the source");
	fixture->cancel = false;
	Check(Scalar(cancelled, sum) == 852750336 && fixture->opens == opens + 2, "Cancellation retained a generation");

	// A cache-local storage failure must not replay or fail the authorized source.
	Connection capacity(db);
	Query(capacity, "SET memory_limit='16MB'");
	Query(capacity, "SET max_temp_directory_size='0B'");
	// Exercise the policy by exhausting storage through a large synthetic fill.
	fixture->rows = 2000000;
	++fixture->version;
	opens = fixture->opens;
	Check(Scalar(capacity, "SELECT count(*) FROM cache_fixture()") == int64_t(fixture->rows),
	      "Cache pressure failed the original Arrow scan");
	Check(fixture->opens == opens + 1, "Cache pressure restarted the source");
	opens = fixture->opens;
	Query(capacity, "SELECT count(*) FROM cache_fixture()");
	Check(fixture->opens == opens + 1, "Failed cache fill retained a complete entry");
	Query(capacity, "SET memory_limit='4GB'");
	Query(capacity, "SET max_temp_directory_size='64MB'");
	fixture->rows = 8192;
	Benchmark(db, *fixture);
	std::cout << "Guarded hits, stale/denied/missing validation, partial coverage and no-replay failures passed\n";
}
