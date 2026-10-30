#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "onelake_cached_scan.hpp"
#include "onelake_cache_storage.hpp"

#include <iostream>
#include <chrono>
#include <algorithm>

using namespace duckdb;

namespace {
struct Fixture {
	shared_ptr<OneLakeCacheLifetime> lifetime;
	idx_t opens = 0;
	idx_t batches = 0;
	idx_t version = 0;
	idx_t policy = 0;
	idx_t rows = 8192;
	bool granted = true;
	bool validation = true;
	bool fail = false;
	bool cancel = false;
	idx_t probes = 0;
	vector<string> names {"Price", "id"};
	vector<LogicalType> types {LogicalType::DOUBLE, LogicalType::BIGINT};
	vector<string> requested;
	bool null_carrier = false;
	bool drift = false;
};

class Stream : public ArrowArrayStreamWrapper {
public:
	Stream(ClientContext &context, shared_ptr<Fixture> fixture, vector<string> names)
	    : context(context), fixture(std::move(fixture)), names(std::move(names)),
	      properties(context.GetClientProperties()) {
		for (auto &name : this->names) {
			auto found = std::find(this->fixture->names.begin(), this->fixture->names.end(), name);
			types.push_back(this->fixture->types[found - this->fixture->names.begin()]);
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
				if (fixture->null_carrier && names[c] == "carrier") {
					chunk.SetValue(c, i, Value(types[c]));
				} else if (types[c] == LogicalType::DOUBLE) {
					FlatVector::GetData<double>(chunk.data[c])[i] = double(value);
				} else if (types[c] == LogicalType::BIGINT) {
					FlatVector::GetData<int64_t>(chunk.data[c])[i] = int64_t(value);
				} else if (types[c] == LogicalType::VARCHAR) {
					chunk.SetValue(c, i, Value(string(256, 'x')));
				} else {
					chunk.SetValue(c, i, Value::INTEGER(int32_t(value % 100)).DefaultCastAs(types[c]));
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
	explicit Source(shared_ptr<Fixture> fixture) : fixture(std::move(fixture)), lifetime(this->fixture->lifetime) {
	}
	string CacheKey() const override {
		return "synthetic-test-source";
	}
	OneLakeCacheLifetime &Lifetime() const override {
		return *lifetime;
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
			fixture->requested = columns;
			if (fixture->drift) {
				fixture->types.back() = LogicalType::DOUBLE;
			}
		} else {
			++fixture->probes;
		}
		OneLakeOpenedRead opened;
		opened.stream = make_uniq<Stream>(context, fixture, columns.empty() ? fixture->names : columns);
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
	shared_ptr<OneLakeCacheLifetime> lifetime;
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

class FixtureTable : public OneLakeTableEntry {
public:
	FixtureTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, shared_ptr<Source> source)
	    : OneLakeTableEntry(catalog, schema, info), source(std::move(source)) {
	}
	shared_ptr<Source> source;

protected:
	TableFunction BindTableRead(ClientContext &context, unique_ptr<FunctionData> &data, vector<string> &names,
	                            vector<LogicalType> &types) override {
		data = BindOneLakeCachedScan(context, source, false, names, types);
		return OneLakeCachedScanFunction();
	}
};

struct CatalogBindInfo : TableFunctionInfo {
	explicit CatalogBindInfo(shared_ptr<FixtureTable> table) : table(std::move(table)) {
	}
	shared_ptr<FixtureTable> table;
	bool hydrate = true;
	bool fail_after_hydrate = false;
	bool deny_after_hydrate = false;
	bool second_bind = false;
};

unique_ptr<FunctionData> BindCatalogFixture(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &types, vector<string> &names) {
	auto &info = static_cast<CatalogBindInfo &>(*input.info);
	if (info.hydrate) {
		info.table->EnsureColumnDefinitions(context);
	}
	if (info.fail_after_hydrate) {
		throw IOException("Synthetic post-lookup failure");
	}
	if (info.deny_after_hydrate) {
		info.table->source->fixture->granted = false;
	}
	unique_ptr<FunctionData> data;
	info.table->GetScanFunction(context, data);
	if (info.second_bind) {
		info.table->GetScanFunction(context, data);
	}
	for (const auto &column : info.table->GetColumns().Logical()) {
		names.push_back(column.Name());
		types.push_back(column.Type());
	}
	return data;
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

void TestCountProjection(DuckDB &db) {
	auto fixture = make_shared_ptr<Fixture>();
	fixture->lifetime = make_shared_ptr<OneLakeCacheLifetime>(*db.instance);
	fixture->names = {"wide", "Price", "carrier", "tie"};
	fixture->types = {LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::TINYINT, LogicalType::TINYINT};
	fixture->null_carrier = true;
	auto function = OneLakeCachedScanFunction();
	function.name = "count_fixture";
	function.bind = Bind;
	// Match the row-id projection supplied by TableCatalogEntry for count-only scans.
	function.get_virtual_columns = [](ClientContext &, optional_ptr<FunctionData>) {
		virtual_column_map_t columns;
		columns.emplace(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::ROW_TYPE));
		return columns;
	};
	function.function_info = make_shared_ptr<Info>(make_shared_ptr<Source>(fixture));
	ExtensionLoader loader(*db.instance, "count_tests");
	loader.RegisterFunction(std::move(function));
	Connection connection(db);
	Check(Scalar(connection, "SELECT count(*) FROM count_fixture()") == 8192 &&
	          fixture->requested == vector<string> {"carrier"},
	      "Count did not choose the first narrow primitive, or counted its NULLs instead of rows");
	auto opens = fixture->opens;
	Check(Scalar(connection, "SELECT count(*) FROM count_fixture()") == 8192 && fixture->opens == opens,
	      "Narrow-carrier cached count failed");
	Check(Scalar(connection, "SELECT count(carrier) FROM count_fixture()") == 0, "COUNT(column) lost NULL semantics");
	Check(Scalar(connection, "SELECT count(*) FROM count_fixture() WHERE Price >= 8000") == 192 &&
	          fixture->requested == vector<string> {"Price"},
	      "Count carrier displaced a filter-only column");
	Check(Scalar(connection, "SELECT sum(Price)::BIGINT FROM (SELECT Price FROM count_fixture() LIMIT 10)") == 45 &&
	          fixture->requested == vector<string> {"Price"},
	      "Count carrier displaced a real LIMIT value projection");
	Query(connection, "SET enable_external_file_cache=false");
	Check(Scalar(connection, "SELECT count(*) FROM (SELECT wide FROM count_fixture() LIMIT 10)") == 10 &&
	          fixture->requested == vector<string> {"carrier"},
	      "Count-over-LIMIT did not use the narrow authorized carrier");
	fixture->rows = 0;
	Check(Scalar(connection, "SELECT count(*) FROM count_fixture()") == 0, "Empty count carrier failed");
	fixture->rows = 8192;
	fixture->validation = false;
	fixture->names = {"wide", "carrier"};
	fixture->types = {LogicalType::VARCHAR, LogicalType::BOOLEAN};
	Check(Scalar(connection, "SELECT count(*) FROM count_fixture()") == 8192 &&
	          fixture->requested == vector<string> {"carrier"},
	      "Boolean count carrier failed");
	fixture->drift = true;
	Check(connection.Query("SELECT count(*) FROM count_fixture()")->HasError(),
	      "Count-only execution ignored a changed selected-column layout");
	fixture->drift = false;
	fixture->null_carrier = false;
	fixture->types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	Check(Scalar(connection, "SELECT count(*) FROM count_fixture()") == 8192 &&
	          fixture->requested == vector<string> {"wide"},
	      "Variable-width-only schema did not preserve the first-column fallback");
	Query(connection, "SET enable_external_file_cache=true");
	std::cout << "Narrow count projection, NULLs, filters, LIMIT, cache and schema drift passed\n";
}
} // namespace

void TestCatalogRetirement() {
	DBConfig config;
	config.options.temporary_directory = "";
	DuckDB db(nullptr, &config);
	Connection connection(db);
	Query(connection, "SET temp_directory=''");
	Query(connection, "LOAD onelake");
	Query(connection, "CREATE SECRET detach_test (TYPE ONELAKE, PROVIDER credential_chain, CHAIN 'env')");
	Query(connection, "ATTACH '00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002' "
	                  "AS files_default (TYPE ONELAKE)");
	auto files = DatabaseManager::Get(*db.instance).GetDatabase("files_default");
	auto &files_catalog = static_cast<OneLakeCatalog &>(files->GetCatalog());
	Check(!files_catalog.UsesTableAPI(), "Omitted READ_MODE did not select the direct-file catalog path");
	files_catalog.RequireWriteSupport();
	Query(connection, "DETACH files_default");
	Query(connection, "ATTACH '00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002' "
	                  "AS retirement (TYPE ONELAKE, READ_MODE 'TABLE_API', READ_CACHE 'PARQUET')");
	// Keep the catalog alive after DETACH: cleanup must use the hook, not just destruction.
	auto attached = DatabaseManager::Get(*db.instance).GetDatabase("retirement");
	auto &catalog = static_cast<OneLakeCatalog &>(attached->GetCatalog());
	Check(catalog.UsesTableAPI(), "Explicit TABLE_API did not select the secured catalog path");
	CreateSchemaInfo schema_info;
	schema_info.schema = "fixture";
	OneLakeSchemaEntry schema(catalog, schema_info);
	CreateTableInfo table_info;
	table_info.table = "source";
	auto bind_fixture = make_shared_ptr<Fixture>();
	bind_fixture->lifetime = catalog.ReadCacheLifetime();
	auto fixture_table =
	    make_shared_ptr<FixtureTable>(catalog, schema, table_info, make_shared_ptr<Source>(bind_fixture));
	auto bind_info = make_shared_ptr<CatalogBindInfo>(fixture_table);
	auto bind_function = OneLakeCachedScanFunction();
	bind_function.name = "catalog_fixture";
	bind_function.bind = BindCatalogFixture;
	bind_function.function_info = bind_info;
	ExtensionLoader bind_loader(*db.instance, "bind_tests");
	bind_loader.RegisterFunction(std::move(bind_function));
	const string bind_query = "SELECT sum(Price)::BIGINT FROM catalog_fixture()";
	for (idx_t i = 0; i < 2; ++i) {
		auto probes = bind_fixture->probes;
		Check(Scalar(connection, bind_query) == 33550336 && bind_fixture->probes == probes + 1,
		      "Catalog hydration did not hand off its schema bind");
	}
	Query(connection, "BEGIN");
	auto probes = bind_fixture->probes;
	Query(connection, bind_query);
	Query(connection, bind_query);
	Check(bind_fixture->probes == probes + 2, "Schema bind crossed statement boundaries in a transaction");
	Query(connection, "COMMIT");
	bind_info->second_bind = true;
	probes = bind_fixture->probes;
	Query(connection, bind_query);
	Check(bind_fixture->probes == probes + 2, "Pending schema bind was consumed more than once");
	bind_info->second_bind = false;
	probes = bind_fixture->probes;
	Query(connection, "SELECT sum(a.Price+b.Price) FROM catalog_fixture() a JOIN catalog_fixture() b ON a.id=b.id");
	Check(bind_fixture->probes == probes + 2, "Multiple table references shared mutable bind data");
	bind_info->fail_after_hydrate = true;
	Check(connection.Query(bind_query)->HasError(), "Synthetic bind failure was ignored");
	bind_info->fail_after_hydrate = false;
	bind_info->hydrate = false;
	probes = bind_fixture->probes;
	Query(connection, bind_query);
	Check(bind_fixture->probes == probes + 1, "Failed query retained pending bind data");
	bind_info->hydrate = true;
	bind_info->deny_after_hydrate = true;
	Check(connection.Query(bind_query)->HasError(), "Bind handoff skipped identity validation");
	bind_info->deny_after_hydrate = false;
	bind_fixture->granted = true;
	bind_fixture->types[0] = LogicalType::BIGINT;
	Check(connection.Query(bind_query)->HasError(), "Later statement reused an obsolete schema");
	bind_fixture->types[0] = LogicalType::DOUBLE;
	Connection bind_isolated(db);
	probes = bind_fixture->probes;
	Query(bind_isolated, bind_query);
	Check(bind_fixture->probes == probes + 1, "Bind data crossed connection boundaries");
	unique_ptr<FunctionData> standalone;
	probes = bind_fixture->probes;
	fixture_table->EnsureColumnDefinitions(*connection.context);
	fixture_table->GetScanFunction(*connection.context, standalone);
	Check(bind_fixture->probes == probes + 2, "No-active-query binding reused pending state");
	std::cout << "Statement-local, consume-once schema handoff and validation passed\n";
	auto fixture = make_shared_ptr<Fixture>();
	fixture->lifetime = catalog.ReadCacheLifetime();
	auto function = OneLakeCachedScanFunction();
	function.name = "detach_fixture";
	function.bind = Bind;
	function.function_info = make_shared_ptr<Info>(make_shared_ptr<Source>(fixture));
	ExtensionLoader loader(*db.instance, "detach_tests");
	loader.RegisterFunction(std::move(function));
	Query(connection, "SET onelake_enable_read_metrics=true");
	Query(connection, "PREPARE detached_scan AS SELECT sum(Price)::BIGINT FROM detach_fixture()");
	Check(Scalar(connection, "EXECUTE detached_scan") == 33550336, "Attached synthetic source failed");
	const string output_rows =
	    "SELECT value FROM onelake_read_metrics() WHERE phase='execution' AND metric='output_rows'";
	Check(Scalar(connection, output_rows) == 8192, "Arrow output metrics did not count native scan rows");
	Check(Scalar(connection, "SELECT value > 0 FROM onelake_read_metrics() "
	                         "WHERE phase='execution' AND metric='arrow_operator_ns'") == 1,
	      "Arrow operator timing was not recorded");
	Connection isolated(db);
	Check(Scalar(isolated, "SELECT sum(value)::BIGINT FROM onelake_read_metrics()") == 0,
	      "Read metrics crossed connection boundaries");
	auto opens = fixture->opens;
	Check(Scalar(connection, "EXECUTE detached_scan") == 33550336 && fixture->opens == opens,
	      "Attached synthetic cache did not hit");
	Check(Scalar(connection, output_rows) == 8192, "Parquet cache hit was incorrectly counted as Arrow output");
	Query(connection, "DETACH retirement");
	Check(connection.Query(bind_query)->HasError(), "Retired catalog permitted another schema handoff");
	Check(connection.Query("EXECUTE detached_scan")->HasError() && fixture->opens == opens,
	      "SQL DETACH left an old prepared cache usable while catalog was retained");
	std::cout << "SQL DETACH retired cached prepared scans before catalog destruction\n";
}

void TestCachedScan(DuckDB &db) {
	TestCountProjection(db);
	auto fixture = make_shared_ptr<Fixture>();
	fixture->lifetime = make_shared_ptr<OneLakeCacheLifetime>(*db.instance);
	auto function = OneLakeCachedScanFunction();
	function.name = "cache_fixture";
	function.bind = Bind;
	auto info = make_shared_ptr<Info>(make_shared_ptr<Source>(fixture));
	function.function_info = info;
	ExtensionLoader loader(*db.instance, "onelake_cache_tests");
	loader.RegisterFunction(std::move(function));
	RegisterOneLakeCacheOptimizer(*db.instance);
	Connection connection(db);
	Query(connection, "SET threads=2");
	const string sum = "SELECT sum(Price)::BIGINT FROM cache_fixture()";
	auto &manager = BufferManager::GetBufferManager(*db.instance);
	auto directory = manager.GetTemporaryDirectory();
	Check(OneLakeCacheDirectoryIsPrivate(directory), "Test requires an existing private spill directory");
	Check(!OneLakeCacheDirectoryIsPrivate("relative-cache-directory"), "Relative directory was admitted");
	Check(!OneLakeCacheDirectoryIsPrivate(directory + "\\missing"), "Missing directory was admitted");
	Query(connection, "SET temp_directory='" + directory + "\\missing'");
	auto admission_opens = fixture->opens;
	Query(connection, sum);
	Query(connection, sum);
	Check(fixture->opens == admission_opens + 2, "Unverified spill directory reused or filled cache");
	Query(connection, "SET temp_directory='" + directory + "'");
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
	auto other_fixture = make_shared_ptr<Fixture>();
	other_fixture->lifetime = make_shared_ptr<OneLakeCacheLifetime>(*db.instance);
	auto other_function = OneLakeCachedScanFunction();
	other_function.name = "cache_other";
	other_function.bind = Bind;
	other_function.function_info = make_shared_ptr<Info>(make_shared_ptr<Source>(other_fixture));
	loader.RegisterFunction(std::move(other_function));
	Query(connection, "SELECT sum(Price) FROM cache_other()");
	Query(connection, "SELECT sum(Price) FROM cache_other()");
	auto other_opens = other_fixture->opens;
	auto previous = Scalar(connection, sum);
	Scalar(partial, sum);
	Query(connection, "PREPARE retired AS SELECT sum(Price)::BIGINT FROM cache_fixture()");
	auto used_before_retire = manager.GetUsedMemory();
	fixture->lifetime->Retire();
	Check(manager.GetUsedMemory() < used_before_retire, "Attachment retirement retained managed data");
	Query(connection, "SELECT sum(Price) FROM cache_other()");
	Check(other_fixture->opens == other_opens, "Retirement flushed an unrelated namespace");
	opens = fixture->opens;
	Check(connection.Query("EXECUTE retired")->HasError() && partial.Query(sum)->HasError() && fixture->opens == opens,
	      "Retired attachment allowed a prepared or cross-connection scan");
	// Use another connection's entry: a failing query intentionally clears its own cache.
	Query(projection, "SELECT sum(Price) FROM cache_other()");
	auto unrelated_opens = other_fixture->opens;
	fixture->lifetime->Retire();
	Query(projection, "SELECT sum(Price) FROM cache_other()");
	Check(other_fixture->opens == unrelated_opens, "Retirement flushed an unrelated namespace");
	fixture->lifetime = make_shared_ptr<OneLakeCacheLifetime>(*db.instance);
	info->source = make_shared_ptr<Source>(fixture);
	++fixture->version;
	Check(Scalar(connection, sum) == previous + 819200000 && fixture->opens == opens + 1,
	      "New attachment generation reused retired data");
	Check(connection.Query("EXECUTE retired")->HasError(), "Old prepared statement adopted a new attachment");
	std::cout << "Guarded hits, stale/denied/missing validation, partial coverage and no-replay failures passed\n";
}
