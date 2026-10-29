#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "onelake_cache_storage.hpp"

#include <iostream>

using namespace duckdb;

void TestCachedScan(DuckDB &db);
static void Require(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

static unique_ptr<MaterializedQueryResult> Query(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error(result->GetError());
	}
	return result;
}

template <class FUNC>
static void MustFail(FUNC function) {
	bool failed = false;
	try {
		function();
	} catch (const std::exception &) {
		failed = true;
	}
	Require(failed, "Expected failure did not occur");
}

static void TestParquet(DuckDB &db) {
	Connection connection(db);
	Query(connection, "SET threads=2");
	auto storage = OneLakeCacheStorage::Get(*connection.context);
	auto path = storage->Create();
	Require(connection.Query("SELECT * FROM read_parquet('" + path + "')")->HasError(),
	        "Unsealed data must not be readable");
	Query(connection, "CREATE TEMP TABLE source AS SELECT i, "
	                  "CASE WHEN i % 7 = 0 THEN NULL ELSE 'value-' || i::VARCHAR END AS text_value, "
	                  "(i / 4.0)::DOUBLE AS price, (i / 8.0)::DECIMAL(18,3) AS decimal_value, "
	                  "TIMESTAMP '2020-01-01' + i * INTERVAL '1 second' AS timestamp_value, "
	                  "[i, NULL] AS list_value, {'key': i, 'flag': i % 2 = 0} AS struct_value "
	                  "FROM range(20000) t(i)");
	Query(connection, "COPY source TO '" + path + "' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION SNAPPY)");
	storage->Seal(path);
	auto rows = Query(connection, "SELECT count(*) FROM read_parquet('" + path + "')");
	Require(rows->GetValue(0, 0).GetValue<int64_t>() == 20000, "Parquet count mismatch");
	auto difference = Query(
	    connection, "SELECT count(*) FROM ((SELECT * FROM source EXCEPT ALL SELECT * FROM read_parquet('" + path +
	                    "')) UNION ALL (SELECT * FROM read_parquet('" + path + "') EXCEPT ALL SELECT * FROM source))");
	Require(difference->GetValue(0, 0).GetValue<int64_t>() == 0, "Parquet type/value round trip mismatch");
	auto filtered = Query(connection, "SELECT sum(i) FROM read_parquet('" + path + "') WHERE i >= 19900");
	Require(filtered->GetValue(0, 0).GetValue<int64_t>() == 1994950, "Parquet filtered sum mismatch");
	Query(connection, "PREPARE cached AS SELECT sum(i) FROM read_parquet('" + path + "')");
	Query(connection, "EXECUTE cached");
	Query(connection, "EXECUTE cached");
	auto cached = Query(connection, "SELECT count(*) FROM duckdb_external_file_cache() WHERE path='" + path + "'");
	Require(cached->GetValue(0, 0).GetValue<int64_t>() > 0, "Native Parquet did not use the external-file cache");
	Connection other(db);
	Require(other.Query("SELECT * FROM read_parquet('" + path + "')")->HasError(), "Cross-connection open succeeded");
	MustFail([&] { storage->Lookup(path)->OpenWriter(); });
	storage->Remove(path);
	Require(connection.Query("EXECUTE cached")->HasError(), "Retired prepared scan must not return cached rows");
	auto retired = Query(connection, "SELECT count(*) FROM duckdb_external_file_cache() WHERE path='" + path + "'");
	Require(retired->GetValue(0, 0).GetValue<int64_t>() == 0, "Retired byte ranges were retained");

	auto input = Query(connection, "SELECT * FROM source");
	auto properties = connection.context->GetClientProperties();
	ArrowSchemaWrapper exported_schema;
	ArrowConverter::ToArrowSchema(&exported_schema.arrow_schema, input->types, input->names, properties);
	ArrowTableSchema arrow_schema;
	ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(*connection.context), arrow_schema,
	                                             exported_schema.arrow_schema);
	connection.BeginTransaction();
	OneLakeParquetFragment streamed(*connection.context, input->names, input->types);
	while (auto chunk = input->Fetch()) {
		auto batch = make_shared_ptr<ArrowArrayWrapper>();
		ArrowConverter::ToArrowArray(*chunk, &batch->arrow_array, properties, {});
		streamed.AppendArrowBatch(batch, arrow_schema);
		Require(batch->arrow_array.release != nullptr, "Cache encoder stole the caller's Arrow batch");
	}
	auto streamed_path = streamed.Finish();
	MustFail([&] { streamed.Finish(); });
	connection.Commit();
	Require(Query(connection, "SELECT count(*) FROM (SELECT * FROM source EXCEPT ALL SELECT * FROM read_parquet('" +
	                              streamed_path + "'))")
	                ->GetValue(0, 0)
	                .GetValue<int64_t>() == 0,
	        "Streaming Parquet encoder lost values");
	Require(Query(connection, "SELECT count(*) FROM read_parquet('" + streamed_path + "')")
	                ->GetValue(0, 0)
	                .GetValue<int64_t>() == 20000,
	        "Streaming Parquet encoder row count mismatch");

	auto partial_input = Query(connection, "SELECT * FROM source");
	auto first_chunk = partial_input->Fetch();
	auto first_batch = make_shared_ptr<ArrowArrayWrapper>();
	ArrowConverter::ToArrowArray(*first_chunk, &first_batch->arrow_array, properties, {});
	connection.BeginTransaction();
	OneLakeParquetFragment partial(*connection.context, partial_input->names, partial_input->types);
	partial.AppendArrowBatch(first_batch, arrow_schema);
	auto partial_path = partial.Finish();
	connection.Commit();
	Require(Query(connection, "SELECT count(*) FROM (SELECT * FROM read_parquet('" + partial_path + "') LIMIT 10)")
	                ->GetValue(0, 0)
	                .GetValue<int64_t>() == 10,
	        "Partial fragment LIMIT mismatch");
	Require(Query(connection, "SELECT count(*) FROM read_parquet('" + partial_path + "')")
	                ->GetValue(0, 0)
	                .GetValue<int64_t>() == first_batch->arrow_array.length,
	        "Already received surplus rows were discarded");
	connection.BeginTransaction();
	{
		OneLakeParquetFragment invalid(*connection.context, partial_input->names, partial_input->types);
		ArrowTableSchema wrong_schema;
		MustFail([&] { invalid.AppendArrowBatch(first_batch, wrong_schema); });
		MustFail([&] { invalid.Finish(); });
	}
	connection.Rollback();

	auto empty = storage->Create();
	Query(connection, "COPY (SELECT 1::INTEGER AS value WHERE false) TO '" + empty + "' (FORMAT PARQUET)");
	storage->Seal(empty);
	Require(
	    Query(connection, "SELECT count(*) FROM read_parquet('" + empty + "')")->GetValue(0, 0).GetValue<int64_t>() ==
	        0,
	    "Empty Parquet mismatch");
	storage->Remove(empty);
	Require(Query(connection, "SELECT count(*) FROM duckdb_external_file_cache() WHERE path='" + streamed_path + "'")
	                ->GetValue(0, 0)
	                .GetValue<int64_t>() > 0,
	        "Invalidation cleared another entry's byte ranges");
	storage->Clear();
	std::cout << "Arrow/Parquet round trip, surplus rows, filtered/prepared reads and isolation passed\n";
}

static void TestSpill(DuckDB &db) {
	Connection connection(db);
	Query(connection, "SET memory_limit='8MB'");
	Query(connection, "SET max_temp_directory_size='64MB'");
	auto &manager = BufferManager::GetBufferManager(*connection.context);
	auto storage = OneLakeCacheStorage::Get(*connection.context);
	auto path = storage->Create();
	auto file = storage->Lookup(path);
	file->OpenWriter();
	const auto block_size = manager.GetBlockSize();
	vector<data_t> bytes(block_size);
	for (idx_t i = 0; i < bytes.size(); ++i) {
		bytes[i] = data_t(i % 251);
	}
	for (idx_t i = 0; i < 80; ++i) {
		file->Append(bytes.data(), bytes.size());
	}
	file->Seal();
	Require(manager.GetUsedSwap() > 0, "Expected buffer-managed spill");
	vector<data_t> actual(block_size + 17);
	file->Read(actual.data(), actual.size(), block_size - 9);
	for (idx_t i = 0; i < actual.size(); ++i) {
		Require(actual[i] == bytes[(block_size - 9 + i) % block_size], "Cross-block spill read mismatch");
	}
	MustFail([&] { file->Read(actual.data(), 1, file->Size()); });
	MustFail([&] { file->Append(bytes.data(), 1); });
	storage->Remove(path);
	file.reset();
	Require(manager.GetUsedSwap() == 0, "Spill blocks were not released");

	Query(connection, "SET max_temp_directory_size='0B'");
	auto full_path = storage->Create();
	auto full = storage->Lookup(full_path);
	full->OpenWriter();
	MustFail([&] {
		for (idx_t i = 0; i < 80; ++i) {
			full->Append(bytes.data(), bytes.size());
		}
	});
	MustFail([&] { full->Seal(); });
	storage->Remove(full_path);
	full.reset();
	Require(manager.GetUsedSwap() == 0, "Failed fill retained spill blocks");
	Query(connection, "SELECT 42");
	std::cout << "Shared memory/spill limits, cross-block reads and failed-fill cleanup passed\n";
}

int main(int argc, char **argv) {
	try {
		if (argc != 2) {
			throw std::runtime_error("Usage: onelake_cache_storage_test <private-unused-temp-directory>");
		}
		DBConfig config;
		config.options.temporary_directory = argv[1];
		DuckDB db(nullptr, &config);
		RegisterOneLakeCacheStorage(*db.instance);
		TestParquet(db);
		TestCachedScan(db);
		TestSpill(db);
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
