#include "onelake_table_reader.hpp"
#include "onelake_table_reader_c_api.hpp"
#include "onelake_table_api.hpp"
#include "onelake_cached_scan.hpp"
#include "onelake_read_metrics.hpp"
#include "storage/onelake_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include <json/json.h>
#include <cstring>

namespace duckdb {
namespace {

struct TableReadTarget {
	string origin;
	string workspace;
	string item;
	string schema;
	string table;
	string catalog_url;
};

class TableReadStream : public ArrowArrayStreamWrapper {
public:
	TableReadStream(ClientContext &context_p, OneLakeCredentials &credentials, const TableReadTarget &target,
	                const vector<string> &columns)
	    : context(context_p), reader(nullptr, ol_table_reader_close), metrics(OneLakeReadMetrics::Get(context)),
	      phase(ReadPhase::EXECUTION) {
		if (columns.empty()) {
			throw InternalException("OneLake execution requires an explicit projection");
		}
		OneLakeReadTimer timer(metrics, phase, ReadMetric::READER_OPEN_NS);
		if (metrics) {
			metrics->Add(phase, ReadMetric::READER_OPENS, 1);
		}
		Json::Value request;
		request["baseUrl"] = target.origin;
		request["workspaceId"] = target.workspace;
		request["itemId"] = target.item;
		request["schemaName"] = target.schema;
		request["tableName"] = target.table;
		request["columns"] = Json::Value(Json::arrayValue);
		for (const auto &column : columns) {
			request["columns"].append(column);
		}
		auto token = OneLakeTableAPI::Token(context, credentials);
		auto json = Json::writeString(Json::StreamWriterBuilder(), request);
		char error[512] = {};
		OlTableReader *handle = nullptr;
		OlReadMetrics initial {};
		auto status = ol_table_reader_open_profiled(json.c_str(), token.c_str(), &handle, metrics ? &initial : nullptr,
		                                            error, sizeof(error));
		Publish(initial);
		if (status != 0) {
			throw IOException("%s", error);
		}
		reader.reset(handle);
		if (ol_table_reader_export(reader.get(), &arrow_array_stream, error, sizeof(error)) != 0) {
			throw IOException("%s", error);
		}
	}

	shared_ptr<ArrowArrayWrapper> GetNextChunk() override {
		OneLakeReadTimer timer(metrics, phase, ReadMetric::ARROW_FETCH_NS);
		if (context.interrupted) {
			ol_table_reader_cancel(reader.get());
			throw InterruptException();
		}
		try {
			auto result = ArrowArrayStreamWrapper::GetNextChunk();
			Update();
			return result;
		} catch (...) {
			Update();
			throw;
		}
	}

private:
	void Update() {
		if (metrics) {
			OlReadMetrics current {};
			if (ol_table_reader_metrics(reader.get(), &current) == 0) {
				Publish(current);
			}
		}
	}
	void Publish(const OlReadMetrics &current) {
		if (!metrics) {
			return;
		}
#define PUBLISH(field, metric) metrics->Add(phase, ReadMetric::metric, current.field - previous.field)
		PUBLISH(allocation_requests, ALLOCATION_REQUESTS);
		PUBLISH(allocation_bytes, ALLOCATION_BYTES);
		PUBLISH(allocation_ns, ALLOCATION_NS);
		PUBLISH(stream_requests, STREAM_REQUESTS);
		PUBLISH(stream_open_ns, STREAM_OPEN_NS);
		PUBLISH(stream_bytes, STREAM_BYTES);
		PUBLISH(stream_read_ns, STREAM_READ_NS);
		PUBLISH(ipc_processing_ns, IPC_PROCESSING_NS);
		PUBLISH(batches, BATCHES);
		PUBLISH(rows, ROWS);
		PUBLISH(first_batches, FIRST_BATCHES);
		PUBLISH(first_batch_ns, FIRST_BATCH_NS);
		PUBLISH(completions, COMPLETIONS);
		PUBLISH(errors, ERRORS);
#undef PUBLISH
		previous = current;
	}
	ClientContext &context;
	std::unique_ptr<OlTableReader, decltype(&ol_table_reader_close)> reader;
	shared_ptr<OneLakeReadMetrics> metrics;
	ReadPhase phase;
	OlReadMetrics previous {};
};

class LiveReadSource : public OneLakeScanSource {
public:
	LiveReadSource(OneLakeCatalog &catalog, TableReadTarget target)
	    : catalog(catalog), target(std::move(target)), lifetime(catalog.ReadCacheLifetime()) {
	}
	string CacheKey() const override {
		string key;
		for (auto &part : {target.origin, target.workspace, target.item, target.schema, target.table}) {
			key += ":" + to_string(part.size()) + ":" + part;
		}
		return key;
	}
	void CheckIdentity(ClientContext &context) override {
		lifetime->CheckActive();
		OneLakeTableAPI::Token(context, catalog.GetCredentials());
	}
	OneLakeCacheLifetime &Lifetime() const override {
		return *lifetime;
	}
	OneLakeTableSchema GetSchema(ClientContext &context) override {
		lifetime->CheckActive();
		OneLakeLakehouse item;
		item.id = target.item;
		item.table_catalog_url = target.catalog_url;
		return OneLakeTableAPI::GetTable(context, item, target.schema, target.table, catalog.GetCredentials());
	}
	OneLakeOpenedRead Open(ClientContext &context, const vector<string> &columns) override {
		CheckIdentity(context);
		OneLakeOpenedRead result;
		const auto started = CacheNow();
		const auto ttl = catalog.ReadCacheTTLSeconds();
		if (catalog.UsesReadCache() && !columns.empty() && ttl > 0 &&
		    ttl <=
		        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::time_point::max() - started)
		            .count()) {
			result.cache_expires_at = started + std::chrono::seconds(ttl);
		}
		result.stream = make_uniq<TableReadStream>(context, catalog.GetCredentials(), target, columns);
		// TTL reuse explicitly accepts stale data/policy; no authoritative validator is claimed.
		return result;
	}

private:
	OneLakeCatalog &catalog;
	TableReadTarget target;
	shared_ptr<OneLakeCacheLifetime> lifetime;
};

} // namespace

TableFunction BindOneLakeTableRead(ClientContext &context, OneLakeCatalog &catalog, const OneLakeLakehouse &item,
                                   const string &schema, const string &table, unique_ptr<FunctionData> &bind_data,
                                   vector<string> &names, vector<LogicalType> &types) {
	OneLakeReadTimer timer(OneLakeReadMetrics::Get(context), ReadPhase::SCHEMA, ReadMetric::BIND_NS);
	TableReadTarget target {item.table_api_origin, catalog.GetWorkspaceId(), item.id, schema, table,
	                        item.table_catalog_url};
	bind_data = BindOneLakeCachedScan(context, make_shared_ptr<LiveReadSource>(catalog, std::move(target)),
	                                  catalog.UsesReadCache(), names, types);
	return OneLakeCachedScanFunction();
}

} // namespace duckdb
