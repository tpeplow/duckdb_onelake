#include "onelake_table_reader.hpp"
#include "onelake_table_reader_c_api.hpp"
#include "onelake_table_api.hpp"
#include "onelake_cached_scan.hpp"
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
};

class TableReadStream : public ArrowArrayStreamWrapper {
public:
	TableReadStream(ClientContext &context_p, OneLakeCredentials &credentials, const TableReadTarget &target,
	                const vector<string> &columns)
	    : context(context_p), reader(nullptr, ol_table_reader_close) {
		Json::Value request;
		request["baseUrl"] = target.origin;
		request["workspaceId"] = target.workspace;
		request["itemId"] = target.item;
		request["schemaName"] = target.schema;
		request["tableName"] = target.table;
		// Omission is the verified wildcard form; ["*"] is treated as a literal column by the service.
		if (!columns.empty()) {
			request["columns"] = Json::Value(Json::arrayValue);
			for (const auto &column : columns) {
				request["columns"].append(column);
			}
		}
		auto token = OneLakeTableAPI::Token(context, credentials);
		auto json = Json::writeString(Json::StreamWriterBuilder(), request);
		char error[512] = {};
		OlTableReader *handle = nullptr;
		if (ol_table_reader_open(json.c_str(), token.c_str(), &handle, error, sizeof(error)) != 0) {
			throw IOException("%s", error);
		}
		reader.reset(handle);
		if (ol_table_reader_export(reader.get(), &arrow_array_stream, error, sizeof(error)) != 0) {
			throw IOException("%s", error);
		}
	}

	shared_ptr<ArrowArrayWrapper> GetNextChunk() override {
		if (context.interrupted) {
			ol_table_reader_cancel(reader.get());
			throw InterruptException();
		}
		return ArrowArrayStreamWrapper::GetNextChunk();
	}

private:
	ClientContext &context;
	std::unique_ptr<OlTableReader, decltype(&ol_table_reader_close)> reader;
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
	OneLakeOpenedRead Open(ClientContext &context, const vector<string> &columns) override {
		CheckIdentity(context);
		OneLakeOpenedRead result;
		result.stream = make_uniq<TableReadStream>(context, catalog.GetCredentials(), target, columns);
		// The deployed read allocation has no supported snapshot/authorization validator.
		// An absent validator deliberately prevents both filling and serving live cache entries.
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
	TableReadTarget target {item.table_api_origin, catalog.GetWorkspaceId(), item.id, schema, table};
	bind_data = BindOneLakeCachedScan(context, make_shared_ptr<LiveReadSource>(catalog, std::move(target)),
	                                  catalog.UsesReadCache(), names, types);
	return OneLakeCachedScanFunction();
}

} // namespace duckdb
