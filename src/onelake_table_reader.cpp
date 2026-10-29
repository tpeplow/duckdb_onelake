#include "onelake_table_reader.hpp"
#include "onelake_table_reader_c_api.hpp"
#include "onelake_table_api.hpp"
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

struct TableReadBindData : public ArrowScanFunctionData {
	TableReadBindData(OneLakeCatalog &catalog_p, TableReadTarget target_p)
	    : ArrowScanFunctionData(nullptr, 0), catalog(catalog_p), target(std::move(target_p)) {
	}
	OneLakeCatalog &catalog;
	TableReadTarget target;
};

bool SameLayout(const ArrowSchema &expected, const ArrowSchema &actual) {
	if (!expected.format || !actual.format || std::strcmp(expected.format, actual.format) ||
	    expected.flags != actual.flags || expected.n_children != actual.n_children ||
	    bool(expected.dictionary) != bool(actual.dictionary)) {
		return false;
	}
	for (int64_t i = 0; i < expected.n_children; i++) {
		if (!SameLayout(*expected.children[i], *actual.children[i])) {
			return false;
		}
	}
	return !expected.dictionary || SameLayout(*expected.dictionary, *actual.dictionary);
}

unique_ptr<GlobalTableFunctionState> InitRead(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<TableReadBindData>();
	vector<string> columns;
	vector<column_t> ids;
	for (auto id : input.column_ids) {
		if (id != COLUMN_IDENTIFIER_ROW_ID) {
			columns.push_back(data.schema_root.arrow_schema.children[id]->name);
			ids.push_back(id);
		}
	}
	if (columns.empty()) {
		// COUNT(*) still needs service-authorized rows, not catalog statistics.
		columns.push_back(data.schema_root.arrow_schema.children[0]->name);
		ids.push_back(0);
	}
	auto stream = make_uniq<TableReadStream>(context, data.catalog.GetCredentials(), data.target, columns);
	ArrowSchemaWrapper actual;
	stream->GetSchema(actual);
	ArrowTableSchema actual_table;
	ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), actual_table, actual.arrow_schema);
	if (actual_table.GetNames() != columns || actual.arrow_schema.n_children != int64_t(ids.size())) {
		throw IOException("OneLake projected schema changed between binding and execution; rebind the query");
	}
	for (idx_t i = 0; i < ids.size(); i++) {
		if (actual_table.GetTypes()[i] != data.all_types[ids[i]] ||
		    !SameLayout(*data.schema_root.arrow_schema.children[ids[i]], *actual.arrow_schema.children[i])) {
			throw IOException("OneLake Arrow layout changed between binding and execution; rebind the query");
		}
	}
	auto result = make_uniq<ArrowScanGlobalState>();
	result->stream = std::move(stream);
	result->max_threads = 1;
	return std::move(result);
}

} // namespace

TableFunction BindOneLakeTableRead(ClientContext &context, OneLakeCatalog &catalog, const OneLakeLakehouse &item,
                                   const string &schema, const string &table, unique_ptr<FunctionData> &bind_data,
                                   vector<string> &names, vector<LogicalType> &types) {
	TableReadTarget target {item.table_api_origin, catalog.GetWorkspaceId(), item.id, schema, table};
	auto data = make_uniq<TableReadBindData>(catalog, target);
	TableReadStream probe(context, catalog.GetCredentials(), target, {});
	probe.GetSchema(data->schema_root);
	ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), data->arrow_table,
	                                             data->schema_root.arrow_schema);
	names = data->arrow_table.GetNames();
	types = data->arrow_table.GetTypes();
	if (names.empty()) {
		throw IOException("OneLake returned no caller-visible columns");
	}
	// DuckDB must not silently rename columns before sending explicit projections back to OneLake.
	for (idx_t i = 0; i < names.size(); i++) {
		if (names[i] != data->schema_root.arrow_schema.children[i]->name) {
			throw IOException("OneLake returned ambiguous column names");
		}
	}
	data->all_types = types;
	bind_data = std::move(data);
	TableFunction function("onelake_table_scan", {}, ArrowTableFunction::ArrowScanFunction, nullptr, InitRead,
	                       ArrowTableFunction::ArrowScanInitLocal);
	function.projection_pushdown = true;
	function.filter_pushdown = false;
	function.filter_prune = false;
	return function;
}

} // namespace duckdb
