#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class OneLakeCatalog;
struct OneLakeLakehouse;

TableFunction BindOneLakeTableRead(ClientContext &context, OneLakeCatalog &catalog, const OneLakeLakehouse &item,
                                   const string &schema, const string &table, unique_ptr<FunctionData> &bind_data,
                                   vector<string> &names, vector<LogicalType> &types);

} // namespace duckdb
