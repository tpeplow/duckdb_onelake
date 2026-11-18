#pragma once

#include "duckdb/common/types.hpp"
#include <optional>

namespace duckdb {

struct OneLakeColumnSchema {
	string name;
	LogicalType type;
	std::optional<bool> nullable;
	vector<OneLakeColumnSchema> children;
};

using OneLakeTableSchema = vector<OneLakeColumnSchema>;

OneLakeTableSchema ParseOneLakeTableSchema(const string &body);
string OneLakeGetTableURL(const string &catalog_url, const string &catalog, const string &schema, const string &table);

} // namespace duckdb
