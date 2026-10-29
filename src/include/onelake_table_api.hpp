#pragma once

#include "onelake_api.hpp"

namespace duckdb {

class OneLakeTableAPI {
public:
	static OneLakeLakehouse ResolveItem(ClientContext &context, const string &workspace_id, const string &item,
	                                    OneLakeCredentials &credentials);
	static vector<string> Schemas(ClientContext &context, const OneLakeLakehouse &item,
	                              OneLakeCredentials &credentials);
	static vector<string> Tables(ClientContext &context, const OneLakeLakehouse &item, const string &schema,
	                             OneLakeCredentials &credentials);
	static string Token(ClientContext &context, OneLakeCredentials &credentials,
	                    OneLakeTokenAudience audience = OneLakeTokenAudience::OneLakeDfs);
};

} // namespace duckdb
