#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class OneLakeCacheLifetime {
public:
	explicit OneLakeCacheLifetime(DatabaseInstance &db);
	~OneLakeCacheLifetime();
	const string &Id() const;
	void CheckActive() const;
	void Track(ClientContext &context);
	void Retire();

private:
	struct State;
	unique_ptr<State> state;
};

struct OneLakeOpenedRead {
	unique_ptr<ArrowArrayStreamWrapper> stream;
	// Must validate the exact snapshot AND effective authorization of this allocation.
	// Empty means unsupported: neither fill nor reuse is allowed.
	std::function<bool(ClientContext &)> revalidate;
};

class OneLakeScanSource {
public:
	virtual ~OneLakeScanSource() = default;
	virtual string CacheKey() const = 0;
	virtual OneLakeCacheLifetime &Lifetime() const = 0;
	virtual void CheckIdentity(ClientContext &context) = 0;
	virtual OneLakeOpenedRead Open(ClientContext &context, const vector<string> &columns) = 0;
};

TableFunction OneLakeCachedScanFunction();
unique_ptr<FunctionData> BindOneLakeCachedScan(ClientContext &context, shared_ptr<OneLakeScanSource> source,
                                               bool cache_enabled, vector<string> &names, vector<LogicalType> &types);
void RegisterOneLakeCacheOptimizer(DatabaseInstance &db);

} // namespace duckdb
