#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table_function.hpp"
#include <chrono>
#include <optional>

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
	// Without this validator or an explicit TTL, neither fill nor reuse is allowed.
	std::function<bool(ClientContext &)> revalidate;
	// Explicit bounded-staleness policy, not an authorization or source-version validator.
	std::optional<std::chrono::steady_clock::time_point> cache_expires_at;
};

class OneLakeScanSource {
public:
	virtual ~OneLakeScanSource() = default;
	virtual string CacheKey() const = 0;
	virtual OneLakeCacheLifetime &Lifetime() const = 0;
	virtual void CheckIdentity(ClientContext &context) = 0;
	virtual OneLakeOpenedRead Open(ClientContext &context, const vector<string> &columns) = 0;
	virtual std::chrono::steady_clock::time_point CacheNow() const noexcept {
		return std::chrono::steady_clock::now();
	}
};

TableFunction OneLakeCachedScanFunction();
void CheckOneLakeScanBinding(ClientContext &context, const FunctionData &data);
unique_ptr<FunctionData> BindOneLakeCachedScan(ClientContext &context, shared_ptr<OneLakeScanSource> source,
                                               bool cache_enabled, vector<string> &names, vector<LogicalType> &types);
void RegisterOneLakeCacheOptimizer(DatabaseInstance &db);

} // namespace duckdb
