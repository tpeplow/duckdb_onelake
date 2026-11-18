#pragma once

#include "duckdb/main/client_context_state.hpp"
#include <array>
#include <chrono>

namespace duckdb {
class ExtensionLoader;

enum class ReadPhase { SHARED, SCHEMA, EXECUTION };
enum class ReadMetric {
	TOKEN_CALLS,
	TOKEN_NS,
	METADATA_REQUESTS,
	METADATA_BYTES,
	METADATA_NS,
	BIND_NS,
	READER_OPENS,
	READER_OPEN_NS,
	ALLOCATION_REQUESTS,
	ALLOCATION_BYTES,
	ALLOCATION_NS,
	STREAM_REQUESTS,
	STREAM_OPEN_NS,
	STREAM_BYTES,
	STREAM_READ_NS,
	IPC_PROCESSING_NS,
	BATCHES,
	ROWS,
	FIRST_BATCHES,
	FIRST_BATCH_NS,
	COMPLETIONS,
	ERRORS,
	ARROW_FETCH_NS,
	ARROW_OPERATOR_NS,
	OUTPUT_ROWS,
	ALLOCATED_STREAMS,
	PEAK_ACTIVE_STREAMS,
	COUNT
};

struct OneLakeReadMetrics : ClientContextState {
	using Counters = std::array<std::array<uint64_t, static_cast<size_t>(ReadMetric::COUNT)>, 3>;
	static shared_ptr<OneLakeReadMetrics> Get(ClientContext &context);
	void Add(ReadPhase phase, ReadMetric metric, uint64_t value);
	void Max(ReadPhase phase, ReadMetric metric, uint64_t value);
	Counters Snapshot();

private:
	mutex lock;
	Counters values {};
};

class OneLakeReadTimer {
public:
	OneLakeReadTimer(shared_ptr<OneLakeReadMetrics> metrics, ReadPhase phase, ReadMetric metric);
	~OneLakeReadTimer();

private:
	shared_ptr<OneLakeReadMetrics> metrics;
	ReadPhase phase;
	ReadMetric metric;
	std::chrono::steady_clock::time_point start;
};

void RegisterOneLakeReadMetrics(ExtensionLoader &loader);
} // namespace duckdb
