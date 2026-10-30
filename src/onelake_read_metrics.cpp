#include "onelake_read_metrics.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace {
constexpr const char *STATE_KEY = "onelake_read_metrics";
constexpr const char *NAMES[] = {"token_calls",
                                 "token_ns",
                                 "metadata_requests",
                                 "metadata_bytes",
                                 "metadata_ns",
                                 "bind_ns",
                                 "reader_opens",
                                 "reader_open_ns",
                                 "allocation_requests",
                                 "allocation_bytes",
                                 "allocation_ns",
                                 "stream_requests",
                                 "stream_open_ns",
                                 "stream_bytes",
                                 "stream_read_ns",
                                 "ipc_processing_ns",
                                 "batches",
                                 "rows",
                                 "first_batches",
                                 "first_batch_ns",
                                 "completions",
                                 "errors",
                                 "arrow_fetch_ns",
                                 "arrow_operator_ns",
                                 "output_rows"};
static_assert(sizeof(NAMES) / sizeof(NAMES[0]) == static_cast<size_t>(ReadMetric::COUNT));

struct MetricsScan : GlobalTableFunctionState {
	OneLakeReadMetrics::Counters values;
	bool done = false;
};

unique_ptr<FunctionData> BindMetrics(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                     vector<string> &names) {
	names = {"phase", "metric", "value"};
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT};
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> InitMetrics(ClientContext &context, TableFunctionInitInput &) {
	auto state = make_uniq<MetricsScan>();
	state->values = context.registered_state->GetOrCreate<OneLakeReadMetrics>(STATE_KEY)->Snapshot();
	return std::move(state);
}

void ScanMetrics(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<MetricsScan>();
	if (state.done) {
		return;
	}
	const char *phases[] = {"shared", "schema", "execution"};
	idx_t row = 0;
	for (idx_t phase = 0; phase < 3; ++phase) {
		for (idx_t metric = 0; metric < static_cast<idx_t>(ReadMetric::COUNT); ++metric) {
			output.SetValue(0, row, Value(phases[phase]));
			output.SetValue(1, row, Value(NAMES[metric]));
			output.SetValue(2, row++, Value::UBIGINT(state.values[phase][metric]));
		}
	}
	output.SetCardinality(row);
	state.done = true;
}
} // namespace

shared_ptr<OneLakeReadMetrics> OneLakeReadMetrics::Get(ClientContext &context) {
	Value enabled;
	if (!context.TryGetCurrentSetting("onelake_enable_read_metrics", enabled) || !enabled.GetValue<bool>()) {
		return nullptr;
	}
	try {
		return context.registered_state->GetOrCreate<OneLakeReadMetrics>(STATE_KEY);
	} catch (...) {
		// Optional diagnostics must not prevent an otherwise valid read under memory pressure.
		return nullptr;
	}
}

void OneLakeReadMetrics::Add(ReadPhase phase, ReadMetric metric, uint64_t value) {
	lock_guard<mutex> guard(lock);
	values[static_cast<size_t>(phase)][static_cast<size_t>(metric)] += value;
}

OneLakeReadMetrics::Counters OneLakeReadMetrics::Snapshot() {
	lock_guard<mutex> guard(lock);
	return values;
}

OneLakeReadTimer::OneLakeReadTimer(shared_ptr<OneLakeReadMetrics> metrics_p, ReadPhase phase_p, ReadMetric metric_p)
    : metrics(std::move(metrics_p)), phase(phase_p), metric(metric_p) {
	if (metrics) {
		start = std::chrono::steady_clock::now();
	}
}

OneLakeReadTimer::~OneLakeReadTimer() {
	if (metrics) {
		metrics->Add(phase, metric,
		             static_cast<uint64_t>(
		                 std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start)
		                     .count()));
	}
}

void RegisterOneLakeReadMetrics(ExtensionLoader &loader) {
	DBConfig::GetConfig(loader.GetDatabaseInstance())
	    .AddExtensionOption("onelake_enable_read_metrics", "Collect connection-local numeric Table API read metrics",
	                        LogicalType::BOOLEAN, Value(false));
	loader.RegisterFunction(TableFunction("onelake_read_metrics", {}, ScanMetrics, BindMetrics, InitMetrics));
}
} // namespace duckdb
