#include "onelake_cached_scan.hpp"
#include "onelake_cache_storage.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <cstring>

namespace duckdb {
namespace {

struct Entry {
	explicit Entry(ClientContext &context)
	    : storage(OneLakeCacheStorage::Get(context)), allocator(BufferAllocator::Get(context)), paths(allocator) {
	}
	~Entry() {
		for (auto &path : paths) {
			try {
				storage->Remove(string(path.data(), path.size()));
			} catch (...) {
			}
		}
	}
	shared_ptr<OneLakeCacheStorage> storage;
	Allocator &allocator;
	OneLakeCachePaths paths;
	std::function<bool(ClientContext &)> revalidate;
	idx_t rows = 0;
	bool complete = false;
	atomic<bool> available {false};
};

struct CacheState : ClientContextState {
	explicit CacheState(ClientContext &context) : allocator(BufferAllocator::Get(context)), entries(allocator) {
	}
	Allocator &allocator;
	mutex lock;
	OneLakeCacheMap<shared_ptr<Entry>> entries;
	shared_ptr<Entry> Get(const string &key) {
		lock_guard<mutex> guard(lock);
		auto found = entries.find(key);
		return found == entries.end() ? nullptr : found->second;
	}
	void Put(const string &key, shared_ptr<Entry> entry) {
		lock_guard<mutex> guard(lock);
		entries.insert_or_assign(CacheString(allocator, key), std::move(entry));
	}
	void EraseNamespace(const string &prefix) {
		lock_guard<mutex> guard(lock);
		for (auto it = entries.begin(); it != entries.end();) {
			if (std::string_view(it->first.data(), it->first.size()).substr(0, prefix.size()) == prefix) {
				it = entries.erase(it);
			} else {
				++it;
			}
		}
	}
	void Erase(const string &key, shared_ptr<Entry> expected = nullptr) {
		lock_guard<mutex> guard(lock);
		auto found = entries.find(key);
		if (found != entries.end() && (!expected || found->second == expected)) {
			entries.erase(found);
		}
	}
	void QueryEnd(ClientContext &, optional_ptr<ErrorData> error) override {
		if (error) {
			lock_guard<mutex> guard(lock);
			entries.clear();
		}
	}
};

struct BindData : ArrowScanFunctionData {
	BindData(shared_ptr<OneLakeScanSource> source, bool enabled)
	    : ArrowScanFunctionData(nullptr, 0), source(std::move(source)), enabled(enabled) {
	}
	shared_ptr<OneLakeScanSource> source;
	bool enabled;
	optional_idx partial_limit;
	TableFilterSet advisory_filters;
};

struct GlobalState : GlobalTableFunctionState {
	GlobalState() : function("uninitialized", {}, nullptr) {
	}
	TableFunction function;
	unique_ptr<FunctionData> parquet_bind;
	optional_ptr<const FunctionData> child_bind;
	unique_ptr<GlobalTableFunctionState> child;
	vector<column_t> columns;
	shared_ptr<CacheState> cache;
	shared_ptr<Entry> entry;
	string key;
	bool emitted = false;
	unique_ptr<TableFilterSet> filters;
	idx_t MaxThreads() const override {
		return 1;
	}
};

struct LocalState : LocalTableFunctionState {
	explicit LocalState(ExecutionContext &context) : thread(context.thread), pipeline(context.pipeline) {
	}
	ThreadContext &thread;
	optional_ptr<Pipeline> pipeline;
	unique_ptr<LocalTableFunctionState> child;
};

bool SameLayout(const ArrowSchema &a, const ArrowSchema &b) {
	if (!a.format || !b.format || std::strcmp(a.format, b.format) || a.flags != b.flags ||
	    a.n_children != b.n_children || bool(a.dictionary) != bool(b.dictionary)) {
		return false;
	}
	for (int64_t i = 0; i < a.n_children; ++i) {
		if (!SameLayout(*a.children[i], *b.children[i])) {
			return false;
		}
	}
	return !a.dictionary || SameLayout(*a.dictionary, *b.dictionary);
}

bool Enabled(ClientContext &context, const BindData &data) {
	Value setting;
	return data.enabled && context.TryGetCurrentSetting("enable_external_file_cache", setting) &&
	       setting.GetValue<bool>() &&
	       OneLakeCacheDirectoryIsPrivate(BufferManager::GetBufferManager(context).GetTemporaryDirectory());
}

class TeeStream : public ArrowArrayStreamWrapper {
public:
	~TeeStream() override {
		if (entry) {
			entry->available = true;
		}
	}
	TeeStream(ClientContext &context, unique_ptr<ArrowArrayStreamWrapper> &parent, shared_ptr<Entry> entry,
	          shared_ptr<CacheState> cache, string key)
	    : context(context), entry(std::move(entry)), cache(std::move(cache)), key(std::move(key)) {
		arrow_array_stream = {};
		arrow_array_stream.get_schema = [](ArrowArrayStream *stream, ArrowSchema *out) {
			auto &parent = *static_cast<ArrowArrayStreamWrapper *>(stream->private_data);
			return parent.arrow_array_stream.get_schema(&parent.arrow_array_stream, out);
		};
		arrow_array_stream.get_last_error = [](ArrowArrayStream *stream) {
			auto &parent = *static_cast<ArrowArrayStreamWrapper *>(stream->private_data);
			return parent.arrow_array_stream.get_last_error(&parent.arrow_array_stream);
		};
		arrow_array_stream.release = [](ArrowArrayStream *stream) {
			stream->release = nullptr;
		};
		ArrowSchemaWrapper schema;
		parent->GetSchema(schema);
		ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), table, schema.arrow_schema);
		source = std::move(parent);
		arrow_array_stream.private_data = source.get();
	}

	shared_ptr<ArrowArrayWrapper> GetNextChunk() override {
		shared_ptr<ArrowArrayWrapper> batch;
		try {
			if (context.interrupted) {
				throw InterruptException();
			}
			batch = source->GetNextChunk();
		} catch (...) {
			Discard();
			throw;
		}
		if (!entry) {
			return batch;
		}
		try {
			entry->paths.reserve(entry->paths.size() + 1);
			if (!batch->arrow_array.release) {
				if (entry->paths.empty()) {
					OneLakeParquetFragment empty(context, table.GetNames(), table.GetTypes(), true);
					Keep(empty);
				}
				entry->complete = true;
			} else {
				OneLakeParquetFragment fragment(context, table.GetNames(), table.GetTypes(), true);
				fragment.AppendArrowBatch(batch, table);
				Keep(fragment);
				entry->rows += idx_t(batch->arrow_array.length);
			}
		} catch (...) {
			Discard();
			if (context.interrupted) {
				throw InterruptException();
			}
		}
		return batch;
	}

private:
	void Keep(OneLakeParquetFragment &fragment) {
		auto path = fragment.Finish();
		try {
			entry->paths.push_back(CacheString(entry->allocator, path));
		} catch (...) {
			entry->storage->Remove(path);
			throw;
		}
	}
	void Discard() {
		if (entry) {
			cache->Erase(key, entry);
			entry.reset();
		}
	}
	ClientContext &context;
	unique_ptr<ArrowArrayStreamWrapper> source;
	shared_ptr<Entry> entry;
	shared_ptr<CacheState> cache;
	string key;
	ArrowTableSchema table;
};

vector<string> Columns(const BindData &data, const vector<column_t> &ids) {
	vector<string> names;
	for (auto id : ids) {
		if (id != COLUMN_IDENTIFIER_ROW_ID) {
			names.emplace_back(data.schema_root.arrow_schema.children[id]->name);
		}
	}
	if (names.empty()) {
		names.emplace_back(data.schema_root.arrow_schema.children[0]->name);
	}
	return names;
}

void StartArrow(ClientContext &context, const BindData &data, GlobalState &state, bool allow_fill) {
	if (context.interrupted) {
		throw InterruptException();
	}
	auto names = Columns(data, state.columns);
	auto opened = data.source->Open(context, names);
	ArrowSchemaWrapper actual;
	opened.stream->GetSchema(actual);
	ArrowTableSchema table;
	ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), table, actual.arrow_schema);
	if (table.GetNames() != names) {
		throw IOException("OneLake projected schema changed between binding and execution; rebind the query");
	}
	idx_t projected = 0;
	for (auto id : state.columns) {
		if (id == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		if (table.GetTypes()[projected] != data.all_types[id] ||
		    !SameLayout(*data.schema_root.arrow_schema.children[id], *actual.arrow_schema.children[projected])) {
			throw IOException("OneLake Arrow layout changed between binding and execution; rebind the query");
		}
		++projected;
	}
	if (projected == 0 && !SameLayout(*data.schema_root.arrow_schema.children[0], *actual.arrow_schema.children[0])) {
		throw IOException("OneLake Arrow layout changed between binding and execution; rebind the query");
	}
	if (allow_fill && opened.revalidate && Enabled(context, data)) {
		try {
			auto entry = MakeCacheShared<Entry>(BufferAllocator::Get(context), context);
			entry->revalidate = std::move(opened.revalidate);
			state.cache->Put(state.key, entry);
			auto tee = make_uniq<TeeStream>(context, opened.stream, entry, state.cache, state.key);
			opened.stream = std::move(tee);
		} catch (...) {
			state.cache->Erase(state.key);
		}
	}
	auto arrow = make_uniq<ArrowScanGlobalState>();
	arrow->stream = std::move(opened.stream);
	arrow->max_threads = 1;
	state.function = TableFunction("arrow", {}, ArrowTableFunction::ArrowScanFunction, nullptr, nullptr,
	                               ArrowTableFunction::ArrowScanInitLocal);
	state.child.reset();
	state.parquet_bind.reset();
	state.filters.reset();
	state.child_bind = &data;
	state.child = std::move(arrow);
	state.entry.reset();
}

void StartParquet(ClientContext &context, const BindData &data, GlobalState &state, shared_ptr<Entry> entry) {
	OneLakeCacheReadScope scope(&entry->paths);
	auto &catalog_entry =
	    Catalog::GetEntry<TableFunctionCatalogEntry>(context, SYSTEM_CATALOG, DEFAULT_SCHEMA, "parquet_scan");
	state.function = catalog_entry.functions.GetFunctionByOffset(0);
	vector<Value> paths;
	for (auto &path : entry->paths) {
		paths.emplace_back(string(path.data(), path.size()));
	}
	vector<Value> inputs {Value::LIST(LogicalType::VARCHAR, std::move(paths))};
	named_parameter_map_t parameters;
	vector<LogicalType> types;
	vector<string> names;
	vector<LogicalType> input_types;
	vector<string> input_names;
	TableFunctionRef ref;
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(make_uniq<ConstantExpression>(inputs[0]));
	ref.function = make_uniq<FunctionExpression>("parquet_scan", std::move(arguments));
	TableFunctionBindInput bind_input(inputs, parameters, input_types, input_names, nullptr, nullptr, state.function,
	                                  ref);
	state.parquet_bind = state.function.bind(context, bind_input, types, names);
	if (names != Columns(data, state.columns)) {
		throw IOException("OneLake cached projection changed");
	}
	vector<column_t> ids;
	idx_t position = 0;
	for (auto id : state.columns) {
		if (id == COLUMN_IDENTIFIER_ROW_ID) {
			ids.push_back(id);
		} else {
			if (types[position] != data.all_types[id]) {
				throw IOException("OneLake cached type changed");
			}
			ids.push_back(position++);
		}
	}
	state.child_bind = state.parquet_bind.get();
	state.filters = make_uniq<TableFilterSet>();
	for (idx_t i = 0; i < state.columns.size(); ++i) {
		auto found = data.advisory_filters.filters.find(state.columns[i]);
		if (found != data.advisory_filters.filters.end()) {
			state.filters->filters.emplace(i, found->second->Copy());
		}
	}
	TableFunctionInitInput init(state.child_bind, ids, {}, state.filters.get());
	state.child = state.function.init_global(context, init);
	state.entry = std::move(entry);
}

unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<BindData>();
	data.source->Lifetime().CheckActive();
	data.source->CheckIdentity(context);
	auto state = make_uniq<GlobalState>();
	state->columns = input.column_ids;
	try {
		state->cache = context.registered_state->GetOrCreate<CacheState>("onelake_read_cache", context);
		state->key = data.source->Lifetime().Id() + ":" + data.source->CacheKey();
		for (auto &name : Columns(data, state->columns)) {
			state->key += ":" + to_string(name.size()) + ":" + name;
		}
	} catch (...) {
		StartArrow(context, data, *state, false);
		return std::move(state);
	}
	const auto enabled = Enabled(context, data);
	auto entry = state->cache->Get(state->key);
	if (entry) {
		bool valid = false;
		try {
			data.source->Lifetime().Track(context);
			valid =
			    enabled && entry->available &&
			    (entry->complete || (data.partial_limit.IsValid() && entry->rows >= data.partial_limit.GetIndex())) &&
			    entry->revalidate && entry->revalidate(context);
		} catch (...) {
		}
		if (valid) {
			try {
				StartParquet(context, data, *state, entry);
				return std::move(state);
			} catch (...) {
			}
		}
		state->cache->Erase(state->key, entry);
	}
	StartArrow(context, data, *state, enabled);
	return std::move(state);
}

vector<column_t> ChildColumns(const GlobalState &state) {
	if (!state.entry) {
		return state.columns;
	}
	vector<column_t> result;
	idx_t position = 0;
	for (auto id : state.columns) {
		result.push_back(id == COLUMN_IDENTIFIER_ROW_ID ? id : position++);
	}
	return result;
}

unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                              GlobalTableFunctionState *global) {
	auto &state = global->Cast<GlobalState>();
	auto local = make_uniq<LocalState>(context);
	TableFunctionInitInput child(state.child_bind, ChildColumns(state), {}, state.filters.get());
	try {
		OneLakeCacheReadScope scope(state.entry ? &state.entry->paths : nullptr);
		local->child = state.function.init_local(context, child, state.child.get());
	} catch (...) {
		if (!state.entry || context.client.interrupted) {
			throw;
		}
		state.cache->Erase(state.key, state.entry);
		StartArrow(context.client, input.bind_data->Cast<BindData>(), state, false);
		TableFunctionInitInput fallback(state.child_bind, state.columns, {}, nullptr);
		local->child = state.function.init_local(context, fallback, state.child.get());
	}
	return std::move(local);
}

void Scan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<GlobalState>();
	auto &local = input.local_state->Cast<LocalState>();
	try {
		OneLakeCacheReadScope scope(state.entry ? &state.entry->paths : nullptr);
		TableFunctionInput child(state.child_bind, local.child.get(), state.child.get());
		state.function.function(context, child, output);
		state.emitted |= output.size() != 0;
	} catch (...) {
		if (!state.entry) {
			throw;
		}
		state.cache->Erase(state.key, state.entry);
		if (state.emitted || context.interrupted) {
			throw;
		}
		local.child.reset();
		auto &data = input.bind_data->Cast<BindData>();
		StartArrow(context, data, state, false);
		ExecutionContext execution(context, local.thread, local.pipeline);
		TableFunctionInitInput init(state.child_bind, state.columns, {}, nullptr);
		local.child = state.function.init_local(execution, init, state.child.get());
		output.Reset();
		TableFunctionInput child(state.child_bind, local.child.get(), state.child.get());
		state.function.function(context, child, output);
		state.emitted |= output.size() != 0;
	}
}

void AdvisoryFilters(ClientContext &, LogicalGet &get, FunctionData *bind,
                     vector<unique_ptr<Expression>> &expressions) {
	auto &data = bind->Cast<BindData>();
	for (auto &expression : expressions) {
		if (expression->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
			continue;
		}
		auto &comparison = expression->Cast<BoundComparisonExpression>();
		if (comparison.left->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    comparison.right->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
		    comparison.type < ExpressionType::COMPARE_EQUAL ||
		    comparison.type > ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			continue;
		}
		auto &column = comparison.left->Cast<BoundColumnRefExpression>();
		auto &constant = comparison.right->Cast<BoundConstantExpression>();
		if (column.binding.table_index != get.table_index) {
			continue;
		}
		auto id = get.GetColumnIds()[column.binding.column_index].GetPrimaryIndex();
		data.advisory_filters.PushFilter(ColumnIndex(id), make_uniq<ConstantFilter>(comparison.type, constant.value));
	}
	// All expressions remain residual filters, so an Arrow miss has identical semantics.
}

void PartialLimit(OptimizerExtensionInput &, unique_ptr<LogicalOperator> &plan) {
	optional_idx limit;
	auto node = plan.get();
	while (node) {
		if (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			for (auto &expression : node->expressions) {
				if (expression->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
					return;
				}
			}
		} else if (node->type == LogicalOperatorType::LOGICAL_LIMIT && !limit.IsValid()) {
			auto &op = node->Cast<LogicalLimit>();
			if (op.limit_val.Type() != LimitNodeType::CONSTANT_VALUE || op.offset_val.Type() != LimitNodeType::UNSET) {
				return;
			}
			limit = op.limit_val.GetConstantValue();
		} else if (node->type == LogicalOperatorType::LOGICAL_GET && limit.IsValid()) {
			auto &get = node->Cast<LogicalGet>();
			if (get.function.function == Scan && !get.ordinality_idx.IsValid() && get.table_filters.filters.empty()) {
				get.bind_data->Cast<BindData>().partial_limit = limit;
			}
			return;
		} else {
			return;
		}
		if (node->children.size() != 1) {
			return;
		}
		node = node->children[0].get();
	}
}

} // namespace

struct OneLakeCacheLifetime::State {
	explicit State(DatabaseInstance &db)
	    : db(db), id(UUID::ToString(UUID::GenerateRandomUUID())), prefix(id + ":"), contexts(BufferAllocator::Get(db)) {
	}
	DatabaseInstance &db;
	string id;
	string prefix;
	mutex lock;
	atomic<bool> retired {false};
	OneLakeCacheVector<weak_ptr<CacheState>> contexts;
};

OneLakeCacheLifetime::OneLakeCacheLifetime(DatabaseInstance &db) : state(make_uniq<State>(db)) {
}

OneLakeCacheLifetime::~OneLakeCacheLifetime() {
	Retire();
}

const string &OneLakeCacheLifetime::Id() const {
	return state->id;
}

void OneLakeCacheLifetime::CheckActive() const {
	if (state->retired) {
		throw IOException("OneLake cache attachment has been retired; rebind the query");
	}
}

void OneLakeCacheLifetime::Track(ClientContext &context) {
	if (context.db.get() != &state->db) {
		throw IOException("OneLake cache attachment belongs to another database");
	}
	auto cache = context.registered_state->GetOrCreate<CacheState>("onelake_read_cache", context);
	lock_guard<mutex> guard(state->lock);
	CheckActive();
	for (auto it = state->contexts.begin(); it != state->contexts.end();) {
		auto existing = it->lock();
		if (existing == cache) {
			return;
		}
		if (!existing) {
			it = state->contexts.erase(it);
		} else {
			++it;
		}
	}
	state->contexts.emplace_back(cache);
}

void OneLakeCacheLifetime::Retire() {
	lock_guard<mutex> guard(state->lock);
	state->retired = true;
	for (auto &reference : state->contexts) {
		auto context = reference.lock();
		if (context) {
			context->EraseNamespace(state->prefix);
		}
	}
	OneLakeCacheVector<weak_ptr<CacheState>> empty(state->contexts.get_allocator());
	state->contexts.swap(empty);
}

TableFunction OneLakeCachedScanFunction() {
	TableFunction function("onelake_table_scan", {}, Scan, nullptr, Init, InitLocal);
	function.projection_pushdown = true;
	function.filter_pushdown = false;
	function.filter_prune = false;
	function.pushdown_complex_filter = AdvisoryFilters;
	return function;
}

unique_ptr<FunctionData> BindOneLakeCachedScan(ClientContext &context, shared_ptr<OneLakeScanSource> source,
                                               bool enabled, vector<string> &names, vector<LogicalType> &types) {
	source->Lifetime().CheckActive();
	auto data = make_uniq<BindData>(std::move(source), enabled);
	auto probe = data->source->Open(context, {});
	probe.stream->GetSchema(data->schema_root);
	ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), data->arrow_table,
	                                             data->schema_root.arrow_schema);
	names = data->arrow_table.GetNames();
	types = data->arrow_table.GetTypes();
	if (names.empty()) {
		throw IOException("OneLake returned no caller-visible columns");
	}
	for (idx_t i = 0; i < names.size(); ++i) {
		if (names[i] != data->schema_root.arrow_schema.children[i]->name) {
			throw IOException("OneLake returned ambiguous column names");
		}
	}
	data->all_types = types;
	return std::move(data);
}

void RegisterOneLakeCacheOptimizer(DatabaseInstance &db) {
	OptimizerExtension extension;
	extension.pre_optimize_function = PartialLimit;
	DBConfig::GetConfig(db).optimizer_extensions.push_back(std::move(extension));
}

} // namespace duckdb
