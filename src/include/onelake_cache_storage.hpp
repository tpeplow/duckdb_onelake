#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

class ArrowArrayWrapper;
class ArrowTableSchema;

// Prototype storage only: no service result can become a reusable cache entry here.
class OneLakeCachedFile {
public:
	explicit OneLakeCachedFile(BufferManager &manager, bool guarded = false);
	~OneLakeCachedFile();

	void Append(const_data_ptr_t data, idx_t size);
	void Read(data_ptr_t data, idx_t size, idx_t offset);
	void OpenWriter();
	void Seal();
	idx_t Size();
	bool Sealed();
	bool Guarded() const {
		return guarded;
	}

private:
	const bool guarded;
	BufferManager &manager;
	mutex lock;
	vector<shared_ptr<BlockHandle>> blocks;
	idx_t size = 0;
	idx_t reserved = 0;
	bool opened = false;
	bool sealed = false;
	bool failed = false;
};

class OneLakeCacheStorage : public ClientContextState {
public:
	explicit OneLakeCacheStorage(ClientContext &context);
	~OneLakeCacheStorage() override;

	static shared_ptr<OneLakeCacheStorage> Get(ClientContext &context);
	string Create(bool guarded = false);
	shared_ptr<OneLakeCachedFile> Lookup(const string &path);
	void Seal(const string &path);
	void Remove(const string &path);
	void Clear();

private:
	DatabaseInstance &db;
	mutex lock;
	unordered_map<string, shared_ptr<OneLakeCachedFile>> files;
};

void RegisterOneLakeCacheStorage(DatabaseInstance &db);

// Only the validated scan adapter may open guarded files, including for metadata.
class OneLakeCacheReadScope {
public:
	explicit OneLakeCacheReadScope(const vector<string> *paths);
	~OneLakeCacheReadScope();
	OneLakeCacheReadScope(const OneLakeCacheReadScope &) = delete;
	OneLakeCacheReadScope &operator=(const OneLakeCacheReadScope &) = delete;

private:
	const vector<string> *previous;
};

class OneLakeParquetFragment {
public:
	OneLakeParquetFragment(ClientContext &context, const vector<string> &names, const vector<LogicalType> &types,
	                       bool guarded = false);
	~OneLakeParquetFragment();

	void Append(DataChunk &chunk);
	void AppendArrowBatch(shared_ptr<ArrowArrayWrapper> batch, ArrowTableSchema &schema);
	string Finish();

private:
	struct State;
	unique_ptr<State> state;
};

} // namespace duckdb
