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
	explicit OneLakeCachedFile(BufferManager &manager);
	~OneLakeCachedFile();

	void Append(const_data_ptr_t data, idx_t size);
	void Read(data_ptr_t data, idx_t size, idx_t offset);
	void OpenWriter();
	void Seal();
	idx_t Size();
	bool Sealed();

private:
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
	string Create();
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

class OneLakeParquetFragment {
public:
	OneLakeParquetFragment(ClientContext &context, const vector<string> &names, const vector<LogicalType> &types);
	~OneLakeParquetFragment();

	void Append(DataChunk &chunk);
	void AppendArrowBatch(shared_ptr<ArrowArrayWrapper> batch, ArrowTableSchema &schema);
	string Finish();

private:
	struct State;
	unique_ptr<State> state;
};

} // namespace duckdb
