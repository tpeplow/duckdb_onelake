#include "onelake_cache_storage.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/caching_file_system.hpp"

#include <cstring>

namespace duckdb {
namespace {

constexpr const char *CACHE_STATE = "onelake_cache_storage";
constexpr const char *CACHE_PREFIX = "onelake-cache://";
thread_local const OneLakeCachePaths *authorized_paths = nullptr;

class CacheHandle : public FileHandle {
public:
	CacheHandle(FileSystem &fs, const string &path, FileOpenFlags flags, shared_ptr<OneLakeCachedFile> file)
	    : FileHandle(fs, path, flags), file(std::move(file)) {
	}
	void Close() override {
		file.reset();
	}
	shared_ptr<OneLakeCachedFile> file;
	idx_t position = 0;
};

class CacheFileSystem : public FileSystem {
public:
	string GetName() const override {
		return "OneLakeCache";
	}
	bool CanHandleFile(const string &path) override {
		return path.compare(0, std::strlen(CACHE_PREFIX), CACHE_PREFIX) == 0;
	}
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &) override {
		return false;
	}
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override {
		auto context = FileOpener::TryGetClientContext(opener);
		if (!context) {
			throw IOException("OneLake cache requires its owning connection");
		}
		auto state = context->registered_state->Get<OneLakeCacheStorage>(CACHE_STATE);
		auto file = state ? state->Lookup(path) : nullptr;
		if (!file) {
			throw IOException("OneLake cache entry is unavailable");
		}
		if (flags.OpenForWriting()) {
			file->OpenWriter();
		} else if (!flags.OpenForReading() || !file->Sealed()) {
			throw IOException("OneLake cache entry is not sealed");
		} else if (file->Guarded() &&
		           (!authorized_paths ||
		            std::none_of(authorized_paths->begin(), authorized_paths->end(),
		                         [&](const OneLakeCacheString &candidate) { return candidate == path.c_str(); }))) {
			throw IOException("OneLake cache reads require a revalidated scan");
		}
		if (flags.OpenForReading()) {
			file->was_read = true;
		}
		return make_uniq<CacheHandle>(*this, path, flags, std::move(file));
	}
	int64_t GetFileSize(FileHandle &handle) override {
		return NumericCast<int64_t>(handle.Cast<CacheHandle>().file->Size());
	}
	string GetVersionTag(FileHandle &handle) override {
		return handle.path;
	}
	timestamp_t GetLastModifiedTime(FileHandle &) override {
		return timestamp_t(0);
	}
	FileType GetFileType(FileHandle &) override {
		return FileType::FILE_TYPE_REGULAR;
	}
	void Read(FileHandle &handle, void *buffer, int64_t count, idx_t offset) override {
		if (count < 0 || !handle.flags.OpenForReading()) {
			throw IOException("Invalid OneLake cache read");
		}
		handle.Cast<CacheHandle>().file->Read(data_ptr_cast(buffer), idx_t(count), offset);
	}
	int64_t Read(FileHandle &handle, void *buffer, int64_t count) override {
		auto &h = handle.Cast<CacheHandle>();
		if (count < 0 || h.position > h.file->Size()) {
			throw IOException("Invalid OneLake cache read");
		}
		auto length = MinValue<idx_t>(idx_t(count), h.file->Size() - h.position);
		Read(handle, buffer, NumericCast<int64_t>(length), h.position);
		h.position += length;
		return NumericCast<int64_t>(length);
	}
	void Write(FileHandle &handle, void *buffer, int64_t count, idx_t offset) override {
		auto &h = handle.Cast<CacheHandle>();
		if (count < 0 || !handle.flags.OpenForWriting() || offset != h.file->Size()) {
			throw IOException("OneLake cache writes must be append-only");
		}
		h.file->Append(const_data_ptr_cast(buffer), idx_t(count));
	}
	int64_t Write(FileHandle &handle, void *buffer, int64_t count) override {
		auto &h = handle.Cast<CacheHandle>();
		Write(handle, buffer, count, h.position);
		h.position += idx_t(count);
		return count;
	}
	void Seek(FileHandle &handle, idx_t position) override {
		auto &h = handle.Cast<CacheHandle>();
		if (position > h.file->Size()) {
			throw IOException("OneLake cache seek exceeds file size");
		}
		h.position = position;
	}
	idx_t SeekPosition(FileHandle &handle) override {
		return handle.Cast<CacheHandle>().position;
	}
	void FileSync(FileHandle &) override {
	}
	bool FileExists(const string &path, optional_ptr<FileOpener> opener) override {
		auto context = FileOpener::TryGetClientContext(opener);
		auto state = context ? context->registered_state->Get<OneLakeCacheStorage>(CACHE_STATE) : nullptr;
		auto file = state ? state->Lookup(path) : nullptr;
		return file && file->Sealed();
	}
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener) override {
		auto handle = OpenFile(path, FileFlags::FILE_FLAGS_READ, opener);
		return {OpenFileInfo(path)};
	}
};

// Revalidate just this path through the exported CachingFileSystem interface.
// DuckDB does not export a per-path external-file-cache erase operation.
class RetiredFileSystem : public CacheFileSystem {
public:
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener>) override {
		return make_uniq<CacheHandle>(*this, path, flags, nullptr);
	}
	int64_t GetFileSize(FileHandle &) override {
		return 0;
	}
	string GetVersionTag(FileHandle &handle) override {
		return handle.path + ":retired";
	}
};

void InvalidateRanges(DatabaseInstance &db, const string &path) {
	RetiredFileSystem retired;
	CachingFileSystem cache(retired, db);
	auto handle = cache.OpenFile(OpenFileInfo(path), FileFlags::FILE_FLAGS_READ);
}

} // namespace

OneLakeCachedFile::OneLakeCachedFile(BufferManager &manager, bool guarded)
    : guarded(guarded), manager(manager), blocks(manager.GetBufferAllocator()) {
}

OneLakeCachedFile::~OneLakeCachedFile() {
}

void OneLakeCachedFile::OpenWriter() {
	lock_guard<mutex> guard(lock);
	if (opened || sealed || failed) {
		throw IOException("OneLake cache file already has a writer");
	}
	opened = true;
}

void OneLakeCachedFile::Append(const_data_ptr_t data, idx_t count) {
	lock_guard<mutex> guard(lock);
	if (!opened || sealed || failed || count > idx_t(NumericLimits<int64_t>::Maximum()) - size) {
		throw IOException("Invalid OneLake cache append");
	}
	try {
		const auto block_size = manager.GetBlockSize();
		while (count) {
			const auto offset = size % block_size;
			if (offset == 0) {
				if (blocks.size() == blocks.capacity()) {
					const auto capacity = MaxValue<idx_t>(1, blocks.capacity() * 2);
					blocks.reserve(capacity);
				}
				auto block = manager.Allocate(MemoryTag::EXTENSION, block_size, false);
				blocks.push_back(block.GetBlockHandle());
			}
			auto pin = manager.Pin(blocks.back());
			const auto length = MinValue(count, block_size - offset);
			std::memcpy(pin.Ptr() + offset, data, length);
			data += length;
			count -= length;
			size += length;
		}
	} catch (...) {
		failed = true;
		blocks.clear();
		throw;
	}
}

void OneLakeCachedFile::Read(data_ptr_t data, idx_t count, idx_t offset) {
	lock_guard<mutex> guard(lock);
	if (!sealed || failed || offset > size || count > size - offset) {
		throw IOException("Invalid or incomplete OneLake cache range");
	}
	const auto block_size = manager.GetBlockSize();
	while (count) {
		auto pin = manager.Pin(blocks[offset / block_size]);
		const auto length = MinValue(count, block_size - offset % block_size);
		std::memcpy(data, pin.Ptr() + offset % block_size, length);
		data += length;
		offset += length;
		count -= length;
	}
}

void OneLakeCachedFile::Seal() {
	lock_guard<mutex> guard(lock);
	if (!opened || failed || sealed) {
		throw IOException("Cannot seal failed or unopened OneLake cache file");
	}
	sealed = true;
}

idx_t OneLakeCachedFile::Size() {
	lock_guard<mutex> guard(lock);
	return size;
}

bool OneLakeCachedFile::Sealed() {
	lock_guard<mutex> guard(lock);
	return sealed && !failed;
}

OneLakeCacheStorage::OneLakeCacheStorage(ClientContext &context)
    : db(*context.db), allocator(BufferAllocator::Get(context)), files(allocator) {
}

OneLakeCacheStorage::~OneLakeCacheStorage() {
	try {
		Clear();
	} catch (...) {
		// Destruction must not throw; namespace removal still prevents future opens.
	}
}

shared_ptr<OneLakeCacheStorage> OneLakeCacheStorage::Get(ClientContext &context) {
	return context.registered_state->GetOrCreate<OneLakeCacheStorage>(CACHE_STATE, context);
}

string OneLakeCacheStorage::Create(bool guarded) {
	auto path = string(CACHE_PREFIX) + UUID::ToString(UUID::GenerateRandomUUID()) + ".parquet";
	auto file = MakeCacheShared<OneLakeCachedFile>(allocator, BufferManager::GetBufferManager(db), guarded);
	lock_guard<mutex> guard(lock);
	files.emplace(CacheString(allocator, path), std::move(file));
	return path;
}

shared_ptr<OneLakeCachedFile> OneLakeCacheStorage::Lookup(const string &path) {
	lock_guard<mutex> guard(lock);
	auto entry = files.find(path);
	return entry == files.end() ? nullptr : entry->second;
}

void OneLakeCacheStorage::Seal(const string &path) {
	auto file = Lookup(path);
	if (!file) {
		throw IOException("OneLake cache entry is unavailable");
	}
	file->Seal();
}

void OneLakeCacheStorage::Remove(const string &path) {
	bool invalidate = false;
	{
		lock_guard<mutex> guard(lock);
		auto found = files.find(path);
		if (found != files.end()) {
			invalidate = found->second->was_read;
			files.erase(found);
		}
	}
	if (invalidate) {
		InvalidateRanges(db, path);
	}
}

void OneLakeCacheStorage::Clear() {
	// Do not allocate another managed map when clearing at the memory limit.
	while (true) {
		string path;
		bool invalidate;
		{
			lock_guard<mutex> guard(lock);
			if (files.empty()) {
				return;
			}
			auto entry = files.begin();
			path.assign(entry->first.data(), entry->first.size());
			invalidate = entry->second->was_read;
			files.erase(entry);
		}
		if (invalidate) {
			InvalidateRanges(db, path);
		}
	}
}

void RegisterOneLakeCacheStorage(DatabaseInstance &db) {
	FileSystem::GetFileSystem(db).RegisterSubSystem(make_uniq<CacheFileSystem>());
}

OneLakeCacheReadScope::OneLakeCacheReadScope(const OneLakeCachePaths *paths) : previous(authorized_paths) {
	authorized_paths = paths;
}

OneLakeCacheReadScope::~OneLakeCacheReadScope() {
	authorized_paths = previous;
}

struct OneLakeParquetFragment::State {
	State(ClientContext &context, const vector<string> &names, const vector<LogicalType> &types)
	    : context(context), storage(OneLakeCacheStorage::Get(context)),
	      function(
	          Catalog::GetEntry<CopyFunctionCatalogEntry>(context, SYSTEM_CATALOG, DEFAULT_SCHEMA, "parquet").function),
	      thread(context), execution(context, thread, nullptr), names(names), types(types) {
		CopyInfo info;
		info.format = "parquet";
		info.options["row_group_size"] = {Value::UBIGINT(STANDARD_VECTOR_SIZE)};
		info.options["compression"] = {Value("snappy")};
		CopyFunctionBindInput input(info);
		bind = function.copy_to_bind(context, input, names, types);
	}

	~State() {
		local.reset();
		global.reset();
		if (!finished && !path.empty()) {
			try {
				storage->Remove(path);
			} catch (...) {
			}
		}
	}

	ClientContext &context;
	shared_ptr<OneLakeCacheStorage> storage;
	CopyFunction function;
	ThreadContext thread;
	ExecutionContext execution;
	unique_ptr<FunctionData> bind;
	unique_ptr<GlobalFunctionData> global;
	unique_ptr<LocalFunctionData> local;
	string path;
	vector<string> names;
	vector<LogicalType> types;
	bool finished = false;
	bool failed = false;
};

OneLakeParquetFragment::OneLakeParquetFragment(ClientContext &context, const vector<string> &names,
                                               const vector<LogicalType> &types, bool guarded)
    : state(make_uniq<State>(context, names, types)) {
	state->path = state->storage->Create(guarded);
	state->global = state->function.copy_to_initialize_global(context, *state->bind, state->path);
	state->local = state->function.copy_to_initialize_local(state->execution, *state->bind);
}

OneLakeParquetFragment::~OneLakeParquetFragment() {
}

void OneLakeParquetFragment::Append(DataChunk &chunk) {
	if (state->finished || state->failed) {
		throw IOException("OneLake Parquet fragment is no longer writable");
	}
	try {
		state->function.copy_to_sink(state->execution, *state->bind, *state->global, *state->local, chunk);
	} catch (...) {
		state->failed = true;
		throw;
	}
}

void OneLakeParquetFragment::AppendArrowBatch(shared_ptr<ArrowArrayWrapper> batch, ArrowTableSchema &schema) {
	if (state->finished || state->failed) {
		throw IOException("OneLake Parquet fragment is no longer writable");
	}
	if (!batch || !batch->arrow_array.release || batch->arrow_array.length < 0 ||
	    batch->arrow_array.n_children != int64_t(state->types.size()) || schema.GetTypes() != state->types ||
	    schema.GetNames() != state->names) {
		state->failed = true;
		throw IOException("OneLake cache Arrow schema or batch is invalid");
	}
	try {
		ArrowScanLocalState scan(nullptr, state->context);
		scan.chunk = std::move(batch);
		for (idx_t i = 0; i < state->types.size(); ++i) {
			scan.column_ids.push_back(i);
		}
		DataChunk chunk;
		chunk.Initialize(BufferAllocator::Get(state->context), state->types);
		const auto rows = idx_t(scan.chunk->arrow_array.length);
		while (scan.chunk_offset < rows) {
			chunk.Reset();
			chunk.SetCardinality(MinValue<idx_t>(STANDARD_VECTOR_SIZE, rows - scan.chunk_offset));
			ArrowTableFunction::ArrowToDuckDB(scan, schema.GetColumns(), chunk, scan.chunk_offset);
			Append(chunk);
			scan.chunk_offset += chunk.size();
		}
	} catch (...) {
		state->failed = true;
		throw;
	}
}

string OneLakeParquetFragment::Finish() {
	if (state->finished || state->failed) {
		throw IOException("Cannot finish an invalid OneLake Parquet fragment");
	}
	try {
		state->function.copy_to_combine(state->execution, *state->bind, *state->global, *state->local);
		state->local.reset();
		state->function.copy_to_finalize(state->context, *state->bind, *state->global);
		state->global.reset();
		state->storage->Seal(state->path);
		auto path = state->path;
		state->finished = true;
		return path;
	} catch (...) {
		state->failed = true;
		throw;
	}
}

} // namespace duckdb
