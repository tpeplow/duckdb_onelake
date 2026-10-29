#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include <map>
#include <string_view>
#include <vector>

namespace duckdb {

// DuckDB's vector/string aliases cannot carry an allocator. These containers
// charge allocations (including growth, nodes and string payloads) before use.
template <class T>
class OneLakeCacheAllocator {
public:
	using value_type = T;
	using propagate_on_container_move_assignment = std::true_type;

	OneLakeCacheAllocator() = default;
	OneLakeCacheAllocator(Allocator &allocator) : allocator(&allocator) { // NOLINT: allocator adapter
	}
	template <class U>
	OneLakeCacheAllocator(const OneLakeCacheAllocator<U> &other) : allocator(other.allocator) {
	}
	T *allocate(size_t count) {
		if (!allocator || count > size_t(-1) / sizeof(T)) {
			throw OutOfMemoryException("Invalid OneLake cache metadata allocation");
		}
		return reinterpret_cast<T *>(allocator->AllocateData(count * sizeof(T)));
	}
	void deallocate(T *pointer, size_t count) {
		allocator->FreeData(reinterpret_cast<data_ptr_t>(pointer), count * sizeof(T));
	}
	template <class U>
	bool operator==(const OneLakeCacheAllocator<U> &other) const {
		return allocator == other.allocator;
	}
	template <class U>
	bool operator!=(const OneLakeCacheAllocator<U> &other) const {
		return !(*this == other);
	}
	Allocator *allocator = nullptr;
};

using OneLakeCacheString = std::basic_string<char, std::char_traits<char>, OneLakeCacheAllocator<char>>;
template <class T>
using OneLakeCacheVector = std::vector<T, OneLakeCacheAllocator<T>>;
using OneLakeCachePaths = OneLakeCacheVector<OneLakeCacheString>;

struct OneLakeCacheStringLess {
	using is_transparent = void;
	template <class A, class B>
	bool operator()(const A &a, const B &b) const {
		return std::string_view(a.data(), a.size()) < std::string_view(b.data(), b.size());
	}
};
template <class T>
using OneLakeCacheMap = std::map<OneLakeCacheString, T, OneLakeCacheStringLess,
                                 OneLakeCacheAllocator<std::pair<const OneLakeCacheString, T>>>;

inline OneLakeCacheString CacheString(Allocator &allocator, const string &value) {
	return OneLakeCacheString(value.data(), value.size(), OneLakeCacheAllocator<char>(allocator));
}
template <class T, class... ARGS>
shared_ptr<T> MakeCacheShared(Allocator &allocator, ARGS &&...args) {
	return shared_ptr<T>(std::allocate_shared<T>(OneLakeCacheAllocator<T>(allocator), std::forward<ARGS>(args)...));
}

} // namespace duckdb
