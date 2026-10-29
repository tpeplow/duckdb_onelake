#include "onelake_cache_storage.hpp"
#include "duckdb/common/types/uuid.hpp"

#include <filesystem>
#include <iostream>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <aclapi.h>
#else
#include <sys/stat.h>
#endif

using namespace duckdb;

void TestCacheDirectory(const string &directory) {
	namespace fs = std::filesystem;
	if (!OneLakeCacheDirectoryIsPrivate(directory)) {
		throw std::runtime_error("Test requires an existing private spill directory");
	}
	auto root = fs::u8path(directory);
	auto child = root / ("admission-" + duckdb::UUID::ToString(duckdb::UUID::GenerateRandomUUID()));
	if (!fs::create_directory(child)) {
		throw std::runtime_error("Admission fixture already exists");
	}
#ifdef _WIN32
	if (SetNamedSecurityInfoW(const_cast<wchar_t *>(child.c_str()), SE_FILE_OBJECT,
	                          DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION, nullptr, nullptr,
	                          nullptr, nullptr) != ERROR_SUCCESS) {
		fs::remove(child);
		throw std::runtime_error("Could not set synthetic admission fixture ACL");
	}
#else
	if (chmod(child.c_str(), 0755) != 0) {
		fs::remove(child);
		throw std::runtime_error("Could not set synthetic admission fixture permissions");
	}
#endif
	const bool admitted = OneLakeCacheDirectoryIsPrivate(child.u8string());
	fs::remove(child);
	if (admitted) {
		throw std::runtime_error("World-accessible directory passed cache admission");
	}
	auto link = root / ("link-" + duckdb::UUID::ToString(duckdb::UUID::GenerateRandomUUID()));
	std::error_code error;
	fs::create_directory_symlink(root, link, error);
	if (!error) {
		const bool linked = OneLakeCacheDirectoryIsPrivate(link.u8string());
		fs::remove(link);
		if (linked) {
			throw std::runtime_error("Linked directory passed cache admission");
		}
	} else {
		std::cout << "Symlink fixture unavailable; host did not allow its creation\n";
	}
	if (!OneLakeCacheDirectoryIsPrivate(directory)) {
		throw std::runtime_error("Admission fixtures changed the owning directory");
	}
	std::cout << "Private-directory admission and unsafe ACL/permission rejection passed\n";
}
