#include "onelake_cache_storage.hpp"

#include <filesystem>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <aclapi.h>
#include <sddl.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace duckdb {
namespace {
namespace fs = std::filesystem;

#ifdef _WIN32
struct NativeHandle {
	HANDLE value = INVALID_HANDLE_VALUE;
	~NativeHandle() {
		if (value && value != INVALID_HANDLE_VALUE) {
			CloseHandle(value);
		}
	}
};
struct Descriptor {
	PSECURITY_DESCRIPTOR value = nullptr;
	~Descriptor() {
		LocalFree(value);
	}
};

bool PrivateACL(const fs::path &path, PSID user, PSID installer, bool private_access) {
	NativeHandle handle;
	handle.value =
	    CreateFileW(path.c_str(), READ_CONTROL | FILE_READ_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
	                OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
	if (handle.value == INVALID_HANDLE_VALUE) {
		return false;
	}
	BY_HANDLE_FILE_INFORMATION info;
	if (!GetFileInformationByHandle(handle.value, &info) || (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT)) {
		return false;
	}
	auto trusted = [&](PSID sid) {
		return sid && (EqualSid(sid, user) || EqualSid(sid, installer) || IsWellKnownSid(sid, WinLocalSystemSid) ||
		               IsWellKnownSid(sid, WinBuiltinAdministratorsSid));
	};
	Descriptor descriptor;
	PSID owner = nullptr;
	PACL acl = nullptr;
	if (GetSecurityInfo(handle.value, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION, &owner,
	                    nullptr, &acl, nullptr, &descriptor.value) != ERROR_SUCCESS ||
	    !acl || (private_access && !trusted(owner))) {
		return false;
	}
	for (DWORD i = 0; i < acl->AceCount; ++i) {
		void *raw = nullptr;
		if (!GetAce(acl, i, &raw)) {
			return false;
		}
		auto header = static_cast<ACE_HEADER *>(raw);
		if (!private_access && (header->AceFlags & INHERIT_ONLY_ACE)) {
			continue;
		}
		if (header->AceType == ACCESS_DENIED_ACE_TYPE) {
			continue;
		}
		if (header->AceType != ACCESS_ALLOWED_ACE_TYPE) {
			return false;
		}
		auto ace = static_cast<ACCESS_ALLOWED_ACE *>(raw);
		auto sid = reinterpret_cast<PSID>(&ace->SidStart);
		if (!trusted(sid)) {
			const DWORD mutation = GENERIC_ALL | GENERIC_WRITE | DELETE | WRITE_DAC | WRITE_OWNER | FILE_DELETE_CHILD |
			                       FILE_WRITE_DATA | FILE_WRITE_EA | FILE_WRITE_ATTRIBUTES;
			if (private_access || (ace->Mask & mutation)) {
				return false;
			}
		}
	}
	return true;
}
#else
bool PrivateMode(const fs::path &path, bool private_access) {
	struct stat info {};
	if (lstat(path.c_str(), &info) != 0 || S_ISLNK(info.st_mode)) {
		return false;
	}
	if (private_access) {
		return info.st_uid == geteuid() && !(info.st_mode & 0077);
	}
	return (info.st_uid == 0 || info.st_uid == geteuid()) &&
	       (!(info.st_mode & 0022) || ((info.st_mode & S_ISVTX) && info.st_uid == 0));
}
#endif
} // namespace

bool OneLakeCacheDirectoryIsPrivate(const string &directory) {
	if (directory.empty()) {
		// DuckDB cannot spill without a temporary directory.
		return true;
	}
	try {
		auto path = fs::u8path(directory);
		if (!path.is_absolute() || path != path.lexically_normal()) {
			return false;
		}
#ifdef _WIN32
		if (path.native().compare(0, 2, L"\\\\") == 0 || GetDriveTypeW(path.root_path().c_str()) != DRIVE_FIXED) {
			return false;
		}
		NativeHandle token;
		token.value = nullptr;
		if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token.value)) {
			return false;
		}
		alignas(TOKEN_USER) BYTE token_data[1024];
		DWORD size;
		if (!GetTokenInformation(token.value, TokenUser, token_data, sizeof(token_data), &size)) {
			return false;
		}
		auto user = reinterpret_cast<TOKEN_USER *>(token_data)->User.Sid;
		Descriptor installer;
		// TrustedInstaller, like SYSTEM/Administrators, is a trusted OS principal.
		if (!ConvertStringSidToSidW(L"S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464",
		                            &installer.value)) {
			return false;
		}
		auto check = [&](const fs::path &candidate, bool private_access) {
			return PrivateACL(candidate, user, installer.value, private_access);
		};
#else
		auto check = PrivateMode;
#endif
		// Check ancestors from the volume/root outward before resolving a child,
		// so a parent reparse point cannot redirect even admission I/O elsewhere.
		auto parent = path.root_path();
		if (!check(parent, false)) {
			return false;
		}
		for (auto &component : path.relative_path()) {
			parent /= component;
			if (parent != path && !check(parent, false)) {
				return false;
			}
		}
		if (!check(path, true) || !fs::is_directory(fs::symlink_status(path))) {
			return false;
		}
		for (auto &child : fs::directory_iterator(path)) {
			if (!fs::is_regular_file(child.symlink_status()) || !check(child.path(), true)) {
				return false;
			}
		}
		return true;
	} catch (...) {
		return false;
	}
}

} // namespace duckdb
