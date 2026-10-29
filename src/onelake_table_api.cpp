#include "onelake_table_api.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <curl/curl.h>
#include <json/json.h>
#include <openssl/evp.h>
#include <set>

namespace duckdb {
namespace {

bool IsGuid(const string &value) {
	if (value.size() != 36) {
		return false;
	}
	for (idx_t i = 0; i < value.size(); i++) {
		if (i == 8 || i == 13 || i == 18 || i == 23) {
			if (value[i] != '-') {
				return false;
			}
		} else if (!StringUtil::CharacterIsHex(value[i])) {
			return false;
		}
	}
	return true;
}

string Encode(const string &value) {
	auto escaped = curl_easy_escape(nullptr, value.c_str(), static_cast<int>(value.size()));
	if (!escaped) {
		throw IOException("Cannot encode OneLake catalog identifier");
	}
	string result(escaped);
	curl_free(escaped);
	return result;
}

Json::Value Parse(const string &body) {
	Json::Value result;
	Json::CharReaderBuilder builder;
	std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
	if (!reader->parse(body.data(), body.data() + body.size(), &result, nullptr)) {
		throw IOException("Invalid OneLake metadata response");
	}
	return result;
}

size_t Receive(void *buffer, size_t size, size_t count, void *target) {
	auto &body = *static_cast<string *>(target);
	constexpr size_t limit = 16 * 1024 * 1024;
	if (size != 0 && count > (limit - body.size()) / size) {
		return 0;
	}
	try {
		body.append(static_cast<char *>(buffer), size * count);
	} catch (...) {
		return 0;
	}
	return size * count;
}

Json::Value Get(const string &url, const string &token) {
	std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(), curl_easy_cleanup);
	if (!curl) {
		throw IOException("Cannot initialize OneLake metadata request");
	}
	string body;
	auto raw_headers = curl_slist_append(nullptr, ("Authorization: Bearer " + token).c_str());
	if (!raw_headers) {
		throw IOException("Cannot initialize OneLake metadata headers");
	}
	std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(raw_headers, curl_slist_free_all);
	curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
	curl_easy_setopt(curl.get(), CURLOPT_FOLLOWLOCATION, 0L);
	curl_easy_setopt(curl.get(), CURLOPT_CONNECTTIMEOUT, 15L);
	curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT, 60L);
	curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, Receive);
	curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &body);
	auto status = curl_easy_perform(curl.get());
	long http_status = 0;
	curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &http_status);
	if (status != CURLE_OK || http_status != 200) {
		throw IOException("OneLake metadata request failed (HTTP %ld); no file fallback attempted", http_status);
	}
	return Parse(body);
}

vector<Json::Value> Pages(const string &url, const string &token, const string &field, bool fabric = false) {
	vector<Json::Value> result;
	std::set<string> seen;
	string next;
	do {
		auto page_url = url;
		if (!next.empty()) {
			page_url += (url.find('?') == string::npos ? "?" : "&");
			page_url += (fabric ? "continuationToken=" : "page_token=") + Encode(next);
		}
		auto page = Get(page_url, token);
		if (!page[field].isArray()) {
			throw IOException("OneLake catalog response omitted its entry list");
		}
		for (const auto &entry : page[field]) {
			result.push_back(entry);
		}
		const auto &continuation = page[fabric ? "continuationToken" : "next_page_token"];
		if (!continuation.isNull() && !continuation.isString()) {
			throw IOException("Invalid OneLake catalog continuation");
		}
		next = continuation.isNull() ? string() : continuation.asString();
		if (!next.empty() && !seen.insert(next).second) {
			throw IOException("Repeated OneLake catalog continuation");
		}
	} while (!next.empty());
	return result;
}

string Origin(const string &endpoint, const string &expected_path) {
	std::unique_ptr<CURLU, decltype(&curl_url_cleanup)> url(curl_url(), curl_url_cleanup);
	if (!url || curl_url_set(url.get(), CURLUPART_URL, endpoint.c_str(), 0) != CURLUE_OK) {
		throw IOException("Invalid discovered OneLake table endpoint");
	}
	auto part = [&](CURLUPart component) {
		char *value = nullptr;
		if (curl_url_get(url.get(), component, &value, 0) != CURLUE_OK) {
			return string();
		}
		string result(value);
		curl_free(value);
		return result;
	};
	auto host = StringUtil::Lower(part(CURLUPART_HOST));
	if (part(CURLUPART_SCHEME) != "https" ||
	    !(host == "onelake.table.fabric.microsoft.com" ||
	      StringUtil::EndsWith(host, "-onelake.table.fabric.microsoft.com")) ||
	    !part(CURLUPART_USER).empty() || !part(CURLUPART_PASSWORD).empty() ||
	    (!part(CURLUPART_PORT).empty() && part(CURLUPART_PORT) != "443") || !part(CURLUPART_QUERY).empty() ||
	    !part(CURLUPART_FRAGMENT).empty() || part(CURLUPART_PATH) != expected_path) {
		throw IOException("Discovered OneLake table endpoint did not match the requested item");
	}
	return "https://" + host;
}

vector<string> Names(const vector<Json::Value> &entries) {
	vector<string> names;
	case_insensitive_set_t seen;
	for (const auto &entry : entries) {
		if (!entry["name"].isString() || entry["name"].asString().empty() ||
		    !seen.insert(entry["name"].asString()).second) {
			throw IOException("Invalid or ambiguous OneLake catalog name");
		}
		names.push_back(entry["name"].asString());
	}
	return names;
}

} // namespace

string OneLakeTableAPI::Token(ClientContext &context, OneLakeCredentials &credentials, OneLakeTokenAudience audience) {
	auto token = OneLakeAPI::GetAccessToken(credentials, audience, &context);
	if (token.size() > 65536 || token.find_first_of("\r\n") != string::npos) {
		throw IOException("Invalid OneLake caller token");
	}
	// Claims are used only to isolate the attachment's identity, never to grant access.
	auto parts = StringUtil::Split(token, '.');
	if (parts.size() != 3 || parts[1].empty() || parts[1].size() > 32768) {
		throw IOException("A caller Entra token is required for OneLake table reads");
	}
	auto payload = parts[1];
	for (auto &ch : payload) {
		if (ch == '-') {
			ch = '+';
		} else if (ch == '_') {
			ch = '/';
		}
	}
	while (payload.size() % 4) {
		payload += '=';
	}
	string decoded(payload.size(), '\0');
	auto count = EVP_DecodeBlock(reinterpret_cast<unsigned char *>(&decoded[0]),
	                             reinterpret_cast<const unsigned char *>(payload.data()), int(payload.size()));
	if (count < 0) {
		throw IOException("Invalid caller token encoding");
	}
	while (!payload.empty() && payload.back() == '=') {
		payload.pop_back();
		count--;
	}
	if (count <= 0) {
		throw IOException("Invalid caller token claims");
	}
	decoded.resize(count);
	auto claims = Parse(decoded);
	if (!claims["tid"].isString() || !claims["oid"].isString() || !IsGuid(claims["tid"].asString()) ||
	    !IsGuid(claims["oid"].asString())) {
		throw IOException("OneLake table reads require tenant and object identity claims");
	}
	auto key = claims["tid"].asString() + "/" + claims["oid"].asString();
	std::lock_guard<std::mutex> guard(*credentials.token_mutex);
	if (!credentials.principal_key.empty() && credentials.principal_key != key) {
		throw IOException("OneLake caller identity changed; detach and reattach before querying");
	}
	credentials.principal_key = key;
	return token;
}

OneLakeLakehouse OneLakeTableAPI::ResolveItem(ClientContext &context, const string &workspace_id, const string &item,
                                              OneLakeCredentials &credentials) {
	if (!IsGuid(workspace_id)) {
		throw InvalidInputException("OneLake workspace must resolve to a GUID");
	}
	string id = IsGuid(item) ? StringUtil::Lower(item) : item;
	if (!IsGuid(id)) {
		auto entries = Pages("https://api.fabric.microsoft.com/v1/workspaces/" + workspace_id + "/items",
		                     Token(context, credentials, OneLakeTokenAudience::Fabric), "value", true);
		id.clear();
		for (const auto &entry : entries) {
			if (entry["displayName"].isString() && StringUtil::CIEquals(entry["displayName"].asString(), item)) {
				if (!id.empty()) {
					throw InvalidInputException("Ambiguous OneLake item name; use its GUID");
				}
				id = entry["id"].asString();
			}
		}
		if (!IsGuid(id)) {
			throw CatalogException("OneLake item not found; use an accessible item name or GUID");
		}
	}
	auto root = Get("https://onelake.table.fabric.microsoft.com/api/v2/" + workspace_id + "/" + id + "/endpoints",
	                Token(context, credentials));
	if (root["workspace"]["id"].asString() != workspace_id || !root["items"].isArray()) {
		throw IOException("OneLake discovery identity mismatch");
	}
	for (const auto &entry : root["items"]) {
		if (entry["id"].asString() != id) {
			continue;
		}
		OneLakeLakehouse result;
		result.id = id;
		result.name = entry["name"].asString();
		result.display_name = result.name;
		result.schema_enabled = entry.get("isSchemaEnabled", false).asBool();
		const auto path = "/delta/" + workspace_id + "/" + id + "/api/2.1/unity-catalog";
		for (const auto &engine : entry["engines"]) {
			for (const auto &endpoint : engine["endpoints"]) {
				if (endpoint["type"].asString() != "Delta") {
					continue;
				}
				auto audience = endpoint["auth"]["audience"].asString();
				if (audience != "https://storage.azure.com/" && audience != "https://storage.azure.com") {
					throw IOException("Unexpected OneLake table endpoint audience");
				}
				result.table_api_origin = Origin(endpoint["data"].asString(), path);
				result.table_catalog_url = result.table_api_origin + path;
				return result;
			}
		}
		throw IOException("Item did not advertise a supported OneLake table catalog; no file fallback attempted");
	}
	throw IOException("OneLake discovery omitted the requested item");
}

vector<string> OneLakeTableAPI::Schemas(ClientContext &context, const OneLakeLakehouse &item,
                                        OneLakeCredentials &credentials) {
	return Names(Pages(item.table_catalog_url + "/schemas?catalog_name=" + Encode(item.id), Token(context, credentials),
	                   "schemas"));
}

vector<string> OneLakeTableAPI::Tables(ClientContext &context, const OneLakeLakehouse &item, const string &schema,
                                       OneLakeCredentials &credentials) {
	return Names(
	    Pages(item.table_catalog_url + "/tables?catalog_name=" + Encode(item.id) + "&schema_name=" + Encode(schema),
	          Token(context, credentials), "tables"));
}

} // namespace duckdb
