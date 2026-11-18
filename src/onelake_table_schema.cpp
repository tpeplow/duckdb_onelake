#include "onelake_table_schema.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include <curl/curl.h>
#include <json/json.h>

namespace duckdb {
namespace {

[[noreturn]] void InvalidSchema(const string &reason) {
	throw IOException("Invalid or unsupported OneLake Get table schema: %s; no read fallback attempted", reason);
}

Json::Value ParseJSON(const string &text) {
	Json::CharReaderBuilder builder;
	builder["rejectDupKeys"] = true;
	builder["failIfExtra"] = true;
	builder["stackLimit"] = 128;
	std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
	Json::Value value;
	if (!reader->parse(text.data(), text.data() + text.size(), &value, nullptr)) {
		InvalidSchema("malformed JSON");
	}
	return value;
}

string Name(const Json::Value &value) {
	if (!value.isString() || value.asString().empty() || value.asString().find('\0') != string::npos) {
		InvalidSchema("missing or invalid name");
	}
	return value.asString();
}

std::optional<bool> Nullable(const Json::Value &value) {
	if (value.isNull()) {
		return {};
	}
	if (!value.isBool()) {
		InvalidSchema("invalid nullability");
	}
	return value.asBool();
}

LogicalType Decimal(int precision, int scale) {
	if (precision < 1 || precision > 38 || scale < 0 || scale > precision) {
		InvalidSchema("invalid decimal precision or scale");
	}
	return LogicalType::DECIMAL(uint8_t(precision), uint8_t(scale));
}

LogicalType Primitive(string name) {
	name = StringUtil::Lower(name);
	StringUtil::Trim(name);
	if (name == "boolean") {
		return LogicalType::BOOLEAN;
	}
	if (name == "byte" || name == "tinyint") {
		return LogicalType::TINYINT;
	}
	if (name == "short" || name == "smallint") {
		return LogicalType::SMALLINT;
	}
	if (name == "integer" || name == "int") {
		return LogicalType::INTEGER;
	}
	if (name == "long" || name == "bigint") {
		return LogicalType::BIGINT;
	}
	if (name == "float") {
		return LogicalType::FLOAT;
	}
	if (name == "double") {
		return LogicalType::DOUBLE;
	}
	if (name == "string") {
		return LogicalType::VARCHAR;
	}
	if (name == "binary") {
		return LogicalType::BLOB;
	}
	if (name == "date") {
		return LogicalType::DATE;
	}
	if (name == "timestamp") {
		return LogicalType::TIMESTAMP_TZ;
	}
	if (name == "timestamp_ntz") {
		return LogicalType::TIMESTAMP;
	}
	if (StringUtil::StartsWith(name, "decimal(") && name.back() == ')') {
		auto parts = StringUtil::Split(name.substr(8, name.size() - 9), ',');
		if (parts.size() == 2) {
			int numbers[2] = {};
			for (idx_t i = 0; i < 2; ++i) {
				StringUtil::Trim(parts[i]);
				if (parts[i].empty() || parts[i].size() > 2) {
					InvalidSchema("invalid decimal type");
				}
				for (auto ch : parts[i]) {
					if (ch < '0' || ch > '9') {
						InvalidSchema("invalid decimal type");
					}
					numbers[i] = numbers[i] * 10 + ch - '0';
				}
			}
			return Decimal(numbers[0], numbers[1]);
		}
	}
	InvalidSchema("type '" + name + "' requires a supported, complete type description");
}

OneLakeColumnSchema Type(const Json::Value &value, idx_t depth = 0) {
	if (depth > 64) {
		InvalidSchema("type nesting exceeds 64 levels");
	}
	OneLakeColumnSchema result;
	if (value.isString()) {
		result.type = Primitive(value.asString());
		return result;
	}
	if (!value.isObject() || !value["type"].isString()) {
		InvalidSchema("missing type description");
	}
	auto kind = StringUtil::Lower(value["type"].asString());
	if (kind == "struct") {
		if (!value["fields"].isArray() || value["fields"].empty()) {
			InvalidSchema("struct omitted its fields");
		}
		child_list_t<LogicalType> fields;
		case_insensitive_set_t seen;
		for (const auto &field : value["fields"]) {
			if (!field.isObject()) {
				InvalidSchema("invalid struct field");
			}
			auto child = Type(field["type"], depth + 1);
			child.name = Name(field["name"]);
			child.nullable = Nullable(field["nullable"]);
			if (!seen.insert(child.name).second) {
				InvalidSchema("ambiguous struct field names");
			}
			fields.emplace_back(child.name, child.type);
			result.children.push_back(std::move(child));
		}
		result.type = LogicalType::STRUCT(std::move(fields));
	} else if (kind == "array") {
		auto child = Type(value["elementType"], depth + 1);
		child.nullable = Nullable(value["containsNull"]);
		result.type = LogicalType::LIST(child.type);
		result.children.push_back(std::move(child));
	} else if (kind == "map") {
		auto key = Type(value["keyType"], depth + 1);
		auto val = Type(value["valueType"], depth + 1);
		key.nullable = false;
		val.nullable = Nullable(value["valueContainsNull"]);
		result.type = LogicalType::MAP(key.type, val.type);
		result.children.push_back(std::move(key));
		result.children.push_back(std::move(val));
	} else {
		result.type = Primitive(kind);
	}
	return result;
}

class TextType {
public:
	explicit TextType(const string &text) : text(text) {
	}

	OneLakeColumnSchema Parse() {
		auto result = Read(0);
		Space();
		if (position != text.size()) {
			InvalidSchema("trailing type_text content");
		}
		return result;
	}

private:
	void Space() {
		while (position < text.size() && StringUtil::CharacterIsSpace(text[position])) {
			++position;
		}
	}
	bool Take(char ch) {
		Space();
		if (position < text.size() && text[position] == ch) {
			++position;
			return true;
		}
		return false;
	}
	void Expect(char ch) {
		if (!Take(ch)) {
			InvalidSchema("incomplete type_text");
		}
	}
	string Word(bool field = false) {
		Space();
		string result;
		if (field && Take('`')) {
			for (;;) {
				if (position == text.size()) {
					InvalidSchema("unterminated quoted field name");
				}
				auto ch = text[position++];
				if (ch == '`') {
					if (position == text.size() || text[position] != '`') {
						break;
					}
					++position;
				}
				result += ch;
			}
		} else {
			while (position < text.size() &&
			       (StringUtil::CharacterIsAlphaNumeric(text[position]) || text[position] == '_')) {
				result += text[position++];
			}
		}
		if (result.empty() || result.find('\0') != string::npos) {
			InvalidSchema("invalid type_text identifier");
		}
		return result;
	}
	OneLakeColumnSchema Read(idx_t depth) {
		if (depth > 64) {
			InvalidSchema("type nesting exceeds 64 levels");
		}
		auto kind = StringUtil::Lower(Word());
		OneLakeColumnSchema result;
		if (kind == "array" || kind == "map") {
			Expect('<');
			result.children.push_back(Read(depth + 1));
			if (kind == "map") {
				Expect(',');
				result.children.push_back(Read(depth + 1));
				result.children[0].nullable = false;
				result.type = LogicalType::MAP(result.children[0].type, result.children[1].type);
			} else {
				result.type = LogicalType::LIST(result.children[0].type);
			}
			Expect('>');
		} else if (kind == "struct") {
			Expect('<');
			child_list_t<LogicalType> fields;
			case_insensitive_set_t seen;
			do {
				auto name = Word(true);
				Expect(':');
				auto child = Read(depth + 1);
				child.name = name;
				if (!seen.insert(name).second) {
					InvalidSchema("ambiguous type_text field names");
				}
				fields.emplace_back(name, child.type);
				result.children.push_back(std::move(child));
			} while (Take(','));
			Expect('>');
			result.type = LogicalType::STRUCT(std::move(fields));
		} else {
			if (kind == "decimal") {
				Expect('(');
				auto precision = Word();
				Expect(',');
				auto scale = Word();
				Expect(')');
				kind += "(" + precision + "," + scale + ")";
			}
			result.type = Primitive(kind);
		}
		return result;
	}

	const string &text;
	idx_t position = 0;
};

OneLakeColumnSchema Column(const Json::Value &column) {
	auto kind = StringUtil::Lower(Name(column["type_name"]));
	OneLakeColumnSchema result;
	const auto &json = column["type_json"];
	if (!json.isNull()) {
		if (!json.isString()) {
			InvalidSchema("type_json must be a JSON string");
		}
		auto value = ParseJSON(json.asString());
		// Unity Catalog type_json contains a Spark StructField, not only its type.
		if (value.isObject() && value.isMember("name")) {
			if (Name(value["name"]) != Name(column["name"])) {
				InvalidSchema("type_json field name mismatch");
			}
			result = Type(value["type"]);
			result.nullable = Nullable(value["nullable"]);
		} else {
			result = Type(value);
		}
	} else if (!column["type_text"].isNull()) {
		result = TextType(Name(column["type_text"])).Parse();
	} else if (kind == "decimal") {
		if (!column["type_precision"].isInt() || !column["type_scale"].isInt()) {
			InvalidSchema("decimal omitted precision or scale");
		}
		result.type = Decimal(column["type_precision"].asInt(), column["type_scale"].asInt());
	} else {
		result.type = Primitive(kind);
	}
	LogicalType expected;
	if (kind == "array" || kind == "struct" || kind == "map") {
		auto id = kind == "array" ? LogicalTypeId::LIST : kind == "struct" ? LogicalTypeId::STRUCT : LogicalTypeId::MAP;
		if (result.type.id() != id) {
			InvalidSchema("type_name disagrees with nested type description");
		}
	} else {
		if (kind == "decimal") {
			if (column["type_precision"].isNull() && column["type_scale"].isNull() &&
			    result.type.id() == LogicalTypeId::DECIMAL) {
				expected = result.type;
			} else {
				if (!column["type_precision"].isInt() || !column["type_scale"].isInt()) {
					InvalidSchema("decimal omitted precision or scale");
				}
				expected = Decimal(column["type_precision"].asInt(), column["type_scale"].asInt());
			}
		} else {
			expected = Primitive(kind);
		}
		if (result.type != expected) {
			InvalidSchema("type_name disagrees with type description");
		}
	}
	if (!column["type_text"].isNull() && TextType(Name(column["type_text"])).Parse().type != result.type) {
		InvalidSchema("type_text disagrees with type description");
	}
	auto nullable = Nullable(column["nullable"]);
	if (nullable && result.nullable && nullable != result.nullable) {
		InvalidSchema("conflicting field nullability");
	}
	if (nullable) {
		result.nullable = nullable;
	}
	result.name = Name(column["name"]);
	return result;
}

string Encode(const string &value) {
	if (value.empty() || value.find('\0') != string::npos) {
		InvalidSchema("empty or invalid table identifier");
	}
	auto escaped = curl_easy_escape(nullptr, value.c_str(), static_cast<int>(value.size()));
	if (!escaped) {
		throw IOException("Cannot encode OneLake table identifier");
	}
	string result(escaped);
	curl_free(escaped);
	return result;
}

} // namespace

OneLakeTableSchema ParseOneLakeTableSchema(const string &body) {
	auto root = ParseJSON(body);
	if (!root.isObject() || !root["columns"].isArray() || root["columns"].empty()) {
		InvalidSchema("no columns returned");
	}
	const auto &columns = root["columns"];
	OneLakeTableSchema result(columns.size());
	vector<bool> positions(columns.size(), false);
	case_insensitive_set_t names;
	for (const auto &column : columns) {
		if (!column.isObject() || !column["position"].isUInt() || column["position"].asUInt() >= columns.size()) {
			InvalidSchema("invalid column position");
		}
		auto position = column["position"].asUInt();
		if (positions[position]) {
			InvalidSchema("duplicate column position");
		}
		positions[position] = true;
		auto parsed = Column(column);
		if (!names.insert(parsed.name).second) {
			InvalidSchema("ambiguous column names");
		}
		result[position] = std::move(parsed);
	}
	return result;
}

string OneLakeGetTableURL(const string &catalog_url, const string &catalog, const string &schema, const string &table) {
	// Explicit qualifiers disambiguate identifiers that themselves contain dots.
	auto catalog_component = Encode(catalog);
	auto schema_component = Encode(schema);
	auto table_component = Encode(table);
	return catalog_url + "/tables/" + catalog_component + "." + schema_component + "." + table_component +
	       "?catalog_name=" + catalog_component + "&schema_name=" + schema_component;
}

} // namespace duckdb
