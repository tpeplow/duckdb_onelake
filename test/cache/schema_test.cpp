#include "onelake_table_schema.hpp"
#include "duckdb/common/exception.hpp"
#include <iostream>

using namespace duckdb;

namespace {

void Check(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

string Column(const string &type, const string &extra = "") {
	return "{\"name\":\"value\",\"position\":0,\"type_name\":\"" + type + "\"" + extra + "}";
}

OneLakeTableSchema Parse(const string &columns) {
	return ParseOneLakeTableSchema("{\"columns\":[" + columns + "]}");
}

void Reject(const string &body) {
	try {
		ParseOneLakeTableSchema(body);
	} catch (const IOException &error) {
		Check(string(error.what()).find("no read fallback") != string::npos, "Missing actionable schema error");
		return;
	}
	throw std::runtime_error("Invalid metadata was accepted: " + body);
}

} // namespace

void TestTableSchema() {
	auto schema =
	    Parse(R"({"name":"price","position":1,"type_name":"double","type_json":null,"type_text":null,"nullable":true},
	                     {"name":"id","position":0,"type_name":"string","type_precision":0,"type_scale":0,"nullable":false})");
	Check(schema.size() == 2 && schema[0].name == "id" && schema[0].type == LogicalType::VARCHAR &&
	          schema[0].nullable == false && schema[1].type == LogicalType::DOUBLE && schema[1].nullable == true,
	      "Documented Get table response or position ordering failed");
	const vector<std::pair<string, LogicalType>> primitives {{"boolean", LogicalType::BOOLEAN},
	                                                         {"byte", LogicalType::TINYINT},
	                                                         {"short", LogicalType::SMALLINT},
	                                                         {"integer", LogicalType::INTEGER},
	                                                         {"long", LogicalType::BIGINT},
	                                                         {"float", LogicalType::FLOAT},
	                                                         {"double", LogicalType::DOUBLE},
	                                                         {"string", LogicalType::VARCHAR},
	                                                         {"binary", LogicalType::BLOB},
	                                                         {"date", LogicalType::DATE},
	                                                         {"timestamp", LogicalType::TIMESTAMP_TZ},
	                                                         {"timestamp_ntz", LogicalType::TIMESTAMP}};
	for (const auto &entry : primitives) {
		auto parsed = Parse(Column(entry.first));
		Check(parsed[0].type == entry.second && !parsed[0].nullable, "Scalar type or unknown nullability failed");
	}
	Check(Parse(Column("DECIMAL", R"json(,"type_precision":38,"type_scale":12,"type_text":"decimal(38,12)")json"))[0]
	              .type == LogicalType::DECIMAL(38, 12),
	      "Decimal precision/scale lost");
	Check(Parse(Column("decimal", R"(,"type_json":"\"decimal(18,3)\"")"))[0].type == LogicalType::DECIMAL(18, 3),
	      "Complete JSON decimal unnecessarily required redundant precision/scale");
	auto nested = Parse(Column(
	    "STRUCT",
	    R"(,"type_json":"{\"name\":\"value\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"items\",\"nullable\":false,\"type\":{\"type\":\"array\",\"containsNull\":true,\"elementType\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"decimal(18,3)\",\"valueContainsNull\":false}}}]},\"nullable\":true}")"));
	auto expected = LogicalType::STRUCT(
	    {{"items", LogicalType::LIST(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::DECIMAL(18, 3)))}});
	Check(nested[0].type == expected && nested[0].nullable == true && nested[0].children[0].nullable == false &&
	          nested[0].children[0].children[0].nullable == true &&
	          nested[0].children[0].children[0].children[1].nullable == false,
	      "Nested schema or nullability lost");
	auto text_nested = Parse(Column("struct", R"(,"type_text":"STRUCT<items: ARRAY<MAP<STRING, DECIMAL(18,3)>>>")"));
	Check(text_nested[0].type == expected && !text_nested[0].children[0].nullable,
	      "Nested type_text parsing failed or invented nullability");
	Check(Parse(Column("struct", R"(,"type_text":"struct<`a.b``c`:int>")"))[0].children[0].name == "a.b`c",
	      "Quoted nested field name failed");
	for (auto &body : {string("{}"), string("{\"columns\":null}"), string("{\"columns\":[]}"),
	                   string("{\"columns\":[],\"columns\":[]}"), string("{\"columns\":[null]}"),
	                   string("{\"columns\":[]} trailing")}) {
		Reject(body);
	}
	const vector<string> invalid {
	    Column("variant"),
	    Column("array"),
	    Column("struct", R"(,"type_text":"struct<a:>")"),
	    Column("struct", R"(,"type_text":"struct<a:int,A:string>")"),
	    Column("array", R"(,"type_text":"array<int> trailing")"),
	    Column("array", R"(,"type_text":"array<>")"),
	    Column(
	        "struct",
	        R"(,"type_json":"{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}","type_text":"struct<a:int>")"),
	    Column("decimal"),
	    Column("decimal", R"(,"type_precision":39,"type_scale":0)"),
	    Column("decimal", R"(,"type_precision":8,"type_scale":9)"),
	    Column("decimal", R"(,"type_precision":8,"type_scale":-1)"),
	    Column("decimal", R"json(,"type_precision":8,"type_scale":1,"type_text":"decimal(8,2)")json"),
	    Column("int", R"(,"nullable":"true")"),
	    Column("int", R"(,"type_text":"string")"),
	    Column("int", R"(,"type_json":"not json")"),
	    Column("int", R"(,"type_json":"{\"name\":\"other\",\"type\":\"integer\"}")"),
	    Column("int", R"(,"type_json":"{\"name\":\"value\",\"type\":\"integer\",\"nullable\":false}","nullable":true)"),
	    Column("struct", R"(,"type_json":"{\"type\":\"struct\",\"fields\":[]}")"),
	    Column("array", R"(,"type_json":"{\"type\":\"array\"}")"),
	    R"({"name":"a","type_name":"int"})",
	    R"({"name":"a","type_name":"int","position":-1})",
	    R"({"name":"a","type_name":"int","position":1})",
	    R"({"name":"","type_name":"int","position":0})",
	    R"({"name":"a\u0000b","type_name":"int","position":0})",
	    R"({"name":"a","type_name":"int","position":0},{"name":"b","type_name":"int","position":0})",
	    R"({"name":"a","type_name":"int","position":0},{"name":"A","type_name":"int","position":1})"};
	for (auto &columns : invalid) {
		Reject("{\"columns\":[" + columns + "]}");
	}
	const string catalog = "00000000-0000-0000-0000-000000000001";
	const string base =
	    "https://region-onelake.table.fabric.microsoft.com/delta/workspace/" + catalog + "/api/2.1/unity-catalog";
	Check(OneLakeGetTableURL(base, catalog, "dbo", "table") ==
	          base + "/tables/" + catalog + ".dbo.table?catalog_name=" + catalog + "&schema_name=dbo",
	      "Get table did not preserve the discovered endpoint or GUID catalog");
	Check(OneLakeGetTableURL(base, catalog, "a.b", "c.d /?%") ==
	          base + "/tables/" + catalog + ".a.b.c.d%20%2F%3F%25?catalog_name=" + catalog + "&schema_name=a.b",
	      "Get table identifier encoding or dotted qualifiers failed");
	Check(OneLakeGetTableURL(base, catalog, "dbo", "\xc3\xa9").find(".dbo.%C3%A9?") != string::npos,
	      "Get table Unicode encoding failed");
	std::cout << "Get table schema parsing, type errors and identifier encoding passed\n";
}
