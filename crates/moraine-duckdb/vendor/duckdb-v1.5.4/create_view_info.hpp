// Vendored by hand, verbatim except for its own `#include`s, from
// https://raw.githubusercontent.com/duckdb/duckdb/v1.5.4/src/include/duckdb/parser/parsed_data/create_view_info.hpp
// (git tag v1.5.4, commit 08e34c447b). Same amalgamation gap as
// `storage_extension.hpp`. Its includes (`create_info.hpp`,
// `select_statement.hpp`) are already fully defined in `duckdb.hpp`, so this
// class body is self-contained against it. Kept byte-for-byte identical to
// upstream — see `create_schema_info.hpp` for why. Must be included after
// `duckdb.hpp` and `create_schema_info.hpp`.
#pragma once

namespace duckdb {
class SchemaCatalogEntry;

struct CreateViewInfo : public CreateInfo {
public:
	CreateViewInfo();
	CreateViewInfo(SchemaCatalogEntry &schema, string view_name);
	CreateViewInfo(string catalog_p, string schema_p, string view_name);

public:
	//! View name
	string view_name;
	//! Aliases of the view
	vector<string> aliases;
	//! Return types
	vector<LogicalType> types;
	//! Names of the query
	vector<string> names;
	//! Comments on columns of the query. Note: vector can be empty when no comments are set
	unordered_map<string, Value> column_comments_map;
	//! The SelectStatement of the view
	unique_ptr<SelectStatement> query;

public:
	unique_ptr<CreateInfo> Copy() const override;

	//! Gets a bound CreateViewInfo object from a SELECT statement and a view name, schema name, etc
	DUCKDB_API static unique_ptr<CreateViewInfo> FromSelect(ClientContext &context, unique_ptr<CreateViewInfo> info);
	//! Gets a bound CreateViewInfo object from a CREATE VIEW statement
	DUCKDB_API static unique_ptr<CreateViewInfo> FromCreateView(ClientContext &context, SchemaCatalogEntry &schema,
	                                                            const string &sql);
	//! Parse a SELECT statement from a SQL string
	DUCKDB_API static unique_ptr<SelectStatement> ParseSelect(const string &sql);

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override;

private:
	CreateViewInfo(vector<string> names, vector<Value> comments, unordered_map<string, Value> column_comments);

	vector<Value> GetColumnCommentsList() const;
};

} // namespace duckdb
