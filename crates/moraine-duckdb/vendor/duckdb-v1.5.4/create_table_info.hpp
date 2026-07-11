// Vendored by hand, verbatim except for its own `#include`s, from
// https://raw.githubusercontent.com/duckdb/duckdb/v1.5.4/src/include/duckdb/parser/parsed_data/create_table_info.hpp
// (git tag v1.5.4, commit 08e34c447b). Same amalgamation gap as
// `storage_extension.hpp`. Its includes (`create_info.hpp`, `constraint.hpp`,
// `select_statement.hpp`, `column_list.hpp`) are all already fully defined
// in `duckdb.hpp`, so this class body is self-contained against it. Kept
// byte-for-byte identical to upstream so this translation unit's
// `CreateTableInfo` has the exact same virtual-method set as the one the
// host DuckDB process was compiled from — see `create_schema_info.hpp` for
// why that matters. Must be included after `duckdb.hpp` and
// `create_schema_info.hpp`.
#pragma once

namespace duckdb {
class SchemaCatalogEntry;

struct CreateTableInfo : public CreateInfo {
	DUCKDB_API CreateTableInfo();
	DUCKDB_API CreateTableInfo(string catalog, string schema, string name);
	DUCKDB_API CreateTableInfo(SchemaCatalogEntry &schema, string name);

	//! Table name to insert to
	string table;
	//! List of columns of the table
	ColumnList columns;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! CREATE TABLE as QUERY
	unique_ptr<SelectStatement> query;
	//! Table Partition definitions
	vector<unique_ptr<ParsedExpression>> partition_keys;
	//! Table Sort definitions
	vector<unique_ptr<ParsedExpression>> sort_keys;
	//! Extra Table options if any
	case_insensitive_map_t<unique_ptr<ParsedExpression>> options;

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ExtraOptionsToString() const;
	string ToString() const override;
};

} // namespace duckdb
