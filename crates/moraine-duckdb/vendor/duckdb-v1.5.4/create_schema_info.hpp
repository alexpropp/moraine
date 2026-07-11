// Vendored by hand, verbatim except for its own `#include`, from
// https://raw.githubusercontent.com/duckdb/duckdb/v1.5.4/src/include/duckdb/parser/parsed_data/create_schema_info.hpp
// (git tag v1.5.4, commit 08e34c447b). Same amalgamation gap as
// `storage_extension.hpp`: the amalgamation never reaches
// `duckdb/parser/parsed_data/create_schema_info.hpp`, but its base
// (`CreateInfo`) is already fully defined in `duckdb.hpp`, so this class
// body is self-contained against it. Kept byte-for-byte identical to
// upstream (not slimmed down) so this translation unit's `CreateSchemaInfo`
// has the exact same virtual-method set as the one the host DuckDB process
// was compiled from — the pure-virtual bodies it declares here (`Copy`,
// `Serialize`, `Deserialize`, `ToString`) are implemented in the host's own
// compiled `duckdb.cpp` and resolved at `dlopen` time, same as
// `StorageExtension::Register`. Must be included after `duckdb.hpp`.
#pragma once

namespace duckdb {

struct CreateSchemaInfo : public CreateInfo {
	CreateSchemaInfo();

public:
	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
