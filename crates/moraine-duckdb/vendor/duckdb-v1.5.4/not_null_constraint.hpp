// Vendored by hand, verbatim except for its own `#include`, from
// https://raw.githubusercontent.com/duckdb/duckdb/v1.5.4/src/include/duckdb/parser/constraints/not_null_constraint.hpp
// (git tag v1.5.4, commit 08e34c447b). Same amalgamation gap as
// `storage_extension.hpp`: the amalgamation defines the `Constraint` base
// class and the `ConstraintType` enum but never reaches
// `duckdb/parser/constraints/not_null_constraint.hpp`. Its one include
// (`constraint.hpp`) is already fully defined in `duckdb.hpp`, as are
// `LogicalIndex`, `Serializer`/`Deserializer`, `string`, and `unique_ptr`,
// so this class body is self-contained against it. Kept byte-for-byte
// identical to upstream — see `create_schema_info.hpp` for why. Must be
// included after `duckdb.hpp`.
#pragma once

namespace duckdb {

class NotNullConstraint : public Constraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::NOT_NULL;

public:
	DUCKDB_API explicit NotNullConstraint(LogicalIndex index);
	DUCKDB_API ~NotNullConstraint() override;

	//! Column index this constraint pertains to
	LogicalIndex index;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
