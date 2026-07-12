// The TableFunction moraine's own user-table scans bind to.
//
// User-table *data* is served only through DuckLake (`ATTACH
// 'ducklake:moraine:<store>' ...`), never through this standalone attach:
// DuckLake owns delete-file merge-on-read and row lineage, and a second
// independent reader would silently return stale/deleted rows once
// merge-on-read exists here. So this scan still binds normally — DuckDB's
// planner needs a complete `(TableFunction, FunctionData)` pair for
// `DESCRIBE`/`EXPLAIN` to work — but its `init_global` (called once per
// query *execution*, not once per bind) unconditionally throws, naming the
// table and the DuckLake attach to use instead.
#pragma once

#include <string>

#include "duckdb.hpp"

namespace moraine_duckdb {

// Bind data for a moraine standalone-attach user-table scan: just enough to
// name the table in the redirect error thrown at execution time.
struct MoraineScanBindData : public duckdb::FunctionData {
	// Schema-qualified table name, for the redirect error message.
	std::string qualified_table_name;
	// The attach's store path, for the redirect error's `ducklake:moraine:`
	// form. Never resolved further (no filesystem access here).
	std::string store_path;
	// Surfaced through the TableFunction's `get_bind_info` callback so plan
	// consumers that trace a column back to its base table (DESCRIBE /
	// `SHOW`'s `FindBaseTableColumn` walks `LogicalGet::GetTable()`, which
	// returns null unless `get_bind_info` provides the entry) can read the
	// entry's constraints — without this, DESCRIBE hardcodes every column as
	// nullable. Non-owning: the entry lives in the transaction-scoped schema
	// cache (see MoraineSchemaEntry), which outlives any plan built inside
	// that transaction.
	duckdb::TableCatalogEntry *table_entry = nullptr;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
	bool Equals(const duckdb::FunctionData &other) const override;
};

// Builds the TableFunction struct itself. Every call returns an equivalent,
// freshly constructed `TableFunction`; callers pair it with a
// `MoraineScanBindData` they populate themselves — no framework-driven
// `bind` call is involved (see MoraineTableEntry::GetScanFunction, which is
// the only caller and matches the base `TableCatalogEntry::GetScanFunction`
// contract: the override itself is responsible for producing complete bind
// data synchronously, not DuckDB's binder).
duckdb::TableFunction MoraineScanFunction();

} // namespace moraine_duckdb
