// The TableFunction moraine table scans delegate to.
//
// `duckdb.hpp`'s amalgamation never defines `duckdb::TableFunctionRef` (a
// SQL parser AST node) or the parser types it in turn requires
// (`SelectStatement`, `ParsedExpression`, ...) — the same gap that keeps
// view query binding unsupported this slice (see catalog.hpp's
// `MoraineViewEntry` doc comment). Constructing one is the only way to
// build a `duckdb::TableFunctionBindInput` and call a catalog-registered
// TableFunction's own `bind` callback (e.g. `read_parquet`'s), so that
// route is unavailable here without vendoring the full parser — the
// plan's own "large, non-self-contained transitive chain" escalation
// trigger.
//
// Instead, this delegates through SQL text: at Init time (once per query
// *execution*, not once per bind/DESCRIBE — see MoraineScanFunction's own
// comment) it opens a fresh, short-lived `duckdb::Connection` to the same
// `duckdb::DatabaseInstance` the table is attached to and starts
// `SELECT * FROM read_parquet([...])` over the bind data's already-resolved
// file paths as a *streaming* query (`Connection::SendQuery`, never
// `Query()` — no full in-memory materialization), then pulls one of that
// query's result chunks per `.function` call and hands it straight through
// as this function's output. Pulling per call (rather than materializing
// everything inside init) also keeps the outer engine's per-chunk
// scheduling/interrupt cadence: a long scan yields back to DuckDB between
// chunks instead of running to completion inside one callback.
// `Connection`/`QueryResult` are DuckDB's public C++ embedding API — fully
// defined in `duckdb.hpp`, no further vendoring needed — and
// parse/bind/execute entirely inside the host's own compiled code; this
// shim never touches a parser type itself.
#pragma once

#include <string>
#include <vector>

#include "duckdb.hpp"

namespace moraine_duckdb {

// Bind data for a moraine-backed table scan: the fully resolved
// object-store paths of the table's live data files (see
// MoraineTableEntry::GetScanFunction for how `path`/`path_is_relative` are
// resolved into these), plus the `DatabaseInstance` to scan them against.
// Empty `file_paths` means the table has no live data files — the scan
// produces zero rows without ever running a query (DuckDB's `read_parquet`
// errors on an empty file list, so this case is deliberately never handed
// to it).
struct MoraineScanBindData : public duckdb::FunctionData {
	std::vector<std::string> file_paths;
	// A raw pointer, not `duckdb::optional_ptr`: `optional_ptr<T>::operator*`
	// returns `const T&` when called through a const-qualified
	// `optional_ptr` (which is what `Cast<MoraineScanBindData>() const`
	// yields), which would make it impossible to hand `*database` to
	// `Connection`'s non-const-reference constructor — a raw pointer keeps
	// the usual "pointer-through-const-object is still non-const-pointee"
	// C++ semantics instead.
	duckdb::DatabaseInstance *database = nullptr;
	// The catalog schema's column count and the table's qualified name, for
	// the streamed-chunk column-count guard (see MoraineScanFunctionImpl):
	// `DataChunk::Reference`'s own column-count check is a debug-only
	// D_ASSERT, so a Parquet file with more columns than the catalog
	// declares would be an out-of-bounds vector write in release builds
	// without an explicit check here. Type agreement is deferred; count
	// agreement is the memory-safety line.
	size_t catalog_column_count = 0;
	std::string table_name;
	// The catalog entry this scan reads, surfaced through the
	// TableFunction's `get_bind_info` callback so plan consumers that
	// trace a column back to its base table (DESCRIBE / `SHOW`'s
	// `FindBaseTableColumn` walks `LogicalGet::GetTable()`, which returns
	// null unless `get_bind_info` provides the entry) can read the
	// entry's constraints — without this, DESCRIBE hardcodes every
	// column as nullable. Non-owning: the entry lives in the
	// transaction-scoped schema cache (see MoraineSchemaEntry), which
	// outlives any plan built inside that transaction.
	duckdb::TableCatalogEntry *table_entry = nullptr;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
	bool Equals(const duckdb::FunctionData &other) const override;
};

// Builds the TableFunction struct itself (the `.function`/`.init_global`
// callbacks described above). Every call returns an equivalent, freshly
// constructed `TableFunction`; callers pair it with a `MoraineScanBindData`
// they populate themselves — no framework-driven `bind` call is involved
// (see MoraineTableEntry::GetScanFunction, which is the only caller and
// matches the base `TableCatalogEntry::GetScanFunction` contract: the
// override itself is responsible for producing complete bind data
// synchronously, not DuckDB's binder).
duckdb::TableFunction MoraineScanFunction();

} // namespace moraine_duckdb
