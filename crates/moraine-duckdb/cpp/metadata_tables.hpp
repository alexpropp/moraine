// Synthesized `ducklake_*` catalog tables: DuckLake's own metadata
// connection (a nested `ATTACH 'moraine:<path>' ...` this shim serves)
// speaks generic SQL against these, not the store's real user tables. Every
// column shape here is pinned verbatim from DuckLake's own
// `DuckLakeMetadataManager::InitializeDuckLake` (the `CREATE TABLE
// {METADATA_CATALOG}.ducklake_*(...)` text), read from the pinned DuckLake
// source commit — never guessed. `ducklake_metadata` is the one exception:
// it has no store-modeled source of truth, so its rows are synthesized
// in-process rather than read from the dump ABI (see metadata_tables.cpp).
//
// Only the tables the store models are served; DuckLake tables covering
// unmodeled features this slice (macros, partitioning, name mapping, data
// inlining, per-object tags) are absent, matching the plan's "absent kinds
// are absent tables" rule.
#pragma once

#include <vector>

#include "duckdb.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "moraine_abi.h"

namespace moraine_duckdb {

// One column of a synthesized `ducklake_*` table: `ducklake_type` is a
// DuckLake column-type string (fed through the existing `MapColumnType`,
// same as real user-table columns) so both paths share one type mapper.
struct MetadataColumnSpec {
	const char *name;
	const char *ducklake_type;
	bool not_null;
};

// Fetches every row of one `ducklake_*` table, already converted to typed
// `duckdb::Value`s in column-declaration order. Reads through the Task 3
// dump ABI for store-backed tables; `ducklake_metadata`'s provider ignores
// `handle` and returns fixed rows instead (see metadata_tables.cpp).
using MetadataRowProvider = std::vector<std::vector<duckdb::Value>> (*)(MoraineCatalogHandle *handle);

// `moraine_txn_stage`'s `table_kind` wire values (moraine_abi.h), mirrored
// here so `MetadataTableSpec::write_table_kind` and the staged-write Sink
// (staged_write.cpp) share one source of truth instead of each hand-coding
// the discriminant order.
constexpr int32_t kNotWritable = -1;

struct MetadataTableSpec {
	const char *name;
	std::vector<MetadataColumnSpec> columns;
	MetadataRowProvider provider;
	// `moraine_txn_stage`'s `table_kind` for this table, or `kNotWritable`
	// for the always-empty stand-ins and `ducklake_metadata` (writes to
	// those are out of scope this slice — DDL/unsupported-DML naming the
	// statement kind, per PlanInsert's NotImplementedException).
	int32_t write_table_kind = kNotWritable;
	// Indices into `columns` of the ended row's entity-key columns, in
	// exactly the order the staged ABI's update-set-end decoder consumes
	// them — NOT necessarily this table's declared column order (e.g.
	// `ducklake_column`'s key is read as `table_id` (col 3) then
	// `column_id` (col 0)). Non-empty only for the six versioned kinds;
	// `UPDATE ... SET end_snapshot` against any other table is not
	// translatable and throws at plan time.
	std::vector<duckdb::idx_t> end_key_columns;
	// Index into `columns` of `end_snapshot`; meaningful only when
	// `end_key_columns` is non-empty. Verifies an UPDATE's single SET
	// target is exactly the lifecycle column — the one interpreted
	// convention on the staged-row path.
	duckdb::idx_t end_snapshot_column = 0;
	// Indices into `columns` of a removed row's key columns, in exactly
	// the order the staged ABI's raw-delete decoder consumes them.
	// Non-empty only for the three unversioned statistics kinds; a raw
	// DELETE against any other table is not translatable and throws at
	// plan time.
	std::vector<duckdb::idx_t> delete_key_columns;
};

// The fixed list of synthesized tables, in the order they're registered.
// Built once; returns the same static instance every call.
const std::vector<MetadataTableSpec> &MoraineMetadataTableSpecs();

// A synthesized `ducklake_*` table entry: pure read, materializes every row
// up front (metadata-sized, not data-sized) at scan time via `spec`'s
// provider.
class MoraineMetadataTableEntry : public duckdb::TableCatalogEntry {
public:
	MoraineMetadataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
	                           duckdb::CreateTableInfo &info, const MetadataTableSpec &spec,
	                           MoraineCatalogHandle *handle);

	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                          duckdb::column_t column_id) override;
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                       duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

	// Exposed for the staged-write path (staged_write.cpp): the column
	// shape and `table_kind` needed to translate an incoming DataChunk row
	// into a `moraine_txn_stage` call, and the catalog handle
	// `moraine_txn_begin` opens against.
	const MetadataTableSpec &Spec() const {
		return spec_;
	}
	MoraineCatalogHandle *Handle() const {
		return handle_;
	}

private:
	const MetadataTableSpec &spec_;
	MoraineCatalogHandle *handle_;
};

// Builds a `MoraineMetadataTableEntry` for every table in
// `MoraineMetadataTableSpecs()` and adds it to `tables` (keyed by name, via
// `emplace` — a same-named entry already present wins, never overwritten).
void PopulateMetadataTables(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, MoraineCatalogHandle *handle,
                             duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> &tables);

} // namespace moraine_duckdb
