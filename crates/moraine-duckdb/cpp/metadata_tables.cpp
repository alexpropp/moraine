#include "metadata_tables.hpp"

#include "catalog.hpp"
#include "owned_array.hpp"

namespace moraine_duckdb {

namespace {

duckdb::Value OptVarchar(const char *s) {
	if (s == nullptr) {
		return duckdb::Value(duckdb::LogicalType::VARCHAR);
	}
	return duckdb::Value(std::string(s));
}

duckdb::Value Varchar(const char *s) {
	return OptVarchar(s);
}

duckdb::Value Bigint(uint64_t v) {
	return duckdb::Value::BIGINT(static_cast<int64_t>(v));
}

duckdb::Value OptBigint(bool has, uint64_t v) {
	if (!has) {
		return duckdb::Value(duckdb::LogicalType::BIGINT);
	}
	return Bigint(v);
}

duckdb::Value Boolean(bool v) {
	return duckdb::Value::BOOLEAN(v);
}

duckdb::Value OptBoolean(bool has, bool v) {
	if (!has) {
		return duckdb::Value(duckdb::LogicalType::BOOLEAN);
	}
	return Boolean(v);
}

duckdb::Value Uuid(const char *s) {
	if (s == nullptr) {
		return duckdb::Value(duckdb::LogicalType::UUID);
	}
	return duckdb::Value::UUID(std::string(s));
}

duckdb::Value TimestampTz(int64_t micros) {
	return duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(micros));
}

// `ducklake_column.column_type` is read by DuckLake's own
// `DuckLakeTypes::FromString` (pinned from the DuckLake source's
// `common/ducklake_types.cpp`), which accepts only DuckLake's own lowercase
// vocabulary ("int64", "float64", "timestamptz", ...) — a different naming
// scheme than the DuckDB SQL type names moraine's own catalog stores in
// this same field (`ColumnDef::column_type`, e.g. "BIGINT", "DOUBLE"; also
// what `MapColumnType` above maps from for the standalone attach's
// `DESCRIBE`). Discovered live: serving the stored string verbatim throws
// `Invalid Input Error: Failed to parse DuckLake type - unsupported type
// 'BIGINT'` the moment DuckLake resolves a table's columns. This
// re-derives the `duckdb::LogicalType` via the same `MapColumnType` the
// standalone attach already trusts, then names it DuckLake's way — one
// translation point, not two independently-maintained type tables.
// `DECIMAL`'s width/scale suffix isn't reproduced (DuckLake's own
// `ToStringBaseType` returns the bare "decimal" for every precision this
// slice never exercises live); every other supported type maps exactly.
duckdb::Value DuckLakeColumnType(const char *sql_type) {
	if (sql_type == nullptr) {
		return duckdb::Value(duckdb::LogicalType::VARCHAR);
	}
	auto type = MapColumnType(sql_type);
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		return duckdb::Value("boolean");
	case duckdb::LogicalTypeId::TINYINT:
		return duckdb::Value("int8");
	case duckdb::LogicalTypeId::SMALLINT:
		return duckdb::Value("int16");
	case duckdb::LogicalTypeId::INTEGER:
		return duckdb::Value("int32");
	case duckdb::LogicalTypeId::BIGINT:
		return duckdb::Value("int64");
	case duckdb::LogicalTypeId::HUGEINT:
		return duckdb::Value("int128");
	case duckdb::LogicalTypeId::UTINYINT:
		return duckdb::Value("uint8");
	case duckdb::LogicalTypeId::USMALLINT:
		return duckdb::Value("uint16");
	case duckdb::LogicalTypeId::UINTEGER:
		return duckdb::Value("uint32");
	case duckdb::LogicalTypeId::UBIGINT:
		return duckdb::Value("uint64");
	case duckdb::LogicalTypeId::FLOAT:
		return duckdb::Value("float32");
	case duckdb::LogicalTypeId::DOUBLE:
		return duckdb::Value("float64");
	case duckdb::LogicalTypeId::DECIMAL:
		return duckdb::Value("decimal");
	case duckdb::LogicalTypeId::TIME:
		return duckdb::Value("time");
	case duckdb::LogicalTypeId::DATE:
		return duckdb::Value("date");
	case duckdb::LogicalTypeId::TIMESTAMP:
		return duckdb::Value("timestamp");
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		return duckdb::Value("timestamptz");
	case duckdb::LogicalTypeId::VARCHAR:
		return duckdb::Value("varchar");
	case duckdb::LogicalTypeId::BLOB:
		return duckdb::Value("blob");
	case duckdb::LogicalTypeId::UUID:
		return duckdb::Value("uuid");
	default:
		// `MapColumnType` only ever returns one of the ids above (it
		// throws NotImplementedException for anything else), so this is
		// unreachable by construction, not a silent fallback.
		throw duckdb::InternalException("moraine: unmapped DuckLake type for \"%s\"", sql_type);
	}
}

// column order for both tables matches
// `CREATE TABLE ducklake_snapshot(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)`
// and `ducklake_snapshot_changes(snapshot_id, changes_made, author, commit_message, commit_extra_info)`,
// pinned from `DuckLakeMetadataManager::InitializeDuckLake`. One dump call
// (`moraine_dump_snapshots`) feeds both, since the store models them as one
// merged record.
std::vector<std::vector<duckdb::Value>> ProvideSnapshots(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineSnapshotRow> rows(moraine_dump_snapshots_free);
	MoraineError err{};
	auto code = moraine_dump_snapshots(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.snapshot_id),
		    TimestampTz(r.snapshot_time_micros),
		    Bigint(r.schema_version),
		    Bigint(r.next_catalog_id),
		    Bigint(r.next_file_id),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideSnapshotChanges(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineSnapshotRow> rows(moraine_dump_snapshots_free);
	MoraineError err{};
	auto code = moraine_dump_snapshots(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.snapshot_id),
		    Varchar(r.changes_made),
		    OptVarchar(r.author),
		    OptVarchar(r.commit_message),
		    OptVarchar(r.commit_extra_info),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideSchemas(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineSchemaRow> rows(moraine_dump_schemas_free);
	MoraineError err{};
	auto code = moraine_dump_schemas(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.schema_id),
		    Uuid(r.schema_uuid),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		    Varchar(r.schema_name),
		    Varchar(r.path),
		    Boolean(r.path_is_relative),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideTables(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineTableRow> rows(moraine_dump_tables_free);
	MoraineError err{};
	auto code = moraine_dump_tables(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.table_id),
		    Uuid(r.table_uuid),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		    Bigint(r.schema_id),
		    Varchar(r.table_name),
		    Varchar(r.path),
		    Boolean(r.path_is_relative),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideViews(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineViewRow> rows(moraine_dump_views_free);
	MoraineError err{};
	auto code = moraine_dump_views(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.view_id),
		    Uuid(r.view_uuid),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		    Bigint(r.schema_id),
		    Varchar(r.view_name),
		    Varchar(r.dialect),
		    Varchar(r.sql),
		    OptVarchar(r.column_aliases),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideColumns(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineColumnRow> rows(moraine_dump_columns_free);
	MoraineError err{};
	auto code = moraine_dump_columns(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.column_id),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		    Bigint(r.table_id),
		    Bigint(r.column_order),
		    Varchar(r.column_name),
		    DuckLakeColumnType(r.column_type),
		    OptVarchar(r.initial_default),
		    OptVarchar(r.default_value),
		    Boolean(r.nulls_allowed),
		    OptBigint(r.has_parent_column, r.parent_column),
		    OptVarchar(r.default_value_type),
		    OptVarchar(r.default_value_dialect),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideDataFiles(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineDataFileRow> rows(moraine_dump_data_files_free);
	MoraineError err{};
	auto code = moraine_dump_data_files(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.data_file_id),
		    Bigint(r.table_id),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		    OptBigint(r.has_file_order, r.file_order),
		    Varchar(r.path),
		    Boolean(r.path_is_relative),
		    Varchar(r.file_format),
		    Bigint(r.record_count),
		    Bigint(r.file_size_bytes),
		    Bigint(r.footer_size),
		    Bigint(r.row_id_start),
		    OptBigint(r.has_partition_id, r.partition_id),
		    OptVarchar(r.encryption_key),
		    OptBigint(r.has_mapping_id, r.mapping_id),
		    OptBigint(r.has_partial_max, r.partial_max),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideDeleteFiles(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineDeleteFileRow> rows(moraine_dump_delete_files_free);
	MoraineError err{};
	auto code = moraine_dump_delete_files(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.delete_file_id),
		    Bigint(r.table_id),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		    Bigint(r.data_file_id),
		    Varchar(r.path),
		    Boolean(r.path_is_relative),
		    Varchar(r.format),
		    Bigint(r.delete_count),
		    Bigint(r.file_size_bytes),
		    Bigint(r.footer_size),
		    OptVarchar(r.encryption_key),
		    OptBigint(r.has_partial_max, r.partial_max),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideTableStats(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineTableStatsRow> rows(moraine_dump_table_stats_free);
	MoraineError err{};
	auto code = moraine_dump_table_stats(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.table_id),
		    Bigint(r.record_count),
		    Bigint(r.next_row_id),
		    Bigint(r.file_size_bytes),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideTableColumnStats(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineTableColumnStatsRow> rows(moraine_dump_table_column_stats_free);
	MoraineError err{};
	auto code = moraine_dump_table_column_stats(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.table_id),
		    Bigint(r.column_id),
		    OptBoolean(r.has_contains_null, r.contains_null),
		    OptBoolean(r.has_contains_nan, r.contains_nan),
		    OptVarchar(r.min_value),
		    OptVarchar(r.max_value),
		    OptVarchar(r.extra_stats),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideFileColumnStats(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineFileColumnStatsRow> rows(moraine_dump_file_column_stats_free);
	MoraineError err{};
	auto code = moraine_dump_file_column_stats(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.data_file_id),
		    Bigint(r.table_id),
		    Bigint(r.column_id),
		    Bigint(r.column_size_bytes),
		    Bigint(r.value_count),
		    Bigint(r.null_count),
		    OptVarchar(r.min_value),
		    OptVarchar(r.max_value),
		    OptBoolean(r.has_contains_nan, r.contains_nan),
		    OptVarchar(r.extra_stats),
		});
	}
	return result;
}

// `ducklake_schema_versions` rows are flattened out of the snapshot
// records they fold into (the staged path stores only the per-snapshot
// table-id set — begin_snapshot/schema_version are the snapshot's own
// values, revalidated at commit).
std::vector<std::vector<duckdb::Value>> ProvideSchemaVersions(MoraineCatalogHandle *handle) {
	OwnedArray<MoraineSchemaVersionRow> rows(moraine_dump_schema_versions_free);
	MoraineError err{};
	auto code = moraine_dump_schema_versions(handle, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.begin_snapshot),
		    Bigint(r.schema_version),
		    Bigint(r.table_id),
		});
	}
	return result;
}

// Always-empty stand-in for a `ducklake_*` table covering a feature this
// slice doesn't model in the store (tags, data inlining, column mapping,
// macros, partitioning, sorting). None of these are optional at the SQL
// level: `DuckLakeMetadataManager::BuildCatalogForSnapshot` — the query
// DuckLake's own attach/snapshot-load always runs, discovered live against
// the real extension, not guessed — correlated-subqueries and joins every
// one of them unconditionally while resolving basic table/view/schema
// info, so a *missing* table is a bind-time Catalog Error even though the
// query would otherwise happily return zero rows for it. "Absent kinds are
// absent tables" (the plan's stated rule) turned out to mean "absent
// store-modeled row *data*", not "absent SQL table" — this is that
// adaptation, recorded here rather than left implicit.
std::vector<std::vector<duckdb::Value>> ProvideEmpty(MoraineCatalogHandle *) {
	return {};
}

// `ducklake_metadata` has no store-modeled source of truth (it is
// DuckLake's own bootstrap bookkeeping, not a store entity kind), so its
// rows are fixed here rather than read through the dump ABI. Pinned from
// DuckLakeInitializer::LoadExistingDuckLake (the exact keys read after the
// exists-probe `SELECT NULL FROM ducklake_metadata LIMIT 1` succeeds):
//
//   - "version": compared against "1.0"; anything else triggers migration
//     logic this slice never needs, or an error if migration is disallowed.
//     Always "1.0" here, matching the schema shape actually served (every
//     column DuckLake's v1.0 migrations add is already present).
//   - "encrypted": "true"/"false", read unconditionally; always "false"
//     (moraine has no encryption support this slice).
//   - "data_path" is deliberately NOT served: LoadExistingDuckLake only
//     acts on it when the row is present (loads/validates
//     `options.data_path` against it), and moraine has no store-level
//     source of truth for a lake-wide data path to serve faithfully.
//     Omitting the row leaves the ATTACH statement's own DATA_PATH option
//     as the sole authority, which is exactly the value the live proof's
//     ATTACH already supplies.
//   - "created_by" is never read back by DuckLake's own init path; included
//     anyway since DuckLake writes it at bootstrap and it costs nothing to
//     serve.
//   - "data_inlining_row_limit": served as "0" to declare, catalog-wide,
//     that this catalog does not inline row data (data inlining is
//     unsupported this slice). Load-bearing for the write path:
//     DuckLake's `WriteNewInlinedTables` gates its per-new-table
//     `INSERT INTO ducklake_inlined_data_tables` on
//     `DuckLakeCatalog::DataInliningRowLimit(...) != 0`, whose only input
//     is this catalog config option (every global-scope
//     `ducklake_metadata` row lands in `options.config_options` at
//     attach — `DuckLakeInitializer::LoadExistingDuckLake`) with a
//     default of **10**: inlining is ON by default, so without this row
//     every `CREATE TABLE` tries to register an inlined-data table
//     against a catalog that cannot store one (discovered live; pinned
//     from the pinned DuckLake source's `DataInliningRowLimit` and
//     `WriteNewInlinedTables`).
// All rows are global (scope/scope_id NULL) — moraine has no schema/table-
// scoped DuckLake settings to serve this slice.
std::vector<std::vector<duckdb::Value>> ProvideMetadata(MoraineCatalogHandle *) {
	auto null_varchar = duckdb::Value(duckdb::LogicalType::VARCHAR);
	auto null_bigint = duckdb::Value(duckdb::LogicalType::BIGINT);
	return {
	    {Varchar("version"), Varchar("1.0"), null_varchar, null_bigint},
	    {Varchar("created_by"), Varchar("moraine"), null_varchar, null_bigint},
	    {Varchar("encrypted"), Varchar("false"), null_varchar, null_bigint},
	    {Varchar("data_inlining_row_limit"), Varchar("0"), null_varchar, null_bigint},
	};
}

// Column shapes below are transcribed verbatim (name, type, declared
// nullability) from `DuckLakeMetadataManager::InitializeDuckLake`'s
// `CREATE TABLE {METADATA_CATALOG}.ducklake_*(...)` text, read from the
// pinned DuckLake source (commit `d318a545571d7d46eb751fa2aa5f6f4389285d3c`).
// `not_null` is set only for columns DuckLake itself declares `NOT NULL` or
// `PRIMARY KEY` (which implies `NOT NULL`); every other column, including
// ids DuckLake always populates in practice (e.g. `ducklake_table.table_id`,
// which has no `PRIMARY KEY` in DuckLake's own schema), is left nullable to
// match DuckLake's literal schema, not its runtime behavior.
const std::vector<MetadataTableSpec> &MetadataTableSpecsImpl() {
	static const std::vector<MetadataTableSpec> specs = {
	    {
	        "ducklake_snapshot",
	        {
	            {"snapshot_id", "BIGINT", true},
	            {"snapshot_time", "TIMESTAMPTZ", false},
	            {"schema_version", "BIGINT", false},
	            {"next_catalog_id", "BIGINT", false},
	            {"next_file_id", "BIGINT", false},
	        },
	        ProvideSnapshots,
	        0,
	    },
	    {
	        "ducklake_snapshot_changes",
	        {
	            {"snapshot_id", "BIGINT", true},
	            {"changes_made", "VARCHAR", false},
	            {"author", "VARCHAR", false},
	            {"commit_message", "VARCHAR", false},
	            {"commit_extra_info", "VARCHAR", false},
	        },
	        ProvideSnapshotChanges,
	        1,
	    },
	    {
	        "ducklake_schema",
	        {
	            {"schema_id", "BIGINT", true},
	            {"schema_uuid", "UUID", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"schema_name", "VARCHAR", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	        },
	        ProvideSchemas,
	        2,
	        /* end key: schema_id */ {0},
	        /* end_snapshot col */ 3,
	    },
	    {
	        "ducklake_table",
	        {
	            {"table_id", "BIGINT", false},
	            {"table_uuid", "UUID", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"schema_id", "BIGINT", false},
	            {"table_name", "VARCHAR", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	        },
	        ProvideTables,
	        3,
	        /* end key: table_id */ {0},
	        /* end_snapshot col */ 3,
	    },
	    {
	        "ducklake_view",
	        {
	            {"view_id", "BIGINT", false},
	            {"view_uuid", "UUID", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"schema_id", "BIGINT", false},
	            {"view_name", "VARCHAR", false},
	            {"dialect", "VARCHAR", false},
	            {"sql", "VARCHAR", false},
	            {"column_aliases", "VARCHAR", false},
	        },
	        ProvideViews,
	        4,
	        /* end key: view_id */ {0},
	        /* end_snapshot col */ 3,
	    },
	    {
	        "ducklake_column",
	        {
	            {"column_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"column_order", "BIGINT", false},
	            {"column_name", "VARCHAR", false},
	            {"column_type", "VARCHAR", false},
	            {"initial_default", "VARCHAR", false},
	            {"default_value", "VARCHAR", false},
	            {"nulls_allowed", "BOOLEAN", false},
	            {"parent_column", "BIGINT", false},
	            {"default_value_type", "VARCHAR", false},
	            {"default_value_dialect", "VARCHAR", false},
	        },
	        ProvideColumns,
	        5,
	        /* end key: table_id, column_id (decoder order) */ {3, 0},
	        /* end_snapshot col */ 2,
	    },
	    {
	        "ducklake_data_file",
	        {
	            {"data_file_id", "BIGINT", true},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"file_order", "BIGINT", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	            {"file_format", "VARCHAR", false},
	            {"record_count", "BIGINT", false},
	            {"file_size_bytes", "BIGINT", false},
	            {"footer_size", "BIGINT", false},
	            {"row_id_start", "BIGINT", false},
	            {"partition_id", "BIGINT", false},
	            {"encryption_key", "VARCHAR", false},
	            {"mapping_id", "BIGINT", false},
	            {"partial_max", "BIGINT", false},
	        },
	        ProvideDataFiles,
	        6,
	        /* end key: table_id, data_file_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	    },
	    {
	        "ducklake_delete_file",
	        {
	            {"delete_file_id", "BIGINT", true},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"data_file_id", "BIGINT", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	            {"format", "VARCHAR", false},
	            {"delete_count", "BIGINT", false},
	            {"file_size_bytes", "BIGINT", false},
	            {"footer_size", "BIGINT", false},
	            {"encryption_key", "VARCHAR", false},
	            {"partial_max", "BIGINT", false},
	        },
	        ProvideDeleteFiles,
	        7,
	        /* end key: table_id, delete_file_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	    },
	    {
	        "ducklake_table_stats",
	        {
	            {"table_id", "BIGINT", false},
	            {"record_count", "BIGINT", false},
	            {"next_row_id", "BIGINT", false},
	            {"file_size_bytes", "BIGINT", false},
	        },
	        ProvideTableStats,
	        8,
	        {},
	        0,
	        /* delete key: table_id */ {0},
	    },
	    {
	        "ducklake_table_column_stats",
	        {
	            {"table_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"contains_null", "BOOLEAN", false},
	            {"contains_nan", "BOOLEAN", false},
	            {"min_value", "VARCHAR", false},
	            {"max_value", "VARCHAR", false},
	            {"extra_stats", "VARCHAR", false},
	        },
	        ProvideTableColumnStats,
	        9,
	        {},
	        0,
	        /* delete key: table_id, column_id */ {0, 1},
	    },
	    {
	        "ducklake_file_column_stats",
	        {
	            {"data_file_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"column_size_bytes", "BIGINT", false},
	            {"value_count", "BIGINT", false},
	            {"null_count", "BIGINT", false},
	            {"min_value", "VARCHAR", false},
	            {"max_value", "VARCHAR", false},
	            {"contains_nan", "BOOLEAN", false},
	            {"extra_stats", "VARCHAR", false},
	        },
	        ProvideFileColumnStats,
	        10,
	        {},
	        0,
	        /* delete key: data_file_id, table_id, column_id (decoder order) */ {0, 1, 2},
	    },
	    {
	        // Shape pinned from DuckLake's migration path (`CREATE TABLE
	        // {METADATA_CATALOG}.ducklake_schema_versions(begin_snapshot
	        // BIGINT, schema_version BIGINT, table_id BIGINT)`), the
	        // same three-column form v1.0 stores carry.
	        "ducklake_schema_versions",
	        {
	            {"begin_snapshot", "BIGINT", false},
	            {"schema_version", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	        },
	        ProvideSchemaVersions,
	        11,
	    },
	    // Always-empty stand-ins (see `ProvideEmpty`'s doc comment): columns
	    // transcribed the same way as every table above, but no dump ABI
	    // call backs them — the store models none of these kinds this
	    // slice.
	    {
	        "ducklake_tag",
	        {
	            {"object_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"key", "VARCHAR", false},
	            {"value", "VARCHAR", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_column_tag",
	        {
	            {"table_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"key", "VARCHAR", false},
	            {"value", "VARCHAR", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_inlined_data_tables",
	        {
	            {"table_id", "BIGINT", false},
	            {"table_name", "VARCHAR", false},
	            {"schema_version", "BIGINT", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_macro",
	        {
	            {"schema_id", "BIGINT", false},
	            {"macro_id", "BIGINT", false},
	            {"macro_name", "VARCHAR", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_macro_impl",
	        {
	            {"macro_id", "BIGINT", false},
	            {"impl_id", "BIGINT", false},
	            {"dialect", "VARCHAR", false},
	            {"sql", "VARCHAR", false},
	            {"type", "VARCHAR", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_macro_parameters",
	        {
	            {"macro_id", "BIGINT", false},
	            {"impl_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"parameter_name", "VARCHAR", false},
	            {"parameter_type", "VARCHAR", false},
	            {"default_value", "VARCHAR", false},
	            {"default_value_type", "VARCHAR", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_partition_info",
	        {
	            {"partition_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_partition_column",
	        {
	            {"partition_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"partition_key_index", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"transform", "VARCHAR", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_sort_info",
	        {
	            {"sort_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_sort_expression",
	        {
	            {"sort_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"sort_key_index", "BIGINT", false},
	            {"expression", "VARCHAR", false},
	            {"dialect", "VARCHAR", false},
	            {"sort_direction", "VARCHAR", false},
	            {"null_order", "VARCHAR", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_metadata",
	        {
	            {"key", "VARCHAR", true},
	            {"value", "VARCHAR", true},
	            {"scope", "VARCHAR", false},
	            {"scope_id", "BIGINT", false},
	        },
	        ProvideMetadata,
	    },
	};
	return specs;
}

// Bind data for a metadata table scan: every row, already materialized
// (these tables are metadata-sized, not data-sized, so eager materialization
// at bind time — unlike the streaming Parquet scan in scan.cpp — is the
// simplest correct approach).
struct MetadataScanBindData : public duckdb::FunctionData {
	std::vector<std::vector<duckdb::Value>> rows;
	// The synthesized entry this scan reads, exposed through the table
	// function's `get_bind_info` so `LogicalGet::GetTable()` resolves it —
	// the binder's UPDATE/DELETE paths require a resolvable base table
	// ("Can only update base table" otherwise; discovered live driving
	// DuckLake's own metadata UPDATE, exactly how postgres_scanner's scan
	// exposes its entries).
	duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<MetadataScanBindData>();
		result->rows = rows;
		result->table_entry = table_entry;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<MetadataScanBindData>();
		return rows == other.rows && table_entry.get() == other.table_entry.get();
	}
};

duckdb::BindInfo MetadataScanBindInfo(const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
	auto &data = bind_data->Cast<MetadataScanBindData>();
	duckdb::BindInfo info(duckdb::ScanType::TABLE);
	info.table = data.table_entry;
	return info;
}

struct MetadataScanGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	// The columns DuckDB actually asked for, by index into a materialized
	// row, in output order. Empty for a zero-column "virtual column" probe
	// (e.g. `ducklake_metadata`'s own exists-probe, `SELECT NULL FROM
	// ducklake_metadata LIMIT 1`) — DuckDB only takes that plan shape when
	// the table function advertises `projection_pushdown = true`; without
	// it, DuckDB throws "Virtual columns require projection pushdown"
	// before this scan is ever reached (discovered live against the real
	// probe query — see the report).
	std::vector<duckdb::column_t> column_ids;

	idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
MetadataScanInitGlobal(duckdb::ClientContext &, duckdb::TableFunctionInitInput &input) {
	auto state = duckdb::make_uniq<MetadataScanGlobalState>();
	state->column_ids = input.column_ids;
	return std::move(state);
}

void MetadataScanFunctionImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<MetadataScanBindData>();
	auto &state = data.global_state->Cast<MetadataScanGlobalState>();
	if (state.offset >= bind_data.rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
		auto &row = bind_data.rows[state.offset + out_row];
		for (duckdb::idx_t out_col = 0; out_col < state.column_ids.size(); out_col++) {
			auto col_id = state.column_ids[out_col];
			if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
				// The rowid virtual column (the base TableCatalogEntry's
				// default row identity, which UPDATE/DELETE plans project to
				// address rows) is the row's index in this scan's
				// materialized row set: the provider's output order is
				// deterministic for a fixed committed head (the dump ABI
				// scans the store in key order), so the staged-write Sink
				// (staged_write.cpp) can re-materialize the same provider
				// and resolve this index back to the row's key cells.
				output.SetValue(out_col, out_row,
				                duckdb::Value::BIGINT(static_cast<int64_t>(state.offset + out_row)));
				continue;
			}
			if (duckdb::IsVirtualColumn(col_id) || col_id >= row.size()) {
				// Any other virtual column has no synthesized value; an
				// out-of-range id would be a DuckDB/shim mismatch. Both
				// serve an untyped NULL rather than read out of bounds —
				// `Vector::SetValue` handles a null `Value` regardless of
				// its own (absent) type, unlike a mismatched non-null one.
				output.SetValue(out_col, out_row, duckdb::Value());
				continue;
			}
			output.SetValue(out_col, out_row, row[col_id]);
		}
	}
	state.offset += count;
	output.SetCardinality(count);
}

duckdb::TableFunction MetadataScanTableFunction() {
	// No `bind` callback, same reasoning as `MoraineScanFunction` (scan.cpp):
	// `MoraineMetadataTableEntry::GetScanFunction` already produces complete
	// bind data itself.
	duckdb::TableFunction function("moraine_metadata_scan", {}, MetadataScanFunctionImpl, nullptr,
	                               MetadataScanInitGlobal, nullptr);
	// Required for DuckDB's zero-real-column "virtual column" scan shape,
	// which the exists-probe query actually uses (see
	// `MetadataScanGlobalState::column_ids`'s doc). Real projection
	// pushdown (serving only `state.column_ids`' columns instead of every
	// materialized column) falls out of the same mechanism for free.
	function.projection_pushdown = true;
	// Resolves `LogicalGet::GetTable()` (see `MetadataScanBindData::
	// table_entry`'s doc) so UPDATE/DELETE statements bind against these
	// tables.
	function.get_bind_info = MetadataScanBindInfo;
	return function;
}

} // namespace

const std::vector<MetadataTableSpec> &MoraineMetadataTableSpecs() {
	return MetadataTableSpecsImpl();
}

MoraineMetadataTableEntry::MoraineMetadataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                                                       duckdb::CreateTableInfo &info, const MetadataTableSpec &spec,
                                                       MoraineCatalogHandle *handle)
    : duckdb::TableCatalogEntry(catalog, schema, info), spec_(spec), handle_(handle) {
}

duckdb::unique_ptr<duckdb::BaseStatistics> MoraineMetadataTableEntry::GetStatistics(duckdb::ClientContext &,
                                                                                     duckdb::column_t) {
	throw duckdb::NotImplementedException("moraine: column statistics are not supported yet");
}

duckdb::TableFunction
MoraineMetadataTableEntry::GetScanFunction(duckdb::ClientContext &, duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	auto scan_bind_data = duckdb::make_uniq<MetadataScanBindData>();
	scan_bind_data->rows = spec_.provider(handle_);
	scan_bind_data->table_entry = this;
	bind_data = std::move(scan_bind_data);
	return MetadataScanTableFunction();
}

duckdb::TableStorageInfo MoraineMetadataTableEntry::GetStorageInfo(duckdb::ClientContext &) {
	return duckdb::TableStorageInfo();
}

void PopulateMetadataTables(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, MoraineCatalogHandle *handle,
                             duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> &tables) {
	for (auto &spec : MoraineMetadataTableSpecs()) {
		duckdb::CreateTableInfo info(schema, spec.name);
		duckdb::idx_t column_index = 0;
		for (auto &col : spec.columns) {
			info.columns.AddColumn(duckdb::ColumnDefinition(col.name, MapColumnType(col.ducklake_type)));
			if (col.not_null) {
				info.constraints.push_back(duckdb::make_uniq_base<duckdb::Constraint, duckdb::NotNullConstraint>(
				    duckdb::LogicalIndex(column_index)));
			}
			column_index++;
		}
		tables.emplace(spec.name,
		               duckdb::make_uniq<MoraineMetadataTableEntry>(catalog, schema, info, spec, handle));
	}
}

} // namespace moraine_duckdb
