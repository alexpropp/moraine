#include "metadata_tables.hpp"

#include "catalog.hpp"
#include "inline_tables.hpp"
#include "owned_array.hpp"
#include "transaction_manager.hpp"

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

// `ducklake_column.column_type` must carry DuckLake's own lowercase type
// vocabulary ("int64", "float64", "timestamptz", ...), not the DuckDB SQL
// type names moraine stores in this field. Re-derives the
// `duckdb::LogicalType` via `MapColumnType`, then names it DuckLake's way.
// `DECIMAL` reproduces its width/scale suffix ("decimal(18,4)"), which
// DuckLake needs to reconstruct the type; every other supported type maps
// exactly.
duckdb::Value DuckLakeColumnType(const char *sql_type) {
	if (sql_type == nullptr) {
		return duckdb::Value(duckdb::LogicalType::VARCHAR);
	}
	// A nested type is stored as its DuckLake marker ("list"/"struct"/"map")
	// with the element/field types carried by child `ducklake_column` rows
	// (linked by `parent_column`). Pass the marker through unchanged so
	// DuckLake reconstructs the type from the hierarchy; there is no scalar
	// `LogicalType` to normalize it against.
	if (duckdb::StringUtil::CIEquals(sql_type, "list") || duckdb::StringUtil::CIEquals(sql_type, "struct") ||
	    duckdb::StringUtil::CIEquals(sql_type, "map")) {
		return duckdb::Value(duckdb::StringUtil::Lower(sql_type));
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
		return duckdb::Value(duckdb::StringUtil::Format("decimal(%d,%d)", duckdb::DecimalType::GetWidth(type),
		                                                duckdb::DecimalType::GetScale(type)));
	case duckdb::LogicalTypeId::INTERVAL:
		return duckdb::Value("interval");
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

// One dump call (`moraine_dump_snapshots`) feeds both ProvideSnapshots and
// ProvideSnapshotChanges, since the store models them as one merged record;
// each emits its columns in the declared order of its `ducklake_*` table.
std::vector<std::vector<duckdb::Value>> ProvideSnapshots(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                         void *probe_ctx) {
	OwnedArray<MoraineSnapshotRow> rows(moraine_dump_snapshots_free);
	MoraineError err {};
	auto code = moraine_dump_snapshots(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

std::vector<std::vector<duckdb::Value>> ProvideSnapshotChanges(MoraineCatalogHandle *handle,
                                                               MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineSnapshotRow> rows(moraine_dump_snapshots_free);
	MoraineError err {};
	auto code = moraine_dump_snapshots(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

std::vector<std::vector<duckdb::Value>> ProvideSchemas(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                       void *probe_ctx) {
	OwnedArray<MoraineSchemaRow> rows(moraine_dump_schemas_free);
	MoraineError err {};
	auto code = moraine_dump_schemas(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

std::vector<std::vector<duckdb::Value>> ProvideTables(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                      void *probe_ctx) {
	OwnedArray<MoraineTableRow> rows(moraine_dump_tables_free);
	MoraineError err {};
	auto code = moraine_dump_tables(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

std::vector<std::vector<duckdb::Value>> ProvideViews(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                     void *probe_ctx) {
	OwnedArray<MoraineViewRow> rows(moraine_dump_views_free);
	MoraineError err {};
	auto code = moraine_dump_views(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

std::vector<std::vector<duckdb::Value>> ProvideMacros(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                      void *probe_ctx) {
	OwnedArray<MoraineMacroRow> rows(moraine_dump_macros_free);
	MoraineError err {};
	auto code = moraine_dump_macros(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.schema_id),
		    Bigint(r.macro_id),
		    Varchar(r.macro_name),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		});
	}
	return result;
}

// Impl and parameter rows come back flattened from the embedded children
// in `(macro_id, impl_id[, column_id])` order, and that order is served
// as-is: DuckLake reconstructs macros with LIST() aggregations that carry
// no ORDER BY, so served row order is the reconstruction order.
std::vector<std::vector<duckdb::Value>> ProvideMacroImpls(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                          void *probe_ctx) {
	OwnedArray<MoraineMacroImplRow> rows(moraine_dump_macro_impls_free);
	MoraineError err {};
	auto code = moraine_dump_macro_impls(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.macro_id),
		    Bigint(r.impl_id),
		    Varchar(r.dialect),
		    Varchar(r.sql),
		    Varchar(r.macro_type),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideMacroParameters(MoraineCatalogHandle *handle,
                                                               MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineMacroParameterRow> rows(moraine_dump_macro_parameters_free);
	MoraineError err {};
	auto code = moraine_dump_macro_parameters(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.macro_id),
		    Bigint(r.impl_id),
		    Bigint(r.column_id),
		    Varchar(r.parameter_name),
		    Varchar(r.parameter_type),
		    OptVarchar(r.default_value),
		    Varchar(r.default_value_type),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideColumnMappings(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                              void *probe_ctx) {
	OwnedArray<MoraineColumnMappingRow> rows(moraine_dump_column_mappings_free);
	MoraineError err {};
	auto code = moraine_dump_column_mappings(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.mapping_id),
		    Bigint(r.table_id),
		    Varchar(r.map_type),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideNameMappings(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                            void *probe_ctx) {
	OwnedArray<MoraineNameMappingRow> rows(moraine_dump_name_mappings_free);
	MoraineError err {};
	auto code = moraine_dump_name_mappings(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.mapping_id),
		    Bigint(r.column_id),
		    Varchar(r.source_name),
		    Bigint(r.target_field_id),
		    OptBigint(r.has_parent_column, r.parent_column),
		    Boolean(r.is_partition),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideColumns(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                       void *probe_ctx) {
	OwnedArray<MoraineColumnRow> rows(moraine_dump_columns_free);
	MoraineError err {};
	auto code = moraine_dump_columns(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

std::vector<std::vector<duckdb::Value>> ProvideDataFiles(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                         void *probe_ctx) {
	OwnedArray<MoraineDataFileRow> rows(moraine_dump_data_files_free);
	MoraineError err {};
	auto code = moraine_dump_data_files(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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
		    OptBigint(r.has_row_id_start, r.row_id_start),
		    OptBigint(r.has_partition_id, r.partition_id),
		    OptVarchar(r.encryption_key),
		    OptBigint(r.has_mapping_id, r.mapping_id),
		    OptBigint(r.has_partial_max, r.partial_max),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideDeleteFiles(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                           void *probe_ctx) {
	OwnedArray<MoraineDeleteFileRow> rows(moraine_dump_delete_files_free);
	MoraineError err {};
	auto code = moraine_dump_delete_files(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

std::vector<std::vector<duckdb::Value>> ProvideTableStats(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                          void *probe_ctx) {
	OwnedArray<MoraineTableStatsRow> rows(moraine_dump_table_stats_free);
	MoraineError err {};
	auto code = moraine_dump_table_stats(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

std::vector<std::vector<duckdb::Value>> ProvideTableColumnStats(MoraineCatalogHandle *handle,
                                                                MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineTableColumnStatsRow> rows(moraine_dump_table_column_stats_free);
	MoraineError err {};
	auto code = moraine_dump_table_column_stats(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

std::vector<std::vector<duckdb::Value>> ProvideFileColumnStats(MoraineCatalogHandle *handle,
                                                               MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineFileColumnStatsRow> rows(moraine_dump_file_column_stats_free);
	MoraineError err {};
	auto code = moraine_dump_file_column_stats(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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
std::vector<std::vector<duckdb::Value>> ProvideSchemaVersions(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                              void *probe_ctx) {
	OwnedArray<MoraineSchemaVersionRow> rows(moraine_dump_schema_versions_free);
	MoraineError err {};
	auto code = moraine_dump_schema_versions(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
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

// Always-empty stand-in for a `ducklake_*` table covering a feature the
// store doesn't model (variant statistics). The table must still exist
// as a SQL table: DuckLake's attach/snapshot-load query joins every one
// of them unconditionally, so a missing table is a bind-time Catalog
// Error even where the query would return zero rows for it.
std::vector<std::vector<duckdb::Value>> ProvideEmpty(MoraineCatalogHandle *, MoraineInterruptProbe, void *) {
	return {};
}

std::vector<std::vector<duckdb::Value>> ProvidePartitionInfo(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                             void *probe_ctx) {
	OwnedArray<MorainePartitionInfoRow> rows(moraine_dump_partition_info_free);
	MoraineError err {};
	auto code = moraine_dump_partition_info(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.partition_id),
		    Bigint(r.table_id),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvidePartitionColumns(MoraineCatalogHandle *handle,
                                                                MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MorainePartitionColumnRow> rows(moraine_dump_partition_columns_free);
	MoraineError err {};
	auto code = moraine_dump_partition_columns(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.partition_id),
		    Bigint(r.table_id),
		    Bigint(r.partition_key_index),
		    Bigint(r.column_id),
		    Varchar(r.transform),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideFilePartitionValues(MoraineCatalogHandle *handle,
                                                                   MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineFilePartitionValueRow> rows(moraine_dump_file_partition_values_free);
	MoraineError err {};
	auto code = moraine_dump_file_partition_values(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.data_file_id),
		    Bigint(r.table_id),
		    Bigint(r.partition_key_index),
		    Varchar(r.partition_value),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideSortInfo(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                        void *probe_ctx) {
	OwnedArray<MoraineSortInfoRow> rows(moraine_dump_sort_info_free);
	MoraineError err {};
	auto code = moraine_dump_sort_info(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.sort_id),
		    Bigint(r.table_id),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideSortExpressions(MoraineCatalogHandle *handle,
                                                               MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineSortExpressionRow> rows(moraine_dump_sort_expressions_free);
	MoraineError err {};
	auto code = moraine_dump_sort_expressions(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.sort_id),
		    Bigint(r.table_id),
		    Bigint(r.sort_key_index),
		    Varchar(r.expression),
		    Varchar(r.dialect),
		    Varchar(r.sort_direction),
		    Varchar(r.null_order),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideTags(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                    void *probe_ctx) {
	OwnedArray<MoraineTagRow> rows(moraine_dump_tags_free);
	MoraineError err {};
	auto code = moraine_dump_tags(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.object_id),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		    Varchar(r.key),
		    Varchar(r.value),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideColumnTags(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                          void *probe_ctx) {
	OwnedArray<MoraineColumnTagRow> rows(moraine_dump_column_tags_free);
	MoraineError err {};
	auto code = moraine_dump_column_tags(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.table_id),
		    Bigint(r.column_id),
		    Bigint(r.begin_snapshot),
		    OptBigint(r.has_end_snapshot, r.end_snapshot),
		    Varchar(r.key),
		    Varchar(r.value),
		});
	}
	return result;
}

std::vector<std::vector<duckdb::Value>> ProvideScheduledDeletions(MoraineCatalogHandle *handle,
                                                                  MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineScheduledDeletionRow> rows(moraine_dump_scheduled_deletions_free);
	MoraineError err {};
	auto code = moraine_dump_scheduled_deletions(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.data_file_id),
		    Varchar(r.path),
		    duckdb::Value::BOOLEAN(r.path_is_relative),
		    TimestampTz(r.schedule_start_micros),
		});
	}
	return result;
}

// `ducklake_metadata` rows. All are fixed here except `encrypted`, which
// is the store's creation-time flag. Constraints on the values DuckLake
// reads back:
//   - "version": must be "1.0"; any other value triggers migration logic.
//   - "encrypted": the stored flag (moraine_catalog_encrypted). DuckLake
//     compares it against the attach's requested encryption and, when
//     "true", encrypts new data files and records their keys in
//     `ducklake_data_file`/`ducklake_delete_file` rows.
//   - "data_path" is deliberately omitted: DuckLake acts on it only when the
//     row is present, and there is no store-level lake-wide data path to
//     serve; omitting it leaves the ATTACH DATA_PATH option as sole authority.
//   - "created_by": never read back; served because it costs nothing.
//   - "data_inlining_row_limit": "10" (DuckLake's compiled default). Load-
//     bearing: a non-zero value is what makes DuckLake's write path emit
//     `INSERT INTO ducklake_inlined_data_tables`; "0" would suppress inlining
//     entirely. inline_tables.cpp serves the dynamic inline catalog surface
//     this drives.
// All rows are global (scope/scope_id NULL).
std::vector<std::vector<duckdb::Value>> ProvideMetadata(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                        void *probe_ctx) {
	bool encrypted = false;
	MoraineError err {};
	auto code = moraine_catalog_encrypted(handle, &encrypted, probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	auto null_varchar = duckdb::Value(duckdb::LogicalType::VARCHAR);
	auto null_bigint = duckdb::Value(duckdb::LogicalType::BIGINT);
	return {
	    {Varchar("version"), Varchar("1.0"), null_varchar, null_bigint},
	    {Varchar("created_by"), Varchar("moraine"), null_varchar, null_bigint},
	    {Varchar("encrypted"), Varchar(encrypted ? "true" : "false"), null_varchar, null_bigint},
	    {Varchar("data_inlining_row_limit"), Varchar("10"), null_varchar, null_bigint},
	};
}

// Feeds `ducklake_inlined_data_tables`: one row per `(table_id,
// schema_version)` with a recorded `inline/schema`. The table_name column
// carries `InlinedDataTableName` (inline_tables.cpp), matching DuckLake's
// own inline-table naming.
std::vector<std::vector<duckdb::Value>> ProvideInlinedDataTables(MoraineCatalogHandle *handle,
                                                                 MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineInlineTableRow> rows(moraine_inline_registered_tables_free);
	MoraineError err {};
	auto code = moraine_inline_registered_tables(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back({
		    Bigint(r.table_id),
		    Varchar(InlinedDataTableName(r.table_id, r.schema_version).c_str()),
		    Bigint(r.schema_version),
		});
	}
	return result;
}

// Column shapes below match each `ducklake_*` table's own
// `CREATE TABLE` shape (name, type, declared nullability). `not_null` is set
// only for columns DuckLake declares `NOT NULL` or `PRIMARY KEY`; every
// other column is left nullable to match DuckLake's literal schema, even ids
// it always populates in practice.
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
	        {},
	        0,
	        /* delete key: snapshot_id */ {0},
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
	        {},
	        0,
	        /* delete key: snapshot_id */ {0},
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
	        /* delete key: schema_id, end_snapshot */ {0, 3},
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
	        /* delete key: table_id, end_snapshot */ {0, 3},
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
	        /* delete key: view_id, end_snapshot */ {0, 3},
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
	        /* delete key: table_id, column_id, end_snapshot */ {3, 0, 2},
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
	        /* delete key: table_id, data_file_id, end_snapshot */ {1, 0, 3},
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
	        /* delete key: table_id, delete_file_id, end_snapshot */ {1, 0, 3},
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
	        /* overlay updates */ true,
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
	        /* overlay updates */ true,
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
	        /* overlay updates */ true,
	    },
	    {
	        // Three-column form: (begin_snapshot, schema_version, table_id).
	        "ducklake_schema_versions",
	        {
	            {"begin_snapshot", "BIGINT", false},
	            {"schema_version", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	        },
	        ProvideSchemaVersions,
	        11,
	        {},
	        0,
	        /* delete key: begin_snapshot, schema_version, table_id */ {0, 1, 2},
	    },
	    {
	        "ducklake_tag",
	        {
	            {"object_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"key", "VARCHAR", false},
	            {"value", "VARCHAR", false},
	        },
	        ProvideTags,
	        17,
	        /* end key: object_id, key (decoder order) */ {0, 3},
	        /* end_snapshot col */ 2,
	        /* delete key: object_id, key, begin_snapshot */ {0, 3, 1},
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
	        ProvideColumnTags,
	        18,
	        /* end key: table_id, column_id, key (decoder order) */ {0, 1, 4},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, column_id, key, begin_snapshot */ {0, 1, 4, 2},
	    },
	    // Always-empty stand-ins (see `ProvideEmpty`): no dump ABI call backs
	    // them — the store models none of these kinds.
	    {
	        "ducklake_inlined_data_tables",
	        {
	            {"table_id", "BIGINT", false},
	            {"table_name", "VARCHAR", false},
	            {"schema_version", "BIGINT", false},
	        },
	        ProvideInlinedDataTables,
	        kVoidInsertable,
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
	        ProvideMacros,
	        20,
	        /* end key: macro_id */ {1},
	        /* end_snapshot col */ 4,
	        /* delete key: macro_id, end_snapshot */ {1, 4},
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
	        ProvideMacroImpls,
	        21,
	        {},
	        0,
	        /* delete key: macro_id */ {0},
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
	        ProvideMacroParameters,
	        22,
	        {},
	        0,
	        /* delete key: macro_id */ {0},
	    },
	    {
	        "ducklake_partition_info",
	        {
	            {"partition_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	        },
	        ProvidePartitionInfo,
	        12,
	        /* end key: table_id, partition_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, partition_id, end_snapshot */ {1, 0, 3},
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
	        ProvidePartitionColumns,
	        13,
	        {},
	        0,
	        /* delete key: partition_id, table_id */ {0, 1},
	    },
	    {
	        "ducklake_file_partition_value",
	        {
	            {"data_file_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"partition_key_index", "BIGINT", false},
	            {"partition_value", "VARCHAR", false},
	        },
	        ProvideFilePartitionValues,
	        14,
	        {},
	        0,
	        /* delete key: data_file_id, table_id */ {0, 1},
	    },
	    {
	        "ducklake_file_variant_stats",
	        {
	            {"data_file_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"variant_path", "VARCHAR", false},
	            {"shredded_type", "VARCHAR", false},
	            {"column_size_bytes", "BIGINT", false},
	            {"value_count", "BIGINT", false},
	            {"null_count", "BIGINT", false},
	            {"min_value", "VARCHAR", false},
	            {"max_value", "VARCHAR", false},
	            {"contains_nan", "BOOLEAN", false},
	            {"extra_stats", "VARCHAR", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_files_scheduled_for_deletion",
	        {
	            {"data_file_id", "BIGINT", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	            {"schedule_start", "TIMESTAMPTZ", false},
	        },
	        ProvideScheduledDeletions,
	        19,
	        {},
	        0,
	        /* delete key: data_file_id */ {0},
	    },
	    {
	        "ducklake_column_mapping",
	        {
	            {"mapping_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"type", "VARCHAR", false},
	        },
	        ProvideColumnMappings,
	        23,
	        {},
	        0,
	        /* delete key: mapping_id, table_id */ {0, 1},
	    },
	    {
	        "ducklake_name_mapping",
	        {
	            {"mapping_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"source_name", "VARCHAR", false},
	            {"target_field_id", "BIGINT", false},
	            {"parent_column", "BIGINT", false},
	            {"is_partition", "BOOLEAN", false},
	        },
	        ProvideNameMappings,
	        24,
	        {},
	        0,
	        /* delete key: mapping_id */ {0},
	    },
	    {
	        "ducklake_sort_info",
	        {
	            {"sort_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	        },
	        ProvideSortInfo,
	        15,
	        /* end key: table_id, sort_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, sort_id, end_snapshot */ {1, 0, 3},
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
	        ProvideSortExpressions,
	        16,
	        {},
	        0,
	        /* delete key: sort_id, table_id */ {0, 1},
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

duckdb::BindInfo MetadataScanBindInfo(const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
	auto &data = bind_data->Cast<MetadataScanBindData>();
	duckdb::BindInfo info(duckdb::ScanType::TABLE);
	info.table = data.table_entry;
	return info;
}

struct MetadataScanGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	// The columns DuckDB asked for, by index into a materialized row, in
	// output order. Empty for a zero-column "virtual column" probe (e.g.
	// `SELECT NULL FROM ducklake_metadata LIMIT 1`), which DuckDB emits only
	// when the table function advertises `projection_pushdown = true`.
	std::vector<duckdb::column_t> column_ids;

	idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MetadataScanInitGlobal(duckdb::ClientContext &,
                                                                            duckdb::TableFunctionInitInput &input) {
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
				// The rowid is the row's index in this scan's materialized
				// row set. The provider's output order is deterministic for a
				// fixed committed head, so the staged-write Sink
				// (staged_write.cpp) resolves this index back to the row by
				// re-materializing the same provider.
				output.SetValue(out_col, out_row, duckdb::Value::BIGINT(static_cast<int64_t>(state.offset + out_row)));
				continue;
			}
			if (duckdb::IsVirtualColumn(col_id) || col_id >= row.size()) {
				// Any other virtual column has no synthesized value, and an
				// out-of-range id would be a DuckDB/shim mismatch. Serve an
				// untyped NULL rather than read out of bounds:
				// `Vector::SetValue` accepts a null `Value` of any type.
				output.SetValue(out_col, out_row, duckdb::Value());
				continue;
			}
			output.SetValue(out_col, out_row, row[col_id]);
		}
	}
	state.offset += count;
	output.SetCardinality(count);
}

} // namespace

duckdb::unique_ptr<duckdb::FunctionData> MetadataScanBindData::Copy() const {
	auto result = duckdb::make_uniq<MetadataScanBindData>();
	result->rows = rows;
	result->table_entry = table_entry;
	return std::move(result);
}

bool MetadataScanBindData::Equals(const duckdb::FunctionData &other_p) const {
	auto &other = other_p.Cast<MetadataScanBindData>();
	return rows == other.rows && table_entry.get() == other.table_entry.get();
}

duckdb::TableFunction MetadataScanTableFunction() {
	// No `bind` callback (as in `MoraineScanFunction`, scan.cpp): the caller
	// already produces complete bind data itself.
	duckdb::TableFunction function("moraine_metadata_scan", {}, MetadataScanFunctionImpl, nullptr,
	                               MetadataScanInitGlobal, nullptr);
	// Required for the zero-real-column "virtual column" scan shape the
	// exists-probe query uses (see `MetadataScanGlobalState::column_ids`);
	// real projection pushdown falls out of the same mechanism.
	function.projection_pushdown = true;
	// Resolves `LogicalGet::GetTable()` so UPDATE/DELETE statements bind
	// against these tables.
	function.get_bind_info = MetadataScanBindInfo;
	return function;
}

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

// The two snapshot-backed tables share one dump; a scan inside a write
// transaction that already staged rows must observe that transaction's
// own snapshot deletes (the expiry cascade's `NOT EXISTS` subqueries
// re-read `ducklake_snapshot` after staging them), so their rows come
// from the tx-aware dump when a staged tx is open. Every other kind — and
// every scan outside a write transaction — serves committed state.
static std::vector<std::vector<duckdb::Value>> TxAwareSnapshotRows(MoraineTxHandle *tx, bool changes_shape) {
	OwnedArray<MoraineSnapshotRow> rows(moraine_dump_snapshots_free);
	MoraineError err {};
	auto code = moraine_tx_dump_snapshots(tx, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		if (changes_shape) {
			result.push_back({
			    Bigint(r.snapshot_id),
			    Varchar(r.changes_made),
			    OptVarchar(r.author),
			    OptVarchar(r.commit_message),
			    OptVarchar(r.commit_extra_info),
			});
		} else {
			result.push_back({
			    Bigint(r.snapshot_id),
			    TimestampTz(r.snapshot_time_micros),
			    Bigint(r.schema_version),
			    Bigint(r.next_catalog_id),
			    Bigint(r.next_file_id),
			});
		}
	}
	return result;
}

duckdb::TableFunction MoraineMetadataTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                                                 duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	auto scan_bind_data = duckdb::make_uniq<MetadataScanBindData>();

	// write_table_kind 0/1 are ducklake_snapshot / ducklake_snapshot_changes.
	MoraineTxHandle *staged_tx = nullptr;
	if (spec_.write_table_kind == 0 || spec_.write_table_kind == 1) {
		auto catalog_transaction = ParentCatalog().GetCatalogTransaction(context);
		staged_tx = catalog_transaction.transaction->Cast<MoraineTransaction>().StagedTxIfOpen();
	}
	if (staged_tx != nullptr) {
		scan_bind_data->rows = TxAwareSnapshotRows(staged_tx, spec_.write_table_kind == 1);
	} else {
		scan_bind_data->rows = spec_.provider(handle_, moraine_shim_is_interrupted, &context);
	}

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
		tables.emplace(spec.name, duckdb::make_uniq<MoraineMetadataTableEntry>(catalog, schema, info, spec, handle));
	}
}

} // namespace moraine_duckdb
