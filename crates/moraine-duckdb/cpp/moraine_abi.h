// C declarations for the Rust ABI exported by `moraine-duckdb`'s `src/abi.rs`
// and `src/error.rs`. Hand-written, not generated: kept in lockstep with the
// Rust side by `src/abi.rs`'s `header_declares_every_abi_symbol` test, which
// asserts every symbol name declared here appears verbatim in this file's
// text (no `cbindgen` step in this build).
//
// Every struct here must match its `#[repr(C)]` Rust counterpart field for
// field, in declaration order: a raw memory layout contract, not a
// name-matching one.
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handles: a C++ caller only ever holds a pointer, obtained from
// and released through the functions below.
typedef struct MoraineCatalogHandle MoraineCatalogHandle;
typedef struct MoraineSnapshotHandle MoraineSnapshotHandle;

// Mirrors `moraine_duckdb::error::codes`.
enum {
	MORAINE_OK = 0,
	MORAINE_NOT_FOUND = 1,
	MORAINE_ALREADY_EXISTS = 2,
	MORAINE_CONSTRAINT = 3,
	MORAINE_COMMIT_CONFLICT = 4,
	MORAINE_CORRUPTION = 5,
	MORAINE_STORE = 6,
	MORAINE_INVALID_ARGUMENT = 7,
	MORAINE_INTERNAL = 8,
	MORAINE_INTERRUPTED = 9,
};

// Mirrors `moraine_duckdb::error::MoraineError`.
typedef struct MoraineError {
	int32_t code;
	char *message;
} MoraineError;

// Mirrors `moraine_duckdb::abi::MoraineSchemaDesc`.
typedef struct MoraineSchemaDesc {
	uint64_t id;
	char *name;
} MoraineSchemaDesc;

// Mirrors `moraine_duckdb::abi::MoraineTableDesc`.
typedef struct MoraineTableDesc {
	uint64_t id;
	uint64_t schema_id;
	char *name;
} MoraineTableDesc;

// Mirrors `moraine_duckdb::abi::MoraineColumnDesc`.
typedef struct MoraineColumnDesc {
	uint64_t id;
	char *name;
	char *sql_type;
	bool nulls_allowed;
	bool has_parent_column;
	uint64_t parent_column;
} MoraineColumnDesc;

// Mirrors `moraine_duckdb::abi::MoraineViewDesc`.
typedef struct MoraineViewDesc {
	uint64_t id;
	uint64_t schema_id;
	char *name;
	char *dialect;
	char *sql;
} MoraineViewDesc;

// Mirrors `moraine_duckdb::abi::MoraineDataFileDesc`.
typedef struct MoraineDataFileDesc {
	uint64_t id;
	char *path;
	bool path_is_relative;
	uint64_t record_count;
	bool has_row_id_start;
	uint64_t row_id_start;
	uint64_t file_size_bytes;
	uint64_t footer_size;
} MoraineDataFileDesc;

// A cancellation probe a cancellable call polls (immediately, then ~100ms)
// while blocked on store I/O; returning true cancels the call with
// MORAINE_INTERRUPTED and the out-params are left unwritten. Must be safe
// to call with its probe_ctx from any thread for the duration of the call.
// NULL disables polling for that call. moraine_interrupt remains the
// push-channel alternative.
typedef bool (*MoraineInterruptProbe)(void *probe_ctx);

// `encrypted` requests DuckLake data-file encryption; creation-time only —
// recorded when a fresh store bootstraps, ignored on an existing store,
// whose stored flag (moraine_catalog_encrypted) is authoritative.
// `flush_interval_ms` sets the store's WAL flush cadence (bounds
// per-commit latency); 0 means "not given" and keeps the default.
//
// `cache_dir` is a local directory backing SlateDB's on-disk block cache;
// NULL or empty disables it. Worthwhile for remote (`s3://`) stores, whose
// warm reads then avoid repeat object-store GETs and survive restarts.
//
// `s3` carries S3 credentials for an `s3://` path, sourced from a DuckDB
// secret. Any field may be NULL/unset, in which case the AWS_* environment
// supplies it. `use_ssl`: -1 unset, 0 false, 1 true. Pass NULL to use the
// environment alone; ignored for non-`s3://` paths.
typedef struct {
	const char *key_id;
	const char *secret;
	const char *region;
	const char *session_token;
	const char *endpoint;
	const char *url_style;
	int32_t use_ssl;
} MoraineS3Config;

int32_t moraine_attach(const char *path, const MoraineS3Config *s3, bool read_only, bool encrypted,
                       uint64_t flush_interval_ms, const char *cache_dir, const char *data_path,
                       MoraineCatalogHandle **out, MoraineError *err);
void moraine_detach(MoraineCatalogHandle *handle);

// The lake's recorded data root (stored global `data_path` option), written
// to `*out` as an owned C string or null when none was recorded. Free a
// non-null result with `moraine_string_free`. Served back as the
// `ducklake_metadata` `data_path` row so a re-attach need not repeat it.
int32_t moraine_data_path(MoraineCatalogHandle *handle, MoraineInterruptProbe probe, void *probe_ctx, char **out,
                          MoraineError *err);
void moraine_string_free(char *str);

// The stored `encrypted` flag, fixed when the store was created; stores
// predating the flag read as false.
int32_t moraine_catalog_encrypted(MoraineCatalogHandle *handle, bool *out_encrypted, MoraineInterruptProbe probe,
                                  void *probe_ctx, MoraineError *err);

int32_t moraine_snapshot(MoraineCatalogHandle *handle, MoraineSnapshotHandle **out, MoraineInterruptProbe probe,
                         void *probe_ctx, MoraineError *err);
void moraine_interrupt(MoraineCatalogHandle *handle);
void moraine_snapshot_free(MoraineSnapshotHandle *snapshot);

void moraine_error_free(char *message);

int32_t moraine_snapshot_schemas(MoraineSnapshotHandle *snapshot, MoraineSchemaDesc **out_items, size_t *out_len,
                                  MoraineError *err);
void moraine_snapshot_schemas_free(MoraineSchemaDesc *items, size_t len);

int32_t moraine_snapshot_tables_in(MoraineSnapshotHandle *snapshot, uint64_t schema_id, MoraineTableDesc **out_items,
                                    size_t *out_len, MoraineError *err);
void moraine_snapshot_tables_in_free(MoraineTableDesc *items, size_t len);

int32_t moraine_snapshot_columns_of(MoraineSnapshotHandle *snapshot, uint64_t table_id,
                                     MoraineColumnDesc **out_items, size_t *out_len, MoraineError *err);
void moraine_snapshot_columns_of_free(MoraineColumnDesc *items, size_t len);

int32_t moraine_snapshot_views_in(MoraineSnapshotHandle *snapshot, uint64_t schema_id, MoraineViewDesc **out_items,
                                   size_t *out_len, MoraineError *err);
void moraine_snapshot_views_in_free(MoraineViewDesc *items, size_t len);

int32_t moraine_snapshot_data_files_of(MoraineSnapshotHandle *snapshot, uint64_t table_id,
                                        MoraineDataFileDesc **out_items, size_t *out_len, MoraineError *err);
void moraine_snapshot_data_files_of_free(MoraineDataFileDesc *items, size_t len);

// Row-faithful ducklake_* dumps (src/dumps.rs): every cur AND hist row of one
// kind, verbatim, for the DuckLake metadata-table projections. Optional
// scalar fields carry a `has_<field>` companion flag (no sentinel value is
// safe for an id/count/flag); optional strings are null for absent, exactly
// like every other string field here.

// Mirrors `moraine_duckdb::dumps::MoraineSnapshotRow`.
typedef struct MoraineSnapshotRow {
	uint64_t snapshot_id;
	int64_t snapshot_time_micros;
	uint64_t schema_version;
	uint64_t next_catalog_id;
	uint64_t next_file_id;
	char *changes_made;
	char *author;
	char *commit_message;
	char *commit_extra_info;
} MoraineSnapshotRow;

// Mirrors `moraine_duckdb::dumps::MoraineSchemaRow`.
typedef struct MoraineSchemaRow {
	uint64_t schema_id;
	char *schema_uuid;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	char *schema_name;
	char *path;
	bool path_is_relative;
} MoraineSchemaRow;

// Mirrors `moraine_duckdb::dumps::MoraineTableRow`.
typedef struct MoraineTableRow {
	uint64_t table_id;
	char *table_uuid;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	uint64_t schema_id;
	char *table_name;
	char *path;
	bool path_is_relative;
} MoraineTableRow;

// Mirrors `moraine_duckdb::dumps::MoraineViewRow`.
typedef struct MoraineViewRow {
	uint64_t view_id;
	char *view_uuid;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	uint64_t schema_id;
	char *view_name;
	char *dialect;
	char *sql;
	char *column_aliases;
} MoraineViewRow;

// Mirrors `moraine_duckdb::dumps::MoraineMacroRow`.
typedef struct MoraineMacroRow {
	uint64_t schema_id;
	uint64_t macro_id;
	char *macro_name;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
} MoraineMacroRow;

// Mirrors `moraine_duckdb::dumps::MoraineMacroImplRow`.
typedef struct MoraineMacroImplRow {
	uint64_t macro_id;
	uint64_t impl_id;
	char *dialect;
	char *sql;
	char *macro_type;
} MoraineMacroImplRow;

// Mirrors `moraine_duckdb::dumps::MoraineMacroParameterRow`.
typedef struct MoraineMacroParameterRow {
	uint64_t macro_id;
	uint64_t impl_id;
	uint64_t column_id;
	char *parameter_name;
	char *parameter_type;
	char *default_value;
	char *default_value_type;
} MoraineMacroParameterRow;

// Mirrors `moraine_duckdb::dumps::MoraineColumnMappingRow`.
typedef struct MoraineColumnMappingRow {
	uint64_t mapping_id;
	uint64_t table_id;
	char *map_type;
} MoraineColumnMappingRow;

// Mirrors `moraine_duckdb::dumps::MoraineNameMappingRow`.
typedef struct MoraineNameMappingRow {
	uint64_t mapping_id;
	uint64_t column_id;
	char *source_name;
	uint64_t target_field_id;
	bool has_parent_column;
	uint64_t parent_column;
	bool is_partition;
} MoraineNameMappingRow;

// Mirrors `moraine_duckdb::dumps::MoraineColumnRow`.
typedef struct MoraineColumnRow {
	uint64_t column_id;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	uint64_t table_id;
	uint64_t column_order;
	char *column_name;
	char *column_type;
	char *initial_default;
	char *default_value;
	bool nulls_allowed;
	bool has_parent_column;
	uint64_t parent_column;
	char *default_value_type;
	char *default_value_dialect;
} MoraineColumnRow;

// Mirrors `moraine_duckdb::dumps::MoraineDataFileRow`.
typedef struct MoraineDataFileRow {
	uint64_t data_file_id;
	uint64_t table_id;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	bool has_file_order;
	uint64_t file_order;
	char *path;
	bool path_is_relative;
	char *file_format;
	uint64_t record_count;
	uint64_t file_size_bytes;
	uint64_t footer_size;
	bool has_row_id_start;
	uint64_t row_id_start;
	bool has_partition_id;
	uint64_t partition_id;
	char *encryption_key;
	bool has_mapping_id;
	uint64_t mapping_id;
	bool has_partial_max;
	uint64_t partial_max;
} MoraineDataFileRow;

// Mirrors `moraine_duckdb::dumps::MoraineDeleteFileRow`.
typedef struct MoraineDeleteFileRow {
	uint64_t delete_file_id;
	uint64_t table_id;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	uint64_t data_file_id;
	char *path;
	bool path_is_relative;
	char *format;
	uint64_t delete_count;
	uint64_t file_size_bytes;
	uint64_t footer_size;
	char *encryption_key;
	bool has_partial_max;
	uint64_t partial_max;
} MoraineDeleteFileRow;

// Mirrors `moraine_duckdb::dumps::MoraineTableStatsRow`.
typedef struct MoraineTableStatsRow {
	uint64_t table_id;
	uint64_t record_count;
	uint64_t next_row_id;
	uint64_t file_size_bytes;
} MoraineTableStatsRow;

// Mirrors `moraine_duckdb::dumps::MoraineTableColumnStatsRow`.
typedef struct MoraineTableColumnStatsRow {
	uint64_t table_id;
	uint64_t column_id;
	bool has_contains_null;
	bool contains_null;
	bool has_contains_nan;
	bool contains_nan;
	char *min_value;
	char *max_value;
	char *extra_stats;
} MoraineTableColumnStatsRow;

// Mirrors `moraine_duckdb::dumps::MoraineFileColumnStatsRow`.
typedef struct MoraineFileColumnStatsRow {
	uint64_t data_file_id;
	uint64_t table_id;
	uint64_t column_id;
	uint64_t column_size_bytes;
	uint64_t value_count;
	uint64_t null_count;
	char *min_value;
	char *max_value;
	bool has_contains_nan;
	bool contains_nan;
	char *extra_stats;
} MoraineFileColumnStatsRow;

int32_t moraine_dump_snapshots(MoraineCatalogHandle *handle, MoraineSnapshotRow **out_items, size_t *out_len,
                                MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_snapshots_free(MoraineSnapshotRow *items, size_t len);

int32_t moraine_dump_schemas(MoraineCatalogHandle *handle, MoraineSchemaRow **out_items, size_t *out_len,
                              MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_schemas_free(MoraineSchemaRow *items, size_t len);

int32_t moraine_dump_tables(MoraineCatalogHandle *handle, MoraineTableRow **out_items, size_t *out_len,
                             MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_tables_free(MoraineTableRow *items, size_t len);

int32_t moraine_dump_columns(MoraineCatalogHandle *handle, MoraineColumnRow **out_items, size_t *out_len,
                              MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_columns_free(MoraineColumnRow *items, size_t len);

int32_t moraine_dump_views(MoraineCatalogHandle *handle, MoraineViewRow **out_items, size_t *out_len,
                            MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_views_free(MoraineViewRow *items, size_t len);

int32_t moraine_dump_macros(MoraineCatalogHandle *handle, MoraineMacroRow **out_items, size_t *out_len,
                             MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_macros_free(MoraineMacroRow *items, size_t len);

int32_t moraine_dump_macro_impls(MoraineCatalogHandle *handle, MoraineMacroImplRow **out_items, size_t *out_len,
                                  MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_macro_impls_free(MoraineMacroImplRow *items, size_t len);

int32_t moraine_dump_macro_parameters(MoraineCatalogHandle *handle, MoraineMacroParameterRow **out_items,
                                       size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_macro_parameters_free(MoraineMacroParameterRow *items, size_t len);

int32_t moraine_dump_column_mappings(MoraineCatalogHandle *handle, MoraineColumnMappingRow **out_items,
                                      size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_column_mappings_free(MoraineColumnMappingRow *items, size_t len);

int32_t moraine_dump_name_mappings(MoraineCatalogHandle *handle, MoraineNameMappingRow **out_items,
                                    size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_name_mappings_free(MoraineNameMappingRow *items, size_t len);

int32_t moraine_dump_data_files(MoraineCatalogHandle *handle, MoraineDataFileRow **out_items, size_t *out_len,
                                 MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_data_files_free(MoraineDataFileRow *items, size_t len);

int32_t moraine_dump_delete_files(MoraineCatalogHandle *handle, MoraineDeleteFileRow **out_items, size_t *out_len,
                                   MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_delete_files_free(MoraineDeleteFileRow *items, size_t len);

int32_t moraine_dump_table_stats(MoraineCatalogHandle *handle, MoraineTableStatsRow **out_items, size_t *out_len,
                                  MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_table_stats_free(MoraineTableStatsRow *items, size_t len);

int32_t moraine_dump_table_column_stats(MoraineCatalogHandle *handle, MoraineTableColumnStatsRow **out_items,
                                         size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_table_column_stats_free(MoraineTableColumnStatsRow *items, size_t len);

int32_t moraine_dump_file_column_stats(MoraineCatalogHandle *handle, MoraineFileColumnStatsRow **out_items,
                                        size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_file_column_stats_free(MoraineFileColumnStatsRow *items, size_t len);

// Mirrors `moraine_duckdb::dumps::MoraineSchemaVersionRow`: one
// ducklake_schema_versions row, flattened from the snapshot records the
// table-id sets fold into.
typedef struct MoraineSchemaVersionRow {
	uint64_t begin_snapshot;
	uint64_t schema_version;
	uint64_t table_id;
} MoraineSchemaVersionRow;

int32_t moraine_dump_schema_versions(MoraineCatalogHandle *handle, MoraineSchemaVersionRow **out_items,
                                      size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_schema_versions_free(MoraineSchemaVersionRow *items, size_t len);

// Mirrors `moraine_duckdb::dumps::MorainePartitionInfoRow`.
typedef struct MorainePartitionInfoRow {
	uint64_t partition_id;
	uint64_t table_id;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
} MorainePartitionInfoRow;

int32_t moraine_dump_partition_info(MoraineCatalogHandle *handle, MorainePartitionInfoRow **out_items,
                                     size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_partition_info_free(MorainePartitionInfoRow *items, size_t len);

// Mirrors `moraine_duckdb::dumps::MorainePartitionColumnRow`: one
// ducklake_partition_column row, flattened from the partition record's
// embedded columns.
typedef struct MorainePartitionColumnRow {
	uint64_t partition_id;
	uint64_t table_id;
	uint64_t partition_key_index;
	uint64_t column_id;
	char *transform;
} MorainePartitionColumnRow;

int32_t moraine_dump_partition_columns(MoraineCatalogHandle *handle, MorainePartitionColumnRow **out_items,
                                        size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_partition_columns_free(MorainePartitionColumnRow *items, size_t len);

// Mirrors `moraine_duckdb::dumps::MoraineFilePartitionValueRow`: one
// ducklake_file_partition_value row, flattened from the data-file
// record's embedded partition values.
typedef struct MoraineFilePartitionValueRow {
	uint64_t data_file_id;
	uint64_t table_id;
	uint64_t partition_key_index;
	char *partition_value;
} MoraineFilePartitionValueRow;

int32_t moraine_dump_file_partition_values(MoraineCatalogHandle *handle, MoraineFilePartitionValueRow **out_items,
                                            size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_file_partition_values_free(MoraineFilePartitionValueRow *items, size_t len);

// Mirrors `moraine_duckdb::dumps::MoraineSortInfoRow`.
typedef struct MoraineSortInfoRow {
	uint64_t sort_id;
	uint64_t table_id;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
} MoraineSortInfoRow;

int32_t moraine_dump_sort_info(MoraineCatalogHandle *handle, MoraineSortInfoRow **out_items,
                                size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_sort_info_free(MoraineSortInfoRow *items, size_t len);

// Mirrors `moraine_duckdb::dumps::MoraineSortExpressionRow`: one
// ducklake_sort_expression row, flattened from the sort record's
// embedded expressions.
typedef struct MoraineSortExpressionRow {
	uint64_t sort_id;
	uint64_t table_id;
	uint64_t sort_key_index;
	char *expression;
	char *dialect;
	char *sort_direction;
	char *null_order;
} MoraineSortExpressionRow;

int32_t moraine_dump_sort_expressions(MoraineCatalogHandle *handle, MoraineSortExpressionRow **out_items,
                                       size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_sort_expressions_free(MoraineSortExpressionRow *items, size_t len);

// Mirrors `moraine_duckdb::dumps::MoraineTagRow`: one ducklake_tag row,
// flattened from the object's container record; ended entries included.
typedef struct MoraineTagRow {
	uint64_t object_id;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	char *key;
	char *value;
} MoraineTagRow;

int32_t moraine_dump_tags(MoraineCatalogHandle *handle, MoraineTagRow **out_items,
                           size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_tags_free(MoraineTagRow *items, size_t len);

// Mirrors `moraine_duckdb::dumps::MoraineColumnTagRow`: one
// ducklake_column_tag row, flattened from the column's latest record.
typedef struct MoraineColumnTagRow {
	uint64_t table_id;
	uint64_t column_id;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	char *key;
	char *value;
} MoraineColumnTagRow;

int32_t moraine_dump_column_tags(MoraineCatalogHandle *handle, MoraineColumnTagRow **out_items,
                                  size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_column_tags_free(MoraineColumnTagRow *items, size_t len);

// Mirrors `moraine_duckdb::dumps::MoraineScheduledDeletionRow`: one
// ducklake_files_scheduled_for_deletion row.
typedef struct MoraineScheduledDeletionRow {
	uint64_t data_file_id;
	char *path;
	bool path_is_relative;
	int64_t schedule_start_micros;
} MoraineScheduledDeletionRow;

int32_t moraine_dump_scheduled_deletions(MoraineCatalogHandle *handle, MoraineScheduledDeletionRow **out_items,
                                          size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_dump_scheduled_deletions_free(MoraineScheduledDeletionRow *items, size_t len);

// The inline read ABI (src/inline.rs): materializes DuckLake's four inline
// scan variants and the per-table Arrow schema / registered-table list over
// the inline/* keyspace. Owned-first, one _free per array; chunk_body is an
// owned copy of its chunk's full Arrow IPC body per returned row (see
// src/inline.rs's module doc for the ownership rationale).

// Mirrors `moraine_duckdb::inline::MoraineInlineRow`.
typedef struct MoraineInlineRow {
	uint64_t row_id;
	uint64_t schema_version;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	uint8_t *chunk_body;
	size_t chunk_body_len;
	uint64_t offset_in_chunk;
} MoraineInlineRow;

// `scan_kind`: 0 SCAN_TABLE, 1 SCAN_INSERTIONS, 2 SCAN_DELETIONS, 3
// SCAN_FOR_FLUSH. `start` is read only by SCAN_INSERTIONS/SCAN_DELETIONS.
int32_t moraine_inline_scan(MoraineCatalogHandle *handle, uint64_t table_id, int32_t scan_kind, uint64_t snapshot,
                             uint64_t start, MoraineInlineRow **out_items, size_t *out_len,
                             MoraineInterruptProbe probe, void *probe_ctx, MoraineError *err);
void moraine_inline_scan_free(MoraineInlineRow *items, size_t len);

// Mirrors `moraine_duckdb::inline::MoraineInlineSchemaRow`.
typedef struct MoraineInlineSchemaRow {
	uint64_t schema_version;
	uint8_t *arrow_schema;
	size_t arrow_schema_len;
} MoraineInlineSchemaRow;

int32_t moraine_inline_schemas(MoraineCatalogHandle *handle, uint64_t table_id, MoraineInlineSchemaRow **out_items,
                                size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_inline_schemas_free(MoraineInlineSchemaRow *items, size_t len);

// Mirrors `moraine_duckdb::inline::MoraineInlineTableRow`.
typedef struct MoraineInlineTableRow {
	uint64_t table_id;
	uint64_t schema_version;
} MoraineInlineTableRow;

int32_t moraine_inline_registered_tables(MoraineCatalogHandle *handle, MoraineInlineTableRow **out_items,
                                          size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_inline_registered_tables_free(MoraineInlineTableRow *items, size_t len);

// Reports via *out_exists whether table_id has at least one recorded
// inline/fdel record — existence for the ducklake_inlined_delete_<table_id>
// catalog lookup (DuckLake exists-probes this table with a plain SELECT
// before trusting it, so a table with no fdel ever staged must not resolve
// in the catalog at all).
int32_t moraine_inline_file_delete_table_exists(MoraineCatalogHandle *handle, uint64_t table_id, bool *out_exists,
                                          MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);

// Mirrors `moraine_duckdb::inline::MoraineInlineFileDeleteRow`.
typedef struct MoraineInlineFileDeleteRow {
	uint64_t file_id;
	uint64_t row_id;
	uint64_t begin_snapshot;
} MoraineInlineFileDeleteRow;

int32_t moraine_inline_file_deletes(MoraineCatalogHandle *handle, uint64_t table_id,
                                          MoraineInlineFileDeleteRow **out_items, size_t *out_len,
                                          MoraineInterruptProbe probe, void *probe_ctx,
                MoraineError *err);
void moraine_inline_file_deletes_free(MoraineInlineFileDeleteRow *items, size_t len);

// The staged-row write path (src/staged.rs): DuckLake authors row
// mutations against the ducklake_* metadata tables over ordinary SQL; the
// shim translates each row into a MoraineCell array and stages it here,
// landing every staged row in one atomic batch at commit. No internal
// retry: a lost race at commit returns MORAINE_COMMIT_CONFLICT with the
// literal substring "conflict" in the message.

typedef struct MoraineTxHandle MoraineTxHandle;

// One value in a staged row. `kind`: 0 = NULL, 1 = u64, 2 = i64, 3 = bool,
// 4 = string (borrowed, NUL-terminated UTF-8; moraine_tx_stage copies it,
// never retains the pointer past that call).
typedef struct MoraineCell {
	int32_t kind;
	uint64_t u64_value;
	int64_t i64_value;
	bool bool_value;
	const char *str_value;
} MoraineCell;

int32_t moraine_tx_begin(MoraineCatalogHandle *handle, MoraineTxHandle **out, MoraineInterruptProbe probe,
                          void *probe_ctx, MoraineError *err);

// `table_kind` (moraine::ffi_support::staged::TableKind's discriminant
// order): 0 ducklake_snapshot, 1 ducklake_snapshot_changes, 2
// ducklake_schema, 3 ducklake_table, 4 ducklake_view, 5 ducklake_column, 6
// ducklake_data_file, 7 ducklake_delete_file, 8 ducklake_table_stats, 9
// ducklake_table_column_stats, 10 ducklake_file_column_stats, 11
// ducklake_schema_versions, 12 ducklake_partition_info, 13
// ducklake_partition_column, 14 ducklake_file_partition_value, 15
// ducklake_sort_info, 16 ducklake_sort_expression.
// `operation_kind`: 0 insert, 1 delete, 2 update-sets-end_snapshot. `cells` are
// positional in the exact column order metadata_tables.cpp declares for
// `table_kind`'s table (a delete/update-set-end row carries only the key
// columns).
int32_t moraine_tx_stage(MoraineTxHandle *tx, int32_t table_kind, int32_t operation_kind, const MoraineCell *cells,
                           size_t cells_len, MoraineError *err);

// Consumes `tx`. On success, `*out_snapshot_id` is the new snapshot id.
// Snapshot rows as `tx` sees them: committed rows at the transaction's
// read point minus its staged snapshot deletes. Freed with
// moraine_dump_snapshots_free.
int32_t moraine_tx_dump_snapshots(MoraineTxHandle *tx, MoraineSnapshotRow **out_items, size_t *out_len,
                                   MoraineError *err);

int32_t moraine_tx_commit(MoraineTxHandle *tx, uint64_t *out_snapshot_id, MoraineError *err);

// Consumes `tx`, discarding every staged row. A null `tx` is a no-op.
void moraine_tx_rollback(MoraineTxHandle *tx);

// Inline write ops (src/staged.rs): extend the staged-tx handle with the
// inline/* record shapes. Every value here is stored verbatim, per the same
// rule ordinary staged rows follow; `moraine_tx_stage_inline_insert`
// allocates its chunk's chunk_seq at translate time, not from the caller.

int32_t moraine_tx_stage_inline_schema(MoraineTxHandle *tx, uint64_t table_id, uint64_t schema_version,
                                         const uint8_t *arrow_schema, size_t arrow_schema_len, MoraineError *err);

int32_t moraine_tx_stage_inline_insert(MoraineTxHandle *tx, uint64_t table_id, uint64_t schema_version,
                                         uint64_t begin_snapshot, uint64_t row_id_start, uint64_t row_count,
                                         const uint8_t *arrow_body, size_t arrow_body_len, MoraineError *err);

int32_t moraine_tx_stage_inline_inline_delete(MoraineTxHandle *tx, uint64_t table_id, uint64_t row_id,
                                       uint64_t end_snapshot, MoraineError *err);

int32_t moraine_tx_stage_inline_file_delete(MoraineTxHandle *tx, uint64_t table_id, uint64_t data_file_id,
                                       uint64_t row_id, uint64_t begin_snapshot, MoraineError *err);

// Removes every inline/insert chunk begun at or before flush_snapshot for
// (table_id, schema_version), plus the inline/idel tombstones those chunks'
// rows consumed.
int32_t moraine_tx_stage_inline_flush_delete(MoraineTxHandle *tx, uint64_t table_id, uint64_t schema_version,
                                               uint64_t flush_snapshot, MoraineError *err);

// Removes every inline/* record for table_id.
int32_t moraine_tx_stage_inline_drop(MoraineTxHandle *tx, uint64_t table_id, MoraineError *err);

// Removes only the inline/schema record for (table_id, schema_version),
// leaving any other schema version's inline/* records untouched — the
// superseded-inlined-table cleanup a flush issues once its chunks are
// gone. Distinct from moraine_tx_stage_inline_drop, which is table-wide.
int32_t moraine_tx_stage_inline_schema_drop(MoraineTxHandle *tx, uint64_t table_id, uint64_t schema_version,
                                              MoraineError *err);

// Arrow IPC bridge (`src/arrow_ipc.rs`). The shim converts a DuckDB
// `DataChunk` to the Arrow C Data Interface with DuckDB's own converter and
// hands those structs here for IPC serialization; decode reverses it. The
// C Data Interface structs are defined by DuckDB's Arrow headers, so they
// are only forward-declared here.
struct ArrowSchema;
struct ArrowArray;

// Mirrors `moraine_duckdb::arrow_ipc::MoraineArrowBytes`: a heap buffer
// owned by Rust, freed with `moraine_arrow_bytes_free`.
typedef struct MoraineArrowBytes {
	uint8_t *data;
	size_t len;
	size_t cap;
} MoraineArrowBytes;

// Mirrors `moraine_duckdb::arrow_ipc::MoraineArrowError`: a status/message
// pair; a non-null message is freed with `moraine_arrow_error_free`.
typedef struct MoraineArrowError {
	int32_t failed;
	char *message;
} MoraineArrowError;

// Serializes an exported Arrow schema to a schema-only IPC stream. Consumes
// `schema` (releases its buffers); returns non-zero and sets `err` on failure.
int32_t moraine_arrow_encode_schema(struct ArrowSchema *schema, MoraineArrowBytes *out, MoraineArrowError *err);

// Serializes an exported Arrow array to a self-contained IPC stream (schema
// and one batch). Consumes both `schema` and `array`.
int32_t moraine_arrow_encode_chunk(struct ArrowSchema *schema, struct ArrowArray *array, MoraineArrowBytes *out,
                                    MoraineArrowError *err);

// Decodes an IPC stream into exported C Data Interface structs the caller
// (via DuckDB's importer) releases. A schema-only stream yields a zero-row
// array. Used to reconstruct a table's columns from its `inline/schema` record.
int32_t moraine_arrow_decode_stream(const uint8_t *body, size_t body_len, struct ArrowSchema *out_schema,
                                    struct ArrowArray *out_array, MoraineArrowError *err);

// Decodes a body-only chunk (from `moraine_arrow_encode_chunk`) against the
// schema stored for its version; `schema_ipc` is that version's schema-only
// IPC stream. Produces exported C Data Interface structs the caller releases.
int32_t moraine_arrow_decode_body(const uint8_t *schema_ipc, size_t schema_ipc_len, const uint8_t *body,
                                  size_t body_len, struct ArrowSchema *out_schema, struct ArrowArray *out_array,
                                  MoraineArrowError *err);

// Frees a buffer returned by an encode call.
void moraine_arrow_bytes_free(MoraineArrowBytes bytes);

// Frees a message set by a failed Arrow bridge call.
void moraine_arrow_error_free(char *message);

// Equality-index SQL surface (`src/abi.rs`). The DDL calls commit
// autonomously (their own moraine commit, outside any enclosing DuckDB
// transaction).

// Mirrors `moraine_duckdb::abi::MoraineIndexDesc`.
typedef struct MoraineIndexDesc {
	uint64_t index_id;
	bool unique;
	bool building;
	char *name;
} MoraineIndexDesc;

// Creates an equality index over `schema_name.table_name`, committing
// autonomously. When the table already holds data, its files are scoped-read
// from the handle's DATA_PATH store (resolved at attach from META_DATA_PATH)
// to backfill the index.
int32_t moraine_index_create(MoraineCatalogHandle *handle, const char *schema_name, const char *table_name,
                             const char *index_name, const char *const *column_names, size_t column_count, bool unique,
                             MoraineInterruptProbe probe, void *probe_ctx, MoraineError *err);

// Drops an equality index by name, committing autonomously.
int32_t moraine_index_drop(MoraineCatalogHandle *handle, const char *schema_name, const char *table_name,
                           const char *index_name, MoraineInterruptProbe probe, void *probe_ctx, MoraineError *err);

// Lists a table's live equality indexes.
int32_t moraine_indexes(MoraineCatalogHandle *handle, const char *schema_name, const char *table_name,
                        MoraineIndexDesc **out_items, size_t *out_len, MoraineInterruptProbe probe, void *probe_ctx,
                        MoraineError *err);
void moraine_indexes_free(MoraineIndexDesc *items, size_t len);

// Mirrors `moraine_duckdb::abi::MoraineRowLocation`.
typedef struct MoraineRowLocation {
	uint64_t row_id;
	uint64_t data_file_id;
	bool is_inline;
} MoraineRowLocation;

// Mirrors `moraine_duckdb::abi::MoraineLookupValue`: a lookup value tagged by
// kind (1=i64, 2=u64, 3=f64, 4=bool, 5=string, 6=bytes). The shim fills the
// field matching `kind`; the ABI coerces it to the indexed column's type.
typedef struct MoraineLookupValue {
	int32_t kind;
	int64_t i64_value;
	uint64_t u64_value;
	double f64_value;
	bool bool_value;
	const char *str_value;
	const uint8_t *bytes_value;
	size_t bytes_len;
} MoraineLookupValue;

// Resolves an equality lookup on a single-column index to the rows holding
// `lookup_value`, coerced to the indexed column's type.
int32_t moraine_index_lookup(MoraineCatalogHandle *handle, const char *schema_name, const char *table_name,
                             const char *index_name, const MoraineLookupValue *lookup_value,
                             MoraineRowLocation **out_items, size_t *out_len, MoraineInterruptProbe probe,
                             void *probe_ctx, MoraineError *err);
void moraine_index_lookup_free(MoraineRowLocation *items, size_t len);

#ifdef __cplusplus
} // extern "C"
#endif
