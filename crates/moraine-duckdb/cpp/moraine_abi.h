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
	uint64_t row_id_start;
	uint64_t file_size_bytes;
	uint64_t footer_size;
} MoraineDataFileDesc;

int32_t moraine_attach(const char *path, const char *object_store_uri, MoraineCatalogHandle **out, MoraineError *err);
void moraine_detach(MoraineCatalogHandle *handle);

int32_t moraine_snapshot(MoraineCatalogHandle *handle, MoraineSnapshotHandle **out, MoraineError *err);
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

#ifdef __cplusplus
} // extern "C"
#endif
