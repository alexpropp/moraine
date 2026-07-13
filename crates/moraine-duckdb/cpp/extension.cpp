// Extension registration, called from the Rust entry point
// (`moraine_duckdb_duckdb_cpp_init` in src/entrypoint.rs). The entry point
// lives in Rust so it lands in the cdylib's dynamic symbol table on every
// platform; this function does the work: register moraine's StorageExtension
// (attach type `moraine`) against the loading database's config.
#include "duckdb.hpp"

namespace moraine_duckdb {
// Defined in storage_extension.cpp.
void RegisterMoraineStorageExtension(duckdb::DBConfig &config);
} // namespace moraine_duckdb

extern "C" {

// Not exported from the shared object: the Rust entry point references it, so
// it is resolved at static-link time. Receives the `ExtensionLoader` DuckDB
// handed the entry point, forwarded through Rust as a pointer.
void moraine_duckdb_register(duckdb::ExtensionLoader *loader) {
	loader->SetDescription("moraine: a SlateDB-backed DuckLake catalog, read-only this slice");
	moraine_duckdb::RegisterMoraineStorageExtension(loader->GetDatabaseInstance().config);
}

} // extern "C"
