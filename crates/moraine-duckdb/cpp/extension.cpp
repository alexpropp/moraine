// Extension entry point: registers moraine's StorageExtension (attach
// type `moraine`) against the loading database's config.
#include "duckdb.hpp"

namespace moraine_duckdb {
// Defined in storage_extension.cpp.
void RegisterMoraineStorageExtension(duckdb::DBConfig &config);
} // namespace moraine_duckdb

extern "C" {

// Symbol name contract: the loader dlopen()s the file and calls
// `<filebase>_duckdb_cpp_init`, where filebase is the artifact's base
// filename with the `.duckdb_extension` suffix stripped. The artifact
// here is named `moraine_duckdb`, so the exported symbol must be
// `moraine_duckdb_duckdb_cpp_init`.
DUCKDB_EXTENSION_API void moraine_duckdb_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
	loader.SetDescription("moraine: a SlateDB-backed DuckLake catalog, read-only this slice");
	moraine_duckdb::RegisterMoraineStorageExtension(loader.GetDatabaseInstance().config);
}

} // extern "C"
