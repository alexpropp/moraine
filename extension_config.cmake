# Included by DuckDB's build system to discover which extension to build.
# DONT_LINK: build only the loadable `.duckdb_extension`, don't statically
# link moraine (and its Rust core) into DuckDB's own CLI binary.
duckdb_extension_load(moraine
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/crates/moraine-duckdb/cpp
    DONT_LINK
)
