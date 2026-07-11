// Vendored by hand, verbatim except for its own `#include`s, from
// https://raw.githubusercontent.com/duckdb/duckdb/v1.5.4/src/include/duckdb/storage/database_size.hpp
// (git tag v1.5.4, commit 08e34c447b). Same amalgamation gap as
// `storage_extension.hpp`: `Catalog::GetDatabaseSize` names `DatabaseSize`
// by value, but the amalgamation only forward-declares it. Self-contained
// against types already in `duckdb.hpp` (`idx_t`, `block_id_t`, `vector`).
// Kept byte-for-byte identical to upstream — see `create_schema_info.hpp`
// for why. Must be included after `duckdb.hpp`.
#pragma once

namespace duckdb {

struct DatabaseSize {
	idx_t total_blocks = 0;
	idx_t block_size = 0;
	idx_t free_blocks = 0;
	idx_t used_blocks = 0;
	idx_t bytes = 0;
	idx_t wal_size = 0;
};

struct MetadataBlockInfo {
	block_id_t block_id;
	idx_t total_blocks;
	vector<idx_t> free_list;
};

} // namespace duckdb
