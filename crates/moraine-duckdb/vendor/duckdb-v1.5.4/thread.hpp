// Vendored by hand, verbatim except for its own `#include`s, from
// https://raw.githubusercontent.com/duckdb/duckdb/v1.5.4/src/include/duckdb/common/thread.hpp
// (git tag v1.5.4, commit 08e34c447b). Pulled in transitively by
// `view_catalog_entry.hpp` (its `atomic<thread_id> bind_thread` member needs
// `thread_id` to be a complete type): the amalgamation doesn't reach this
// header either. Self-contained against the standard library plus
// `optional_ptr`, already defined in `duckdb.hpp`. Kept byte-for-byte
// identical to upstream — see `create_schema_info.hpp` for why. Must be
// included after `duckdb.hpp`.
#pragma once

#ifndef DUCKDB_NO_THREADS
#include <thread>

namespace duckdb {
using std::thread;
using thread_id = std::thread::id;

} // namespace duckdb

#else
using thread_id = uint64_t;
#endif

namespace duckdb {

class ClientContext;

struct ThreadUtil {
	static void SleepMs(idx_t ms, optional_ptr<ClientContext> context = nullptr);
	static void SleepMicroSeconds(idx_t micros);
	static thread_id GetThreadId();
	static string GetThreadIdString();
};

} // namespace duckdb
