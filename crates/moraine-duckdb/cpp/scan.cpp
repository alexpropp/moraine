#include "scan.hpp"

namespace moraine_duckdb {

namespace {

// Holds the nested Connection/streaming query for the lifetime of one scan
// execution. `current_chunk` must outlive each `.function` call:
// `DataChunk::Reference` makes `output` alias its vectors rather than copy
// them; the next call replaces (and frees) the previous chunk.
struct MoraineScanGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::unique_ptr<duckdb::Connection> connection;
	duckdb::unique_ptr<duckdb::QueryResult> result;
	duckdb::unique_ptr<duckdb::DataChunk> current_chunk;

	idx_t MaxThreads() const override {
		// One QueryResult cursor feeds this scan; nothing to parallelize.
		return 1;
	}
};

// Escapes a path for embedding in a single-quoted SQL string literal by
// doubling embedded `'`. No backslash handling: DuckDB's default string
// literal dialect gives backslashes no special meaning.
std::string EscapeSqlStringLiteral(const std::string &path) {
	std::string escaped;
	escaped.reserve(path.size());
	for (char c : path) {
		if (c == '\'') {
			escaped += "''";
		} else {
			escaped += c;
		}
	}
	return escaped;
}

std::string BuildReadParquetQuery(const std::vector<std::string> &file_paths) {
	std::string list = "[";
	for (size_t i = 0; i < file_paths.size(); ++i) {
		if (i > 0) {
			list += ", ";
		}
		list += "'" + EscapeSqlStringLiteral(file_paths[i]) + "'";
	}
	list += "]";
	return "SELECT * FROM read_parquet(" + list + ")";
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MoraineScanInitGlobal(duckdb::ClientContext &context,
                                                                           duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MoraineScanBindData>();
	auto state = duckdb::make_uniq<MoraineScanGlobalState>();
	if (bind_data.file_paths.empty()) {
		// Leave `result` null: MoraineScanFunctionImpl reads that as
		// "always empty" without calling read_parquet, which errors on an
		// empty file list.
		return std::move(state);
	}

	state->connection = duckdb::make_uniq<duckdb::Connection>(*bind_data.database);
	// Streaming (SendQuery's default), not Query(): a materialized result
	// would pull the whole table into memory inside this one call. Actual
	// reading happens one chunk at a time in Fetch, below.
	state->result = state->connection->SendQuery(BuildReadParquetQuery(bind_data.file_paths));
	if (state->result->HasError()) {
		state->result->ThrowError("moraine: scanning table data files failed: ");
	}
	return std::move(state);
}

void MoraineScanFunctionImpl(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
                             duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<MoraineScanGlobalState>();
	if (!state.result) {
		output.SetCardinality(0);
		return;
	}
	// Fetch throws on a mid-stream error; the HasError check below covers
	// the no-throw error path so a failed stream can't look like clean EOF.
	state.current_chunk = state.result->Fetch();
	if (!state.current_chunk || state.current_chunk->size() == 0) {
		if (state.result->HasError()) {
			state.result->ThrowError("moraine: scanning table data files failed: ");
		}
		output.SetCardinality(0);
		return;
	}
	auto &bind_data = data.bind_data->Cast<MoraineScanBindData>();
	if (state.current_chunk->ColumnCount() != bind_data.catalog_column_count) {
		// DataChunk::Reference's own column-count check is a debug-only
		// D_ASSERT, compiled out in release builds, so this explicit check
		// is the load-bearing memory-safety guard against an overrun write.
		throw duckdb::InvalidInputException(
		    "moraine: data files of table \"%s\" have %llu columns but the catalog declares %llu",
		    bind_data.table_name, static_cast<idx_t>(state.current_chunk->ColumnCount()),
		    static_cast<idx_t>(bind_data.catalog_column_count));
	}
	// Column types/order come from the Parquet file's schema, not the
	// catalog's declared types; per-column type mismatch is unchecked.
	output.Reference(*state.current_chunk);
}

} // namespace

duckdb::unique_ptr<duckdb::FunctionData> MoraineScanBindData::Copy() const {
	auto result = duckdb::make_uniq<MoraineScanBindData>();
	result->file_paths = file_paths;
	result->database = database;
	result->catalog_column_count = catalog_column_count;
	result->table_name = table_name;
	result->table_entry = table_entry;
	return std::move(result);
}

bool MoraineScanBindData::Equals(const duckdb::FunctionData &other_p) const {
	auto &other = other_p.Cast<MoraineScanBindData>();
	return file_paths == other.file_paths && database == other.database &&
	       catalog_column_count == other.catalog_column_count && table_name == other.table_name &&
	       table_entry == other.table_entry;
}

duckdb::TableFunction MoraineScanFunction() {
	// No `bind`/`bind_replace` callback: MoraineTableEntry::GetScanFunction
	// (the only caller) already produces complete bind data itself; DuckDB's
	// binder uses that TableFunction + bind_data pair directly.
	auto function = duckdb::TableFunction("moraine_scan", {}, MoraineScanFunctionImpl, nullptr,
	                                      MoraineScanInitGlobal, nullptr);
	// Lets `LogicalGet::GetTable()` find the catalog entry behind this scan,
	// which is how DESCRIBE/SHOW trace a plan column back to the base table
	// to read its NOT NULL constraints (otherwise every column looks nullable).
	function.get_bind_info = [](const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
		auto &data = bind_data->Cast<MoraineScanBindData>();
		if (data.table_entry != nullptr) {
			return duckdb::BindInfo(*data.table_entry);
		}
		return duckdb::BindInfo(duckdb::ScanType::EXTERNAL);
	};
	return function;
}

} // namespace moraine_duckdb
