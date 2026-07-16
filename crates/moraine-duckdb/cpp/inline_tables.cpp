#include "inline_tables.hpp"

#include "catalog.hpp"
#include "metadata_tables.hpp"
#include "owned_array.hpp"
#include "transaction_manager.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

namespace moraine_duckdb {

namespace {

duckdb::Value Bigint(uint64_t v) {
	return duckdb::Value::BIGINT(static_cast<int64_t>(v));
}

duckdb::Value OptBigint(bool has, uint64_t v) {
	if (!has) {
		return duckdb::Value(duckdb::LogicalType::BIGINT);
	}
	return Bigint(v);
}

uint64_t CellAsU64(const duckdb::Value &v) {
	return static_cast<uint64_t>(v.GetValue<int64_t>());
}

// Raises a `MoraineArrowError` returned by the Rust IPC bridge as a DuckDB
// exception, consuming and freeing its message. `context_msg` names the
// operation for the surfaced text.
[[noreturn]] void ThrowArrowError(MoraineArrowError &err, const char *context_msg) {
	std::string message = context_msg;
	if (err.message) {
		message += ": ";
		message += err.message;
		moraine_arrow_error_free(err.message);
		err.message = nullptr;
	}
	throw duckdb::InternalException(std::string("moraine: ") + message);
}

} // namespace

std::string InlinedDataTableName(uint64_t table_id, uint64_t schema_version) {
	return "ducklake_inlined_data_" + std::to_string(table_id) + "_" + std::to_string(schema_version);
}

std::string InlinedDeleteTableName(uint64_t table_id) {
	return "ducklake_inlined_delete_" + std::to_string(table_id);
}

namespace {

// Parses a run of ASCII digits from `s` starting at `pos`, requiring at
// least one digit and consuming to the end of `s`. Returns nullopt on any
// non-digit, empty digit run, or overflow.
std::optional<uint64_t> ParseTrailingU64(const std::string &s, size_t pos) {
	if (pos >= s.size()) {
		return std::nullopt;
	}
	uint64_t value = 0;
	for (size_t i = pos; i < s.size(); i++) {
		char c = s[i];
		if (c < '0' || c > '9') {
			return std::nullopt;
		}
		auto digit = static_cast<uint64_t>(c - '0');
		if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
			return std::nullopt;
		}
		value = value * 10 + digit;
	}
	return value;
}

} // namespace

std::optional<InlinedDataTableId> ParseInlinedDataTableName(const std::string &name) {
	static const std::string prefix = "ducklake_inlined_data_";
	if (name.rfind(prefix, 0) != 0) {
		return std::nullopt;
	}
	auto rest = name.substr(prefix.size());
	auto underscore = rest.rfind('_');
	if (underscore == std::string::npos || underscore == 0) {
		return std::nullopt;
	}
	auto table_id = ParseTrailingU64(rest.substr(0, underscore), 0);
	auto schema_version = ParseTrailingU64(rest, underscore + 1);
	if (!table_id || !schema_version) {
		return std::nullopt;
	}
	return InlinedDataTableId {*table_id, *schema_version};
}

std::optional<uint64_t> ParseInlinedDeleteTableName(const std::string &name) {
	static const std::string prefix = "ducklake_inlined_delete_";
	if (name.rfind(prefix, 0) != 0) {
		return std::nullopt;
	}
	return ParseTrailingU64(name, prefix.size());
}

std::vector<uint8_t> EncodeInlineSchema(duckdb::ClientContext &context,
                                        const std::vector<DecodedInlineColumn> &user_columns) {
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<std::string> names;
	types.reserve(user_columns.size());
	names.reserve(user_columns.size());
	for (auto &col : user_columns) {
		// Inline data is serialized through Arrow, and DuckDB's Arrow format
		// has no VARIANT support (unlike GEOMETRY, which the spatial extension
		// registers). Reject it here with a clear message instead of letting
		// `ToArrowSchema` throw a bare "Unsupported Arrow type VARIANT".
		if (col.type.id() == duckdb::LogicalTypeId::VARIANT) {
			throw duckdb::NotImplementedException(
			    "moraine: column \"%s\" is VARIANT, which moraine cannot store — its inline "
			    "data is serialized through Arrow, and DuckDB's Arrow format has no VARIANT "
			    "support. Use JSON (or another type) instead.",
			    col.name);
		}
		types.push_back(col.type);
		names.push_back(col.name);
	}

	auto options = context.GetClientProperties();
	ArrowSchema c_schema;
	duckdb::ArrowConverter::ToArrowSchema(&c_schema, types, names, options);

	MoraineArrowBytes bytes {};
	MoraineArrowError err {};
	// Consumes `c_schema` (releases DuckDB's buffers); do not release it here.
	if (moraine_arrow_encode_schema(&c_schema, &bytes, &err) != 0) {
		ThrowArrowError(err, "encoding inline schema");
	}
	std::vector<uint8_t> out(bytes.data, bytes.data + bytes.len);
	moraine_arrow_bytes_free(bytes);
	return out;
}

std::vector<DecodedInlineColumn> DecodeInlineSchema(duckdb::ClientContext &context, const uint8_t *data, size_t len) {
	ArrowSchema c_schema;
	ArrowArray c_array;
	MoraineArrowError err {};
	if (moraine_arrow_decode_stream(data, len, &c_schema, &c_array, &err) != 0) {
		ThrowArrowError(err, "decoding inline schema");
	}
	// A schema-only stream still yields a (zero-row) array; release it.
	if (c_array.release) {
		c_array.release(&c_array);
	}

	std::vector<DecodedInlineColumn> result;
	result.reserve(static_cast<size_t>(c_schema.n_children));
	for (int64_t i = 0; i < c_schema.n_children; i++) {
		ArrowSchema &child = *c_schema.children[i];
		std::string name = child.name ? child.name : "";
		auto arrow_type = duckdb::ArrowType::GetTypeFromSchema(context, child);
		result.push_back(DecodedInlineColumn {std::move(name), arrow_type->GetDuckType()});
	}
	if (c_schema.release) {
		c_schema.release(&c_schema);
	}
	return result;
}

std::vector<uint8_t> EncodeInlineChunkRows(duckdb::ClientContext &context, duckdb::DataChunk &chunk,
                                           duckdb::idx_t user_col_start) {
	auto user_count = chunk.ColumnCount() - user_col_start;
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<std::string> names;
	types.reserve(user_count);
	names.reserve(user_count);
	for (duckdb::idx_t i = 0; i < user_count; i++) {
		types.push_back(chunk.data[user_col_start + i].GetType());
		// Column identity comes from the `inline/schema` record; these names
		// only ride the chunk stream and are never read back.
		names.push_back("c" + std::to_string(i));
	}

	// Export just the user columns: `ToArrowArray` serializes every column of
	// the chunk it is handed, so reference the tail into a standalone view.
	duckdb::DataChunk user_chunk;
	user_chunk.InitializeEmpty(types);
	for (duckdb::idx_t i = 0; i < user_count; i++) {
		user_chunk.data[i].Reference(chunk.data[user_col_start + i]);
	}
	user_chunk.SetCardinality(chunk.size());

	auto options = context.GetClientProperties();
	ArrowSchema c_schema;
	duckdb::ArrowConverter::ToArrowSchema(&c_schema, types, names, options);
	ArrowArray c_array;
	duckdb::ArrowConverter::ToArrowArray(user_chunk, &c_array, options, {});

	MoraineArrowBytes bytes {};
	MoraineArrowError err {};
	// Consumes both `c_schema` and `c_array`; do not release them here.
	if (moraine_arrow_encode_chunk(&c_schema, &c_array, &bytes, &err) != 0) {
		ThrowArrowError(err, "encoding inline chunk");
	}
	std::vector<uint8_t> out(bytes.data, bytes.data + bytes.len);
	moraine_arrow_bytes_free(bytes);
	return out;
}

std::vector<std::vector<duckdb::Value>> DecodeInlineChunkRows(duckdb::ClientContext &context, const uint8_t *schema_ipc,
                                                              size_t schema_ipc_len, const uint8_t *data, size_t len,
                                                              const std::vector<duckdb::LogicalType> &user_types) {
	ArrowSchema c_schema;
	ArrowArray c_array;
	MoraineArrowError err {};
	if (moraine_arrow_decode_body(schema_ipc, schema_ipc_len, data, len, &c_schema, &c_array, &err) != 0) {
		ThrowArrowError(err, "decoding inline chunk");
	}

	// Build a per-column `ArrowType` map from the stream's own embedded schema.
	duckdb::ArrowTableSchema arrow_table;
	duckdb::ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, c_schema);
	auto &columns = arrow_table.GetColumns();
	if (columns.size() != user_types.size()) {
		if (c_schema.release) {
			c_schema.release(&c_schema);
		}
		if (c_array.release) {
			c_array.release(&c_array);
		}
		throw duckdb::InternalException("moraine: inline chunk has %llu columns, expected %llu — schema/body mismatch",
		                                static_cast<unsigned long long>(columns.size()),
		                                static_cast<unsigned long long>(user_types.size()));
	}

	// Drive DuckDB's own record-batch importer (the arrow-scan path), which
	// applies each column's validity and offset. The decoded array is a
	// struct whose children are the columns; the scan state owns it and
	// releases it once, at end of scope, after every value is copied out.
	// `arrow_scan_is_projected = false` maps output columns 1:1 to the arrow
	// columns, so no projection `column_ids` are needed.
	auto chunk_wrapper = duckdb::make_uniq<duckdb::ArrowArrayWrapper>();
	chunk_wrapper->arrow_array = c_array;
	auto total = static_cast<duckdb::idx_t>(chunk_wrapper->arrow_array.length);

	duckdb::ArrowScanLocalState scan_state(std::move(chunk_wrapper), context);
	for (duckdb::idx_t col = 0; col < user_types.size(); col++) {
		scan_state.column_ids.push_back(col);
	}

	duckdb::vector<duckdb::LogicalType> chunk_types(user_types.begin(), user_types.end());
	std::vector<std::vector<duckdb::Value>> rows;
	rows.reserve(total);
	while (scan_state.chunk_offset < total) {
		auto size = std::min<duckdb::idx_t>(total - scan_state.chunk_offset, STANDARD_VECTOR_SIZE);
		duckdb::DataChunk out;
		out.Initialize(context, chunk_types);
		// `ArrowToDuckDB` reads `output.size()` as the row count to convert, so
		// the cardinality must be set before the call, not after.
		out.SetCardinality(size);
		duckdb::ArrowTableFunction::ArrowToDuckDB(scan_state, columns, out, /* arrow_scan_is_projected */ false);
		for (duckdb::idx_t row = 0; row < size; row++) {
			std::vector<duckdb::Value> cells;
			cells.reserve(user_types.size());
			for (duckdb::idx_t col = 0; col < user_types.size(); col++) {
				cells.push_back(out.GetValue(col, row));
			}
			rows.push_back(std::move(cells));
		}
		scan_state.chunk_offset += size;
	}

	if (c_schema.release) {
		c_schema.release(&c_schema);
	}
	return rows;
}

namespace {

duckdb::CreateTableInfo BuildInlineDataTableInfo(duckdb::SchemaCatalogEntry &schema, uint64_t table_id,
                                                 uint64_t schema_version,
                                                 const std::vector<DecodedInlineColumn> &user_columns) {
	duckdb::CreateTableInfo info(schema, InlinedDataTableName(table_id, schema_version));
	info.columns.AddColumn(duckdb::ColumnDefinition("row_id", duckdb::LogicalType::BIGINT));
	info.columns.AddColumn(duckdb::ColumnDefinition("begin_snapshot", duckdb::LogicalType::BIGINT));
	info.columns.AddColumn(duckdb::ColumnDefinition("end_snapshot", duckdb::LogicalType::BIGINT));
	for (auto &col : user_columns) {
		info.columns.AddColumn(duckdb::ColumnDefinition(col.name, col.type));
	}
	return info;
}

// Materializes every live row of `table_id` (the `ForFlush` scan at the
// maximum snapshot) so DuckDB's query engine applies the WHERE clause; the
// shim serves raw rows, never interprets the predicate.
//
// `moraine_inline_scan` scans the whole `table_id` across every schema
// version and a returned row carries no schema-version tag, so decoding
// every body against `user_types` is only correct when `table_id` has a
// single schema version live. A table that underwent a schema change while
// still holding unflushed inlined data under the old version would misdecode
// here.
std::vector<std::vector<duckdb::Value>> ProvideInlineDataRows(duckdb::ClientContext &context,
                                                              MoraineCatalogHandle *handle, uint64_t table_id,
                                                              uint64_t schema_version,
                                                              const std::vector<duckdb::LogicalType> &user_types) {
	// This entry serves one `(table_id, schema_version)`; body-only chunks of
	// that version decode against its schema-only stream (`inline/schema`).
	// The scan below spans every version of the table, so chunks of other
	// versions — a schema-evolved table holds several — are filtered out.
	OwnedArray<MoraineInlineSchemaRow> schemas(moraine_inline_schemas_free);
	MoraineError schema_err {};
	if (moraine_inline_schemas(handle, table_id, schemas.OutItems(), schemas.OutLen(), moraine_shim_is_interrupted,
	                           &context, &schema_err) != MORAINE_OK) {
		ThrowMoraineError(schema_err);
	}
	const uint8_t *schema_ipc = nullptr;
	size_t schema_ipc_len = 0;
	for (auto &s : schemas) {
		if (s.schema_version == schema_version) {
			schema_ipc = s.arrow_schema;
			schema_ipc_len = s.arrow_schema_len;
			break;
		}
	}
	if (!schema_ipc) {
		throw duckdb::InternalException("moraine: no inline schema recorded for table %llu schema version %llu",
		                                static_cast<unsigned long long>(table_id),
		                                static_cast<unsigned long long>(schema_version));
	}

	OwnedArray<MoraineInlineRow> rows(moraine_inline_scan_free);
	MoraineError err {};
	auto code = moraine_inline_scan(handle, table_id, /* SCAN_FOR_FLUSH */ 3, std::numeric_limits<uint64_t>::max(), 0,
	                                rows.OutItems(), rows.OutLen(), moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		// The scan spans every schema version of the table; this entry serves
		// exactly its own version's `ducklake_inlined_data_<t>_<v>`, so a chunk
		// from another version (its columns and schema differ) is not ours.
		if (r.schema_version != schema_version) {
			continue;
		}
		auto decoded =
		    DecodeInlineChunkRows(context, schema_ipc, schema_ipc_len, r.chunk_body, r.chunk_body_len, user_types);
		if (r.offset_in_chunk >= decoded.size()) {
			throw duckdb::InternalException("moraine: inline scan row offset out of range");
		}
		std::vector<duckdb::Value> row;
		row.reserve(3 + user_types.size());
		row.push_back(Bigint(r.row_id));
		row.push_back(Bigint(r.begin_snapshot));
		row.push_back(OptBigint(r.has_end_snapshot, r.end_snapshot));
		auto &cells = decoded[r.offset_in_chunk];
		row.insert(row.end(), cells.begin(), cells.end());
		result.push_back(std::move(row));
	}
	// `moraine_inline_scan`'s `ForFlush` variant already orders by
	// `(row_id, begin_snapshot)`.
	return result;
}

} // namespace

MoraineInlineDataTableEntry::MoraineInlineDataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                                                         duckdb::CreateTableInfo &info, MoraineCatalogHandle *handle,
                                                         uint64_t table_id, uint64_t schema_version)
    : duckdb::TableCatalogEntry(catalog, schema, info), handle_(handle), table_id_(table_id),
      schema_version_(schema_version) {
}

duckdb::unique_ptr<duckdb::BaseStatistics> MoraineInlineDataTableEntry::GetStatistics(duckdb::ClientContext &,
                                                                                      duckdb::column_t) {
	throw duckdb::NotImplementedException("moraine: column statistics are not supported yet");
}

std::vector<duckdb::LogicalType> MoraineInlineDataTableEntry::UserColumnTypes() const {
	std::vector<duckdb::LogicalType> types;
	duckdb::idx_t index = 0;
	for (auto &col : GetColumns().Logical()) {
		if (index >= 3) {
			types.push_back(col.Type());
		}
		index++;
	}
	return types;
}

duckdb::TableFunction
MoraineInlineDataTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                             duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	auto scan_bind_data = duckdb::make_uniq<MetadataScanBindData>();
	scan_bind_data->rows = ProvideInlineDataRows(context, handle_, table_id_, schema_version_, UserColumnTypes());
	scan_bind_data->table_entry = this;
	bind_data = std::move(scan_bind_data);
	return MetadataScanTableFunction();
}

duckdb::TableStorageInfo MoraineInlineDataTableEntry::GetStorageInfo(duckdb::ClientContext &) {
	return duckdb::TableStorageInfo();
}

MoraineInlineDeleteTableEntry::MoraineInlineDeleteTableEntry(duckdb::Catalog &catalog,
                                                             duckdb::SchemaCatalogEntry &schema,
                                                             duckdb::CreateTableInfo &info,
                                                             MoraineCatalogHandle *handle, uint64_t table_id)
    : duckdb::TableCatalogEntry(catalog, schema, info), handle_(handle), table_id_(table_id) {
}

duckdb::unique_ptr<duckdb::BaseStatistics> MoraineInlineDeleteTableEntry::GetStatistics(duckdb::ClientContext &,
                                                                                        duckdb::column_t) {
	throw duckdb::NotImplementedException("moraine: column statistics are not supported yet");
}

duckdb::TableFunction
MoraineInlineDeleteTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                               duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	OwnedArray<MoraineInlineFileDeleteRow> file_deletes(moraine_inline_file_deletes_free);
	MoraineError err {};
	auto code = moraine_inline_file_deletes(handle_, table_id_, file_deletes.OutItems(), file_deletes.OutLen(),
	                                        moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> rows;
	rows.reserve(file_deletes.size());
	for (auto &r : file_deletes) {
		rows.push_back({Bigint(r.file_id), Bigint(r.row_id), Bigint(r.begin_snapshot)});
	}
	auto scan_bind_data = duckdb::make_uniq<MetadataScanBindData>();
	scan_bind_data->rows = std::move(rows);
	scan_bind_data->table_entry = this;
	bind_data = std::move(scan_bind_data);
	return MetadataScanTableFunction();
}

duckdb::TableStorageInfo MoraineInlineDeleteTableEntry::GetStorageInfo(duckdb::ClientContext &) {
	return duckdb::TableStorageInfo();
}

duckdb::unique_ptr<MoraineInlineDataTableEntry>
MakeInlineDataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, MoraineCatalogHandle *handle,
                         uint64_t table_id, uint64_t schema_version,
                         const std::vector<DecodedInlineColumn> &user_columns) {
	auto info = BuildInlineDataTableInfo(schema, table_id, schema_version, user_columns);
	return duckdb::make_uniq<MoraineInlineDataTableEntry>(catalog, schema, info, handle, table_id, schema_version);
}

duckdb::unique_ptr<MoraineInlineDeleteTableEntry> MakeInlineDeleteTableEntry(duckdb::Catalog &catalog,
                                                                             duckdb::SchemaCatalogEntry &schema,
                                                                             MoraineCatalogHandle *handle,
                                                                             uint64_t table_id) {
	duckdb::CreateTableInfo info(schema, InlinedDeleteTableName(table_id));
	info.columns.AddColumn(duckdb::ColumnDefinition("file_id", duckdb::LogicalType::BIGINT));
	info.columns.AddColumn(duckdb::ColumnDefinition("row_id", duckdb::LogicalType::BIGINT));
	info.columns.AddColumn(duckdb::ColumnDefinition("begin_snapshot", duckdb::LogicalType::BIGINT));
	return duckdb::make_uniq<MoraineInlineDeleteTableEntry>(catalog, schema, info, handle, table_id);
}

duckdb::unique_ptr<duckdb::CatalogEntry> LookupInlineTableEntry(duckdb::ClientContext &context,
                                                                duckdb::Catalog &catalog,
                                                                duckdb::SchemaCatalogEntry &schema,
                                                                MoraineCatalogHandle *handle, const std::string &name) {
	if (auto parsed = ParseInlinedDataTableName(name)) {
		OwnedArray<MoraineInlineSchemaRow> schemas(moraine_inline_schemas_free);
		MoraineError err {};
		auto code = moraine_inline_schemas(handle, parsed->table_id, schemas.OutItems(), schemas.OutLen(),
		                                   moraine_shim_is_interrupted, &context, &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		for (auto &row : schemas) {
			if (row.schema_version != parsed->schema_version) {
				continue;
			}
			auto user_columns = DecodeInlineSchema(context, row.arrow_schema, row.arrow_schema_len);
			return MakeInlineDataTableEntry(catalog, schema, handle, parsed->table_id, parsed->schema_version,
			                                user_columns);
		}
		return nullptr;
	}
	if (auto table_id = ParseInlinedDeleteTableName(name)) {
		bool exists = false;
		MoraineError err {};
		auto code = moraine_inline_file_delete_table_exists(handle, *table_id, &exists, moraine_shim_is_interrupted,
		                                                    &context, &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		if (!exists) {
			return nullptr;
		}
		return MakeInlineDeleteTableEntry(catalog, schema, handle, *table_id);
	}
	return nullptr;
}

duckdb::unique_ptr<duckdb::CatalogEntry> CreateInlineDataTable(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                               duckdb::SchemaCatalogEntry &schema,
                                                               MoraineCatalogHandle *handle, MoraineTxHandle *tx,
                                                               duckdb::BoundCreateTableInfo &info, uint64_t table_id,
                                                               uint64_t schema_version) {
	OwnedArray<MoraineInlineSchemaRow> schemas(moraine_inline_schemas_free);
	MoraineError lookup_err {};
	auto lookup_code = moraine_inline_schemas(handle, table_id, schemas.OutItems(), schemas.OutLen(),
	                                          moraine_shim_is_interrupted, &context, &lookup_err);
	if (lookup_code != MORAINE_OK) {
		ThrowMoraineError(lookup_err);
	}
	for (auto &row : schemas) {
		if (row.schema_version != schema_version) {
			continue;
		}
		if (info.Base().on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		}
		throw duckdb::CatalogException("moraine: \"%s\" already exists", info.Base().table);
	}

	std::vector<DecodedInlineColumn> user_columns;
	duckdb::idx_t index = 0;
	for (auto &col : info.Base().columns.Logical()) {
		if (index >= 3) {
			user_columns.push_back(DecodedInlineColumn {col.Name(), col.Type()});
		}
		index++;
	}
	auto schema_bytes = EncodeInlineSchema(context, user_columns);
	MoraineError stage_err {};
	auto stage_code = moraine_tx_stage_inline_schema(tx, table_id, schema_version, schema_bytes.data(),
	                                                 schema_bytes.size(), &stage_err);
	if (stage_code != MORAINE_OK) {
		ThrowMoraineError(stage_err);
	}
	return MakeInlineDataTableEntry(catalog, schema, handle, table_id, schema_version, user_columns);
}

namespace {

// Shared Sink+Source state for every inline DML operator below: the
// affected-row count, whether the one-row `Count` result has been emitted,
// and — for UPDATE/DELETE — the lazily materialized rows a rowid index
// resolves against.
struct InlineDmlState : public duckdb::GlobalSinkState {
	duckdb::idx_t affected_count = 0;
	bool emitted = false;
	bool old_rows_loaded = false;
	std::vector<std::vector<duckdb::Value>> old_rows;
	// DELETE only: the maximum `begin_snapshot` among matched rows, standing
	// in for the flush-snapshot threshold.
	std::optional<uint64_t> max_begin_snapshot;
};

class MoraineInlineDml : public duckdb::PhysicalOperator {
public:
	static constexpr const duckdb::PhysicalOperatorType TYPE = duckdb::PhysicalOperatorType::EXTENSION;

	MoraineInlineDml(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                 duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality)
	    : duckdb::PhysicalOperator(physical_plan, TYPE, std::move(types), estimated_cardinality), catalog_(catalog) {
	}

	duckdb::Catalog &catalog_;

	duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(duckdb::ClientContext &) const override {
		return duckdb::make_uniq<InlineDmlState>();
	}
	bool IsSink() const override {
		return true;
	}
	bool IsSource() const override {
		return true;
	}

protected:
	MoraineTxHandle *StagedTx(duckdb::ClientContext &client) const {
		auto catalog_transaction = catalog_.GetCatalogTransaction(client);
		auto &moraine_tx = catalog_transaction.transaction->Cast<MoraineTransaction>();
		return moraine_tx.StagedTx();
	}

	// Resolves a rowid the entry's scan emitted (its index into
	// `ProvideInlineDataRows`'s output) back to the row itself,
	// re-materializing on first use.
	const std::vector<duckdb::Value> &ResolveRow(duckdb::ClientContext &context, InlineDmlState &state,
	                                             MoraineCatalogHandle *handle, uint64_t table_id,
	                                             uint64_t schema_version,
	                                             const std::vector<duckdb::LogicalType> &user_types,
	                                             const duckdb::Value &row_id) const {
		if (!state.old_rows_loaded) {
			state.old_rows = ProvideInlineDataRows(context, handle, table_id, schema_version, user_types);
			state.old_rows_loaded = true;
		}
		if (row_id.IsNull()) {
			throw duckdb::InternalException("moraine: staged write received a NULL rowid");
		}
		auto index = static_cast<duckdb::idx_t>(row_id.GetValue<int64_t>());
		if (index >= state.old_rows.size()) {
			throw duckdb::InternalException(
			    "moraine: staged write rowid is out of range — the committed head moved between this "
			    "statement's scan and its write, which the supported topology excludes");
		}
		return state.old_rows[index];
	}

public:
	duckdb::SourceResultType GetDataInternal(duckdb::ExecutionContext &, duckdb::DataChunk &chunk,
	                                         duckdb::OperatorSourceInput &) const override {
		auto &state = sink_state->Cast<InlineDmlState>();
		if (state.emitted) {
			chunk.SetCardinality(0);
			return duckdb::SourceResultType::FINISHED;
		}
		chunk.SetValue(0, 0, duckdb::Value::BIGINT(static_cast<int64_t>(state.affected_count)));
		chunk.SetCardinality(1);
		state.emitted = true;
		return duckdb::SourceResultType::FINISHED;
	}
};

class MoraineInlineDataInsertOp : public MoraineInlineDml {
public:
	MoraineInlineDataInsertOp(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                          duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality, uint64_t table_id,
	                          uint64_t schema_version)
	    : MoraineInlineDml(physical_plan, std::move(types), catalog, estimated_cardinality), table_id_(table_id),
	      schema_version_(schema_version) {
	}

	uint64_t table_id_;
	uint64_t schema_version_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<InlineDmlState>();
		if (chunk.size() == 0) {
			return duckdb::SinkResultType::NEED_MORE_INPUT;
		}
		auto *tx = StagedTx(context.client);
		// Columns 0/1 are `row_id`/`begin_snapshot`; every row of one chunk
		// shares one `begin_snapshot`, so the first row's value is enough.
		auto row_id_start = CellAsU64(chunk.GetValue(0, 0));
		auto begin_snapshot = CellAsU64(chunk.GetValue(1, 0));
		auto body = EncodeInlineChunkRows(context.client, chunk, /* user_col_start */ 3);
		MoraineError err {};
		auto code = moraine_tx_stage_inline_insert(tx, table_id_, schema_version_, begin_snapshot, row_id_start,
		                                           chunk.size(), body.data(), body.size(), &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		state.affected_count += chunk.size();
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

class MoraineInlineDataUpdateOp : public MoraineInlineDml {
public:
	MoraineInlineDataUpdateOp(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                          duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality,
	                          MoraineCatalogHandle *handle, uint64_t table_id, uint64_t schema_version,
	                          std::vector<duckdb::LogicalType> user_types, duckdb::idx_t set_ref)
	    : MoraineInlineDml(physical_plan, std::move(types), catalog, estimated_cardinality), handle_(handle),
	      table_id_(table_id), schema_version_(schema_version), user_types_(std::move(user_types)), set_ref_(set_ref) {
	}

	MoraineCatalogHandle *handle_;
	uint64_t table_id_;
	uint64_t schema_version_;
	std::vector<duckdb::LogicalType> user_types_;
	duckdb::idx_t set_ref_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<InlineDmlState>();
		auto *tx = StagedTx(context.client);
		// The row-id column is appended last.
		auto row_id_col = chunk.ColumnCount() - 1;
		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			auto &old_row = ResolveRow(context.client, state, handle_, table_id_, schema_version_, user_types_,
			                           chunk.GetValue(row_id_col, row));
			auto real_row_id = CellAsU64(old_row[0]);
			auto end_snapshot = CellAsU64(chunk.GetValue(set_ref_, row));
			MoraineError err {};
			auto code = moraine_tx_stage_inline_inline_delete(tx, table_id_, real_row_id, end_snapshot, &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

class MoraineInlineDataDeleteOp : public MoraineInlineDml {
public:
	MoraineInlineDataDeleteOp(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                          duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality,
	                          MoraineCatalogHandle *handle, uint64_t table_id, uint64_t schema_version,
	                          std::vector<duckdb::LogicalType> user_types, duckdb::idx_t row_id_chunk_index)
	    : MoraineInlineDml(physical_plan, std::move(types), catalog, estimated_cardinality), handle_(handle),
	      table_id_(table_id), schema_version_(schema_version), user_types_(std::move(user_types)),
	      row_id_chunk_index_(row_id_chunk_index) {
	}

	MoraineCatalogHandle *handle_;
	uint64_t table_id_;
	uint64_t schema_version_;
	std::vector<duckdb::LogicalType> user_types_;
	duckdb::idx_t row_id_chunk_index_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<InlineDmlState>();
		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			auto &old_row = ResolveRow(context.client, state, handle_, table_id_, schema_version_, user_types_,
			                           chunk.GetValue(row_id_chunk_index_, row));
			auto begin_snapshot = CellAsU64(old_row[1]);
			if (!state.max_begin_snapshot.has_value() || begin_snapshot > *state.max_begin_snapshot) {
				state.max_begin_snapshot = begin_snapshot;
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}

	duckdb::SourceResultType GetDataInternal(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                                         duckdb::OperatorSourceInput &input) const override {
		auto &state = sink_state->Cast<InlineDmlState>();
		if (!state.emitted && state.max_begin_snapshot.has_value()) {
			auto *tx = StagedTx(context.client);
			MoraineError err {};
			auto code =
			    moraine_tx_stage_inline_flush_delete(tx, table_id_, schema_version_, *state.max_begin_snapshot, &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
		}
		return MoraineInlineDml::GetDataInternal(context, chunk, input);
	}
};

class MoraineInlineDeleteInsertOp : public MoraineInlineDml {
public:
	MoraineInlineDeleteInsertOp(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                            duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality, uint64_t table_id)
	    : MoraineInlineDml(physical_plan, std::move(types), catalog, estimated_cardinality), table_id_(table_id) {
	}

	uint64_t table_id_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<InlineDmlState>();
		auto *tx = StagedTx(context.client);
		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			auto file_id = CellAsU64(chunk.GetValue(0, row));
			auto row_id = CellAsU64(chunk.GetValue(1, row));
			auto begin_snapshot = CellAsU64(chunk.GetValue(2, row));
			MoraineError err {};
			auto code = moraine_tx_stage_inline_file_delete(tx, table_id_, file_id, row_id, begin_snapshot, &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

} // namespace

duckdb::PhysicalOperator &PlanInlineDataInsert(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalInsert &op,
                                               MoraineInlineDataTableEntry &table_entry) {
	return planner.Make<MoraineInlineDataInsertOp>(op.types, op.table.catalog, op.estimated_cardinality,
	                                               table_entry.TableId(), table_entry.SchemaVersion());
}

duckdb::PhysicalOperator &PlanInlineDataUpdate(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalUpdate &op,
                                               MoraineInlineDataTableEntry &table_entry) {
	if (op.return_chunk) {
		throw duckdb::NotImplementedException("moraine: UPDATE ... RETURNING is not supported on \"%s\"",
		                                      op.table.name);
	}
	if (op.columns.size() != 1 ||
	    !duckdb::StringUtil::CIEquals(table_entry.GetColumns().GetColumn(op.columns[0]).GetName(), "end_snapshot")) {
		throw duckdb::NotImplementedException(
		    "moraine: the only UPDATE supported on \"%s\" is SET end_snapshot (the staged-row lifecycle "
		    "convention)",
		    op.table.name);
	}
	if (op.expressions.size() != 1 || op.expressions[0]->GetExpressionClass() != duckdb::ExpressionClass::BOUND_REF) {
		throw duckdb::NotImplementedException(
		    "moraine: UPDATE with a non-column SET expression is not supported on \"%s\"", op.table.name);
	}
	auto set_ref = op.expressions[0]->Cast<duckdb::BoundReferenceExpression>().index;
	return planner.Make<MoraineInlineDataUpdateOp>(op.types, op.table.catalog, op.estimated_cardinality,
	                                               table_entry.Handle(), table_entry.TableId(),
	                                               table_entry.SchemaVersion(), table_entry.UserColumnTypes(), set_ref);
}

duckdb::PhysicalOperator &PlanInlineDataDelete(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalDelete &op,
                                               MoraineInlineDataTableEntry &table_entry) {
	if (op.return_chunk) {
		throw duckdb::NotImplementedException("moraine: DELETE ... RETURNING is not supported on \"%s\"",
		                                      op.table.name);
	}
	if (op.expressions.size() != 1) {
		throw duckdb::InternalException("moraine: expected exactly one row-id expression for DELETE on \"%s\"",
		                                op.table.name);
	}
	auto &bound_ref = op.expressions[0]->Cast<duckdb::BoundReferenceExpression>();
	return planner.Make<MoraineInlineDataDeleteOp>(
	    op.types, op.table.catalog, op.estimated_cardinality, table_entry.Handle(), table_entry.TableId(),
	    table_entry.SchemaVersion(), table_entry.UserColumnTypes(), bound_ref.index);
}

duckdb::PhysicalOperator &PlanInlineDeleteInsert(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalInsert &op,
                                                 MoraineInlineDeleteTableEntry &table_entry) {
	return planner.Make<MoraineInlineDeleteInsertOp>(op.types, op.table.catalog, op.estimated_cardinality,
	                                                 table_entry.TableId());
}

} // namespace moraine_duckdb
