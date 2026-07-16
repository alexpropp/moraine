#include "catalog.hpp"

#include "inline_tables.hpp"
#include "metadata_tables.hpp"
#include "owned_array.hpp"
#include "scan.hpp"
#include "staged_write.hpp"
#include "transaction_manager.hpp"

#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <cctype>
#include <cstdlib>
#include <unordered_map>
#include <vector>

namespace moraine_duckdb {

namespace {

// Reconstructs a column's DuckDB `LogicalType` from a table's flat,
// position-ordered `ducklake_column` rows, folding nested children (linked
// by `parent_column`) into `LIST`/`STRUCT`/`MAP`. `by_id` maps each column's
// field id to its row; `children_of` maps a parent id to its child ids in
// column order. Scalars go through `MapColumnType`.
duckdb::LogicalType BuildColumnType(const MoraineColumnDesc &column,
                                    const std::unordered_map<uint64_t, const MoraineColumnDesc *> &by_id,
                                    const std::unordered_map<uint64_t, std::vector<uint64_t>> &children_of) {
	auto child_ids = children_of.find(column.id);
	auto children = child_ids == children_of.end() ? std::vector<uint64_t> {} : child_ids->second;

	if (duckdb::StringUtil::CIEquals(column.sql_type, "list")) {
		if (children.size() != 1) {
			throw duckdb::InternalException("moraine: LIST column \"%s\" must have exactly one child", column.name);
		}
		auto &element = *by_id.at(children[0]);
		return duckdb::LogicalType::LIST(BuildColumnType(element, by_id, children_of));
	}
	if (duckdb::StringUtil::CIEquals(column.sql_type, "struct")) {
		duckdb::child_list_t<duckdb::LogicalType> fields;
		fields.reserve(children.size());
		for (auto child_id : children) {
			auto &field = *by_id.at(child_id);
			fields.emplace_back(field.name, BuildColumnType(field, by_id, children_of));
		}
		return duckdb::LogicalType::STRUCT(std::move(fields));
	}
	if (duckdb::StringUtil::CIEquals(column.sql_type, "map")) {
		if (children.size() != 2) {
			throw duckdb::InternalException("moraine: MAP column \"%s\" must have a key and value child", column.name);
		}
		auto &key = *by_id.at(children[0]);
		auto &value = *by_id.at(children[1]);
		return duckdb::LogicalType::MAP(BuildColumnType(key, by_id, children_of),
		                                BuildColumnType(value, by_id, children_of));
	}
	return MapColumnType(column.sql_type);
}

std::string ToUpperAscii(const std::string &s) {
	std::string result = s;
	for (auto &c : result) {
		c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
	}
	return result;
}

} // namespace

extern "C" bool moraine_shim_is_interrupted(void *client_context) {
	return static_cast<duckdb::ClientContext *>(client_context)->IsInterrupted();
}

void ThrowMoraineError(MoraineError &err) {
	std::string message = err.message ? std::string(err.message) : std::string("moraine: unknown error");
	int32_t code = err.code;
	if (err.message != nullptr) {
		moraine_error_free(err.message);
		err.message = nullptr;
	}
	switch (code) {
	case MORAINE_NOT_FOUND:
	case MORAINE_ALREADY_EXISTS:
	case MORAINE_CONSTRAINT:
		throw duckdb::CatalogException(message);
	case MORAINE_COMMIT_CONFLICT:
		throw duckdb::TransactionException(message);
	case MORAINE_INVALID_ARGUMENT:
		throw duckdb::InvalidInputException(message);
	case MORAINE_INTERRUPTED:
		throw duckdb::InterruptException();
	case MORAINE_CORRUPTION:
	case MORAINE_STORE:
		throw duckdb::IOException(message);
	case MORAINE_INTERNAL:
	default:
		throw duckdb::InternalException(message);
	}
}

duckdb::LogicalType MapColumnType(const std::string &ducklake_type) {
	std::string upper = ToUpperAscii(ducklake_type);

	if (upper.rfind("DECIMAL(", 0) == 0 && upper.back() == ')') {
		auto inner = upper.substr(8, upper.size() - 9);
		auto comma = inner.find(',');
		if (comma != std::string::npos) {
			auto width_str = inner.substr(0, comma);
			auto scale_str = inner.substr(comma + 1);
			try {
				auto width = std::stoi(width_str);
				auto scale = std::stoi(scale_str);
				if (width > 0 && width <= 38 && scale >= 0 && scale <= width) {
					return duckdb::LogicalType::DECIMAL(static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
				}
			} catch (const std::exception &) {
				// falls through to NotImplementedException below
			}
		}
		throw duckdb::NotImplementedException("moraine: unsupported DuckLake column type \"%s\"", ducklake_type);
	}

	// Accepts two vocabularies: DuckDB SQL type names (e.g. "BIGINT") and
	// DuckLake's own lowercase names (e.g. "int64", uppercased above).
	// `DuckLakeColumnType` (metadata_tables.cpp) is its inverse.
	if (upper == "BIGINT" || upper == "INT64") {
		return duckdb::LogicalType::BIGINT;
	} else if (upper == "INTEGER" || upper == "INT32") {
		return duckdb::LogicalType::INTEGER;
	} else if (upper == "SMALLINT" || upper == "INT16") {
		return duckdb::LogicalType::SMALLINT;
	} else if (upper == "TINYINT" || upper == "INT8") {
		return duckdb::LogicalType::TINYINT;
	} else if (upper == "UBIGINT" || upper == "UINT64") {
		return duckdb::LogicalType::UBIGINT;
	} else if (upper == "UINTEGER" || upper == "UINT32") {
		return duckdb::LogicalType::UINTEGER;
	} else if (upper == "USMALLINT" || upper == "UINT16") {
		return duckdb::LogicalType::USMALLINT;
	} else if (upper == "UTINYINT" || upper == "UINT8") {
		return duckdb::LogicalType::UTINYINT;
	} else if (upper == "DOUBLE" || upper == "FLOAT64") {
		return duckdb::LogicalType::DOUBLE;
	} else if (upper == "FLOAT" || upper == "FLOAT32") {
		return duckdb::LogicalType::FLOAT;
	} else if (upper == "REAL") {
		return duckdb::LogicalType::FLOAT;
	} else if (upper == "VARCHAR") {
		return duckdb::LogicalType::VARCHAR;
	} else if (upper == "TEXT") {
		return duckdb::LogicalType::VARCHAR;
	} else if (upper == "JSON") {
		// DuckLake's `json`: VARCHAR carrying a `JSON` alias, matched on the
		// alias (not the id) by `DuckLakeColumnType`'s inverse.
		return duckdb::LogicalType::JSON();
	} else if (upper == "GEOMETRY") {
		// Requires the `spatial` extension at runtime for geometry values; the
		// type itself, and its Arrow inline encoding, resolve without it.
		return duckdb::LogicalType::GEOMETRY();
	} else if (upper == "BOOLEAN") {
		return duckdb::LogicalType::BOOLEAN;
	} else if (upper == "DATE") {
		return duckdb::LogicalType::DATE;
	} else if (upper == "TIMESTAMP" || upper == "TIMESTAMP_US") {
		// DuckLake's `timestamp_us` is the microsecond default, spelled
		// `timestamp`; the sub-second widths below are distinct ids.
		return duckdb::LogicalType::TIMESTAMP;
	} else if (upper == "TIMESTAMP_MS") {
		return duckdb::LogicalType::TIMESTAMP_MS;
	} else if (upper == "TIMESTAMP_NS") {
		return duckdb::LogicalType::TIMESTAMP_NS;
	} else if (upper == "TIMESTAMP_S") {
		return duckdb::LogicalType::TIMESTAMP_S;
	} else if (upper == "TIMESTAMPTZ" || upper == "TIMESTAMP WITH TIME ZONE") {
		return duckdb::LogicalType::TIMESTAMP_TZ;
	} else if (upper == "TIME") {
		return duckdb::LogicalType::TIME;
	} else if (upper == "TIME_NS") {
		return duckdb::LogicalType::TIME_NS;
	} else if (upper == "TIMETZ" || upper == "TIME WITH TIME ZONE") {
		return duckdb::LogicalType::TIME_TZ;
	} else if (upper == "BLOB") {
		return duckdb::LogicalType::BLOB;
	} else if (upper == "UUID") {
		return duckdb::LogicalType::UUID;
	} else if (upper == "HUGEINT" || upper == "INT128") {
		return duckdb::LogicalType::HUGEINT;
	} else if (upper == "UHUGEINT" || upper == "UINT128") {
		return duckdb::LogicalType::UHUGEINT;
	} else if (upper == "INTERVAL") {
		return duckdb::LogicalType::INTERVAL;
	}

	throw duckdb::NotImplementedException("moraine: unsupported DuckLake column type \"%s\"", ducklake_type);
}

MoraineTableEntry::MoraineTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                                     duckdb::CreateTableInfo &info, MoraineSnapshotHandle *snapshot, uint64_t table_id)
    : duckdb::TableCatalogEntry(catalog, schema, info), snapshot_(snapshot), table_id_(table_id) {
}

duckdb::unique_ptr<duckdb::BaseStatistics> MoraineTableEntry::GetStatistics(duckdb::ClientContext &context,
                                                                            duckdb::column_t column_id) {
	throw duckdb::NotImplementedException("moraine: column statistics are not supported yet");
}

duckdb::TableFunction MoraineTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                                         duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	// Binds unconditionally (so DESCRIBE/EXPLAIN work); the scan itself
	// always redirects to DuckLake at execution time — see scan.hpp.
	auto scan_bind_data = duckdb::make_uniq<MoraineScanBindData>();
	scan_bind_data->qualified_table_name = ParentSchema().name + "." + name;
	scan_bind_data->store_path = ParentCatalog().GetDBPath();
	scan_bind_data->table_entry = this;
	bind_data = std::move(scan_bind_data);
	return MoraineScanFunction();
}

duckdb::TableStorageInfo MoraineTableEntry::GetStorageInfo(duckdb::ClientContext &context) {
	return duckdb::TableStorageInfo();
}

MoraineViewEntry::MoraineViewEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                                   duckdb::CreateViewInfo &info)
    : duckdb::ViewCatalogEntry(catalog, schema, info) {
}

const duckdb::SelectStatement &MoraineViewEntry::GetQuery() {
	// `query` is always null (view definitions are never parsed yet); throw
	// rather than let the base implementation dereference it.
	throw duckdb::NotImplementedException("moraine: querying a view's definition is not supported yet");
}

void MoraineViewEntry::BindView(duckdb::ClientContext &context, duckdb::BindViewAction action) {
	throw duckdb::NotImplementedException("moraine: binding a view's definition is not supported yet");
}

std::string MoraineViewEntry::ToSQL() const {
	// `query` is null (view definitions are never parsed yet); compose the
	// definition textually from the listing ABI's strings instead.
	std::string result = "CREATE VIEW ";
	result += duckdb::KeywordHelper::WriteOptionallyQuoted(name);
	result += " AS ";
	result += sql;
	result += ";";
	return result;
}

MoraineSchemaEntry::MoraineSchemaEntry(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info,
                                       MoraineSnapshotHandle *snapshot, uint64_t schema_id)
    : duckdb::SchemaCatalogEntry(catalog, info), snapshot_(snapshot), schema_id_(schema_id) {
}

void MoraineSchemaEntry::EnsureTablesLoaded() {
	if (tables_loaded_) {
		return;
	}

	OwnedArray<MoraineTableDesc> table_descs(moraine_snapshot_tables_in_free);
	MoraineError err {};
	auto code = moraine_snapshot_tables_in(snapshot_, schema_id_, table_descs.OutItems(), table_descs.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	for (auto &table_desc : table_descs) {
		OwnedArray<MoraineColumnDesc> column_descs(moraine_snapshot_columns_of_free);
		MoraineError column_err {};
		auto column_code = moraine_snapshot_columns_of(snapshot_, table_desc.id, column_descs.OutItems(),
		                                               column_descs.OutLen(), &column_err);
		if (column_code != MORAINE_OK) {
			ThrowMoraineError(column_err);
		}

		// A nested column arrives as a top-level marker row plus child rows
		// (`parent_column` set), in column order. Index them so nested types
		// reconstruct, and add only the top-level columns.
		std::unordered_map<uint64_t, const MoraineColumnDesc *> by_id;
		std::unordered_map<uint64_t, std::vector<uint64_t>> children_of;
		for (auto &column_desc : column_descs) {
			by_id.emplace(column_desc.id, &column_desc);
			if (column_desc.has_parent_column) {
				children_of[column_desc.parent_column].push_back(column_desc.id);
			}
		}

		duckdb::CreateTableInfo info(*this, table_desc.name);
		duckdb::idx_t column_index = 0;
		for (auto &column_desc : column_descs) {
			if (column_desc.has_parent_column) {
				continue;
			}
			auto type = BuildColumnType(column_desc, by_id, children_of);
			info.columns.AddColumn(duckdb::ColumnDefinition(column_desc.name, type));
			if (!column_desc.nulls_allowed) {
				info.constraints.push_back(duckdb::make_uniq_base<duckdb::Constraint, duckdb::NotNullConstraint>(
				    duckdb::LogicalIndex(column_index)));
			}
			column_index++;
		}
		tables_.emplace(table_desc.name,
		                duckdb::make_uniq<MoraineTableEntry>(catalog, *this, info, snapshot_, table_desc.id));
	}

	if (duckdb::StringUtil::CIEquals(name, "main")) {
		// DuckLake's metadata connection queries every `ducklake_*` table from
		// the default schema, which is "main". A same-named real table wins
		// over the synthesized one via `emplace`'s no-overwrite rule.
		auto &moraine_catalog = ParentCatalog().Cast<MoraineCatalog>();
		PopulateMetadataTables(catalog, *this, moraine_catalog.Handle(), tables_);
	}

	// Set only after full success, so a mid-load exception leaves the next
	// call to retry rather than serve a partial table set.
	tables_loaded_ = true;
}

void MoraineSchemaEntry::EnsureViewsLoaded() {
	if (views_loaded_) {
		return;
	}

	OwnedArray<MoraineViewDesc> view_descs(moraine_snapshot_views_in_free);
	MoraineError err {};
	auto code = moraine_snapshot_views_in(snapshot_, schema_id_, view_descs.OutItems(), view_descs.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	for (auto &view_desc : view_descs) {
		duckdb::CreateViewInfo info(*this, view_desc.name);
		info.sql = view_desc.sql;
		// `info.query` is left null: view definitions are never parsed yet.
		// MoraineViewEntry::GetQuery/BindView throw rather than dereference it.
		views_.emplace(view_desc.name, duckdb::make_uniq<MoraineViewEntry>(catalog, *this, info));
	}

	// Same partial-load guard as EnsureTablesLoaded above.
	views_loaded_ = true;
}

void MoraineSchemaEntry::Scan(duckdb::ClientContext &context, duckdb::CatalogType type,
                              const std::function<void(duckdb::CatalogEntry &)> &callback) {
	Scan(type, callback);
}

void MoraineSchemaEntry::Scan(duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback) {
	if (type == duckdb::CatalogType::TABLE_ENTRY) {
		EnsureTablesLoaded();
		for (auto &entry : tables_) {
			callback(*entry.second);
		}
	} else if (type == duckdb::CatalogType::VIEW_ENTRY) {
		EnsureViewsLoaded();
		for (auto &entry : views_) {
			callback(*entry.second);
		}
	}
	// Every other CatalogType has nothing to enumerate.
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineSchemaEntry::LookupEntry(duckdb::CatalogTransaction transaction,
                                                                           const duckdb::EntryLookupInfo &lookup_info) {
	auto type = lookup_info.GetCatalogType();
	auto &name = lookup_info.GetEntryName();
	if (type == duckdb::CatalogType::TABLE_ENTRY) {
		EnsureTablesLoaded();
		auto it = tables_.find(name);
		if (it != tables_.end()) {
			return it->second.get();
		}
		// A table reference (`FROM m.s.x`) arrives as a TABLE_ENTRY lookup
		// even when `x` is a view; fall back to the view map.
		EnsureViewsLoaded();
		auto view_it = views_.find(name);
		if (view_it != views_.end()) {
			return view_it->second.get();
		}
		// Neither a fixed nor a previously-resolved dynamic entry: try the
		// two inline name families (only ever driven against the metadata
		// connection's default schema, matching PopulateMetadataTables).
		// Cached into `tables_` on success so a repeat lookup within this
		// transaction is free.
		if (duckdb::StringUtil::CIEquals(this->name, "main") && transaction.context) {
			auto &moraine_catalog = ParentCatalog().Cast<MoraineCatalog>();
			auto inline_entry =
			    LookupInlineTableEntry(*transaction.context, catalog, *this, moraine_catalog.Handle(), name);
			if (inline_entry) {
				auto *entry_ptr = inline_entry.get();
				tables_.emplace(name, std::move(inline_entry));
				return entry_ptr;
			}
		}
	} else if (type == duckdb::CatalogType::VIEW_ENTRY) {
		EnsureViewsLoaded();
		auto it = views_.find(name);
		if (it != views_.end()) {
			return it->second.get();
		}
	}
	return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineSchemaEntry::CreateIndex(duckdb::CatalogTransaction transaction,
                                                                           duckdb::CreateIndexInfo &info,
                                                                           duckdb::TableCatalogEntry &table) {
	throw duckdb::NotImplementedException("moraine: creating an index is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineSchemaEntry::CreateFunction(duckdb::CatalogTransaction transaction,
                                                                              duckdb::CreateFunctionInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a function is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineSchemaEntry::CreateTable(duckdb::CatalogTransaction transaction,
                                                                           duckdb::BoundCreateTableInfo &info) {
	auto &table_name = info.Base().table;
	auto &moraine_catalog = ParentCatalog().Cast<MoraineCatalog>();

	if (auto parsed = ParseInlinedDataTableName(table_name)) {
		if (!transaction.transaction) {
			throw duckdb::InternalException("moraine: CREATE TABLE without an active transaction");
		}
		if (!transaction.context) {
			throw duckdb::InternalException("moraine: CREATE TABLE without a client context");
		}
		auto &moraine_tx = transaction.transaction->Cast<MoraineTransaction>();
		auto entry = CreateInlineDataTable(*transaction.context, catalog, *this, moraine_catalog.Handle(),
		                                   moraine_tx.StagedTx(), info, parsed->table_id, parsed->schema_version);
		if (!entry) {
			// IF NOT EXISTS against an already-registered schema version.
			return nullptr;
		}
		auto *entry_ptr = entry.get();
		tables_.emplace(table_name, std::move(entry));
		return entry_ptr;
	}

	if (auto delete_table_id = ParseInlinedDeleteTableName(table_name)) {
		// Fixed shape, no store-side schema to stage — existence follows from
		// the first `inline/fdel` staged against it (see inline_tables.hpp),
		// so CREATE only builds and caches the entry for the rest of this
		// transaction. The find below is a defensive re-check; DuckLake
		// de-duplicates its own CREATE-per-batch.
		auto found = tables_.find(table_name);
		if (found != tables_.end()) {
			if (info.Base().on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
				return nullptr;
			}
			throw duckdb::CatalogException("moraine: \"%s\" already exists", table_name);
		}
		auto entry = MakeInlineDeleteTableEntry(catalog, *this, moraine_catalog.Handle(), *delete_table_id);
		auto *entry_ptr = entry.get();
		tables_.emplace(table_name, std::move(entry));
		return entry_ptr;
	}

	throw duckdb::NotImplementedException("moraine: creating a table is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineSchemaEntry::CreateView(duckdb::CatalogTransaction transaction,
                                                                          duckdb::CreateViewInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a view is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineSchemaEntry::CreateSequence(duckdb::CatalogTransaction transaction,
                                                                              duckdb::CreateSequenceInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a sequence is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
MoraineSchemaEntry::CreateTableFunction(duckdb::CatalogTransaction transaction, duckdb::CreateTableFunctionInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a table function is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
MoraineSchemaEntry::CreateCopyFunction(duckdb::CatalogTransaction transaction, duckdb::CreateCopyFunctionInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a copy function is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
MoraineSchemaEntry::CreatePragmaFunction(duckdb::CatalogTransaction transaction,
                                         duckdb::CreatePragmaFunctionInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a pragma function is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineSchemaEntry::CreateCollation(duckdb::CatalogTransaction transaction,
                                                                               duckdb::CreateCollationInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a collation is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineSchemaEntry::CreateType(duckdb::CatalogTransaction transaction,
                                                                          duckdb::CreateTypeInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a type is not supported (read-only catalog)");
}

void MoraineSchemaEntry::DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info) {
	// The flush cleanup's `DROP TABLE ducklake_inlined_data_<t>_<v>` is the
	// only DROP reaching here: deregister just this schema version, leaving
	// other schema versions' inline/* records untouched. The whole-table
	// cascade (`moraine_tx_stage_inline_drop`) runs on the DuckLake attach's
	// own catalog, not this metadata connection's schema.
	if (info.type == duckdb::CatalogType::TABLE_ENTRY) {
		if (auto parsed = ParseInlinedDataTableName(info.name)) {
			auto catalog_transaction = catalog.GetCatalogTransaction(context);
			auto &moraine_tx = catalog_transaction.transaction->Cast<MoraineTransaction>();
			MoraineError err {};
			auto code = moraine_tx_stage_inline_schema_drop(moraine_tx.StagedTx(), parsed->table_id,
			                                                parsed->schema_version, &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			tables_.erase(info.name);
			return;
		}
	}
	throw duckdb::NotImplementedException("moraine: dropping an entry is not supported (read-only catalog)");
}

void MoraineSchemaEntry::Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo &info) {
	throw duckdb::NotImplementedException("moraine: altering an entry is not supported (read-only catalog)");
}

MoraineCatalog::MoraineCatalog(duckdb::AttachedDatabase &db, MoraineCatalogHandle *handle, std::string path)
    : duckdb::Catalog(db), handle_(handle), path_(std::move(path)) {
}

MoraineCatalog::~MoraineCatalog() {
	if (handle_ != nullptr) {
		moraine_detach(handle_);
		handle_ = nullptr;
	}
}

duckdb::unique_ptr<duckdb::Catalog> MoraineCatalog::Attach(duckdb::optional_ptr<duckdb::StorageExtensionInfo>,
                                                           duckdb::ClientContext &context,
                                                           duckdb::AttachedDatabase &db, const std::string &,
                                                           duckdb::AttachInfo &info, duckdb::AttachOptions &options) {
	MoraineCatalogHandle *handle = nullptr;
	MoraineError err {};
	// DuckDB resolves the attach's `READ_ONLY` flag into this access mode
	// (including through DuckLake's nested metadata attach); a read-only
	// catalog opens a `DbReader` and never fences the writer.
	bool read_only = options.access_mode == duckdb::AccessMode::READ_ONLY;
	// Options reach this attach through DuckLake's `META_` passthrough
	// (`META_ENCRYPTED true`, `META_FLUSH_INTERVAL_MS 5`, `META_CACHE_DIR '…'`),
	// or directly on a standalone `moraine:` attach. `ENCRYPTED` is
	// creation-time only: the ABI records it when a fresh store bootstraps and
	// ignores it afterward. `FLUSH_INTERVAL_MS` sets the WAL flush cadence; 0 on
	// the ABI means "not given", so an explicit zero is refused here.
	// `CACHE_DIR` is a local directory for SlateDB's on-disk block cache; it
	// must outlive the moraine_attach call, so it lives in this scope.
	bool encrypted = false;
	uint64_t flush_interval_ms = 0;
	std::string cache_dir;
	for (auto &option : info.options) {
		auto name = duckdb::StringUtil::Lower(option.first);
		if (name == "encrypted") {
			encrypted = option.second.GetValue<bool>();
		} else if (name == "flush_interval_ms") {
			flush_interval_ms = option.second.GetValue<uint64_t>();
			if (flush_interval_ms == 0) {
				throw duckdb::BinderException("FLUSH_INTERVAL_MS must be a positive number of milliseconds");
			}
		} else if (name == "cache_dir") {
			cache_dir = option.second.GetValue<std::string>();
		}
	}
	// For an s3:// store, resolve credentials from the matching DuckDB secret
	// (the same secret DuckLake/httpfs use for DATA_PATH); fields the secret
	// omits fall back to the AWS_* environment in the core. The backing strings
	// must outlive the moraine_attach call, so they live in this scope.
	MoraineS3Config s3 {};
	s3.use_ssl = -1;
	std::string key_id, secret, region, session_token, endpoint, url_style;
	bool is_s3 = duckdb::StringUtil::StartsWith(info.path, "s3://");
	if (is_s3) {
		auto &secret_manager = duckdb::SecretManager::Get(context);
		auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
		auto match = secret_manager.LookupSecret(transaction, info.path, "s3");
		if (match.HasMatch()) {
			auto &kv = dynamic_cast<const duckdb::KeyValueSecret &>(match.GetSecret());
			auto take = [&](const char *key, std::string &into, const char *&field) {
				duckdb::Value value;
				if (kv.TryGetValue(key, value) && !value.IsNull()) {
					into = value.ToString();
					field = into.c_str();
				}
			};
			take("key_id", key_id, s3.key_id);
			take("secret", secret, s3.secret);
			take("region", region, s3.region);
			take("session_token", session_token, s3.session_token);
			take("endpoint", endpoint, s3.endpoint);
			take("url_style", url_style, s3.url_style);
			duckdb::Value ssl;
			if (kv.TryGetValue("use_ssl", ssl) && !ssl.IsNull()) {
				s3.use_ssl = ssl.GetValue<bool>() ? 1 : 0;
			}
		}
	}
	auto code = moraine_attach(info.path.c_str(), is_s3 ? &s3 : nullptr, read_only, encrypted, flush_interval_ms,
	                           cache_dir.empty() ? nullptr : cache_dir.c_str(), &handle, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	return duckdb::make_uniq<MoraineCatalog>(db, handle, info.path);
}

void MoraineCatalog::Initialize(bool load_builtin) {
	// Nothing to load: content is fetched lazily per transaction from the listing ABI.
}

std::string MoraineCatalog::GetCatalogType() {
	return "moraine";
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineCatalog::CreateSchema(duckdb::CatalogTransaction transaction,
                                                                        duckdb::CreateSchemaInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a schema is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
MoraineCatalog::LookupSchema(duckdb::CatalogTransaction transaction, const duckdb::EntryLookupInfo &schema_lookup,
                             duckdb::OnEntryNotFound if_not_found) {
	if (!transaction.transaction) {
		throw duckdb::InternalException("moraine: schema lookup without an active transaction");
	}
	auto &tx = transaction.transaction->Cast<MoraineTransaction>();

	if (!tx.SchemasLoaded()) {
		OwnedArray<MoraineSchemaDesc> schema_descs(moraine_snapshot_schemas_free);
		MoraineError err {};
		auto code = moraine_snapshot_schemas(tx.Snapshot(), schema_descs.OutItems(), schema_descs.OutLen(), &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		for (auto &schema_desc : schema_descs) {
			duckdb::CreateSchemaInfo info;
			info.schema = schema_desc.name;
			tx.PutSchema(schema_desc.id,
			             duckdb::make_uniq<MoraineSchemaEntry>(*this, info, tx.Snapshot(), schema_desc.id));
		}
		tx.SetSchemasLoaded();
	}

	duckdb::optional_ptr<duckdb::SchemaCatalogEntry> found;
	tx.ForEachSchema([&](duckdb::SchemaCatalogEntry &entry) {
		if (!found && duckdb::StringUtil::CIEquals(entry.name, schema_lookup.GetEntryName())) {
			found = &entry;
		}
	});
	if (!found && if_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
		throw duckdb::CatalogException("schema \"%s\" does not exist", schema_lookup.GetEntryName());
	}
	return found;
}

void MoraineCatalog::ScanSchemas(duckdb::ClientContext &context,
                                 std::function<void(duckdb::SchemaCatalogEntry &)> callback) {
	// Force the schema cache to load via LookupSchema's load-then-search
	// path; RETURN_NULL means the not-found lookup itself throws nothing.
	LookupSchema(GetCatalogTransaction(context), duckdb::EntryLookupInfo(duckdb::CatalogType::SCHEMA_ENTRY, ""),
	             duckdb::OnEntryNotFound::RETURN_NULL);

	auto &tx = GetCatalogTransaction(context).transaction->Cast<MoraineTransaction>();
	tx.ForEachSchema(callback);
}

duckdb::PhysicalOperator &MoraineCatalog::PlanCreateTableAs(duckdb::ClientContext &, duckdb::PhysicalPlanGenerator &,
                                                            duckdb::LogicalCreateTable &, duckdb::PhysicalOperator &) {
	throw duckdb::NotImplementedException("moraine: CREATE TABLE AS is not supported (read-only catalog)");
}

duckdb::PhysicalOperator &MoraineCatalog::PlanInsert(duckdb::ClientContext &, duckdb::PhysicalPlanGenerator &planner,
                                                     duckdb::LogicalInsert &op,
                                                     duckdb::optional_ptr<duckdb::PhysicalOperator> plan) {
	// A writable ducklake_* metadata table (a MoraineMetadataTableEntry
	// whose spec names a moraine_tx_stage table_kind) or either dynamic
	// inline-table family accepts INSERT; every other table stays
	// read-only.
	if (auto *inline_data_table = dynamic_cast<MoraineInlineDataTableEntry *>(&op.table)) {
		auto &insert_op = PlanInlineDataInsert(planner, op, *inline_data_table);
		if (plan) {
			insert_op.children.push_back(*plan);
		}
		return insert_op;
	}
	if (auto *inline_delete_table = dynamic_cast<MoraineInlineDeleteTableEntry *>(&op.table)) {
		auto &insert_op = PlanInlineDeleteInsert(planner, op, *inline_delete_table);
		if (plan) {
			insert_op.children.push_back(*plan);
		}
		return insert_op;
	}
	auto *metadata_table = dynamic_cast<MoraineMetadataTableEntry *>(&op.table);
	if (metadata_table == nullptr) {
		throw duckdb::NotImplementedException("moraine: INSERT is not supported on \"%s\" (read-only catalog)",
		                                      op.table.name);
	}
	const auto &spec = metadata_table->Spec();
	if (spec.write_table_kind == kNotWritable) {
		throw duckdb::NotImplementedException("moraine: INSERT into \"%s\" is not supported this slice", spec.name);
	}
	auto &insert_op = PlanMetadataInsert(planner, op, spec);
	if (plan) {
		insert_op.children.push_back(*plan);
	}
	return insert_op;
}

duckdb::PhysicalOperator &MoraineCatalog::PlanDelete(duckdb::ClientContext &, duckdb::PhysicalPlanGenerator &planner,
                                                     duckdb::LogicalDelete &op, duckdb::PhysicalOperator &plan) {
	// Same target discipline as PlanInsert, but does not reject a
	// `kNotWritable` target outright: the expiry cascade issues DELETEs
	// against always-empty stand-ins unconditionally, which
	// PlanMetadataDelete plans as void-deletes (throwing only if a row
	// ever actually matches). Which DELETE forms translate for real is
	// decided in PlanMetadataDelete/PlanInlineDataDelete.
	if (auto *inline_data_table = dynamic_cast<MoraineInlineDataTableEntry *>(&op.table)) {
		auto &delete_op = PlanInlineDataDelete(planner, op, *inline_data_table);
		delete_op.children.push_back(plan);
		return delete_op;
	}
	auto *metadata_table = dynamic_cast<MoraineMetadataTableEntry *>(&op.table);
	if (metadata_table == nullptr) {
		throw duckdb::NotImplementedException("moraine: DELETE is not supported on \"%s\"", op.table.name);
	}
	auto &delete_op = PlanMetadataDelete(planner, op, metadata_table->Spec());
	delete_op.children.push_back(plan);
	return delete_op;
}

duckdb::PhysicalOperator &MoraineCatalog::PlanUpdate(duckdb::ClientContext &, duckdb::PhysicalPlanGenerator &planner,
                                                     duckdb::LogicalUpdate &op, duckdb::PhysicalOperator &plan) {
	// Same target discipline as PlanInsert, but does not reject a
	// `kNotWritable` target outright: DuckLake's DROP/RENAME batch issues `SET
	// end_snapshot` against unmodeled (always-empty) tables, which
	// PlanMetadataUpdate translates as a no-op. Every other UPDATE shape
	// against a `kNotWritable` table still throws, from within
	// PlanMetadataUpdate itself.
	if (auto *inline_data_table = dynamic_cast<MoraineInlineDataTableEntry *>(&op.table)) {
		auto &update_op = PlanInlineDataUpdate(planner, op, *inline_data_table);
		update_op.children.push_back(plan);
		return update_op;
	}
	auto *metadata_table = dynamic_cast<MoraineMetadataTableEntry *>(&op.table);
	if (metadata_table == nullptr) {
		throw duckdb::NotImplementedException("moraine: UPDATE is not supported on \"%s\"", op.table.name);
	}
	auto &update_op = PlanMetadataUpdate(planner, op, metadata_table->Spec());
	update_op.children.push_back(plan);
	return update_op;
}

duckdb::DatabaseSize MoraineCatalog::GetDatabaseSize(duckdb::ClientContext &) {
	return duckdb::DatabaseSize();
}

bool MoraineCatalog::InMemory() {
	return false;
}

std::string MoraineCatalog::GetDBPath() {
	return path_;
}

void MoraineCatalog::OnDetach(duckdb::ClientContext &context) {
	// Deliberately empty: freeing handle_ here would race a concurrent
	// StartTransaction reading it via Handle(); only the destructor frees it.
}

void MoraineCatalog::DropSchema(duckdb::ClientContext &, duckdb::DropInfo &) {
	throw duckdb::NotImplementedException("moraine: dropping a schema is not supported (read-only catalog)");
}

} // namespace moraine_duckdb
