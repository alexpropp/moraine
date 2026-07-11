#include "catalog.hpp"

#include "scan.hpp"
#include "transaction_manager.hpp"

#include <cctype>
#include <cstdlib>

namespace moraine_duckdb {

namespace {

// RAII wrapper for a caller-owned array from a moraine_snapshot_*_free-paired
// listing call; frees via the matching free function on destruction.
template <typename T>
class OwnedArray {
public:
	OwnedArray(void (*free_fn)(T *, size_t)) : items_(nullptr), len_(0), free_fn_(free_fn) {
	}
	~OwnedArray() {
		free_fn_(items_, len_);
	}
	OwnedArray(const OwnedArray &) = delete;
	OwnedArray &operator=(const OwnedArray &) = delete;

	T **OutItems() {
		return &items_;
	}
	size_t *OutLen() {
		return &len_;
	}
	T *begin() const {
		return items_;
	}
	T *end() const {
		return items_ + len_;
	}
	size_t size() const {
		return len_;
	}

private:
	T *items_;
	size_t len_;
	void (*free_fn_)(T *, size_t);
};

std::string ToUpperAscii(const std::string &s) {
	std::string result = s;
	for (auto &c : result) {
		c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
	}
	return result;
}

// Joins a directory-ish `base` with a `child` path segment without
// duplicating a trailing slash on `base`.
std::string JoinPath(const std::string &base, const std::string &child) {
	if (base.empty()) {
		return child;
	}
	if (base.back() == '/') {
		return base + child;
	}
	return base + "/" + child;
}

} // namespace

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

	if (upper == "BIGINT") {
		return duckdb::LogicalType::BIGINT;
	} else if (upper == "INTEGER") {
		return duckdb::LogicalType::INTEGER;
	} else if (upper == "SMALLINT") {
		return duckdb::LogicalType::SMALLINT;
	} else if (upper == "TINYINT") {
		return duckdb::LogicalType::TINYINT;
	} else if (upper == "UBIGINT") {
		return duckdb::LogicalType::UBIGINT;
	} else if (upper == "UINTEGER") {
		return duckdb::LogicalType::UINTEGER;
	} else if (upper == "USMALLINT") {
		return duckdb::LogicalType::USMALLINT;
	} else if (upper == "UTINYINT") {
		return duckdb::LogicalType::UTINYINT;
	} else if (upper == "DOUBLE") {
		return duckdb::LogicalType::DOUBLE;
	} else if (upper == "FLOAT") {
		return duckdb::LogicalType::FLOAT;
	} else if (upper == "REAL") {
		return duckdb::LogicalType::FLOAT;
	} else if (upper == "VARCHAR") {
		return duckdb::LogicalType::VARCHAR;
	} else if (upper == "TEXT") {
		return duckdb::LogicalType::VARCHAR;
	} else if (upper == "BOOLEAN") {
		return duckdb::LogicalType::BOOLEAN;
	} else if (upper == "DATE") {
		return duckdb::LogicalType::DATE;
	} else if (upper == "TIMESTAMP") {
		return duckdb::LogicalType::TIMESTAMP;
	} else if (upper == "TIMESTAMPTZ" || upper == "TIMESTAMP WITH TIME ZONE") {
		return duckdb::LogicalType::TIMESTAMP_TZ;
	} else if (upper == "TIME") {
		return duckdb::LogicalType::TIME;
	} else if (upper == "BLOB") {
		return duckdb::LogicalType::BLOB;
	} else if (upper == "UUID") {
		return duckdb::LogicalType::UUID;
	} else if (upper == "HUGEINT") {
		return duckdb::LogicalType::HUGEINT;
	}

	throw duckdb::NotImplementedException("moraine: unsupported DuckLake column type \"%s\"", ducklake_type);
}

MoraineTableEntry::MoraineTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                                     duckdb::CreateTableInfo &info, MoraineSnapshotHandle *snapshot,
                                     uint64_t table_id)
    : duckdb::TableCatalogEntry(catalog, schema, info), snapshot_(snapshot), table_id_(table_id) {
}

duckdb::unique_ptr<duckdb::BaseStatistics> MoraineTableEntry::GetStatistics(duckdb::ClientContext &context,
                                                                            duckdb::column_t column_id) {
	throw duckdb::NotImplementedException("moraine: column statistics are not supported yet");
}

duckdb::TableFunction MoraineTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                                          duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	OwnedArray<MoraineDataFileDesc> file_descs(moraine_snapshot_data_files_of_free);
	MoraineError err{};
	auto code =
	    moraine_snapshot_data_files_of(snapshot_, table_id_, file_descs.OutItems(), file_descs.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	// A relative data file path resolves against
	// `<attach_path>/<schema_name>/<table_name>/`; an absolute path passes
	// through unchanged.
	std::string table_dir = JoinPath(JoinPath(ParentCatalog().GetDBPath(), ParentSchema().name), name);

	auto scan_bind_data = duckdb::make_uniq<MoraineScanBindData>();
	scan_bind_data->database = &ParentCatalog().GetDatabase();
	// Feeds the scan's column-count guard and its error message.
	scan_bind_data->catalog_column_count = GetColumns().LogicalColumnCount();
	scan_bind_data->table_name = ParentSchema().name + "." + name;
	scan_bind_data->table_entry = this;
	for (auto &file_desc : file_descs) {
		std::string path(file_desc.path);
		scan_bind_data->file_paths.push_back(file_desc.path_is_relative ? JoinPath(table_dir, path) : path);
	}
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
	// `query` is always null (no SQL parser vendored this slice); the base
	// implementation dereferences it, so this throws instead of crashing.
	throw duckdb::NotImplementedException("moraine: querying a view's definition is not supported yet");
}

void MoraineViewEntry::BindView(duckdb::ClientContext &context, duckdb::BindViewAction action) {
	throw duckdb::NotImplementedException("moraine: binding a view's definition is not supported yet");
}

std::string MoraineViewEntry::ToSQL() const {
	// The base implementation stringifies the parsed `query`, which is null
	// here (no SQL parser this slice); compose the definition textually
	// from the listing ABI's strings instead.
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
	MoraineError err{};
	auto code =
	    moraine_snapshot_tables_in(snapshot_, schema_id_, table_descs.OutItems(), table_descs.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	for (auto &table_desc : table_descs) {
		OwnedArray<MoraineColumnDesc> column_descs(moraine_snapshot_columns_of_free);
		MoraineError column_err{};
		auto column_code = moraine_snapshot_columns_of(snapshot_, table_desc.id, column_descs.OutItems(),
		                                                column_descs.OutLen(), &column_err);
		if (column_code != MORAINE_OK) {
			ThrowMoraineError(column_err);
		}

		duckdb::CreateTableInfo info(*this, table_desc.name);
		duckdb::idx_t column_index = 0;
		for (auto &column_desc : column_descs) {
			auto type = MapColumnType(column_desc.sql_type);
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

	// Set only after full success, so a mid-load exception leaves the next
	// call to retry rather than serve a partial table set.
	tables_loaded_ = true;
}

void MoraineSchemaEntry::EnsureViewsLoaded() {
	if (views_loaded_) {
		return;
	}

	OwnedArray<MoraineViewDesc> view_descs(moraine_snapshot_views_in_free);
	MoraineError err{};
	auto code = moraine_snapshot_views_in(snapshot_, schema_id_, view_descs.OutItems(), view_descs.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	for (auto &view_desc : view_descs) {
		duckdb::CreateViewInfo info(*this, view_desc.name);
		info.sql = view_desc.sql;
		// `info.query` is left null: no SQL parser is vendored this slice.
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

duckdb::optional_ptr<duckdb::CatalogEntry>
MoraineSchemaEntry::LookupEntry(duckdb::CatalogTransaction transaction, const duckdb::EntryLookupInfo &lookup_info) {
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
MoraineSchemaEntry::CreateTableFunction(duckdb::CatalogTransaction transaction,
                                        duckdb::CreateTableFunctionInfo &info) {
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

duckdb::optional_ptr<duckdb::CatalogEntry>
MoraineSchemaEntry::CreateCollation(duckdb::CatalogTransaction transaction, duckdb::CreateCollationInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a collation is not supported (read-only catalog)");
}

duckdb::optional_ptr<duckdb::CatalogEntry> MoraineSchemaEntry::CreateType(duckdb::CatalogTransaction transaction,
                                                                         duckdb::CreateTypeInfo &info) {
	throw duckdb::NotImplementedException("moraine: creating a type is not supported (read-only catalog)");
}

void MoraineSchemaEntry::DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info) {
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
                                                           duckdb::ClientContext &, duckdb::AttachedDatabase &db,
                                                           const std::string &, duckdb::AttachInfo &info,
                                                           duckdb::AttachOptions &) {
	MoraineCatalogHandle *handle = nullptr;
	MoraineError err{};
	auto code = moraine_attach(info.path.c_str(), nullptr, &handle, &err);
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
	auto &txn = transaction.transaction->Cast<MoraineTransaction>();

	if (!txn.SchemasLoaded()) {
		OwnedArray<MoraineSchemaDesc> schema_descs(moraine_snapshot_schemas_free);
		MoraineError err{};
		auto code = moraine_snapshot_schemas(txn.Snapshot(), schema_descs.OutItems(), schema_descs.OutLen(), &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		for (auto &schema_desc : schema_descs) {
			duckdb::CreateSchemaInfo info;
			info.schema = schema_desc.name;
			txn.PutSchema(schema_desc.id,
			              duckdb::make_uniq<MoraineSchemaEntry>(*this, info, txn.Snapshot(), schema_desc.id));
		}
		txn.SetSchemasLoaded();
	}

	duckdb::optional_ptr<duckdb::SchemaCatalogEntry> found;
	txn.ForEachSchema([&](duckdb::SchemaCatalogEntry &entry) {
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

	auto &txn = GetCatalogTransaction(context).transaction->Cast<MoraineTransaction>();
	txn.ForEachSchema(callback);
}

duckdb::PhysicalOperator &MoraineCatalog::PlanCreateTableAs(duckdb::ClientContext &, duckdb::PhysicalPlanGenerator &,
                                                            duckdb::LogicalCreateTable &, duckdb::PhysicalOperator &) {
	throw duckdb::NotImplementedException("moraine: CREATE TABLE AS is not supported (read-only catalog)");
}

duckdb::PhysicalOperator &MoraineCatalog::PlanInsert(duckdb::ClientContext &, duckdb::PhysicalPlanGenerator &,
                                                     duckdb::LogicalInsert &,
                                                     duckdb::optional_ptr<duckdb::PhysicalOperator>) {
	throw duckdb::NotImplementedException("moraine: INSERT is not supported (read-only catalog)");
}

duckdb::PhysicalOperator &MoraineCatalog::PlanDelete(duckdb::ClientContext &, duckdb::PhysicalPlanGenerator &,
                                                     duckdb::LogicalDelete &, duckdb::PhysicalOperator &) {
	throw duckdb::NotImplementedException("moraine: DELETE is not supported (read-only catalog)");
}

duckdb::PhysicalOperator &MoraineCatalog::PlanUpdate(duckdb::ClientContext &, duckdb::PhysicalPlanGenerator &,
                                                     duckdb::LogicalUpdate &, duckdb::PhysicalOperator &) {
	throw duckdb::NotImplementedException("moraine: UPDATE is not supported (read-only catalog)");
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
