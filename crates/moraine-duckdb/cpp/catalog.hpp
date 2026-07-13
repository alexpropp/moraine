// A duckdb::Catalog backed by moraine over the C ABI in moraine_abi.h.
// Translate-only: every callback turns a listing-ABI call into a DuckDB
// catalog entry, or (every write path, out of scope this slice) throws
// duckdb::NotImplementedException.
#pragma once

#include "duckdb.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "moraine_abi.h"

namespace moraine_duckdb {

// Maps a DuckLake column-type string (e.g. "BIGINT", "DECIMAL(18,3)") to a
// DuckDB LogicalType. Scalar types only this slice; an unrecognized or
// nested type throws duckdb::NotImplementedException naming the type
// string verbatim.
duckdb::LogicalType MapColumnType(const std::string &ducklake_type);

// Translates a MoraineError into the matching DuckDB exception (NotFound/
// AlreadyExists/Constraint -> CatalogException, CommitConflict ->
// TransactionException, Corruption/Store/internal -> IOException/
// InternalException) and throws it. Frees `err.message` first if non-null.
[[noreturn]] void ThrowMoraineError(MoraineError &err);

// A moraine-backed table entry. Column/schema translation happens in
// MoraineSchemaEntry; this class only supplies the pure virtuals
// TableCatalogEntry still needs. The scan function binds normally (so
// DESCRIBE/EXPLAIN work) but always redirects to the DuckLake attach at
// execution time (see scan.hpp).
class MoraineTableEntry : public duckdb::TableCatalogEntry {
public:
	MoraineTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateTableInfo &info,
	                   MoraineSnapshotHandle *snapshot, uint64_t table_id);

	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                          duckdb::column_t column_id) override;
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                       duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

private:
	MoraineSnapshotHandle *snapshot_;
	uint64_t table_id_;
};

// A moraine-backed view entry. Cataloging (name/schema lookup, `DESCRIBE`)
// works; binding the defining query is deferred and throws
// duckdb::NotImplementedException instead of dereferencing a null query,
// which the base class would otherwise do.
class MoraineViewEntry : public duckdb::ViewCatalogEntry {
public:
	MoraineViewEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateViewInfo &info);

	const duckdb::SelectStatement &GetQuery() override;
	void BindView(duckdb::ClientContext &context, duckdb::BindViewAction action) override;
	std::string ToSQL() const override;
};

// A moraine-backed schema entry: table/view lookup and enumeration
// translate directly to the listing ABI over the snapshot captured at
// construction (one snapshot per DuckDB transaction). Every write
// callback throws duckdb::NotImplementedException.
class MoraineSchemaEntry : public duckdb::SchemaCatalogEntry {
public:
	MoraineSchemaEntry(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info, MoraineSnapshotHandle *snapshot,
	                    uint64_t schema_id);

	void Scan(duckdb::ClientContext &context, duckdb::CatalogType type,
	          const std::function<void(duckdb::CatalogEntry &)> &callback) override;
	void Scan(duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback) override;

	duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntry(duckdb::CatalogTransaction transaction,
	                                                        const duckdb::EntryLookupInfo &lookup_info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry> CreateIndex(duckdb::CatalogTransaction transaction,
	                                                        duckdb::CreateIndexInfo &info,
	                                                        duckdb::TableCatalogEntry &table) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateFunction(duckdb::CatalogTransaction transaction,
	                                                           duckdb::CreateFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateTable(duckdb::CatalogTransaction transaction,
	                                                        duckdb::BoundCreateTableInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateView(duckdb::CatalogTransaction transaction,
	                                                       duckdb::CreateViewInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateSequence(duckdb::CatalogTransaction transaction,
	                                                           duckdb::CreateSequenceInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateTableFunction(duckdb::CatalogTransaction transaction,
	                                                                duckdb::CreateTableFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateCopyFunction(duckdb::CatalogTransaction transaction,
	                                                               duckdb::CreateCopyFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreatePragmaFunction(duckdb::CatalogTransaction transaction,
	                                                                 duckdb::CreatePragmaFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateCollation(duckdb::CatalogTransaction transaction,
	                                                            duckdb::CreateCollationInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateType(duckdb::CatalogTransaction transaction,
	                                                       duckdb::CreateTypeInfo &info) override;
	void DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info) override;
	void Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo &info) override;

private:
	MoraineSnapshotHandle *snapshot_;
	uint64_t schema_id_;
	bool tables_loaded_ = false;
	bool views_loaded_ = false;
	// Keyed by name (DuckDB catalog lookups are case-insensitive); built
	// lazily and cached for this schema entry's lifetime.
	duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> tables_;
	duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> views_;

	void EnsureTablesLoaded();
	void EnsureViewsLoaded();
};

// A moraine-backed Catalog: ATTACH opens the store via moraine_attach;
// DETACH (or database shutdown) closes it via moraine_detach. Schema
// lookup/enumeration delegate to the active transaction's cached
// MoraineSchemaEntry set; every write path throws
// duckdb::NotImplementedException.
class MoraineCatalog : public duckdb::Catalog {
public:
	MoraineCatalog(duckdb::AttachedDatabase &db, MoraineCatalogHandle *handle, std::string path);
	~MoraineCatalog() override;

	// The attach_function_t the storage extension registers.
	static duckdb::unique_ptr<duckdb::Catalog> Attach(duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
	                                                   duckdb::ClientContext &context, duckdb::AttachedDatabase &db,
	                                                   const std::string &name, duckdb::AttachInfo &info,
	                                                   duckdb::AttachOptions &options);

	void Initialize(bool load_builtin) override;
	std::string GetCatalogType() override;

	duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(duckdb::CatalogTransaction transaction,
	                                                         duckdb::CreateSchemaInfo &info) override;
	duckdb::optional_ptr<duckdb::SchemaCatalogEntry> LookupSchema(duckdb::CatalogTransaction transaction,
	                                                               const duckdb::EntryLookupInfo &schema_lookup,
	                                                               duckdb::OnEntryNotFound if_not_found) override;
	void ScanSchemas(duckdb::ClientContext &context,
	                  std::function<void(duckdb::SchemaCatalogEntry &)> callback) override;

	duckdb::PhysicalOperator &PlanCreateTableAs(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                             duckdb::LogicalCreateTable &op,
	                                             duckdb::PhysicalOperator &plan) override;
	duckdb::PhysicalOperator &PlanInsert(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                      duckdb::LogicalInsert &op,
	                                      duckdb::optional_ptr<duckdb::PhysicalOperator> plan) override;
	duckdb::PhysicalOperator &PlanDelete(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                      duckdb::LogicalDelete &op, duckdb::PhysicalOperator &plan) override;
	duckdb::PhysicalOperator &PlanUpdate(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                      duckdb::LogicalUpdate &op, duckdb::PhysicalOperator &plan) override;

	duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext &context) override;
	bool InMemory() override;
	std::string GetDBPath() override;

	void OnDetach(duckdb::ClientContext &context) override;

	// Private pure virtual in duckdb::Catalog itself; a derived class's
	// access specifier for an override is independent of the base's.
	void DropSchema(duckdb::ClientContext &context, duckdb::DropInfo &info) override;

	MoraineCatalogHandle *Handle() const {
		return handle_;
	}

private:
	MoraineCatalogHandle *handle_;
	std::string path_;

	// Ensures the active transaction's schema cache is populated from the
	// listing ABI, then returns it.
	static duckdb::vector<duckdb::reference<duckdb::SchemaCatalogEntry>> LoadedSchemas(duckdb::Catalog &catalog,
	                                                                                    duckdb::Transaction &txn);
};

} // namespace moraine_duckdb
