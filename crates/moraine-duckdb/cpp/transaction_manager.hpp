// A duckdb::TransactionManager backed by moraine: snapshot-per-transaction,
// read-only. StartTransaction materializes one moraine_snapshot and hands
// out a MoraineTransaction that owns it; CommitTransaction and
// RollbackTransaction both just free the snapshot, since this slice makes
// no writes.
#pragma once

#include "duckdb.hpp"

// Amalgamation gap vendored by hand alongside duckdb.hpp: defines
// duckdb::StorageExtensionInfo, named by MoraineTransactionManager::Create's
// signature.
#include "storage_extension.hpp"

#include "moraine_abi.h"

namespace moraine_duckdb {

class MoraineCatalog;

// One DuckDB transaction's view of a moraine catalog: the snapshot
// materialized at StartTransaction, plus the schema-entry cache built
// lazily against it. Cached CatalogEntry objects must outlive every
// reference to them returned within the transaction.
class MoraineTransaction : public duckdb::Transaction {
public:
	MoraineTransaction(duckdb::TransactionManager &manager, duckdb::ClientContext &context,
	                    MoraineSnapshotHandle *snapshot);
	~MoraineTransaction() override;

	MoraineSnapshotHandle *Snapshot() const {
		return snapshot_;
	}

	bool SchemasLoaded() const {
		return schemas_loaded_;
	}
	void SetSchemasLoaded() {
		schemas_loaded_ = true;
	}
	duckdb::optional_ptr<duckdb::SchemaCatalogEntry> GetCachedSchema(uint64_t schema_id) const;
	void PutSchema(uint64_t schema_id, duckdb::unique_ptr<duckdb::SchemaCatalogEntry> entry);
	void ForEachSchema(const std::function<void(duckdb::SchemaCatalogEntry &)> &callback) const;

	// Frees the snapshot and marks it released, so the destructor's
	// defensive free becomes a no-op.
	void ReleaseSnapshot();

private:
	MoraineSnapshotHandle *snapshot_;
	bool schemas_loaded_ = false;
	std::unordered_map<uint64_t, duckdb::unique_ptr<duckdb::SchemaCatalogEntry>> schema_cache_;
};

class MoraineTransactionManager : public duckdb::TransactionManager {
public:
	MoraineTransactionManager(duckdb::AttachedDatabase &db, MoraineCatalog &catalog);

	// The create_transaction_manager_t the storage extension registers.
	static duckdb::unique_ptr<duckdb::TransactionManager>
	Create(duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info, duckdb::AttachedDatabase &db,
	       duckdb::Catalog &catalog);

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;
	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override;
	void RollbackTransaction(duckdb::Transaction &transaction) override;
	void Checkpoint(duckdb::ClientContext &context, bool force = false) override;

private:
	MoraineCatalog &catalog_;
	std::mutex lock_;
	// Owns every started transaction until it is committed or rolled back,
	// at which point it's erased (and its snapshot freed) here.
	std::vector<duckdb::unique_ptr<duckdb::Transaction>> active_transactions_;
};

} // namespace moraine_duckdb
