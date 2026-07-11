#include "transaction_manager.hpp"

#include "catalog.hpp"

#include <mutex>
#include <unordered_map>
#include <vector>

namespace moraine_duckdb {

MoraineTransaction::MoraineTransaction(duckdb::TransactionManager &manager, duckdb::ClientContext &context,
                                       MoraineSnapshotHandle *snapshot)
    : duckdb::Transaction(manager, context), snapshot_(snapshot) {
}

MoraineTransaction::~MoraineTransaction() {
	// Defensive fallback: normal teardown releases the snapshot via
	// CommitTransaction/RollbackTransaction's call to ReleaseSnapshot.
	if (snapshot_ != nullptr) {
		moraine_snapshot_free(snapshot_);
		snapshot_ = nullptr;
	}
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> MoraineTransaction::GetCachedSchema(uint64_t schema_id) const {
	auto it = schema_cache_.find(schema_id);
	if (it == schema_cache_.end()) {
		return nullptr;
	}
	return it->second.get();
}

void MoraineTransaction::PutSchema(uint64_t schema_id, duckdb::unique_ptr<duckdb::SchemaCatalogEntry> entry) {
	schema_cache_[schema_id] = std::move(entry);
}

void MoraineTransaction::ForEachSchema(const std::function<void(duckdb::SchemaCatalogEntry &)> &callback) const {
	for (auto &entry : schema_cache_) {
		callback(*entry.second);
	}
}

void MoraineTransaction::ReleaseSnapshot() {
	if (snapshot_ != nullptr) {
		moraine_snapshot_free(snapshot_);
		snapshot_ = nullptr;
	}
}

MoraineTransactionManager::MoraineTransactionManager(duckdb::AttachedDatabase &db, MoraineCatalog &catalog)
    : duckdb::TransactionManager(db), catalog_(catalog) {
}

duckdb::unique_ptr<duckdb::TransactionManager>
MoraineTransactionManager::Create(duckdb::optional_ptr<duckdb::StorageExtensionInfo>, duckdb::AttachedDatabase &db,
                                  duckdb::Catalog &catalog) {
	return duckdb::make_uniq<MoraineTransactionManager>(db, catalog.Cast<MoraineCatalog>());
}

duckdb::Transaction &MoraineTransactionManager::StartTransaction(duckdb::ClientContext &context) {
	MoraineSnapshotHandle *snapshot = nullptr;
	MoraineError err{};
	auto code = moraine_snapshot(catalog_.Handle(), &snapshot, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	auto transaction = duckdb::make_uniq<MoraineTransaction>(*this, context, snapshot);
	auto &transaction_ref = *transaction;
	std::lock_guard<std::mutex> guard(lock_);
	active_transactions_.push_back(std::move(transaction));
	return transaction_ref;
}

duckdb::ErrorData MoraineTransactionManager::CommitTransaction(duckdb::ClientContext &, duckdb::Transaction &transaction) {
	std::lock_guard<std::mutex> guard(lock_);
	for (auto it = active_transactions_.begin(); it != active_transactions_.end(); ++it) {
		if (it->get() == &transaction) {
			it->get()->Cast<MoraineTransaction>().ReleaseSnapshot();
			active_transactions_.erase(it);
			break;
		}
	}
	// Read-only this slice: there is never anything to actually commit, so
	// this is always a clean success.
	return duckdb::ErrorData();
}

void MoraineTransactionManager::RollbackTransaction(duckdb::Transaction &transaction) {
	std::lock_guard<std::mutex> guard(lock_);
	for (auto it = active_transactions_.begin(); it != active_transactions_.end(); ++it) {
		if (it->get() == &transaction) {
			it->get()->Cast<MoraineTransaction>().ReleaseSnapshot();
			active_transactions_.erase(it);
			break;
		}
	}
}

void MoraineTransactionManager::Checkpoint(duckdb::ClientContext &, bool) {
	// Nothing to checkpoint: this slice makes no writes.
}

} // namespace moraine_duckdb
