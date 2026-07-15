#include "transaction_manager.hpp"

#include "catalog.hpp"

#include <mutex>
#include <unordered_map>
#include <vector>

namespace moraine_duckdb {

MoraineTransaction::MoraineTransaction(duckdb::TransactionManager &manager, duckdb::ClientContext &context,
                                       MoraineSnapshotHandle *snapshot, MoraineCatalogHandle *catalog_handle)
    : duckdb::Transaction(manager, context), snapshot_(snapshot), catalog_handle_(catalog_handle) {
}

MoraineTransaction::~MoraineTransaction() {
	// Defensive fallback: normal teardown releases the snapshot via
	// CommitTransaction/RollbackTransaction's call to ReleaseSnapshot, and
	// the staged tx via TakeStagedTx (commit) or a direct rollback
	// (RollbackTransaction) — see both below.
	if (snapshot_ != nullptr) {
		moraine_snapshot_free(snapshot_);
		snapshot_ = nullptr;
	}
	if (staged_tx_ != nullptr) {
		moraine_tx_rollback(staged_tx_);
		staged_tx_ = nullptr;
	}
}

MoraineTxHandle *MoraineTransaction::StagedTx() {
	if (staged_tx_ == nullptr) {
		MoraineTxHandle *tx = nullptr;
		MoraineError err {};
		// The owning client context, when still alive, makes the head read
		// cancellable; a gone context degrades to a non-cancellable call.
		auto client = context.lock();
		auto code =
		    moraine_tx_begin(catalog_handle_, &tx, client ? moraine_shim_is_interrupted : nullptr, client.get(), &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		staged_tx_ = tx;
	}
	return staged_tx_;
}

MoraineTxHandle *MoraineTransaction::TakeStagedTx() {
	auto *result = staged_tx_;
	staged_tx_ = nullptr;
	return result;
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
	MoraineError err {};
	auto code = moraine_snapshot(catalog_.Handle(), &snapshot, moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	auto transaction = duckdb::make_uniq<MoraineTransaction>(*this, context, snapshot, catalog_.Handle());
	auto &transaction_ref = *transaction;
	std::lock_guard<std::mutex> guard(lock_);
	active_transactions_.push_back(std::move(transaction));
	return transaction_ref;
}

duckdb::ErrorData MoraineTransactionManager::CommitTransaction(duckdb::ClientContext &,
                                                               duckdb::Transaction &transaction) {
	// A staged tx (opened lazily by the first write this DuckDB
	// transaction made) is taken out from under the lock and committed
	// after releasing it — moraine_tx_commit blocks on store I/O, which
	// must not happen while holding `lock_` and blocking every other
	// transaction on this catalog.
	MoraineTxHandle *staged = nullptr;
	{
		std::lock_guard<std::mutex> guard(lock_);
		for (auto it = active_transactions_.begin(); it != active_transactions_.end(); ++it) {
			if (it->get() == &transaction) {
				auto &moraine_tx = it->get()->Cast<MoraineTransaction>();
				staged = moraine_tx.TakeStagedTx();
				moraine_tx.ReleaseSnapshot();
				active_transactions_.erase(it);
				break;
			}
		}
	}
	if (staged == nullptr) {
		// A read-only transaction (or one whose writes never reached a
		// writable metadata table) never opened a staged tx: nothing to
		// commit, always a clean success.
		return duckdb::ErrorData();
	}
	uint64_t new_snapshot_id = 0;
	MoraineError err {};
	auto code = moraine_tx_commit(staged, &new_snapshot_id, &err);
	if (code != MORAINE_OK) {
		try {
			ThrowMoraineError(err);
		} catch (std::exception &ex) {
			return duckdb::ErrorData(ex);
		}
	}
	return duckdb::ErrorData();
}

void MoraineTransactionManager::RollbackTransaction(duckdb::Transaction &transaction) {
	MoraineTxHandle *staged = nullptr;
	{
		std::lock_guard<std::mutex> guard(lock_);
		for (auto it = active_transactions_.begin(); it != active_transactions_.end(); ++it) {
			if (it->get() == &transaction) {
				auto &moraine_tx = it->get()->Cast<MoraineTransaction>();
				staged = moraine_tx.TakeStagedTx();
				moraine_tx.ReleaseSnapshot();
				active_transactions_.erase(it);
				break;
			}
		}
	}
	if (staged != nullptr) {
		moraine_tx_rollback(staged);
	}
}

void MoraineTransactionManager::Checkpoint(duckdb::ClientContext &, bool) {
	// Nothing to checkpoint: this slice makes no writes.
}

} // namespace moraine_duckdb
