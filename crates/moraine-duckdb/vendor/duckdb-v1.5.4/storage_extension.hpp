// Vendored by hand from
// https://raw.githubusercontent.com/duckdb/duckdb/v1.5.4/src/include/duckdb/storage/storage_extension.hpp
// (git tag v1.5.4, commit 08e34c447b). The amalgamated `duckdb.hpp` forward-
// declares `StorageExtension` (twice) but never pulls in its class body —
// the amalgamation process only concatenates headers actually reached by
// `duckdb.hpp`'s own top-level includes, and `storage_extension.hpp` isn't
// one of them. Its own `#include "duckdb/storage/storage_manager.hpp"` is
// dropped here: every type this header actually names (`Catalog`,
// `TransactionManager`, `AttachedDatabase`, `AttachInfo`, `AttachOptions`,
// `ClientContext`, `DBConfig`, `CheckpointOptions`) is already fully defined
// in `duckdb.hpp`, so the class body below is self-contained against it.
// Must be included after `duckdb.hpp`.
#pragma once

namespace duckdb {
class AttachedDatabase;
struct AttachInfo;
class Catalog;
class TransactionManager;

//! The StorageExtensionInfo holds static information relevant to the storage extension
struct StorageExtensionInfo {
	virtual ~StorageExtensionInfo() {
	}
};

typedef unique_ptr<Catalog> (*attach_function_t)(optional_ptr<StorageExtensionInfo> storage_info,
                                                 ClientContext &context, AttachedDatabase &db, const string &name,
                                                 AttachInfo &info, AttachOptions &options);
typedef unique_ptr<TransactionManager> (*create_transaction_manager_t)(optional_ptr<StorageExtensionInfo> storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog);

class StorageExtension {
public:
	attach_function_t attach;
	create_transaction_manager_t create_transaction_manager;

	//! Additional info passed to the various storage functions
	shared_ptr<StorageExtensionInfo> storage_info;

	virtual ~StorageExtension() {
	}

	virtual void OnCheckpointStart(AttachedDatabase &db, CheckpointOptions checkpoint_options) {
	}

	virtual void OnCheckpointEnd(AttachedDatabase &db, CheckpointOptions checkpoint_options) {
	}

	static optional_ptr<StorageExtension> Find(const DBConfig &config, const string &extension_name);
	static void Register(DBConfig &config, const string &extension_name, shared_ptr<StorageExtension> extension);
};

struct OpenFileStorageExtension {
	static shared_ptr<StorageExtension> Create();
};

} // namespace duckdb
