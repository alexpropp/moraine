// Registers moraine as a DuckDB attach type: `ATTACH '<path>' AS m (TYPE
// moraine)`. Wires the two function pointers `duckdb::StorageExtension`
// needs to MoraineCatalog and MoraineTransactionManager's static factories.
#include "catalog.hpp"
#include "transaction_manager.hpp"

namespace moraine_duckdb {

void RegisterMoraineStorageExtension(duckdb::DBConfig &config) {
	auto extension = duckdb::make_shared_ptr<duckdb::StorageExtension>();
	extension->attach = MoraineCatalog::Attach;
	extension->create_transaction_manager = MoraineTransactionManager::Create;
	duckdb::StorageExtension::Register(config, "moraine", std::move(extension));
}

} // namespace moraine_duckdb
