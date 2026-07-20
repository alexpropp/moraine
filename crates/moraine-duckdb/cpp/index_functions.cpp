// The equality-index SQL surface: moraine_index_create / moraine_index_drop
// (autonomous-commit DDL) and moraine_indexes (introspection). These are the
// first user-callable functions this extension registers; every other
// TableFunction in the shim is reached through a catalog-entry override, not
// a global registration.
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"
#include "owned_array.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace moraine_duckdb {

namespace {

// Resolves a moraine catalog handle from `catalog_name`, accepting either the
// DuckLake lake name (its metadata catalog is `__ducklake_metadata_<lake>`,
// the default naming) or moraine's metadata catalog directly. The downcast is
// unchecked, so the catalog kind is verified first — a non-moraine name errors
// rather than crashing.
MoraineCatalogHandle *ResolveHandle(duckdb::ClientContext &context, const std::string &catalog_name) {
	auto as_moraine = [&](const std::string &name) -> MoraineCatalogHandle * {
		auto catalog = duckdb::Catalog::GetCatalogEntry(context, name);
		if (catalog && catalog->GetCatalogType() == "moraine") {
			return catalog->Cast<MoraineCatalog>().Handle();
		}
		return nullptr;
	};
	if (auto *handle = as_moraine(catalog_name)) {
		return handle;
	}
	if (auto *handle = as_moraine("__ducklake_metadata_" + catalog_name)) {
		return handle;
	}
	throw duckdb::InvalidInputException(
	    "\"%s\" is not a moraine-backed lake; pass the DuckLake lake name, or moraine's "
	    "\"__ducklake_metadata_<lake>\" metadata catalog",
	    catalog_name);
}

// ---- moraine_indexes: list a table's live equality indexes ----

struct IndexesBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	struct Row {
		int64_t index_id;
		std::string name;
		bool unique;
		bool building;
	};
	std::vector<Row> rows;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<IndexesBindData>();
		result->catalog_name = catalog_name;
		result->schema_name = schema_name;
		result->table_name = table_name;
		result->rows = rows;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexesBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> IndexesBind(duckdb::ClientContext &context,
                                                     duckdb::TableFunctionBindInput &input,
                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                     duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<IndexesBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineIndexDesc> descs(moraine_indexes_free);
	MoraineError err {};
	auto code = moraine_indexes(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                            descs.OutItems(), descs.OutLen(), moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto &desc : descs) {
		bind_data->rows.push_back(
		    {static_cast<int64_t>(desc.index_id), std::string(desc.name), desc.unique, desc.building});
	}

	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BOOLEAN};
	names = {"index_id", "index_name", "is_unique", "is_building"};
	return std::move(bind_data);
}

struct IndexesGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IndexesInitGlobal(duckdb::ClientContext &,
                                                                       duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<IndexesGlobalState>();
}

void IndexesImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IndexesBindData>();
	auto &state = data.global_state->Cast<IndexesGlobalState>();
	if (state.offset >= bind_data.rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		output.SetValue(0, i, duckdb::Value::BIGINT(row.index_id));
		output.SetValue(1, i, duckdb::Value(row.name));
		output.SetValue(2, i, duckdb::Value::BOOLEAN(row.unique));
		output.SetValue(3, i, duckdb::Value::BOOLEAN(row.building));
	}
	state.offset += count;
	output.SetCardinality(count);
}

// ---- moraine_index_create / moraine_index_drop: autonomous-commit DDL ----

struct IndexDdlBindData : public duckdb::FunctionData {
	bool is_create = false;
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;
	std::vector<std::string> columns;
	bool unique = false;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<IndexDdlBindData>();
		*result = *this;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexDdlBindData>();
		return is_create == other.is_create && catalog_name == other.catalog_name &&
		       schema_name == other.schema_name && table_name == other.table_name &&
		       index_name == other.index_name && columns == other.columns && unique == other.unique;
	}
};

struct IndexDdlGlobalState : public duckdb::GlobalTableFunctionState {
	bool emitted = false;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

// The DDL is applied once, at execution start.
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IndexDdlInitGlobal(duckdb::ClientContext &context,
                                                                        duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IndexDdlBindData>();
	auto handle = ResolveHandle(context, bind_data.catalog_name);
	MoraineError err {};
	int32_t code;
	if (bind_data.is_create) {
		std::vector<const char *> column_ptrs;
		column_ptrs.reserve(bind_data.columns.size());
		for (auto &column : bind_data.columns) {
			column_ptrs.push_back(column.c_str());
		}
		code = moraine_index_create(handle, bind_data.schema_name.c_str(), bind_data.table_name.c_str(),
		                            bind_data.index_name.c_str(), column_ptrs.data(), column_ptrs.size(),
		                            bind_data.unique, moraine_shim_is_interrupted, &context, &err);
	} else {
		code = moraine_index_drop(handle, bind_data.schema_name.c_str(), bind_data.table_name.c_str(),
		                          bind_data.index_name.c_str(), moraine_shim_is_interrupted, &context, &err);
	}
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	return duckdb::make_uniq<IndexDdlGlobalState>();
}

void IndexDdlImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<IndexDdlGlobalState>();
	auto &bind_data = data.bind_data->Cast<IndexDdlBindData>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	std::string message = (bind_data.is_create ? "created index " : "dropped index ") + bind_data.index_name;
	output.SetValue(0, 0, duckdb::Value(message));
	output.SetCardinality(1);
	state.emitted = true;
}

duckdb::unique_ptr<duckdb::FunctionData> CreateBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<IndexDdlBindData>();
	bind_data->is_create = true;
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();
	for (auto &child : duckdb::ListValue::GetChildren(input.inputs[4])) {
		bind_data->columns.push_back(child.GetValue<std::string>());
	}
	bind_data->unique = input.inputs[5].GetValue<bool>();
	return_types = {duckdb::LogicalType::VARCHAR};
	names = {"result"};
	return std::move(bind_data);
}

duckdb::unique_ptr<duckdb::FunctionData> DropBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                  duckdb::vector<duckdb::LogicalType> &return_types,
                                                  duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<IndexDdlBindData>();
	bind_data->is_create = false;
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();
	return_types = {duckdb::LogicalType::VARCHAR};
	names = {"result"};
	return std::move(bind_data);
}

// ---- moraine_index_lookup: resolve a value to the rows holding it ----

struct LookupBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;
	// The looked-up value in text form, so `Equals` distinguishes lookups of
	// different values (the rows they resolve to differ).
	std::string value_repr;
	struct Row {
		int64_t row_id;
		int64_t data_file_id;
		bool is_inline;
	};
	std::vector<Row> rows;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<LookupBindData>();
		*result = *this;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<LookupBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name && index_name == other.index_name && value_repr == other.value_repr;
	}
};

// Backing storage for a `MoraineLookupValue`'s string/bytes fields, which
// point into these members and so must outlive any use of the value.
struct LookupValueBacking {
	std::string str;
	std::vector<uint8_t> bytes;
};

// Translates a DuckDB `Value` into the tagged `MoraineLookupValue` the ABI
// coerces to the indexed column's type. Throws on a type equality indexes do
// not cover.
MoraineLookupValue BuildLookupValue(const duckdb::Value &value, LookupValueBacking &backing) {
	MoraineLookupValue lookup {};
	switch (value.type().id()) {
	case duckdb::LogicalTypeId::TINYINT:
	case duckdb::LogicalTypeId::SMALLINT:
	case duckdb::LogicalTypeId::INTEGER:
	case duckdb::LogicalTypeId::BIGINT:
	case duckdb::LogicalTypeId::DATE:
	case duckdb::LogicalTypeId::TIME:
	case duckdb::LogicalTypeId::TIMESTAMP:
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
		lookup.kind = 1;
		lookup.i64_value = value.GetValue<int64_t>();
		break;
	case duckdb::LogicalTypeId::UTINYINT:
	case duckdb::LogicalTypeId::USMALLINT:
	case duckdb::LogicalTypeId::UINTEGER:
	case duckdb::LogicalTypeId::UBIGINT:
		lookup.kind = 2;
		lookup.u64_value = value.GetValue<uint64_t>();
		break;
	case duckdb::LogicalTypeId::FLOAT:
	case duckdb::LogicalTypeId::DOUBLE:
		lookup.kind = 3;
		lookup.f64_value = value.GetValue<double>();
		break;
	case duckdb::LogicalTypeId::BOOLEAN:
		lookup.kind = 4;
		lookup.bool_value = value.GetValue<bool>();
		break;
	case duckdb::LogicalTypeId::VARCHAR:
		lookup.kind = 5;
		backing.str = duckdb::StringValue::Get(value);
		lookup.str_value = backing.str.c_str();
		break;
	case duckdb::LogicalTypeId::UUID: {
		lookup.kind = 6;
		backing.bytes.resize(16);
		duckdb::BaseUUID::ToBlob(value.GetValue<duckdb::hugeint_t>(), backing.bytes.data());
		lookup.bytes_value = backing.bytes.data();
		lookup.bytes_len = backing.bytes.size();
		break;
	}
	case duckdb::LogicalTypeId::BLOB: {
		lookup.kind = 6;
		const auto &blob = duckdb::StringValue::Get(value);
		backing.bytes.assign(blob.begin(), blob.end());
		lookup.bytes_value = backing.bytes.data();
		lookup.bytes_len = backing.bytes.size();
		break;
	}
	default:
		throw duckdb::InvalidInputException("moraine_index_lookup: unsupported value type \"%s\"",
		                                    value.type().ToString());
	}
	return lookup;
}

duckdb::unique_ptr<duckdb::FunctionData> LookupBind(duckdb::ClientContext &context,
                                                    duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<LookupBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();
	bind_data->value_repr = input.inputs[4].ToString();

	LookupValueBacking backing;
	MoraineLookupValue lookup_value = BuildLookupValue(input.inputs[4], backing);

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineRowLocation> locations(moraine_index_lookup_free);
	MoraineError err {};
	auto code = moraine_index_lookup(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                                 bind_data->index_name.c_str(), &lookup_value, locations.OutItems(),
	                                 locations.OutLen(), moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto &location : locations) {
		bind_data->rows.push_back({static_cast<int64_t>(location.row_id),
		                           static_cast<int64_t>(location.data_file_id), location.is_inline});
	}

	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN};
	names = {"row_id", "data_file_id", "is_inline"};
	return std::move(bind_data);
}

struct LookupGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> LookupInitGlobal(duckdb::ClientContext &,
                                                                      duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<LookupGlobalState>();
}

void LookupImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<LookupBindData>();
	auto &state = data.global_state->Cast<LookupGlobalState>();
	if (state.offset >= bind_data.rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		output.SetValue(0, i, duckdb::Value::BIGINT(row.row_id));
		output.SetValue(1, i, duckdb::Value::BIGINT(row.data_file_id));
		output.SetValue(2, i, duckdb::Value::BOOLEAN(row.is_inline));
	}
	state.offset += count;
	output.SetCardinality(count);
}

} // namespace

void RegisterMoraineIndexFunctions(duckdb::ExtensionLoader &loader) {
	using duckdb::LogicalType;

	duckdb::TableFunction list("moraine_indexes",
	                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, IndexesImpl,
	                           IndexesBind, IndexesInitGlobal);
	loader.RegisterFunction(list);

	duckdb::TableFunction create(
	    "moraine_index_create",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	     LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BOOLEAN},
	    IndexDdlImpl, CreateBind, IndexDdlInitGlobal);
	loader.RegisterFunction(create);

	duckdb::TableFunction lookup("moraine_index_lookup",
	                             {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                              LogicalType::VARCHAR, LogicalType::ANY},
	                             LookupImpl, LookupBind, LookupInitGlobal);
	loader.RegisterFunction(lookup);

	duckdb::TableFunction drop("moraine_index_drop",
	                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                            LogicalType::VARCHAR},
	                           IndexDdlImpl, DropBind, IndexDdlInitGlobal);
	loader.RegisterFunction(drop);
}

} // namespace moraine_duckdb
