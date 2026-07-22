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

// moraine_indexes: lists a table's live equality indexes.

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

// moraine_index_create / moraine_index_drop: autonomous-commit DDL.

struct IndexDdlBindData : public duckdb::FunctionData {
	bool is_create = false;
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;
	std::vector<std::string> columns;
	// Per-column direction: 1 = descending, parallel to `columns`. Empty
	// leaves the index ascending.
	std::vector<uint8_t> descending;
	// Per-column NULL placement: 1 = NULLS FIRST, parallel to `columns`.
	// Empty leaves the index NULLS LAST.
	std::vector<uint8_t> nulls_first;
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
		       index_name == other.index_name && columns == other.columns &&
		       descending == other.descending && nulls_first == other.nulls_first && unique == other.unique;
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
		// `descending`/`nulls_first` store one 0/1 byte per column; reinterpret
		// as the ABI's `bool` arrays, or pass null to default that axis.
		const bool *directions =
		    bind_data.descending.empty() ? nullptr
		                                 : reinterpret_cast<const bool *>(bind_data.descending.data());
		const bool *nulls_first =
		    bind_data.nulls_first.empty() ? nullptr
		                                  : reinterpret_cast<const bool *>(bind_data.nulls_first.data());
		code = moraine_index_create(handle, bind_data.schema_name.c_str(), bind_data.table_name.c_str(),
		                            bind_data.index_name.c_str(), column_ptrs.data(), column_ptrs.size(),
		                            directions, nulls_first, bind_data.unique, moraine_shim_is_interrupted,
		                            &context, &err);
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
	// Optional `directions := ['asc'|'desc', ...]`, parallel to the columns.
	auto directions = input.named_parameters.find("directions");
	if (directions != input.named_parameters.end() && !directions->second.IsNull()) {
		for (auto &child : duckdb::ListValue::GetChildren(directions->second)) {
			const std::string dir = child.GetValue<std::string>();
			if (dir == "desc" || dir == "DESC") {
				bind_data->descending.push_back(1);
			} else if (dir == "asc" || dir == "ASC") {
				bind_data->descending.push_back(0);
			} else {
				throw duckdb::InvalidInputException(
				    "moraine_index_create: direction must be 'asc' or 'desc', got \"%s\"", dir);
			}
		}
		if (bind_data->descending.size() != bind_data->columns.size()) {
			throw duckdb::InvalidInputException(
			    "moraine_index_create: `directions` must have one entry per column");
		}
	}
	// Optional `nulls := ['first'|'last', ...]`, parallel to the columns.
	auto nulls = input.named_parameters.find("nulls");
	if (nulls != input.named_parameters.end() && !nulls->second.IsNull()) {
		for (auto &child : duckdb::ListValue::GetChildren(nulls->second)) {
			const std::string placement = child.GetValue<std::string>();
			if (placement == "first" || placement == "FIRST") {
				bind_data->nulls_first.push_back(1);
			} else if (placement == "last" || placement == "LAST") {
				bind_data->nulls_first.push_back(0);
			} else {
				throw duckdb::InvalidInputException(
				    "moraine_index_create: nulls placement must be 'first' or 'last', got \"%s\"", placement);
			}
		}
		if (bind_data->nulls_first.size() != bind_data->columns.size()) {
			throw duckdb::InvalidInputException(
			    "moraine_index_create: `nulls` must have one entry per column");
		}
	}
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

// moraine_index_lookup: resolves a value to the rows holding it.

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

struct RangeBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;
	// Both bounds and their inclusivity in text form, so `Equals`
	// distinguishes different ranges (they resolve to different rows).
	std::string bounds_repr;
	struct Row {
		int64_t row_id;
		int64_t data_file_id;
		bool is_inline;
	};
	std::vector<Row> rows;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<RangeBindData>();
		*result = *this;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<RangeBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name && index_name == other.index_name && bounds_repr == other.bounds_repr;
	}
};

// A NULL bound argument is an open (unbounded) side; a present one is
// Included or Excluded per its inclusivity flag.
duckdb::unique_ptr<duckdb::FunctionData> RangeBind(duckdb::ClientContext &context,
                                                   duckdb::TableFunctionBindInput &input,
                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                   duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<RangeBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();
	const bool lower_inclusive = input.inputs[6].GetValue<bool>();
	const bool upper_inclusive = input.inputs[7].GetValue<bool>();
	bind_data->bounds_repr = input.inputs[4].ToString() + (lower_inclusive ? "[" : "(") + "," +
	                         input.inputs[5].ToString() + (upper_inclusive ? "]" : ")");

	LookupValueBacking lower_backing;
	LookupValueBacking upper_backing;
	MoraineLookupValue lower_value {};
	MoraineLookupValue upper_value {};
	const bool has_lower = !input.inputs[4].IsNull();
	const bool has_upper = !input.inputs[5].IsNull();
	if (has_lower) {
		lower_value = BuildLookupValue(input.inputs[4], lower_backing);
	}
	if (has_upper) {
		upper_value = BuildLookupValue(input.inputs[5], upper_backing);
	}

	// Optional `reverse := true` returns the rows in the opposite of the
	// index's declared order.
	auto reverse_it = input.named_parameters.find("reverse");
	const bool reverse =
	    reverse_it != input.named_parameters.end() && !reverse_it->second.IsNull() && reverse_it->second.GetValue<bool>();
	bind_data->bounds_repr += reverse ? "|rev" : "";

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineRowLocation> locations(moraine_index_range_free);
	MoraineError err {};
	auto code = moraine_index_range(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                                bind_data->index_name.c_str(), has_lower ? &lower_value : nullptr,
	                                lower_inclusive, has_upper ? &upper_value : nullptr, upper_inclusive, reverse,
	                                locations.OutItems(), locations.OutLen(), moraine_shim_is_interrupted, &context,
	                                &err);
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

struct RangeGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> RangeInitGlobal(duckdb::ClientContext &,
                                                                     duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<RangeGlobalState>();
}

void RangeImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RangeBindData>();
	auto &state = data.global_state->Cast<RangeGlobalState>();
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

struct NullsBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;
	// The prefix predicates in text form, so `Equals` distinguishes queries.
	std::string prefix_repr;
	struct Row {
		int64_t row_id;
		int64_t data_file_id;
		bool is_inline;
	};
	std::vector<Row> rows;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<NullsBindData>();
		*result = *this;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<NullsBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name && index_name == other.index_name && prefix_repr == other.prefix_repr;
	}
};

// The variadic args after the index name are the leading-prefix predicates: a
// NULL arg is `IS NULL` for that column (ABI kind 0), any other is `= value`.
duckdb::unique_ptr<duckdb::FunctionData> NullsBind(duckdb::ClientContext &context,
                                                   duckdb::TableFunctionBindInput &input,
                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                   duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<NullsBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();

	const idx_t prefix_count = input.inputs.size() - 4;
	std::vector<LookupValueBacking> backings(prefix_count);
	std::vector<MoraineLookupValue> prefix;
	prefix.reserve(prefix_count);
	for (idx_t i = 4; i < input.inputs.size(); i++) {
		bind_data->prefix_repr += input.inputs[i].ToString() + ",";
		if (input.inputs[i].IsNull()) {
			MoraineLookupValue is_null {};
			is_null.kind = 0;
			prefix.push_back(is_null);
		} else {
			prefix.push_back(BuildLookupValue(input.inputs[i], backings[i - 4]));
		}
	}

	// Optional `reverse := true` returns the rows in the opposite order.
	auto reverse_it = input.named_parameters.find("reverse");
	const bool reverse =
	    reverse_it != input.named_parameters.end() && !reverse_it->second.IsNull() && reverse_it->second.GetValue<bool>();
	bind_data->prefix_repr += reverse ? "rev" : "";

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineRowLocation> locations(moraine_index_nulls_free);
	MoraineError err {};
	auto code = moraine_index_nulls(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                                bind_data->index_name.c_str(), prefix.data(), prefix.size(), reverse,
	                                locations.OutItems(), locations.OutLen(), moraine_shim_is_interrupted,
	                                &context, &err);
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

struct NullsGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> NullsInitGlobal(duckdb::ClientContext &,
                                                                     duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<NullsGlobalState>();
}

void NullsImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<NullsBindData>();
	auto &state = data.global_state->Cast<NullsGlobalState>();
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
	// Optional per-column sort directions ['asc'|'desc', ...] and NULL
	// placement ['first'|'last', ...], each parallel to the columns.
	create.named_parameters["directions"] = LogicalType::LIST(LogicalType::VARCHAR);
	create.named_parameters["nulls"] = LogicalType::LIST(LogicalType::VARCHAR);
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

	// (catalog, schema, table, index, lower, upper, lower_inclusive,
	// upper_inclusive); a NULL bound is an open side.
	duckdb::TableFunction range("moraine_index_range",
	                            {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                             LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY,
	                             LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	                            RangeImpl, RangeBind, RangeInitGlobal);
	range.named_parameters["reverse"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(range);

	// (catalog, schema, table, index, then a leading prefix of predicates as
	// variadic args — a NULL arg is IS NULL, any other is = value).
	duckdb::TableFunction nulls("moraine_index_nulls",
	                            {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                             LogicalType::VARCHAR},
	                            NullsImpl, NullsBind, NullsInitGlobal);
	nulls.varargs = LogicalType::ANY;
	nulls.named_parameters["reverse"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(nulls);
}

} // namespace moraine_duckdb
