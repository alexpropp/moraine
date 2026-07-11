// Vendored by hand, verbatim except for its own `#include`s, from
// https://raw.githubusercontent.com/duckdb/duckdb/v1.5.4/src/include/duckdb/catalog/catalog_entry/view_catalog_entry.hpp
// (git tag v1.5.4, commit 08e34c447b). Same amalgamation gap as
// `storage_extension.hpp`. Its includes (`standard_entry.hpp`,
// `select_statement.hpp`, `types.hpp`, `vector.hpp`, `thread.hpp`,
// `mutex.hpp`) are already fully defined in `duckdb.hpp`, so this class
// body is self-contained against it. Kept byte-for-byte identical to
// upstream — see `create_schema_info.hpp` for why. Must be included after
// `duckdb.hpp` and `create_view_info.hpp`.
#pragma once

namespace duckdb {

class DataTable;
struct CreateViewInfo;

enum class ViewBindState { BOUND, BINDING, UNBOUND };
enum class BindViewAction { BIND_IF_UNBOUND, FORCE_REBIND };

struct ViewColumnInfo {
	vector<LogicalType> types;
	vector<string> names;
};

//! A view catalog entry
class ViewCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::VIEW_ENTRY;
	static constexpr const char *Name = "view";

public:
	//! Create a real TableCatalogEntry and initialize storage for it
	ViewCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info);

	//! The query of the view
	unique_ptr<SelectStatement> query;
	//! The SQL query (if any)
	string sql;
	//! The set of aliases associated with the view
	vector<string> aliases;

	//! Returns the view column info, if the view is bound. Otherwise returns `nullptr`
	virtual shared_ptr<ViewColumnInfo> GetColumnInfo() const;
	//! Bind a view so we know the types / names returned by it
	virtual void BindView(ClientContext &context, BindViewAction action = BindViewAction::BIND_IF_UNBOUND);
	//! Update the view with a new set of types / names
	virtual void UpdateBinding(const vector<LogicalType> &types, const vector<string> &names);
	Value GetColumnComment(idx_t column_index);

public:
	unique_ptr<CreateInfo> GetInfo() const override;

	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info) override;

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	virtual const SelectStatement &GetQuery();

	string ToSQL() const override;

private:
	mutable mutex bind_lock;
	//! Columns returned by the view, if bound
	shared_ptr<ViewColumnInfo> view_columns;
	//! The current bind state of the view
	atomic<ViewBindState> bind_state;
	//! Current binding thread
	atomic<thread_id> bind_thread;
	//! The comments on the columns of the view: can be empty if there are no comments
	unordered_map<string, Value> column_comments;

private:
	void Initialize(CreateViewInfo &info);
};
} // namespace duckdb
