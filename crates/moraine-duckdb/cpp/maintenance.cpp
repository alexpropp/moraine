#include "maintenance.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"

#include <algorithm>

namespace moraine_duckdb {

namespace {

// The DuckLake steps a pass runs, in order, with the parameters each
// accepts. Names are DuckLake's own minus the `ducklake_` prefix; an
// attach option is `MAINTENANCE_<step>` to enable a step with DuckLake's
// defaults, or `MAINTENANCE_<step>_<parameter>` to pass one through.
//
// Expiry leads because it is the only step that shrinks the catalog
// rather than adding to it, and every later step is then served a smaller
// snapshot projection. Flush precedes merge so its small files are merge
// input; merge and rewrite precede cleanup because merge schedules its
// superseded bytes directly and cleanup drains that schedule in the same
// pass; cleanup precedes orphan detection so the schedule is drained
// first. The sweep runs last, on what the earlier steps left behind.
struct StepSpec {
	const char *name;
	std::vector<const char *> parameters;
};

const std::vector<StepSpec> &StepSpecs() {
	static const std::vector<StepSpec> specs = {
	    {"expire_snapshots", {"older_than", "versions"}},
	    {"flush_inlined_data", {}},
	    {"merge_adjacent_files", {}},
	    {"rewrite_data_files", {"delete_threshold"}},
	    {"cleanup_old_files", {"older_than", "cleanup_all"}},
	    {"delete_orphaned_files", {"older_than", "cleanup_all"}},
	};
	return specs;
}

// A step's accumulating state while options are parsed in arrival order.
struct PendingStep {
	bool enabled = false;
	bool explicitly_disabled = false;
	std::vector<std::string> arguments;
};

std::string KnownStepList() {
	std::string known;
	for (auto &spec : StepSpecs()) {
		if (!known.empty()) {
			known += ", ";
		}
		known += spec.name;
	}
	return known;
}

} // namespace

MaintenanceConfig ParseMaintenanceOptions(const std::vector<std::pair<std::string, duckdb::Value>> &options) {
	MaintenanceConfig config;
	std::vector<PendingStep> pending(StepSpecs().size());

	for (auto &option : options) {
		auto name = duckdb::StringUtil::Lower(option.first);
		if (!duckdb::StringUtil::StartsWith(name, "maintenance_")) {
			continue;
		}
		auto rest = name.substr(std::string("maintenance_").size());

		if (rest == "interval") {
			// Accept a plain count of seconds or an INTERVAL value; both
			// read naturally at an attach.
			if (option.second.type().id() == duckdb::LogicalTypeId::INTERVAL) {
				auto interval = option.second.GetValue<duckdb::interval_t>();
				config.interval_ms = static_cast<uint64_t>(duckdb::Interval::GetMicro(interval) / 1000);
			} else {
				config.interval_ms = option.second.GetValue<uint64_t>() * 1000;
			}
			if (config.interval_ms == 0) {
				throw duckdb::BinderException("MAINTENANCE_INTERVAL must be a positive duration");
			}
			continue;
		}
		if (rest == "batch_size") {
			config.batch_size = option.second.GetValue<uint64_t>();
			if (config.batch_size == 0) {
				throw duckdb::BinderException("MAINTENANCE_BATCH_SIZE must be positive");
			}
			continue;
		}
		if (rest == "sweep_indexes") {
			config.sweep_indexes = option.second.GetValue<bool>();
			continue;
		}

		// Longest-match the step name, then treat any remainder as one of
		// that step's own parameters. Both contain underscores, so the
		// split cannot be positional.
		bool matched = false;
		for (duckdb::idx_t i = 0; i < StepSpecs().size(); i++) {
			auto &spec = StepSpecs()[i];
			std::string step = spec.name;
			if (rest == step) {
				matched = true;
				if (option.second.GetValue<bool>()) {
					pending[i].enabled = true;
				} else {
					pending[i].explicitly_disabled = true;
				}
				break;
			}
			if (!duckdb::StringUtil::StartsWith(rest, step + "_")) {
				continue;
			}
			auto parameter = rest.substr(step.size() + 1);
			auto &accepted = spec.parameters;
			if (std::find_if(accepted.begin(), accepted.end(), [&](const char *candidate) {
				    return parameter == candidate;
			    }) == accepted.end()) {
				throw duckdb::BinderException(
				    "ducklake_%s takes no parameter named \"%s\"; MAINTENANCE_%s_<parameter> passes "
				    "DuckLake's own parameters through unchanged",
				    step, parameter, duckdb::StringUtil::Upper(step));
			}
			matched = true;
			// Supplying a parameter enables its step.
			pending[i].enabled = true;
			pending[i].arguments.push_back(parameter + " => " + option.second.ToSQLString());
			break;
		}
		if (!matched) {
			throw duckdb::BinderException("unknown maintenance option \"MAINTENANCE_%s\"; known steps are: %s",
			                              duckdb::StringUtil::Upper(rest), KnownStepList());
		}
	}

	for (duckdb::idx_t i = 0; i < pending.size(); i++) {
		auto &step = pending[i];
		if (step.explicitly_disabled && !step.arguments.empty()) {
			throw duckdb::BinderException(
			    "MAINTENANCE_%s is false but one of its parameters was supplied; drop one or the other",
			    duckdb::StringUtil::Upper(StepSpecs()[i].name));
		}
		if (step.enabled && !step.explicitly_disabled) {
			config.ducklake_steps.push_back(DuckLakeStep {StepSpecs()[i].name, step.arguments});
		}
	}
	return config;
}

namespace {

// DuckLake's maintenance functions take the *lake* name, while this
// catalog is attached under the metadata name DuckLake derives from it
// (`__ducklake_metadata_<lake>`, its default). Recover the lake name so
// the pass calls the functions with what they expect. A standalone
// `moraine:` attach carries no such prefix and keeps its own name — it
// has no DuckLake above it to maintain.
std::string LakeNameOf(const std::string &attached_name) {
	const std::string prefix = "__ducklake_metadata_";
	if (duckdb::StringUtil::StartsWith(attached_name, prefix)) {
		return attached_name.substr(prefix.size());
	}
	return attached_name;
}

} // namespace

MaintenanceScheduler::MaintenanceScheduler(duckdb::DatabaseInstance &db, std::string lake,
                                           MoraineCatalogHandle *handle, MaintenanceConfig config)
    : db_(db), lake_(LakeNameOf(lake)), handle_(handle), config_(std::move(config)) {
}

MaintenanceScheduler::~MaintenanceScheduler() {
	Stop();
}

void MaintenanceScheduler::Start() {
	if (config_.interval_ms == 0 || thread_.joinable()) {
		return;
	}
	thread_ = std::thread([this]() { Loop(); });
}

void MaintenanceScheduler::Stop() {
	{
		std::lock_guard<std::mutex> guard(wake_lock_);
		stopping_ = true;
	}
	wake_.notify_all();
	if (thread_.joinable()) {
		thread_.join();
	}
}

void MaintenanceScheduler::Loop() {
	auto interval = std::chrono::milliseconds(config_.interval_ms);
	for (;;) {
		{
			std::unique_lock<std::mutex> guard(wake_lock_);
			// Waiting on the stop flag means detach does not have to
			// outlast a whole interval to shut the thread down.
			if (wake_.wait_for(guard, interval, [this]() { return stopping_; })) {
				return;
			}
		}
		// A tick arriving while a pass is still running skips rather than
		// queueing: two concurrent passes are safe but collide on
		// DuckLake's own conflict rules, and a scheduler that
		// manufactures its own conflicts is indefensible.
		RunPass(true, "scheduled");
	}
}

std::vector<MaintenanceStep> MaintenanceScheduler::RunNow() {
	return RunPass(false, "manual");
}

std::vector<MaintenancePass> MaintenanceScheduler::RecentPasses() const {
	std::lock_guard<std::mutex> guard(report_lock_);
	// Newest first: an operator reading the top rows wants the last pass.
	return std::vector<MaintenancePass>(passes_.rbegin(), passes_.rend());
}

std::vector<MaintenanceStep> MaintenanceScheduler::RunPass(bool skip_if_busy, const char *trigger) {
	std::unique_lock<std::mutex> pass(pass_lock_, std::defer_lock);
	if (skip_if_busy) {
		if (!pass.try_lock()) {
			return {};
		}
	} else {
		pass.lock();
	}

	auto started_at = duckdb::Timestamp::GetCurrentTimestamp();
	std::vector<MaintenanceStep> report;
	// The pass runs on a connection of its own. `ClientContext::Query`
	// takes a non-recursive lock, so a query issued from inside a running
	// operator on the caller's own context would deadlock; a separate
	// connection on the same instance is the supported way in.
	duckdb::Connection connection(db_);

	for (auto &spec : StepSpecs()) {
		auto configured = std::find_if(config_.ducklake_steps.begin(), config_.ducklake_steps.end(),
		                               [&](const DuckLakeStep &step) { return step.name == spec.name; });
		if (configured == config_.ducklake_steps.end()) {
			report.push_back(MaintenanceStep {spec.name, "skipped", "not configured at attach"});
			continue;
		}
		auto outcome = RunDuckLakeStep(connection, *configured);
		bool failed = outcome.status == "failed";
		report.push_back(std::move(outcome));
		if (failed) {
			// A failed step aborts the pass; earlier effects stand, and
			// every step is idempotent, so the next one re-runs from the
			// top.
			break;
		}
	}

	if (report.empty() || report.back().status != "failed") {
		report.push_back(config_.sweep_indexes
		                     ? RunSweep()
		                     : MaintenanceStep {"sweep_indexes", "skipped", "disabled at attach"});
	}

	{
		std::lock_guard<std::mutex> guard(report_lock_);
		passes_.push_back(MaintenancePass {started_at, trigger, report});
		while (passes_.size() > RETAINED_PASSES) {
			passes_.pop_front();
		}
	}
	return report;
}

MaintenanceStep MaintenanceScheduler::RunDuckLakeStep(duckdb::Connection &connection, const DuckLakeStep &step) {
	std::string sql = "CALL ducklake_" + step.name + "(" + duckdb::KeywordHelper::WriteQuoted(lake_, '\'');
	for (auto &argument : step.arguments) {
		sql += ", " + argument;
	}
	sql += ")";

	auto result = connection.Query(sql);
	if (result->HasError()) {
		return MaintenanceStep {step.name, "failed", result->GetError()};
	}
	return MaintenanceStep {step.name, "ran", sql};
}

MaintenanceStep MaintenanceScheduler::RunSweep() {
	uint64_t indexes = 0;
	uint64_t entries = 0;
	MoraineError err {};
	// No interrupt probe: the sweep runs on the scheduler's own thread,
	// which stops through the stop flag rather than a query interrupt.
	auto code = moraine_maintain(handle_, config_.batch_size, &indexes, &entries, nullptr, nullptr, &err);
	if (code != MORAINE_OK) {
		std::string message = err.message != nullptr ? std::string(err.message) : "unknown error";
		if (err.message != nullptr) {
			moraine_error_free(err.message);
		}
		return MaintenanceStep {"sweep_indexes", "failed", message};
	}
	return MaintenanceStep {"sweep_indexes", "ran",
	                        "reclaimed " + std::to_string(entries) + (entries == 1 ? " entry" : " entries") +
	                            " from " + std::to_string(indexes) +
	                            (indexes == 1 ? " dropped index" : " dropped indexes")};
}

namespace {

// Resolves the `MoraineCatalog` behind a lake name, accepting either the
// DuckLake name or moraine's metadata catalog directly — the same pair
// `moraine_index_*` accepts.
MoraineCatalog &ResolveCatalog(duckdb::ClientContext &context, const std::string &catalog_name) {
	auto as_moraine = [&](const std::string &name) -> MoraineCatalog * {
		auto catalog = duckdb::Catalog::GetCatalogEntry(context, name);
		if (catalog && catalog->GetCatalogType() == "moraine") {
			return &catalog->Cast<MoraineCatalog>();
		}
		return nullptr;
	};
	if (auto *catalog = as_moraine(catalog_name)) {
		return *catalog;
	}
	if (auto *catalog = as_moraine("__ducklake_metadata_" + catalog_name)) {
		return *catalog;
	}
	throw duckdb::InvalidInputException(
	    "\"%s\" is not a moraine-backed lake; pass the DuckLake lake name, or moraine's "
	    "\"__ducklake_metadata_<lake>\" metadata catalog",
	    catalog_name);
}

struct MaintenanceBindData : public duckdb::FunctionData {
	std::string catalog_name;
	// True for the status function, which reports without running.
	bool status_only = false;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<MaintenanceBindData>();
		*result = *this;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<MaintenanceBindData>();
		return catalog_name == other.catalog_name && status_only == other.status_only;
	}
};

// One emitted row. The trigger reports a single pass and omits the pass
// columns; the status function reports the retained passes and carries
// them, so a failure stays attributable to when it happened and to what
// drove it.
struct ReportRow {
	duckdb::timestamp_t started_at;
	std::string trigger;
	MaintenanceStep step;
};

struct MaintenanceGlobalState : public duckdb::GlobalTableFunctionState {
	std::vector<ReportRow> rows;
	bool with_pass_columns = false;
	duckdb::idx_t emitted = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> MaintenanceBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                         duckdb::vector<duckdb::LogicalType> &return_types,
                                                         duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<MaintenanceBindData>();
	if (input.inputs[0].IsNull()) {
		throw duckdb::BinderException("moraine_maintenance: the lake name must not be NULL");
	}
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
	names = {"step", "status", "detail"};
	return std::move(bind_data);
}

duckdb::unique_ptr<duckdb::FunctionData> StatusBind(duckdb::ClientContext &context,
                                                    duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	auto bind_data = MaintenanceBind(context, input, return_types, names);
	bind_data->Cast<MaintenanceBindData>().status_only = true;
	// Retained passes carry when they ran and what drove them.
	return_types.insert(return_types.begin(),
	                    {duckdb::LogicalType::TIMESTAMP, duckdb::LogicalType::VARCHAR});
	names.insert(names.begin(), {"started_at", "trigger"});
	return bind_data;
}

// The pass runs once, at execution start.
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MaintenanceInitGlobal(duckdb::ClientContext &context,
                                                                           duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MaintenanceBindData>();
	auto &catalog = ResolveCatalog(context, bind_data.catalog_name);
	auto state = duckdb::make_uniq<MaintenanceGlobalState>();

	if (bind_data.status_only) {
		state->with_pass_columns = true;
		for (auto &pass : catalog.Scheduler().RecentPasses()) {
			for (auto &step : pass.steps) {
				state->rows.push_back(ReportRow {pass.started_at, pass.trigger, step});
			}
		}
		return std::move(state);
	}

	if (catalog.GetAttached().IsReadOnly()) {
		throw duckdb::CatalogException("moraine_maintenance: \"%s\" is attached read-only; maintenance mutates",
		                               bind_data.catalog_name);
	}
	// The caller blocks while a separate connection writes the catalog,
	// so running inside an explicit transaction invites a self-deadlock.
	if (!context.transaction.IsAutoCommit()) {
		throw duckdb::TransactionException(
		    "moraine_maintenance cannot run inside an explicit transaction; COMMIT first");
	}
	for (auto &step : catalog.Scheduler().RunNow()) {
		state->rows.push_back(ReportRow {duckdb::timestamp_t(0), "manual", step});
	}
	return std::move(state);
}

void MaintenanceImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<MaintenanceGlobalState>();
	duckdb::idx_t count = 0;
	while (state.emitted < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.emitted];
		duckdb::idx_t column = 0;
		if (state.with_pass_columns) {
			output.SetValue(column++, count, duckdb::Value::TIMESTAMP(row.started_at));
			output.SetValue(column++, count, duckdb::Value(row.trigger));
		}
		output.SetValue(column++, count, duckdb::Value(row.step.step));
		output.SetValue(column++, count, duckdb::Value(row.step.status));
		output.SetValue(column, count, duckdb::Value(row.step.detail));
		state.emitted++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterMoraineMaintenanceFunctions(duckdb::ExtensionLoader &loader) {
	duckdb::TableFunction maintenance("moraine_maintenance", {duckdb::LogicalType::VARCHAR}, MaintenanceImpl,
	                                  MaintenanceBind, MaintenanceInitGlobal);
	loader.RegisterFunction(maintenance);

	duckdb::TableFunction status("moraine_maintenance_status", {duckdb::LogicalType::VARCHAR}, MaintenanceImpl,
	                             StatusBind, MaintenanceInitGlobal);
	loader.RegisterFunction(status);
}

} // namespace moraine_duckdb
