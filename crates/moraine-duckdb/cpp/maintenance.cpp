#include "maintenance.hpp"

#include "duckdb/main/database_manager.hpp"
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
		if (rest == "fold_slots") {
			config.fold_slots = option.second.GetValue<bool>();
			continue;
		}
		if (rest == "truncate_slots") {
			config.truncate_slots = option.second.GetValue<bool>();
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
			// Attach options are evaluated once, at attach. A timestamp
			// written as `now()` therefore freezes into a literal, and a
			// schedule would keep expiring against its attach-time
			// instant forever — retention silently stopping as the lake
			// moves on. An interval expresses the rolling window that
			// was meant, rendered so DuckLake evaluates it each pass.
			if (parameter == "older_than" && option.second.type().id() == duckdb::LogicalTypeId::INTERVAL) {
				// `now()` is TIMESTAMPTZ, and subtracting an interval from
				// one needs the `icu` extension. Casting to TIMESTAMP uses
				// an operator that is always available, and DuckLake
				// accepts either for this parameter.
				pending[i].arguments.push_back(parameter + " => now()::TIMESTAMP - " +
				                               option.second.ToSQLString());
			} else {
				pending[i].arguments.push_back(parameter + " => " + option.second.ToSQLString());
			}
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

// The name DuckLake gives its metadata catalog by default. An attach
// that passes `METADATA_CATALOG` uses its own name instead, which is why
// this prefix is only ever a fallback.
constexpr const char *METADATA_PREFIX = "__ducklake_metadata_";

} // namespace

MaintenanceScheduler::MaintenanceScheduler(duckdb::DatabaseInstance &db, std::string attached_name,
                                           std::string store_path, MoraineCatalogHandle *handle,
                                           MaintenanceConfig config)
    : db_(db), attached_name_(std::move(attached_name)), store_path_(std::move(store_path)),
      handle_(handle), config_(std::move(config)) {
}

std::string MaintenanceScheduler::ResolveLakeName(duckdb::Connection &connection) {
	// DuckLake's maintenance functions take the *lake* name, while this
	// catalog is attached under its metadata name. The two are related by
	// path — the lake attaches `moraine:<store path>` and this catalog
	// reports `<store path>` — so match on that rather than assuming the
	// default `__ducklake_metadata_<lake>` naming, which an attach can
	// override with `METADATA_CATALOG`.
	auto result = connection.Query("SELECT database_name, path FROM duckdb_databases() "
	                               "WHERE type = 'ducklake'");
	if (!result->HasError()) {
		for (auto &row : *result) {
			auto name = row.GetValue<std::string>(0);
			auto path = row.GetValue<std::string>(1);
			if (duckdb::StringUtil::EndsWith(path, store_path_)) {
				return name;
			}
		}
	}
	// No DuckLake above this catalog (a standalone `moraine:` attach), or
	// the listing failed. Fall back to the default naming so a lake that
	// follows it still works.
	if (duckdb::StringUtil::StartsWith(attached_name_, METADATA_PREFIX)) {
		return attached_name_.substr(std::string(METADATA_PREFIX).size());
	}
	return attached_name_;
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
	auto lake = ResolveLakeName(connection);

	// Fold leads the pass. The reclaim sweep reads the folded store, but a
	// dropped index's definition rides an unfolded slot until folded — so
	// without this first, the sweep cannot see the drop and reclaims
	// nothing. Truncation's horizon is the durable fold cursor, which
	// folding just advanced, so it too runs on what folding left behind.
	report.push_back(config_.fold_slots ? RunFold()
	                                     : MaintenanceStep {"fold_slots", "skipped", "disabled at attach"});

	// A failed step abandons the rest of the *DuckLake* sequence, whose
	// steps depend on each other — cleanup drains what merge scheduled,
	// so running it after a failed merge does partial work on a premise
	// that did not hold. The abandoned steps are still reported, so every
	// pass emits the same rows and two passes can be compared.
	std::string aborted_by;
	for (auto &spec : StepSpecs()) {
		if (!aborted_by.empty()) {
			report.push_back(
			    MaintenanceStep {spec.name, "skipped", "not attempted: " + aborted_by + " failed"});
			continue;
		}
		auto configured = std::find_if(config_.ducklake_steps.begin(), config_.ducklake_steps.end(),
		                               [&](const DuckLakeStep &step) { return step.name == spec.name; });
		if (configured == config_.ducklake_steps.end()) {
			report.push_back(MaintenanceStep {spec.name, "skipped", "not configured at attach"});
			continue;
		}
		auto outcome = RunDuckLakeStep(connection, lake, *configured);
		if (outcome.status == "failed") {
			aborted_by = spec.name;
		}
		report.push_back(std::move(outcome));
	}

	// The sweep runs regardless. It depends on no DuckLake step — it
	// reclaims moraine's own orphaned index ranges — so letting a failed
	// expiry suppress it would stop the reclamation this pass exists for
	// on every future pass too, for as long as the misconfiguration
	// stands, while the leak it fixes kept growing.
	report.push_back(config_.sweep_indexes
	                     ? RunSweep()
	                     : MaintenanceStep {"sweep_indexes", "skipped", "disabled at attach"});

	// Truncation closes the pass, on the fold cursor folding advanced.
	report.push_back(config_.truncate_slots
	                     ? RunTruncate()
	                     : MaintenanceStep {"truncate_slots", "skipped", "disabled at attach"});

	{
		std::lock_guard<std::mutex> guard(report_lock_);
		passes_.push_back(MaintenancePass {started_at, trigger, report});
		while (passes_.size() > RETAINED_PASSES) {
			passes_.pop_front();
		}
	}

	// The pass ran with no ClientContext (its own connection, its own
	// thread), so the core's events from the sweep and DuckLake steps sit
	// buffered; the database-scoped drain is the only one that can reach
	// them here. The outcome line makes a scheduled pass visible without
	// querying the report table: `warn` names each failed step, `info`
	// closes a clean pass.
	DrainMoraineLogs(db_);
	std::string failed;
	for (auto &step : report) {
		if (step.status == "failed") {
			failed += (failed.empty() ? "" : ", ") + step.step + ": " + step.detail;
		}
	}
	if (!failed.empty()) {
		WriteMoraineLog(db_, duckdb::LogLevel::LOG_WARNING,
		                std::string("maintenance pass (") + trigger + ") had failures — " + failed);
	} else {
		WriteMoraineLog(db_, duckdb::LogLevel::LOG_INFO,
		                std::string("maintenance pass (") + trigger + ") completed");
	}
	return report;
}

MaintenanceStep MaintenanceScheduler::RunDuckLakeStep(duckdb::Connection &connection, const std::string &lake,
                                                     const DuckLakeStep &step) {
	std::string sql = "CALL ducklake_" + step.name + "(" + duckdb::KeywordHelper::WriteQuoted(lake, '\'');
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

MaintenanceStep MaintenanceScheduler::RunFold() {
	uint64_t folded = 0;
	uint64_t remaining = 0;
	MoraineError err {};
	// UINT64_MAX folds the whole unfolded tail this pass, so the sweep that
	// follows sees every drop. No interrupt probe: the pass runs on the
	// scheduler's own thread.
	auto code = moraine_fold_sprint(handle_, UINT64_MAX, &folded, &remaining, &err);
	if (code == MORAINE_FENCED) {
		// A duelling folder already holds the writer and is advancing the
		// cursor. Its work stands in for this pass's; wasted effort, not an
		// error, so the pass records it and moves on.
		if (err.message != nullptr) {
			moraine_error_free(err.message);
		}
		return MaintenanceStep {"fold_slots", "skipped", "another session holds the folder"};
	}
	if (code != MORAINE_OK) {
		std::string message = err.message != nullptr ? std::string(err.message) : "unknown error";
		if (err.message != nullptr) {
			moraine_error_free(err.message);
		}
		return MaintenanceStep {"fold_slots", "failed", message};
	}
	return MaintenanceStep {"fold_slots", "ran",
	                        "folded " + std::to_string(folded) + (folded == 1 ? " slot, " : " slots, ") +
	                            std::to_string(remaining) + " remaining"};
}

MaintenanceStep MaintenanceScheduler::RunTruncate() {
	uint64_t removed = 0;
	MoraineError err {};
	auto code = moraine_truncate_slots(handle_, &removed, &err);
	if (code != MORAINE_OK) {
		std::string message = err.message != nullptr ? std::string(err.message) : "unknown error";
		if (err.message != nullptr) {
			moraine_error_free(err.message);
		}
		return MaintenanceStep {"truncate_slots", "failed", message};
	}
	return MaintenanceStep {"truncate_slots", "ran",
	                        "removed " + std::to_string(removed) + (removed == 1 ? " slot" : " slots")};
}

namespace {

// Resolves the `MoraineCatalog` behind a lake name, accepting either the
// DuckLake name or moraine's metadata catalog directly — the same pair
// `moraine_index_*` accepts.
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
	auto &catalog = ResolveMoraineCatalog(context, bind_data.catalog_name);
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
