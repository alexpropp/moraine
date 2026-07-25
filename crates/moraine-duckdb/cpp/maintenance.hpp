// The maintenance pass: DuckLake's own maintenance SQL, in a fixed order,
// followed by moraine's orphaned-index-entry sweep. A pass runs on a
// connection of its own — never the caller's context, whose lock a
// re-entrant query would deadlock on.
#pragma once

#include "duckdb.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct MoraineCatalogHandle;

namespace moraine_duckdb {

// One step's outcome. `status` is "ran", "skipped" (the attach did not
// configure it), or "failed".
struct MaintenanceStep {
	std::string step;
	std::string status;
	std::string detail;
};

// One completed pass. Retained so a failure stays visible: keeping only
// the newest pass would let the next success erase it, and a short
// interval would hide more than a long one.
struct MaintenancePass {
	duckdb::timestamp_t started_at;
	// "scheduled" (the timer) or "manual" (the trigger).
	std::string trigger;
	std::vector<MaintenanceStep> steps;
};

// A DuckLake step the attach configured, with the arguments to pass.
struct DuckLakeStep {
	// The function minus its `ducklake_` prefix, e.g. `expire_snapshots`.
	std::string name;
	// Rendered `name => literal` arguments, in the order supplied.
	std::vector<std::string> arguments;
};

// What the attach configured. Steps absent here are reported "skipped":
// every step that mutates the lake is opt-in, so an attach that
// configures nothing reclaims only what no query can observe.
struct MaintenanceConfig {
	// Zero starts no thread; the trigger still runs passes on demand.
	uint64_t interval_ms = 0;
	// Zero takes the core's own batch default.
	uint64_t batch_size = 0;
	bool sweep_indexes = true;
	std::vector<DuckLakeStep> ducklake_steps;
};

// Parses the `MAINTENANCE_*` attach options, in whatever order they
// arrive. Throws `BinderException` on an unknown step, an unknown
// parameter, or a step disabled while one of its parameters is supplied.
MaintenanceConfig ParseMaintenanceOptions(
    const std::vector<std::pair<std::string, duckdb::Value>> &options);

// Drives maintenance for one attached lake: a timer thread when the
// attach configured an interval, plus the on-demand trigger. Both run the
// same pass, and never two at once.
class MaintenanceScheduler {
public:
	MaintenanceScheduler(duckdb::DatabaseInstance &db, std::string lake, MoraineCatalogHandle *handle,
	                     MaintenanceConfig config);
	~MaintenanceScheduler();

	MaintenanceScheduler(const MaintenanceScheduler &) = delete;
	MaintenanceScheduler &operator=(const MaintenanceScheduler &) = delete;

	// Starts the timer thread if an interval is configured. A read-only
	// attach must never call this — maintenance mutates.
	void Start();

	// Signals the thread to stop and joins it. Idempotent, and safe to
	// call from the detach hook and again from the destructor.
	void Stop();

	// Runs one pass now, blocking until it finishes, and returns its
	// report. If a pass is already running, waits for it and runs after.
	std::vector<MaintenanceStep> RunNow();

	// The retained passes, newest first — empty before the first one. An
	// unattended pass has no one to return an error to, so this is how a
	// failing schedule becomes visible.
	std::vector<MaintenancePass> RecentPasses() const;

	// How many passes are retained. Bounded because this is in-memory
	// per attach, and a fast interval would otherwise grow without limit.
	static constexpr size_t RETAINED_PASSES = 16;

private:
	void Loop();
	// Runs the configured steps in order. `skip_if_busy` makes a tick
	// yield to a pass already in flight rather than queueing behind it.
	std::vector<MaintenanceStep> RunPass(bool skip_if_busy, const char *trigger);
	// Issues one `CALL ducklake_<step>(...)` on `connection`.
	MaintenanceStep RunDuckLakeStep(duckdb::Connection &connection, const DuckLakeStep &step);
	MaintenanceStep RunSweep();

	duckdb::DatabaseInstance &db_;
	std::string lake_;
	MoraineCatalogHandle *handle_;
	MaintenanceConfig config_;

	// Held for the duration of a pass, so the timer and the trigger can
	// never overlap.
	std::mutex pass_lock_;

	mutable std::mutex report_lock_;
	// Newest last; capped at RETAINED_PASSES.
	std::deque<MaintenancePass> passes_;

	std::mutex wake_lock_;
	std::condition_variable wake_;
	bool stopping_ = false;
	std::thread thread_;
};

void RegisterMoraineMaintenanceFunctions(duckdb::ExtensionLoader &loader);

} // namespace moraine_duckdb
