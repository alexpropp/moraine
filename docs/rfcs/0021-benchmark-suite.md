# RFC 0021: Benchmark suite

- **Date:** 2026-07-13

## Summary

A `cargo xtask bench` command that runs identical DuckLake workloads against
three metadata catalogs — moraine's SlateDB store, a stock DuckDB-file
catalog, and a stock Postgres catalog — through the same pinned DuckDB CLI,
and reports per-workload wall-clock timings side by side. The data layer is
Parquet under a local `DATA_PATH` in every configuration; only the catalog
backend varies, so differences isolate metadata-path cost — the thing moraine
replaces.

## Goals

- One command produces a comparable table: workload × backend, median over
  repeats, with min/max spread.
- Every backend runs the byte-identical SQL statement stream through the same
  DuckDB binary, so numbers differ only by catalog backend.
- Workloads cover both catalog-dominated paths (many small commits, DDL,
  attach, snapshot listing, maintenance) and data-dominated paths (bulk load,
  scans) — the latter as a sanity check that the data plane is unaffected.
- The Postgres backend self-provisions when Postgres binaries are on the
  machine and degrades to a skip (with a notice) when they are not; the suite
  never fails because a backend is unavailable.
- Non-goals: concurrency/contention benchmarks, remote object stores,
  cross-machine reproducibility, statistical rigor beyond median/min/max, and
  CI execution (the suite is a local tool; CI keeps running `e2e`).

## Design

### Command surface

```text
cargo xtask bench [--backends moraine,duckdb,postgres]
                  [--workloads <name>,...]
                  [--scale small|medium|large]
                  [--repeat N]
```

Defaults: all backends (Postgres skipped with a notice if unavailable), all
workloads, `--scale small`, `--repeat 3`. `bench` reuses `e2e`'s plumbing: it
downloads/caches the pinned DuckDB CLI, builds and packages the
`moraine_duckdb` extension, and caches `INSTALL ducklake` / `INSTALL postgres`
artifacts under `target/duckdb-extensions/`.

### Backends

Each backend is an `ATTACH` recipe over a fresh per-run directory; everything
else in the session is shared.

- `moraine` — `ATTACH 'ducklake:moraine:<store_dir>' (DATA_PATH '<data_dir>')`
  after `LOAD`ing the packaged extension. SlateDB over the local filesystem.
- `duckdb` — `ATTACH 'ducklake:<dir>/meta.ducklake' (DATA_PATH '<data_dir>')`.
  The stock, all-files DuckLake.
- `postgres` — `ATTACH 'ducklake:postgres:dbname=<db> host=<socket_dir>
  port=<port>' (DATA_PATH '<data_dir>')`. The harness provisions an ephemeral
  cluster per bench run: `initdb` into a temp dir, `pg_ctl start` listening
  only on a Unix socket (no TCP), one database per (workload, repeat), stock
  configuration, `pg_ctl stop` on exit (including on failure, via a drop
  guard). Binary discovery: `$PATH` first, then the newest
  `/opt/homebrew/opt/postgresql@*/bin`. `MORAINE_BENCH_POSTGRES=<libpq DSN>`
  overrides provisioning and points at an existing server (the harness then
  creates and drops its scratch databases there). If neither is available the
  backend reports `skipped`.

The moraine extension is `LOAD`ed in every session — including stock ones —
so session preambles are identical across backends.

### Timing mechanics

One CLI process per (backend, workload, repeat), fed a script on stdin:

```text
.timer on
<statement 1>;
<statement 2>;
...
```

With `.timer on`, the CLI prints `Run Time (s): real R user U sys S` after
every SQL statement, in statement order; dot-commands print none. The harness
builds each workload as an ordered list of statements, each tagged with a
phase label or `setup`, then zips the `Run Time` lines with the SQL statements
by index. A phase's time is the sum of its statements' `real` values; `setup`
statements are executed but not reported. A count mismatch between statements
and `Run Time` lines is a hard error, not a partial report.

Process start-up, extension loading, and fixture seeding are thus excluded;
`ATTACH` itself is a timed statement so catalog-open cost is a first-class
phase. The first statement in a session absorbs some one-time initialization,
so every session opens with a throwaway `SELECT 1` tagged `setup`. Threads
are left at DuckDB's default; both sides of every comparison share it.

Workloads that measure reads against pre-existing state get two sessions: an
untimed seeding session (same backend, same directories), then the measured
session — so the measured `ATTACH` is a genuinely cold open over a populated
catalog.

### Workloads

Scales: `small` / `medium` / `large` set (bulk rows N, small commits K,
tables T) to (100k, 20, 10) / (1M, 50, 25) / (10M, 200, 100).

- `bulk_load` — `CREATE TABLE` + one `INSERT … FROM range(N)`. Phases:
  `attach`, `create_table`, `insert`. Data-plane dominated.
- `small_commits` — K autocommitted single-row `INSERT`s into one table.
  Phases: `attach`, `inserts` (sum of K). The headline catalog-latency
  number; also reported per-commit in the table.
- `many_tables` — T × `CREATE TABLE`. Phases: `attach`, `creates`. DDL
  commit path.
- `scan` — seeded with the `bulk_load` shape, then measured: `attach`,
  `full_scan` (`SELECT sum(...)`), `filtered_scan` (`WHERE id = N/2`),
  `time_travel` (`AT (VERSION => 1)` count), `snapshots`
  (`SELECT count(*) FROM ducklake_snapshots('lake')`).
- `maintenance` — seeded with K small commits, then measured: `attach`,
  `merge` (`CALL ducklake_merge_adjacent_files('lake')`), `expire`
  (`CALL ducklake_expire_snapshots('lake', older_than => now())`), `cleanup`
  (`CALL ducklake_cleanup_old_files('lake', cleanup_all => true)`).

Every (workload, repeat) runs in fresh directories (and, for Postgres, a
fresh database) so repeats are independent; the report is the median across
repeats with min/max.

### Report

Stdout gets one aligned table: rows are `workload/phase`, columns are
backends, cells are `median (min…max)` in adaptive units (µs/ms/s), plus a
`×` ratio column relative to `moraine` where both ran. The same data is
written as JSON to `target/bench/report.json` (schema: run metadata —
date-free machine facts like scale, repeat, backend versions — then
`results[{workload, phase, backend, seconds: [per repeat]}]`), for diffing
across checkouts.

### Structure

`xtask` splits into `main.rs` (dispatch), `e2e.rs`, `duckdb.rs` (shared CLI
download/build/packaging helpers), and `bench.rs` with `bench/` submodules
(`backends.rs`, `workloads.rs`, `timing.rs`, `report.rs`). Timer-line
parsing, statement/phase zipping, statistics, and table formatting are pure
functions with unit tests; proptest is not required (nothing here is a
store codec). `xtask` gains no new dependencies beyond `anyhow` unless
JSON emission demands one — JSON is emitted via a small hand-rolled writer
to keep the tool dependency-free.

## Alternatives considered

- **Criterion/`duckdb-rs` in-process benches** — rejected: the bundled
  DuckDB version would drift from the pinned CLI the extension is packaged
  against, the dependency footprint is large, and it would not exercise the
  real attach chain users run.
- **Timing whole CLI invocations from the harness** — rejected: process
  start-up and `INSTALL`/`LOAD` noise swamps sub-millisecond catalog
  differences; `.timer on` gives per-statement real time inside one session.
- **Docker-provisioned Postgres** — rejected: slower to start, requires a
  running daemon, and local `initdb`/`pg_ctl` is already available wherever
  libpq-based tooling is; the DSN override covers exotic setups.
- **Shell-script harness** — rejected by repository policy: automation is
  Rust under `xtask`.
