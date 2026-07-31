# Benchmarking

`cargo xtask bench` runs identical DuckLake workloads against three metadata
catalogs — moraine's SlateDB store, a stock DuckDB-file catalog, and a stock
Postgres catalog — through the same pinned DuckDB CLI, and reports per-phase
wall-clock timings side by side. The data layer is Parquet under a local
`DATA_PATH` in every configuration; only the catalog backend varies, so the
numbers isolate metadata-path cost — the thing moraine replaces.

## Running it

```text
cargo xtask bench [--backends moraine,duckdb,postgres]
                  [--workloads <name>,...]
                  [--scale small|medium|large]
                  [--repeat N]
```

Defaults: all backends, all workloads, `--scale small`, `--repeat 3`. It
reuses the `e2e` plumbing — downloads/caches the pinned DuckDB CLI, builds and
packages the extension, caches the `ducklake`/`postgres` extensions.

The Postgres backend self-provisions an ephemeral cluster (`initdb` +
`pg_ctl` on a Unix socket, torn down on exit) from Postgres binaries on
`$PATH` or under `/opt/homebrew/opt/postgresql@*`. Set
`MORAINE_BENCH_POSTGRES=<libpq DSN>` to use an existing server instead. If no
Postgres is found, that backend is skipped with a notice — the suite never
fails because a backend is unavailable.

moraine's per-commit latency is bounded by its WAL flush cadence (100ms by
default); the bench pins it low so `small_commits` measures catalog work
rather than the flush wait. Tune it on any attach with
`META_FLUSH_INTERVAL_MS <n>`, at the cost of more frequent object-store PUTs.

## Workloads

`small` / `medium` / `large` scale (bulk rows, small commits, tables) as
(100k, 20, 10) / (1M, 50, 25) / (10M, 200, 100).

| workload | measures |
|---|---|
| `bulk_load` | `CREATE TABLE` + one large `INSERT` — data-plane dominated |
| `small_commits` | K single-row `INSERT`s — the headline catalog-latency number |
| `many_tables` | T × `CREATE TABLE` — the DDL commit path |
| `scan` | full/filtered scans, time travel, snapshot listing over a seeded table |
| `maintenance` | `merge_adjacent_files`, `expire_snapshots`, `cleanup_old_files` |

Every backend runs the byte-identical SQL through the same DuckDB binary, one
statement timed at a time via `.timer on`, so differences come only from the
catalog backend. `ATTACH` is a timed phase, so catalog-open cost is measured
too. Each (workload, repeat) runs in fresh directories, so repeats are
independent.

## Results

Stdout prints an aligned table — rows are `workload/phase`, columns are
backends, cells are `median (min…max)`, plus a ratio column against moraine.
The same data lands in `target/bench/report.json` for diffing across
checkouts.

Not covered: concurrency/contention, remote object stores, cross-machine
reproducibility, or statistical rigor beyond median/min/max. It's a local
tool; CI runs `e2e`, not this.

# Core measurements

`cargo xtask bench` times the whole stack through the DuckDB CLI. A second,
finer harness times moraine's core directly, for questions the end-to-end
tool cannot isolate — materialization cost, commit latency, reclaim
throughput. These live as `#[ignore]`d tests, one per question, in
`crates/moraine/tests/it/measure.rs`:

```text
cargo test -p moraine --test it --release -- --ignored --nocapture measure_
```

Run in `--release`; a debug build's numbers are meaningless. They use the
in-memory `object_store`, so a durable commit performs no network IO — the
durable cost measured is moraine's flush poll plus compute, and a real object
store adds its WAL-object PUT round-trip on top. Each harness prints its
conditions.

The results below are one representative run on an **Apple M2 (Mac14,2),
arm64**. Absolute numbers are machine-specific; the shapes and ratios are the
findings.

**One thing dominates: the WAL flush interval.** A durable commit waits for
the next flush tick, so at the 100 ms default a single commit costs ~100 ms —
regardless of backend, and swamping the ~2 ms of compute underneath. Every
durable-commit-bound cost inherits this.

### Durable-commit latency vs. flush interval

60 sequential `await_durable` commits, per interval:

| flush interval | median commit |
|---|---|
| 1 ms | 2.8 ms |
| 10 ms | 12.7 ms |
| 50 ms | 52.5 ms |
| 100 ms (default) | 102.7 ms |
| 250 ms | 252.7 ms |

Commit latency is `flush_interval + ~2 ms`. The ~2 ms is the real compute
floor; everything above it is the wait for the flush tick. On a real object
store the tick wait is replaced (or joined) by the WAL PUT round-trip, so the
per-backend commit latency is `max(flush cadence, write RTT) + ~2 ms`.

### Reclaim throughput vs. maintenance batch size

50 000 dead index entries swept at the 100 ms default; each batch is one
durable commit, so `commits = ceil(entries / batch)`:

| batch | commits | median sweep | entries/s |
|---|---|---|---|
| 256 | 196 | 20.3 s | 2 467 |
| 1024 (strawman) | 49 | 5.08 s | 9 849 |
| 4096 | 13 | 1.34 s | 37 378 |
| 16384 | 4 | 0.41 s | 120 948 |
| 65536 | 1 | 0.14 s | 355 768 |

Sweep time is `commits × flush_interval` to within a few percent — the
per-entry compute (~1 µs) is negligible. The reclaim loop awaits durability
*per batch*, so the whole sweep is flush-bound. The strawman 1024 spends ~4.9 s
of the 5.08 s waiting on flush ticks. Two independent levers cut that: a much
larger batch (16k–64k here), or not awaiting durability on every intermediate
batch — the entries are tombstoned idempotently, so only the last batch needs
to be durable. The second lever makes batch size almost irrelevant and is the
better fix; recorded as a follow-up in `docs/rfcs/tasks.md` under 0021.

### Materialization cost vs. catalog size

`Catalog::snapshot()` calls `materialize` and never consults the projection
cache, so this cost is paid on *every* public read, not just a cold start.
Median of 9 materializations, 8 columns and 16 files per table:

| tables | live entities | median materialization | µs / 1 000 entities |
|---|---|---|---|
| 10 | 250 | 1.1 ms | 4 300 |
| 50 | 1 250 | 6.8 ms | 5 400 |
| 200 | 5 000 | 17.3 ms | 3 500 |
| 800 | 20 000 | 146 ms | 7 300 |

Materialization is roughly linear at ~5–7 µs per live entity — flush-interval
independent, since it is a read. A 20 000-entity catalog costs ~150 ms per
read; extrapolated, a 100 000-entity catalog would cost most of a second on
every `snapshot()`. That is the quantitative case for the two open 0009 items:
serving readers from the maintained cache instead of rematerializing, and,
further out, lazy materialization to bound the per-read cost on a large
catalog.

### Read concurrency under IO latency

Does slow object-store IO starve the worker pool, so concurrent scans
serialize instead of overlapping? The in-memory store is wrapped in a
`ThrottledStore` adding 10 ms to every GET/LIST, seeded unthrottled, then K
concurrent `snapshot()` materializations run on a fixed 4-worker runtime:

| concurrency | batch | per op |
|---|---|---|
| 1 | 50.5 ms | 50.5 ms |
| 2 | 51.9 ms | 26.0 ms |
| 4 | 53.4 ms | 13.3 ms |
| 8 | 54.9 ms | 6.9 ms |
| 16 | 55.0 ms | 3.4 ms |
| 32 | 63.8 ms | 2.0 ms |

The batch stays flat as concurrency grows to 32 — ~8× the worker count — while
the per-op cost falls almost exactly 1/K. The IO awaits yield the worker back
to the pool, so the latency of 32 reads overlaps into the time of roughly one;
serialization would have made the batch grow to ~1.6 s. So object-store read
latency does not starve the pool, and a separate IO-dedicated runtime is
unnecessary on that axis. This isolates IO latency only: CPU-bound SST decode
monopolizing a worker is a separate axis this does not probe, and is where a
`spawn_blocking` discipline would matter if anywhere.
