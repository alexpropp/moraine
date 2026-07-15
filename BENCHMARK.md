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
