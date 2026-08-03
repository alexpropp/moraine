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
durable-commit-bound cost inherits this. Concurrency is the way out, not a
faster commit: K concurrent commits share one flush.

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
store the tick wait is replaced (or joined) by the WAL PUT round-trip — the
next two sections measure that term rather than assume it.

### Durable-commit latency vs. write round-trip

The composition of the two terms, measured by wrapping the in-memory store in
a `ThrottledStore` that sleeps before every PUT and sweeping that delay
against the flush interval. 30 sequential commits per cell, median:

| PUT round-trip | flush 1 ms | flush 25 ms | flush 100 ms |
|---|---|---|---|
| 0 ms | 2.2 ms | 26.4 ms | 101.4 ms |
| 5 ms | 7.5 ms | 26.3 ms | 100.9 ms |
| 25 ms | 27.5 ms | 27.6 ms | 101.0 ms |
| 100 ms | 102.7 ms | 102.7 ms | 102.6 ms |

Every cell is the **larger** of the two terms plus the ~2 ms compute floor,
never their sum: a commit waits for one flush, and that flush waits for
whichever of the tick and the PUT is slower. So the model is
`max(flush cadence, write RTT) + ~2 ms`, confirmed rather than posited. The
practical reading: lowering the flush interval below the store's write RTT
buys nothing, and a fast store cannot make up for a slow flush cadence.

### Durable-commit latency against a real endpoint

The same sweep against a live S3-compatible endpoint, where the PUT is the
endpoint's own round trip (`cargo xtask s3`, which runs
`object_storage.rs`'s `measure_commit_latency_against_the_endpoint` in
release against a pinned MinIO):

| flush interval | median commit | min | max |
|---|---|---|---|
| 1 ms | 2.5 ms | 1.5 ms | 3.7 ms |
| 25 ms | 25.9 ms | 22.5 ms | 29.9 ms |
| 100 ms | 101.4 ms | 83.0 ms | 116.7 ms |

A loopback MinIO's PUT costs about a millisecond, so it lands in the
`RTT ≈ 0` row of the table above and the flush cadence dominates everywhere:
this *validates the composition against a real object-storage protocol*, but
it understates S3, whose PUT is tens of milliseconds. For the number a given
deployment will see, point the same harness at the real bucket
(`MORAINE_S3_ENDPOINT`/`MORAINE_S3_BUCKET`); the injected-RTT table says what
to expect before you do.

### Commit throughput vs. concurrency

The sequential number above is one commit per flush by construction. This is
the concurrent one, now that concurrent commits coalesce into a shared batch:
K callers commit 128/K times each, every caller appending to a table of its
own, at the 100 ms default interval. Batch size is measured, not inferred —
the harness counts the WAL objects the burst wrote:

| concurrency | wall | commits/s | WAL writes | mean batch |
|---|---|---|---|---|
| 1 | 12.94 s | 9.9 | 128 | 1.0 |
| 2 | 6.47 s | 19.8 | 64 | 2.0 |
| 4 | 3.24 s | 39.6 | 32 | 4.0 |
| 8 | 1.62 s | 79.1 | 16 | 8.0 |
| 16 | 0.81 s | 158.3 | 8 | 16.0 |
| 32 | 0.40 s | 316.3 | 4 | 32.0 |
| 64 | 0.20 s | 632.1 | 2 | 64.0 |
| 128 | 0.20 s | 632.5 | 2 | 64.0 |

**Batch size equals concurrency, exactly, until the member ceiling binds.**
Throughput is therefore `concurrency / flush_interval` — linear, with no
coalescing overhead visible at any level: K commits cost the flushes of one.
The ceiling is `MAX_BATCH_MEMBERS` (64), and it binds precisely where it
says: 128 concurrent commits take the same wall-clock as 64 and land at the
same rate, because they arrive as two full batches instead of one. Raising
the ceiling is the lever if a workload ever needs past ~630 commits/s at the
default cadence; lowering the flush interval is the other, and the two
multiply.

This is what makes the single-writer topology's funnel affordable: routing a
fleet's commits through one process costs them a shared flush, not a queue.

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

This is the cost of a *cold* read — one whose handle holds no cached view.
A warm read serves from the projection cache and pays none of it. Median of
9 materializations, 8 columns and 16 files per table, a fresh handle per
repeat so no cache hit is timed:

| tables | live entities | median materialization | µs / 1 000 entities |
|---|---|---|---|
| 10 | 250 | 1.1 ms | 4 300 |
| 50 | 1 250 | 6.8 ms | 5 400 |
| 200 | 5 000 | 17.3 ms | 3 500 |
| 800 | 20 000 | 146 ms | 7 300 |

Materialization is roughly linear at ~5–7 µs per live entity — flush-interval
independent, since it is a read. A 20 000-entity catalog costs ~150 ms; a
100 000-entity catalog would cost most of a second. Readers now serve from
the cache rather than paying this per read; lazy materialization to bound
the cold cost on a large catalog stays open.

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
unnecessary on that axis. This isolates IO latency only; the next section
takes the compute axis.

### CPU-bound decode under worker pressure

The other half of the same question: a cold materialization decodes every
live record, and decode does not wait on anything — so does it hold its
worker long enough to crowd out a latency-sensitive call? This is the one
place a `spawn_blocking` discipline would earn its complexity.

No throttle here, so every millisecond is compute. K cold materializations
of a 400-table catalog (10 000 live entities) run concurrently on a fixed
4-worker runtime, with a small read on a one-table catalog queued alongside
them. Both catalogs are read through read-only handles, which maintain no
projection cache, so every read is a full materialization. Measured on an
**8-core Intel Xeon @ 2.90 GHz, x86-64** — a different machine from the rows
above, so compare the shape here, not the absolute numbers:

| concurrency | batch | per decode | small read alongside |
|---|---|---|---|
| 1 | 24.8 ms | 24.8 ms | 0.19 ms |
| 2 | 22.9 ms | 11.4 ms | 0.17 ms |
| 4 | 24.4 ms | 6.1 ms | 0.15 ms |
| 8 | 58.4 ms | 7.3 ms | 0.43 ms |

The same small read with an idle pool costs 0.11 ms.

Decode does not monopolize a worker. Four concurrent decodes cost what one
does (24.4 ms against 24.8 ms), so they genuinely overlap across the four
workers; the batch doubles only past the worker count, which is what CPU
work is supposed to do. The finding is the last column: the small read never
approaches a decode's duration. At 8 concurrent decodes — twice the worker
count — it costs 0.43 ms, ~4× its idle cost and ~1/50th of one decode. A
monopolized worker would have made it wait a whole decode.

So the decode's awaits yield often enough that a short call still gets
scheduled promptly, and a `spawn_blocking` discipline buys nothing on this
axis. Both halves of the worker-pool question are now answered the same
way: the pool does not need protecting from either IO latency or decode
compute.
