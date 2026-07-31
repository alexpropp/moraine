//! Measurement harnesses for the `MEASURE` items in `docs/rfcs/tasks.md`.
//! These are not assertions — they print timings, and are `#[ignore]`d so
//! the ordinary suite stays fast. Run one explicitly and read its output:
//!
//! ```text
//! cargo test -p moraine --test it --release -- --ignored --nocapture measure_
//! ```
//!
//! Every number is machine- and backend-specific; the run prints its
//! conditions so a recorded result stays interpretable. They run on the
//! in-memory `object_store`, where a durable commit performs no network IO
//! — so the durable-commit cost measured here is moraine's flush poll plus
//! compute, and a real object store adds its WAL-object PUT round-trip on
//! top. The harness makes that structure explicit rather than hiding it.

// Benchmark code: entity/entry counts cast to f64 for rate math (precision
// loss on a count is immaterial), and `Stats`' fields share a `_ms` unit
// suffix by design.
#![allow(clippy::cast_precision_loss, clippy::struct_field_names)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use moraine::{
    Catalog, CatalogOptions, ColumnId, IndexDef, IndexEntry, IndexKeyValue, IntWidth,
    MaintenanceRequest,
};
use object_store::{
    memory::InMemory,
    throttle::{ThrottleConfig, ThrottledStore},
};

use crate::fixtures::{col, datafile};

/// Median, min, and max of a set of samples, in milliseconds.
struct Stats {
    median_ms: f64,
    min_ms: f64,
    max_ms: f64,
}

impl Stats {
    fn of(mut samples: Vec<Duration>) -> Self {
        samples.sort();
        let ms = |d: Duration| d.as_secs_f64() * 1_000.0;
        Self {
            median_ms: ms(samples[samples.len() / 2]),
            min_ms: ms(samples[0]),
            max_ms: ms(samples[samples.len() - 1]),
        }
    }
}

#[allow(clippy::unwrap_used)]
async fn open_with(store: Arc<InMemory>, flush_ms: u64) -> Catalog {
    let mut options = CatalogOptions::default();
    options.flush_interval = Duration::from_millis(flush_ms);
    Catalog::open(store, options).await.unwrap()
}

/// The default handle flush interval, so a "seed fast, measure at default"
/// harness names the realistic figure rather than a magic number.
const DEFAULT_FLUSH_MS: u64 = 100;
/// A fast interval for seeding, whose durable waits are not being timed.
const SEED_FLUSH_MS: u64 = 1;

/// 0009 — catalog materialization versus live-entity count.
///
/// `Catalog::snapshot()` calls `materialize` directly and never consults
/// the projection cache (which only committers populate), so this is not a
/// cold-start cost — it is paid on *every* public read. The RFC leaves open
/// whether that full materialization stays cheap enough to make partial or
/// lazy materialization unnecessary; this puts a curve under it. Timing
/// covers only the read, so the seed's flush interval is irrelevant and set
/// fast.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_materialization_by_catalog_size() {
    const COLS_PER_TABLE: usize = 8;
    const FILES_PER_TABLE: usize = 16;
    const REPEATS: usize = 9;
    let ladder = [10usize, 50, 200, 800];

    println!("\n# 0009 materialization per snapshot() (in-memory object_store)");
    println!(
        "# {COLS_PER_TABLE} columns and {FILES_PER_TABLE} files per table, \
         median of {REPEATS} materializations\n"
    );
    println!(
        "{:>7}  {:>10}  {:>11}  {:>9}  {:>9}  {:>12}",
        "tables", "entities", "median_ms", "min_ms", "max_ms", "us_per_1k"
    );

    for &tables in &ladder {
        let store = Arc::new(InMemory::new());
        let catalog = open_with(store.clone(), SEED_FLUSH_MS).await;

        let columns: Vec<_> = (0..COLS_PER_TABLE).map(|c| col(&format!("c{c}"))).collect();
        for t in 0..tables {
            let columns = columns.clone();
            catalog
                .commit(move |tx| {
                    let schema = tx.schema_by_name("main").expect("bootstrap").id;
                    let table = tx.create_table(schema, &format!("t{t}"), &columns)?;
                    for _ in 0..FILES_PER_TABLE {
                        tx.register_data_file(table, datafile(100), &[])?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
        }
        catalog.close().await.unwrap();

        // Live entity count, measured rather than assumed.
        let probe = open_with(store.clone(), SEED_FLUSH_MS).await;
        let view = probe.snapshot().await.unwrap();
        let entities: usize = view
            .schemas()
            .iter()
            .flat_map(|s| view.tables_in(s.id))
            .map(|t| 1 + view.columns_of(t.id).len() + view.data_files_of(t.id).len())
            .sum();

        probe.close().await.unwrap();

        // A fresh handle per repeat: `snapshot()` serves from the maintained
        // cache, so reusing one would time a cache hit and report
        // materialization as free. Only the read is timed, not the open.
        let mut samples = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let probe = open_with(store.clone(), SEED_FLUSH_MS).await;
            let start = Instant::now();
            let view = probe.snapshot().await.unwrap();
            samples.push(start.elapsed());
            std::hint::black_box(&view);
            probe.close().await.unwrap();
        }

        let stats = Stats::of(samples);
        let us_per_1k = (stats.median_ms * 1_000.0) / (entities as f64 / 1_000.0);
        println!(
            "{tables:>7}  {entities:>10}  {:>11.3}  {:>9.3}  {:>9.3}  {us_per_1k:>12.1}",
            stats.median_ms, stats.min_ms, stats.max_ms
        );
    }
    println!();
}

/// 0004 — durable-commit latency versus flush interval.
///
/// `Catalog::commit` awaits durability, and on the in-memory store the WAL
/// "flush" is driven by the handle's flush-poll timer, not an immediate
/// write. So the durable wait is dominated by the flush interval, and this
/// sweep exposes that: the compute floor shows at a 1 ms interval, and the
/// default 100 ms interval shows the realistic per-commit latency. A real
/// object store replaces the poll wait with its WAL-object PUT round-trip;
/// this is the term the 0021 sweep multiplies by its commit count.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_commit_latency_by_flush_interval() {
    const COMMITS: usize = 60;
    let intervals = [1u64, 10, 50, 100, 250];

    println!("\n# 0004 durable-commit latency by flush interval (in-memory)");
    println!("# {COMMITS} sequential one-table commits, each await_durable\n");
    println!(
        "{:>10}  {:>11}  {:>9}  {:>9}",
        "flush_ms", "median_ms", "min_ms", "max_ms"
    );

    for &flush_ms in &intervals {
        let store = Arc::new(InMemory::new());
        let catalog = open_with(store, flush_ms).await;
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                tx.create_table(schema, "warmup", &[col("a")])?;
                Ok(())
            })
            .await
            .unwrap();

        let mut samples = Vec::with_capacity(COMMITS);
        for i in 0..COMMITS {
            let start = Instant::now();
            catalog
                .commit(move |tx| {
                    let schema = tx.schema_by_name("main").expect("bootstrap").id;
                    tx.create_table(schema, &format!("t{i}"), &[col("a")])?;
                    Ok(())
                })
                .await
                .unwrap();
            samples.push(start.elapsed());
        }
        catalog.close().await.unwrap();

        let stats = Stats::of(samples);
        println!(
            "{flush_ms:>10}  {:>11.3}  {:>9.3}  {:>9.3}",
            stats.median_ms, stats.min_ms, stats.max_ms
        );
    }
    println!();
}

/// 0021 — reclaim throughput versus maintenance batch size.
///
/// Seeds a dropped (dead) non-unique index carrying many entries, then
/// times a full sweep at a ladder of batch sizes. Each batch is one durable
/// commit, so at the default flush interval reclaim time is roughly
/// (commit count x per-commit durable latency) + entry compute — and
/// commit count is entries/batch. The measured curve says where enlarging
/// the batch stops buying anything, which is the defensible-default
/// question. Seeded at a fast interval (untimed), swept at the default.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_reclaim_by_batch_size() {
    const DEAD_ENTRIES: u64 = 50_000;
    const REPEATS: usize = 3;

    async fn seed(store: Arc<InMemory>) {
        let catalog = open_with(store, SEED_FLUSH_MS).await;
        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                let table = tx.create_table(schema, "t", &[col("a")])?;
                let def = IndexDef {
                    name: "idx".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                };
                let entries: Vec<_> = (0..DEAD_ENTRIES)
                    .map(|row_id| IndexEntry {
                        row_id,
                        values: vec![Some(IndexKeyValue::Int {
                            value: i128::from(row_id),
                            width: IntWidth::I64,
                        })],
                    })
                    .collect();
                index.set(Some(tx.create_index(table, &def, &entries)?));
                Ok(())
            })
            .await
            .unwrap();
        let index = index.get().expect("index created");
        catalog
            .commit(move |tx| tx.drop_index(index))
            .await
            .unwrap();
        catalog.close().await.unwrap();
    }

    let ladder = [256usize, 1024, 4096, 16384, 65536];

    println!("\n# 0021 reclaim sweep by batch size (in-memory object_store)");
    println!(
        "# {DEAD_ENTRIES} dead entries in one index, median of {REPEATS} sweeps at the \
         {DEFAULT_FLUSH_MS} ms default flush interval;"
    );
    println!("# commits is analytic ceil(entries/batch)\n");
    println!(
        "{:>10}  {:>8}  {:>11}  {:>9}  {:>9}  {:>14}",
        "batch", "commits", "median_ms", "min_ms", "max_ms", "entries_per_s"
    );

    for &batch in &ladder {
        let mut samples = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let store = Arc::new(InMemory::new());
            seed(store.clone()).await;

            let mut request = MaintenanceRequest::default();
            request.batch_size = batch;

            let catalog = open_with(store, DEFAULT_FLUSH_MS).await;
            let start = Instant::now();
            let report = catalog.maintain(request).await.unwrap();
            let elapsed = start.elapsed();
            assert_eq!(report.index_entries_reclaimed, DEAD_ENTRIES);
            samples.push(elapsed);
            catalog.close().await.unwrap();
        }

        let commits = DEAD_ENTRIES.div_ceil(batch as u64);
        let stats = Stats::of(samples);
        let entries_per_s = f64::from(u32::try_from(DEAD_ENTRIES).unwrap_or(u32::MAX))
            / (stats.median_ms / 1_000.0);
        println!(
            "{batch:>10}  {commits:>8}  {:>11.3}  {:>9.3}  {:>9.3}  {entries_per_s:>14.0}",
            stats.median_ms, stats.min_ms, stats.max_ms
        );
    }
    println!();
}

/// 0010 — does object-store read latency starve the shared worker pool?
///
/// The concern is a multi-threaded runtime whose worker threads all block
/// on slow object-store reads, so concurrent scans serialize instead of
/// overlapping their waits. This wraps the in-memory store in a
/// `ThrottledStore` that adds a fixed latency to every GET/LIST, seeds a
/// small catalog *unthrottled*, then runs K concurrent `snapshot()`
/// materializations on a fixed 4-worker runtime and times the batch.
///
/// If the IO awaits yield the worker back to the pool, K concurrent
/// materializations overlap their latency and the batch stays near the
/// time of one — flat as K grows past the worker count. If SlateDB blocked
/// a worker for the duration of each read, the batch would grow with K once
/// the workers are exhausted. This isolates the IO-latency axis only;
/// CPU-bound decode monopolizing a worker is a separate question this does
/// not probe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_read_concurrency_under_io_latency() {
    const READ_LATENCY_MS: u64 = 10;
    const TABLES: usize = 20;
    const REPEATS: usize = 5;
    let concurrency = [1usize, 2, 4, 8, 16, 32];

    // Seed unthrottled: writes are not the axis under test.
    let raw = Arc::new(InMemory::new());
    {
        let catalog = open_with(raw.clone(), SEED_FLUSH_MS).await;
        for t in 0..TABLES {
            catalog
                .commit(move |tx| {
                    let schema = tx.schema_by_name("main").expect("bootstrap").id;
                    let table = tx.create_table(schema, &format!("t{t}"), &[col("a")])?;
                    for _ in 0..8 {
                        tx.register_data_file(table, datafile(100), &[])?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
        }
        catalog.close().await.unwrap();
    }

    let config = ThrottleConfig {
        wait_get_per_call: Duration::from_millis(READ_LATENCY_MS),
        wait_list_per_call: Duration::from_millis(READ_LATENCY_MS),
        ..ThrottleConfig::default()
    };
    // A derived `InMemory` clone shares the backing store (only `fork`
    // copies), so the throttled handle sees the seeded data.
    let throttled = Arc::new(ThrottledStore::new((*raw).clone(), config));
    let mut options = CatalogOptions::default();
    options.flush_interval = Duration::from_millis(DEFAULT_FLUSH_MS);
    let catalog = Catalog::open(throttled, options).await.unwrap();

    println!("\n# 0010 read concurrency under {READ_LATENCY_MS} ms IO latency");
    println!("# {TABLES}-table catalog, 4-worker runtime, median of {REPEATS} batches\n");
    println!(
        "{:>12}  {:>11}  {:>13}",
        "concurrency", "batch_ms", "per_op_ms"
    );

    for &k in &concurrency {
        let mut samples = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let start = Instant::now();
            let handles: Vec<_> = (0..k)
                .map(|_| {
                    let catalog = catalog.clone();
                    tokio::spawn(async move { catalog.snapshot().await.map(|_| ()) })
                })
                .collect();
            for handle in handles {
                handle.await.unwrap().unwrap();
            }
            samples.push(start.elapsed());
        }
        let stats = Stats::of(samples);
        let per_op = stats.median_ms / k as f64;
        println!("{k:>12}  {:>11.3}  {per_op:>13.3}", stats.median_ms);
    }
    catalog.close().await.unwrap();
    println!();
}
