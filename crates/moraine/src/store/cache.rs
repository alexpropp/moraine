//! The block cache every store in the process shares: one instance, two
//! slots, one budget.
//!
//! The meta slot holds SST indexes, filters, and stats in memory and
//! nothing evicts them but their own size — every point probe walks a
//! filter and an index before it can reach a data block, and letting data
//! blocks compete for that space is how a scan makes every later probe
//! pay a fetch to learn "not here". The block slot holds data blocks,
//! tiered to disk when a cache directory is configured.
//!
//! Sharing is what makes the budget mean anything: a per-store cache is
//! bounded per store, so a host attaching several catalogs is
//! over-committed by however many it attached.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
};
use slatedb::db_cache::{
    CachedEntry, CachedKey, DbCache, SplitCache,
    foyer::{FoyerCache, FoyerCacheOptions},
    foyer_hybrid::FoyerHybridCache,
    stats,
};
use slatedb_common::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};
use tokio::sync::OnceCell;
use tracing::{info, warn};

/// Memory the cache takes when no budget is configured: what SlateDB
/// gives a *single* store by default (512 MiB of blocks, 128 MiB of
/// metadata), now for the whole process. A single-store host is therefore
/// unchanged and a multi-store one is strictly smaller than before.
const DEFAULT_CACHE_MEMORY: u64 = 640 * 1024 * 1024;

/// The share of the memory budget the meta slot takes, as a divisor —
/// one fifth, which is SlateDB's own 128:512 split between metadata and
/// blocks. Metadata is a small fraction of any real store, so this is
/// meant to hold all of it; `moraine_store_census` reports the store's
/// index and filter bytes for sizing the budget against a store that
/// disagrees.
const META_SHARE_DIVISOR: u64 = 5;

/// Bytes of disk the block slot's device takes when no cap is
/// configured — SlateDB's own default for a store's cache.
pub(crate) const DEFAULT_CACHE_DISK: u64 = 16 * 1024 * 1024 * 1024;

/// How the process's cache is built. The first store to open decides it;
/// later stores share what it built, whatever they ask for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CacheConfig {
    /// Memory budget across both slots.
    pub(crate) memory: Option<u64>,
    /// The block slot's disk device, when one is configured.
    pub(crate) dir: Option<PathBuf>,
    /// That device's byte cap.
    pub(crate) disk_size: Option<u64>,
}

impl CacheConfig {
    /// Bytes for the meta slot and the block slot's memory tier.
    fn slots(&self) -> (u64, u64) {
        let budget = self.memory.unwrap_or(DEFAULT_CACHE_MEMORY).max(2);
        let meta = (budget / META_SHARE_DIVISOR).max(1);
        (meta, budget.saturating_sub(meta).max(1))
    }
}

/// The process's cache, built on first use. A `OnceCell` rather than a
/// per-store build: two catalogs attached in one process share one
/// budget, which is the whole point, and a cache that failed to build
/// once is not retried — the store simply runs uncached rather than
/// failing an attach over a cache.
static SHARED: OnceCell<Option<Arc<dyn DbCache>>> = OnceCell::const_new();

/// What the cache has served, by tier. Every sizing claim about the
/// budget is checkable only if the tiers report, so they do.
///
/// The counts are the process's, like the cache: every store's reads run
/// through the one instance and land here together.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheTally {
    /// Filter, index, and stats lookups the meta slot served.
    pub metadata_hits: u64,
    /// Those it did not, each one a fetch and a decode.
    pub metadata_misses: u64,
    /// Data-block lookups the block slot served, from either of its
    /// tiers — foyer reports a hybrid hit without saying which.
    pub block_hits: u64,
    /// Those it did not, each one an object-store read.
    pub block_misses: u64,
    /// Lookups the cache itself failed, which read through rather than
    /// failing the caller.
    pub errors: u64,
}

impl CacheTally {
    /// The share of lookups the cache served, `None` before it has served
    /// any. Metadata and blocks are counted apart because they are sized
    /// apart: a healthy stack has metadata near 1.0 and blocks wherever
    /// the working set puts them.
    #[must_use]
    pub fn metadata_hit_rate(&self) -> Option<f64> {
        rate(self.metadata_hits, self.metadata_misses)
    }

    /// As [`metadata_hit_rate`](Self::metadata_hit_rate), for data blocks.
    #[must_use]
    pub fn block_hit_rate(&self) -> Option<f64> {
        rate(self.block_hits, self.block_misses)
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a hit rate is a ratio; f64 holds counts far past any real one exactly"
)]
fn rate(hits: u64, misses: u64) -> Option<f64> {
    let total = hits.checked_add(misses).filter(|total| *total > 0)?;
    Some(hits as f64 / total as f64)
}

/// The counters SlateDB increments as the cache serves. Registered once
/// and shared by every store the process opens, so the tally spans them
/// all rather than the last one to attach.
#[derive(Debug, Default)]
struct CacheCounters {
    metadata_hits: AtomicU64,
    metadata_misses: AtomicU64,
    block_hits: AtomicU64,
    block_misses: AtomicU64,
    errors: AtomicU64,
}

impl CacheCounters {
    fn tally(&self) -> CacheTally {
        CacheTally {
            metadata_hits: self.metadata_hits.load(Ordering::Relaxed),
            metadata_misses: self.metadata_misses.load(Ordering::Relaxed),
            block_hits: self.block_hits.load(Ordering::Relaxed),
            block_misses: self.block_misses.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// One counter's home in [`CacheCounters`], or nowhere: SlateDB registers
/// far more than the cache's, and everything else increments a sink.
struct Slot {
    counters: Arc<CacheCounters>,
    which: Option<Which>,
}

#[derive(Clone, Copy)]
enum Which {
    MetadataHit,
    MetadataMiss,
    BlockHit,
    BlockMiss,
    Error,
}

impl CounterFn for Slot {
    fn increment(&self, value: u64) {
        let counter = match self.which {
            Some(Which::MetadataHit) => &self.counters.metadata_hits,
            Some(Which::MetadataMiss) => &self.counters.metadata_misses,
            Some(Which::BlockHit) => &self.counters.block_hits,
            Some(Which::BlockMiss) => &self.counters.block_misses,
            Some(Which::Error) => &self.counters.errors,
            None => return,
        };
        counter.fetch_add(value, Ordering::Relaxed);
    }
}

/// Routes SlateDB's cache counters into [`CacheCounters`] and drops
/// everything else on the floor.
///
/// The cache reports one counter per `(entry_kind, result)` pair —
/// `filter`, `index`, and `stats` are the meta slot's, `data_block` the
/// block slot's — so the routing is by label, and a kind SlateDB adds
/// later lands in neither rather than being miscounted as one.
#[derive(Debug)]
struct CacheRecorder {
    counters: Arc<CacheCounters>,
}

impl MetricsRecorder for CacheRecorder {
    fn register_counter(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let label = |key: &str| {
            labels
                .iter()
                .find_map(|(name, value)| (*name == key).then_some(*value))
        };
        let which = match name {
            stats::ERROR_COUNT => Some(Which::Error),
            stats::ACCESS_COUNT => match (label("entry_kind"), label("result")) {
                (Some("filter" | "index" | "stats"), Some("hit")) => Some(Which::MetadataHit),
                (Some("filter" | "index" | "stats"), Some("miss")) => Some(Which::MetadataMiss),
                (Some("data_block"), Some("hit")) => Some(Which::BlockHit),
                (Some("data_block"), Some("miss")) => Some(Which::BlockMiss),
                _ => None,
            },
            _ => None,
        };
        Arc::new(Slot {
            counters: Arc::clone(&self.counters),
            which,
        })
    }

    fn register_gauge(&self, _: &str, _: &str, _: &[(&str, &str)]) -> Arc<dyn GaugeFn> {
        Arc::new(Sink)
    }

    fn register_up_down_counter(
        &self,
        _: &str,
        _: &str,
        _: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        Arc::new(Sink)
    }

    fn register_histogram(
        &self,
        _: &str,
        _: &str,
        _: &[(&str, &str)],
        _: &[f64],
    ) -> Arc<dyn HistogramFn> {
        Arc::new(Sink)
    }
}

/// Every instrument this crate does not read.
struct Sink;

impl GaugeFn for Sink {
    fn set(&self, _: i64) {}
}

impl UpDownCounterFn for Sink {
    fn increment(&self, _: i64) {}
}

impl HistogramFn for Sink {
    fn record(&self, _: f64) {}
}

/// The counters behind the process's cache, alive from the first
/// registration so a store opened before anything reads them still counts.
static COUNTERS: std::sync::LazyLock<Arc<CacheCounters>> =
    std::sync::LazyLock::new(|| Arc::new(CacheCounters::default()));

/// The recorder every store's builder is given, so their reads tally
/// together.
pub(crate) fn recorder() -> Arc<dyn MetricsRecorder> {
    Arc::new(CacheRecorder {
        counters: Arc::clone(&COUNTERS),
    })
}

/// What the process's block cache has served since it was built —
/// metadata and data blocks counted apart, because they are sized apart.
///
/// Process-wide, like the cache itself: every catalog a process attaches
/// reads through the one instance, so these are the host's numbers rather
/// than any one catalog's. Use them to size
/// [`CatalogOptions::cache_memory`](crate::CatalogOptions::cache_memory)
/// and [`cache_size`](crate::CatalogOptions::cache_size) from measured
/// curves rather than from the defaults.
///
/// ```
/// let tally = moraine::cache_tally();
/// // Nothing has read yet, so there is no rate to report.
/// assert_eq!(
///     tally.metadata_hit_rate().is_none(),
///     tally.metadata_hits == 0 && tally.metadata_misses == 0
/// );
/// ```
#[must_use]
pub fn cache_tally() -> CacheTally {
    COUNTERS.tally()
}

/// The cache every store in this process shares, built to `config` if
/// nothing has built it yet. `None` when the disk tier could not be
/// opened *and* nothing else was configured — never an error: a cache is
/// an optimization, and no attach should fail because a device would not
/// open.
pub(crate) async fn shared(config: &CacheConfig) -> Option<Arc<dyn DbCache>> {
    let cache = SHARED
        .get_or_init(|| async {
            let built = build(config).await;
            let mut settled = BUILT_WITH
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *settled = Some(config.clone());
            built
        })
        .await
        .clone();

    // A later store asking for something else gets what was built. That
    // is the budget being the process's rather than the store's, and
    // refusing would fail an attach over a cache — but a mismatch nobody
    // can see is a host sized from options that never took effect, so it
    // is said out loud once per attach that disagrees.
    let settled = BUILT_WITH
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(settled) = settled.as_ref()
        && settled != config
    {
        warn!(
            requested_memory = config.memory,
            requested_disk_size = config.disk_size,
            requested_dir = ?config.dir,
            in_force_memory = settled.memory,
            in_force_disk_size = settled.disk_size,
            in_force_dir = ?settled.dir,
            "the block cache is process-wide and already built; this attach's cache options              are ignored. Set them on the first attach in the process."
        );
    }

    cache
}

/// What the process's cache was built with, for telling a later attach
/// that its own numbers did not take effect.
static BUILT_WITH: std::sync::Mutex<Option<CacheConfig>> = std::sync::Mutex::new(None);

async fn build(config: &CacheConfig) -> Option<Arc<dyn DbCache>> {
    let (meta_bytes, block_bytes) = config.slots();

    let meta: Arc<dyn DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: meta_bytes,
        ..FoyerCacheOptions::default()
    }));

    let block: Arc<dyn DbCache> = match &config.dir {
        Some(dir) => match hybrid(dir, block_bytes, config.disk_size).await {
            Some(cache) => cache,
            // The device is the only part that can fail, and a memory
            // tier alone is strictly better than no cache at all.
            None => Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: block_bytes,
                ..FoyerCacheOptions::default()
            })),
        },
        None => Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: block_bytes,
            ..FoyerCacheOptions::default()
        })),
    };

    info!(
        meta_bytes,
        block_bytes,
        disk = config.dir.is_some(),
        "built the shared block cache"
    );

    Some(Arc::new(
        SplitCache::new()
            .with_meta_cache(Some(meta))
            .with_block_cache(Some(block))
            .build(),
    ) as Arc<dyn DbCache>)
}

/// The block slot backed by memory over a disk device at `dir`. `None`
/// if the device will not open, which the caller degrades from rather
/// than failing.
async fn hybrid(dir: &PathBuf, memory: u64, disk: Option<u64>) -> Option<Arc<dyn DbCache>> {
    if let Err(error) = std::fs::create_dir_all(dir) {
        warn!(
            directory = %dir.display(),
            %error,
            "could not create the cache directory; the block cache stays in memory"
        );
        return None;
    }

    let capacity = usize::try_from(disk.unwrap_or(DEFAULT_CACHE_DISK)).unwrap_or(usize::MAX);
    let device = match FsDeviceBuilder::new(dir).with_capacity(capacity).build() {
        Ok(device) => device,
        Err(error) => {
            warn!(
                directory = %dir.display(),
                %error,
                "could not open the cache device; the block cache stays in memory"
            );
            return None;
        }
    };

    let built = HybridCacheBuilder::new()
        .memory(usize::try_from(memory).unwrap_or(usize::MAX))
        .with_weighter(|_: &CachedKey, value: &CachedEntry| value.size())
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(BlockEngineConfig::new(device))
        .build()
        .await;

    match built {
        Ok(cache) => Some(Arc::new(FoyerHybridCache::new_with_cache(cache))),
        Err(error) => {
            warn!(
                directory = %dir.display(),
                %error,
                "could not build the hybrid block cache; it stays in memory"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The budget splits into two slots that add up to it, and the meta
    /// slot is the smaller one — data blocks are what needs the room.
    #[test]
    fn the_budget_splits_across_both_slots() {
        let config = CacheConfig {
            memory: Some(1_000),
            ..CacheConfig::default()
        };
        let (meta, block) = config.slots();
        assert_eq!(meta + block, 1_000);
        assert!(meta < block, "meta {meta} should be the smaller slot");
    }

    /// An unset budget is what SlateDB gives one store, so a single-store
    /// host is unchanged by the move to a shared cache.
    #[test]
    fn an_unset_budget_matches_one_stores_defaults() {
        let (meta, block) = CacheConfig::default().slots();
        assert_eq!(meta + block, DEFAULT_CACHE_MEMORY);
        assert_eq!(meta, 128 * 1024 * 1024);
        assert_eq!(block, 512 * 1024 * 1024);
    }

    /// A later store's cache options do not take effect, and the process
    /// says so rather than leaving a host sized from numbers that never
    /// applied. The first config to arrive is what stands.
    #[tokio::test]
    async fn a_later_attachs_cache_options_are_reported_as_ignored() {
        let first = CacheConfig {
            memory: Some(4 * 1024 * 1024),
            ..CacheConfig::default()
        };
        let built = shared(&first).await;

        // Whatever the second asks for, it is served the first's cache.
        let second = CacheConfig {
            memory: Some(64 * 1024 * 1024),
            dir: Some(std::path::PathBuf::from("/tmp/moraine-ignored")),
            disk_size: Some(1),
        };
        let served = shared(&second).await;
        assert_eq!(built.is_some(), served.is_some());
        assert!(
            BUILT_WITH
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_some_and(|settled| settled.memory != second.memory),
            "the first config must be the one in force"
        );
    }

    /// The recorder routes SlateDB's cache counters by label: the meta
    /// slot's three entry kinds tally together, data blocks apart, and a
    /// kind nobody here models lands in neither rather than being
    /// miscounted as one.
    #[test]
    fn the_recorder_routes_counters_by_entry_kind() {
        let counters = Arc::new(CacheCounters::default());
        let recorder = CacheRecorder {
            counters: Arc::clone(&counters),
        };
        let counter = |kind: &str, result: &str| {
            recorder.register_counter(
                stats::ACCESS_COUNT,
                "",
                &[("entry_kind", kind), ("result", result)],
            )
        };

        counter("filter", "hit").increment(1);
        counter("index", "hit").increment(2);
        counter("stats", "miss").increment(3);
        counter("data_block", "hit").increment(4);
        counter("data_block", "miss").increment(5);
        counter("something_new", "hit").increment(99);
        recorder
            .register_counter(stats::ERROR_COUNT, "", &[])
            .increment(6);

        let tally = counters.tally();
        assert_eq!(tally.metadata_hits, 3);
        assert_eq!(tally.metadata_misses, 3);
        assert_eq!(tally.block_hits, 4);
        assert_eq!(tally.block_misses, 5);
        assert_eq!(tally.errors, 6);
    }

    /// A rate is reported only once something has been looked up, and is
    /// the served share of those lookups.
    #[test]
    fn hit_rates_need_a_lookup_to_report() {
        assert_eq!(CacheTally::default().metadata_hit_rate(), None);
        assert_eq!(CacheTally::default().block_hit_rate(), None);

        let tally = CacheTally {
            metadata_hits: 3,
            metadata_misses: 1,
            block_hits: 1,
            block_misses: 3,
            errors: 0,
        };
        assert!((tally.metadata_hit_rate().unwrap() - 0.75).abs() < f64::EPSILON);
        assert!((tally.block_hit_rate().unwrap() - 0.25).abs() < f64::EPSILON);
    }

    /// A budget too small to split still yields usable slots rather than
    /// a zero-capacity cache.
    #[test]
    fn a_tiny_budget_still_splits() {
        let config = CacheConfig {
            memory: Some(1),
            ..CacheConfig::default()
        };
        let (meta, block) = config.slots();
        assert!(meta >= 1 && block >= 1);
    }
}
