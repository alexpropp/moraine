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

use std::{path::PathBuf, sync::Arc};

use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
};
use slatedb::db_cache::{
    CachedEntry, CachedKey, DbCache, SplitCache,
    foyer::{FoyerCache, FoyerCacheOptions},
    foyer_hybrid::FoyerHybridCache,
};
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

/// The cache every store in this process shares, built to `config` if
/// nothing has built it yet. `None` when the disk tier could not be
/// opened *and* nothing else was configured — never an error: a cache is
/// an optimization, and no attach should fail because a device would not
/// open.
pub(crate) async fn shared(config: &CacheConfig) -> Option<Arc<dyn DbCache>> {
    SHARED
        .get_or_init(|| async { build(config).await })
        .await
        .clone()
}

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
