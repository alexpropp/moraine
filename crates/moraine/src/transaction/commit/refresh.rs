//! Advancing a held view to head at a cost proportional to churn.
//!
//! A held view differs from head only in the entities the intervening
//! commits touched, and each of those commits recorded exactly which
//! `current` keys it wrote. Reading those records names the changed keys;
//! re-reading the keys themselves supplies their final state. Both, like a
//! materialization, run under one pinned handle, so the refreshed view is
//! one consistent cut rather than a mix of per-step reads torn by a
//! concurrent commit.
//!
//! Final state per key, rather than each commit's delta in order, is what
//! collapses the work: a file registered and compacted away across the gap
//! costs one read that finds nothing, not two applications.

use std::{collections::BTreeSet, sync::Arc};

use super::{fold, refuse_mid_migration};
use crate::{
    catalog::CatalogSnapshot,
    error::{Error, Result},
    store::{handle::ReadHandle, key::Key, read},
};

/// Churn past this fraction of the held view's live records costs more to
/// replay than to rescan, so the refresh abandons. Measured: a scan runs
/// ~1.4 µs per live entity, while a replay pays ~0.07 µs per entity to copy
/// the view plus ~3 µs per changed key, which meet near two fifths. Reads
/// against a real object store make a point read dearer relative to a
/// sequential scan, so the true crossover there sits lower; erring low only
/// forfeits some speedup, while erring high is slower than not refreshing.
const CHURN_BUDGET_NUMERATOR: usize = 2;
const CHURN_BUDGET_DENOMINATOR: usize = 5;

/// What a refresh attempt concluded about a held view.
pub(crate) enum Refreshed {
    /// Head has not moved; the held view is already current and the caller
    /// keeps it as-is. This is the common case for a polling reader, and
    /// resolving it without producing a new view is what keeps it cheap.
    Unchanged,
    /// The view advanced to a new head. Shared rather than owned, because
    /// every caller installs it in the cache and hands it out.
    Advanced(Arc<CatalogSnapshot>),
    /// The gap cannot be replayed, or replaying it would cost more than a
    /// scan; the caller rematerializes. Both paths produce the same view,
    /// so this is always a cost or a missing-input answer, never a
    /// correctness one.
    Rescan,
}

/// Advances `base` to the current head.
pub(crate) async fn refresh(handle: ReadHandle<'_>, base: &CatalogSnapshot) -> Result<Refreshed> {
    refuse_mid_migration(handle).await?;

    let head = read::read_head(handle)
        .await?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))?
        .snapshot_id;
    let from = base.snapshot.snapshot_id;

    if head == from {
        return Ok(Refreshed::Unchanged);
    }
    // A head behind the held view means the view did not come from this
    // store's timeline; rematerializing is the only defined answer.
    if head < from {
        return Ok(Refreshed::Rescan);
    }

    let budget = base.live_record_count() * CHURN_BUDGET_NUMERATOR / CHURN_BUDGET_DENOMINATOR;
    let Some(changed) = changed_keys(handle, from, head, budget).await? else {
        return Ok(Refreshed::Rescan);
    };

    let snapshot = read::read_snapshot(handle, head).await?.ok_or_else(|| {
        Error::SnapshotExpired(format!("snapshot {head} (expired or never minted)"))
    })?;

    let mut view = base.clone();
    view.snapshot = snapshot;
    for key in changed {
        let bytes = handle.get(&key).await?;
        match Key::decode(&key)? {
            Key::Current(current) => fold::apply_current(&mut view, current, bytes.as_deref())?,
            other => {
                return Err(Error::Corruption(format!(
                    "commit delta names a non-current key: {other:?}"
                )));
            }
        }
    }

    Ok(Refreshed::Advanced(Arc::new(view)))
}

/// The distinct `current` keys written across `(from, to]`, or `None` when
/// a commit in the gap recorded none — expired with its snapshot, or
/// suppressed for exceeding the record cap — or when re-reading them can no
/// longer beat one full scan.
///
/// `budget` is that crossover: once the distinct keys outnumber the live
/// records the view already holds, a rematerialization reads no more than
/// this refresh would and needs no per-key round trip.
async fn changed_keys(
    handle: ReadHandle<'_>,
    from: u64,
    to: u64,
    budget: usize,
) -> Result<Option<BTreeSet<Vec<u8>>>> {
    let mut changed = BTreeSet::new();
    for snapshot_id in (from + 1)..=to {
        let Some(delta) = read::read_commit_delta(handle, snapshot_id).await? else {
            return Ok(None);
        };
        changed.extend(delta.current_keys);
        if changed.len() > budget {
            return Ok(None);
        }
    }

    Ok(Some(changed))
}
