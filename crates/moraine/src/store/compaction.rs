//! Submitting a full merge of a segment's tree, and waiting for it.
//!
//! Nothing here plans a merge. SlateDB's own scheduler turns "all of this
//! tree" into a spec — every sorted run a source, the lowest run id the
//! destination — and that shape is what lets the merge drop superseded
//! versions and tombstones instead of carrying them forward.
//!
//! Submitting only queues: the compactor running inside the writer promotes
//! the entry on its own poll tick. A store nobody has attached read-write
//! executes nothing, which is why the catalog verb above this refuses a
//! read-only handle.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::{
    VersionedCompactions,
    admin::AdminBuilder,
    compactor::{
        Compaction, CompactionRequest, CompactionSchedulerSupplier, CompactionStatus,
        CompactorStateView, SizeTieredCompactionSchedulerSupplier,
    },
    config::CompactorOptions,
};

use crate::error::{Error, Result};

/// How often a wait re-reads the compactions file. Derived from SlateDB's
/// own compactor poll cadence, so a wait costs about one read per tick the
/// compactor takes rather than a cadence of moraine's choosing.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// One merge a caller is waiting on: the segment it targets and the record
/// to poll it by.
#[derive(Debug, Clone)]
pub(crate) struct SubmittedMerge {
    pub(crate) segment: Vec<u8>,
    pub(crate) compaction: Compaction,
}

/// How a merge ended, or that it has not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeEnd {
    /// Committed to the manifest.
    Completed,
    /// Ended without committing.
    Failed,
    /// Still running when the wait ran out. The merge is not cancelled.
    Pending,
}

/// Plans and submits a full merge of `segment` (of every segment, when
/// `None`).
///
/// Returns one entry per merge the caller can wait on. A tree yields none
/// only when it holds no compacted sorted runs — what SlateDB's whole-store
/// request does silently, and what a store whose bulk is still in L0 will
/// report.
///
/// A tree already being merged is **adopted rather than skipped**: its
/// in-flight compaction is returned to be waited on. Submitting a second
/// plan for it would claim a destination the executor refuses, but the
/// caller asked for a merged tree and one is on its way — and a writer's
/// own compactor starts proposing the moment it opens, so skipping would
/// make an on-demand merge reclaim nothing precisely when it is asked for
/// straight after an attach.
pub(crate) async fn submit_full_merge(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    segment: Option<&[u8]>,
) -> Result<Vec<SubmittedMerge>> {
    let admin = AdminBuilder::new(path, object_store).build();
    let state = admin
        .read_compactor_state_view()
        .await
        .map_err(Error::from)?;

    let request = match segment {
        Some(prefix) => CompactionRequest::FullSegment {
            segment: Bytes::copy_from_slice(prefix),
        },
        None => CompactionRequest::Full,
    };

    // The scheduler is asked only to *plan*; it never runs here. Going
    // through it rather than assembling a spec keeps the choice of sources
    // and destination upstream's.
    let scheduler =
        SizeTieredCompactionSchedulerSupplier.compaction_scheduler(&CompactorOptions::default());
    let specs = match scheduler.generate(&state, &request) {
        Ok(specs) => specs,
        // A named tree with nothing to merge is an upstream error and a
        // skip here, so both request shapes report the same way.
        Err(_) if segment.is_some() => return Ok(Vec::new()),
        Err(err) => return Err(Error::from(err)),
    };

    let busy = merges_in_flight(&state);

    let mut submitted = Vec::with_capacity(specs.len());
    for spec in specs {
        let target = spec.segment().to_vec();
        // A full-tree plan destines the tree's lowest sorted-run id, which
        // is what the background scheduler destines when it merges the same
        // runs, and two jobs claiming one destination is a state the
        // executor refuses. So the in-flight one is adopted: the caller
        // waits on it instead of on a submission that cannot be made.
        if let Some(in_flight) = busy
            .iter()
            .find(|(segment, _)| segment == &target)
            .map(|(_, compaction)| compaction.clone())
        {
            submitted.push(SubmittedMerge {
                segment: target,
                compaction: in_flight,
            });
            continue;
        }

        let compaction = admin.submit_compaction(spec).await.map_err(Error::from)?;
        submitted.push(SubmittedMerge {
            segment: target,
            compaction,
        });
    }

    Ok(submitted)
}

/// Polls `compaction` until it commits, fails, or `budget` runs out.
///
/// A merge that outlives the budget keeps running: nothing is cancelled,
/// and a later census shows the result.
pub(crate) async fn await_merge(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    compaction: &Compaction,
    budget: Duration,
) -> Result<MergeEnd> {
    let admin = AdminBuilder::new(path, object_store).build();
    let deadline = tokio::time::Instant::now() + budget;

    loop {
        match admin
            .read_compaction(compaction.id(), None)
            .await
            .map_err(Error::from)?
            .map(|found| found.status())
        {
            // A compaction the compactions file no longer carries has been
            // retired from it; the manifest holds its result either way.
            Some(CompactionStatus::Completed) | None => return Ok(MergeEnd::Completed),
            Some(CompactionStatus::Failed) => return Ok(MergeEnd::Failed),
            Some(_) => {}
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok(MergeEnd::Pending);
        }
        tokio::time::sleep(POLL_INTERVAL.min(deadline - now)).await;
    }
}

/// Every compaction that has not reached a terminal status, by the segment
/// it targets.
fn merges_in_flight(state: &CompactorStateView) -> Vec<(Vec<u8>, Compaction)> {
    state
        .compactions()
        .into_iter()
        .flat_map(VersionedCompactions::recent_compactions)
        .filter(|compaction| {
            !matches!(
                compaction.status(),
                CompactionStatus::Completed | CompactionStatus::Failed
            )
        })
        .map(|compaction| (compaction.spec().segment().to_vec(), compaction.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use object_store::memory::InMemory;
    use slatedb::{
        Db, IsolationLevel, WriteBatch,
        compactor::{CompactionSpec, SourceId},
        config::{FlushOptions, FlushType, Settings},
    };

    use super::*;
    use crate::store::{
        census::{ManifestCensus, read_manifest_census},
        key::{EntityKey, Key, Subspace, subspace_prefix},
        segment::TagSegmentExtractor,
    };

    /// How many distinct keys each churn round rewrites.
    const CHURN_KEYS: u64 = 16;

    /// A store whose trees hold no compacted sorted runs has nothing to
    /// merge, and says so by submitting nothing — for either request shape.
    #[tokio::test]
    async fn nothing_to_merge_submits_nothing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open_writer("merge/fresh", Arc::clone(&store)).await;
        churn(&db, 1).await;
        db.close().await.unwrap();

        let whole = submit_full_merge("merge/fresh", Arc::clone(&store), None)
            .await
            .unwrap();
        assert!(whole.is_empty(), "{whole:?}");

        let named = submit_full_merge("merge/fresh", store, Some(&current_prefix()))
            .await
            .unwrap();
        assert!(named.is_empty(), "{named:?}");
    }

    /// A full merge reclaims: the subspace's physical bytes fall, its sorted
    /// runs collapse to one, and every key still reads back the last value
    /// written. Rewriting one small key set many times means nearly every
    /// byte the merge drops is a superseded version.
    #[tokio::test]
    async fn a_full_merge_reclaims_superseded_versions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rounds = 8;
        let before = churned_store("merge/reclaim", Arc::clone(&store), rounds).await;
        let before = before.segment(&current_prefix()).cloned().expect("current");
        assert!(before.sorted_runs > 1, "{before:?}");

        let db = open_writer("merge/reclaim", Arc::clone(&store)).await;
        merge_and_wait("merge/reclaim", Arc::clone(&store), Some(&current_prefix())).await;

        let after = read_manifest_census("merge/reclaim", Arc::clone(&store))
            .await
            .unwrap();
        let after = after.segment(&current_prefix()).cloned().expect("current");

        assert!(
            after.bytes < before.bytes,
            "no reclaim: {before:?} -> {after:?}"
        );
        assert_eq!(after.sorted_runs, 1, "{after:?}");

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for schema_id in 0..CHURN_KEYS {
            let value = tx
                .get(Key::current(EntityKey::Schema { schema_id }).encode())
                .await
                .unwrap()
                .expect("key survives the merge");
            assert_eq!(value.as_ref(), round_value(rounds - 1).as_bytes());
        }
        tx.rollback();
    }

    /// A merge drops the tombstone of a deleted key, not only its superseded
    /// values: a full-tree plan destines the bottom run, which is what
    /// permits the drop rather than carrying the marker forward forever.
    #[tokio::test]
    async fn a_full_merge_drops_tombstones() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        churned_store("merge/tombstone", Arc::clone(&store), 8).await;

        let db = open_writer("merge/tombstone", Arc::clone(&store)).await;
        let mut batch = WriteBatch::new();
        for schema_id in 0..CHURN_KEYS {
            batch.delete(Key::current(EntityKey::Schema { schema_id }).encode());
        }
        db.write(batch).await.unwrap();
        flush_to_l0(&db).await;
        seed_sorted_run("merge/tombstone", Arc::clone(&store), &current_prefix()).await;

        merge_and_wait(
            "merge/tombstone",
            Arc::clone(&store),
            Some(&current_prefix()),
        )
        .await;

        let census = read_manifest_census("merge/tombstone", Arc::clone(&store))
            .await
            .unwrap();
        let current = census.segment(&current_prefix()).cloned().expect("current");

        // Every key of the subspace was deleted, so a merge that dropped the
        // tombstones leaves the bottom run holding no SST at all.
        assert_eq!(current.sorted_run_ssts, 0, "{current:?}");

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for schema_id in 0..CHURN_KEYS {
            assert!(
                tx.get(Key::current(EntityKey::Schema { schema_id }).encode())
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        tx.rollback();
    }

    /// Merging one subspace leaves every other tree exactly as it was.
    #[tokio::test]
    async fn merging_one_subspace_leaves_the_others_alone() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let snapshots = subspace_prefix(Subspace::Snapshot);
        let before = churned_store("merge/isolated", Arc::clone(&store), 8).await;
        let before = before.segment(&snapshots).cloned().expect("snapshot");

        let _db = open_writer("merge/isolated", Arc::clone(&store)).await;
        merge_and_wait(
            "merge/isolated",
            Arc::clone(&store),
            Some(&current_prefix()),
        )
        .await;

        let after = read_manifest_census("merge/isolated", store).await.unwrap();
        assert_eq!(after.segment(&snapshots), Some(&before));
    }

    /// A tree already being merged is adopted rather than skipped: the
    /// second caller waits on the first caller's merge instead of being
    /// told there was nothing to do.
    ///
    /// Skipping instead would make an on-demand merge reclaim nothing in
    /// the case it is most often asked for — straight after an attach,
    /// whose writer starts a compactor that proposes immediately.
    #[tokio::test]
    async fn a_tree_already_merging_is_adopted() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        churned_store("merge/busy", Arc::clone(&store), 8).await;

        let first = submit_full_merge("merge/busy", Arc::clone(&store), Some(&current_prefix()))
            .await
            .unwrap();
        assert_eq!(first.len(), 1, "{first:?}");

        let second = submit_full_merge("merge/busy", store, Some(&current_prefix()))
            .await
            .unwrap();
        assert_eq!(second.len(), 1, "{second:?}");
        // The same merge, not a second one claiming its destination.
        assert_eq!(second[0].compaction.id(), first[0].compaction.id());
    }

    fn current_prefix() -> Vec<u8> {
        subspace_prefix(Subspace::Current)
    }

    fn round_value(round: u64) -> String {
        format!("schema-{round}-{}", "x".repeat(64))
    }

    /// A writer whose scheduler proposes nothing of its own, so every merge
    /// a test observes is one it submitted. This is also the state a
    /// production store that stopped writing settles into.
    async fn open_writer(path: &str, object_store: Arc<dyn ObjectStore>) -> Db {
        let settings = Settings {
            compactor_options: Some(CompactorOptions {
                poll_interval: Duration::from_millis(25),
                scheduler_options: HashMap::from([
                    ("min_compaction_sources".to_string(), "1000".to_string()),
                    ("max_compaction_sources".to_string(), "1000".to_string()),
                ]),
                ..CompactorOptions::default()
            }),
            ..Settings::default()
        };

        Db::builder(path, object_store)
            .with_settings(settings)
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .build()
            .await
            .unwrap()
    }

    /// Freezes the memtable and writes it out, so each round lands exactly
    /// one L0 SST per subspace it touched.
    async fn flush_to_l0(db: &Db) {
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }

    /// Rewrites the same key set in `current` and `snapshot`, one L0 SST per
    /// round per subspace.
    async fn churn(db: &Db, rounds: u64) {
        for round in 0..rounds {
            let mut batch = WriteBatch::new();
            for id in 0..CHURN_KEYS {
                batch.put(
                    Key::current(EntityKey::Schema { schema_id: id }).encode(),
                    round_value(round).as_bytes(),
                );
                batch.put(
                    Key::Snapshot { snapshot_id: id }.encode(),
                    round_value(round).as_bytes(),
                );
            }
            db.write(batch).await.unwrap();
            flush_to_l0(db).await;
        }
    }

    /// Leaves `current` and `snapshot` each holding two sorted runs over
    /// overlapping keys — the shape a full-tree merge exists to collapse —
    /// and reports the census it left behind.
    ///
    /// The runs are seeded by submitting L0-source specs rather than by
    /// waiting on the size-tiered scheduler, so a test never races it.
    async fn churned_store(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
        rounds: u64,
    ) -> ManifestCensus {
        let snapshots = subspace_prefix(Subspace::Snapshot);
        let db = open_writer(path, Arc::clone(&object_store)).await;

        for _ in 0..2 {
            churn(&db, rounds).await;
            for prefix in [current_prefix(), snapshots.clone()] {
                seed_sorted_run(path, Arc::clone(&object_store), &prefix).await;
            }
        }
        db.close().await.unwrap();

        read_manifest_census(path, object_store).await.unwrap()
    }

    /// Compacts every L0 SST of one segment into a fresh sorted run.
    async fn seed_sorted_run(path: &str, object_store: Arc<dyn ObjectStore>, prefix: &[u8]) {
        let admin = AdminBuilder::new(path, Arc::clone(&object_store)).build();
        let state = admin.read_compactor_state_view().await.unwrap();
        let manifest = state.manifest();
        let segment = manifest.segment(prefix).expect("segment");

        let sources: Vec<SourceId> = segment
            .l0()
            .iter()
            .map(|view| SourceId::SstView(view.id))
            .collect();
        assert!(!sources.is_empty(), "no L0 to seed a run from");

        // A submitted L0-only spec must destine an id above every existing
        // sorted run, in any tree.
        let destination = manifest
            .segments()
            .iter()
            .flat_map(|segment| segment.compacted().iter().map(|run| run.id + 1))
            .max()
            .unwrap_or(0);
        let spec =
            CompactionSpec::for_segment(Bytes::copy_from_slice(prefix), sources, destination);
        let compaction = admin.submit_compaction(spec).await.unwrap();

        let end = await_merge(path, object_store, &compaction, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(end, MergeEnd::Completed);
    }

    /// Submits a full merge and waits for every submission to commit.
    async fn merge_and_wait(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
        segment: Option<&[u8]>,
    ) {
        let submitted = submit_full_merge(path, Arc::clone(&object_store), segment)
            .await
            .unwrap();
        assert!(!submitted.is_empty(), "nothing submitted");

        for merge in &submitted {
            let end = await_merge(
                path,
                Arc::clone(&object_store),
                &merge.compaction,
                Duration::from_secs(60),
            )
            .await
            .unwrap();
            assert_eq!(end, MergeEnd::Completed, "{merge:?}");
        }
    }
}
