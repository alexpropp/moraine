//! Maintained projections: decoded snapshot and statistics rows a
//! read-write catalog serves without rescanning, folded forward from each
//! committed batch. Every serve is guarded by the head snapshot id the
//! caller observed; a mismatch (or an undecodable fold) degrades to a
//! fresh scan, never to wrong rows.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    store::{
        key::{CurrentKey, EntityKey, Key},
        proto::{SnapshotValue, TableColumnStatsValue, TableStatsValue},
        read::EntityRecord,
        value,
    },
    transaction::commit::StagedWrite,
};

/// One maintained projection: decoded rows stamped with the head snapshot
/// id they are valid at. `head: None` means not installed — serves refuse
/// and folds skip until a fresh scan installs it.
struct Maintained<K: Ord, V> {
    head: Option<u64>,
    rows: BTreeMap<K, V>,
}

impl<K: Ord, V> Maintained<K, V> {
    fn empty() -> Self {
        Self {
            head: None,
            rows: BTreeMap::new(),
        }
    }

    fn install(&mut self, head: u64, rows: BTreeMap<K, V>) {
        self.head = Some(head);
        self.rows = rows;
    }

    fn clear(&mut self) {
        self.head = None;
        self.rows.clear();
    }

    fn advance(&mut self, new_head: u64) {
        if self.head.is_some() {
            self.head = Some(new_head);
        }
    }

    fn serve(&self, expected_head: u64) -> Option<Vec<V>>
    where
        V: Clone,
    {
        (self.head == Some(expected_head)).then(|| self.rows.values().cloned().collect())
    }

    /// Applies one folded write; on an undecodable put, clears — the
    /// projection degrades to a rescan rather than serving wrong rows.
    fn fold(&mut self, key: K, bytes: Option<&[u8]>)
    where
        V: prost::Message + Default,
    {
        if self.head.is_none() {
            return;
        }
        match bytes {
            None => {
                self.rows.remove(&key);
            }
            Some(bytes) => match value::decode_value(bytes) {
                Ok(decoded) => {
                    self.rows.insert(key, decoded);
                }
                Err(_) => self.clear(),
            },
        }
    }
}

/// Folds a just-committed batch into the shared cache. A poisoned lock is
/// recovered rather than propagated: panics cannot originate inside the
/// fold (non-panicking map operations end to end), so the state under a
/// poisoned lock is never half-applied.
pub(crate) fn fold_committed_batch(
    cache: &std::sync::RwLock<ProjectionCache>,
    writes: &[StagedWrite],
    new_head: u64,
) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .apply_batch(writes, new_head);
}

/// The projections DuckLake re-reads per transaction, maintained on a
/// read-write catalog so serving them does not rescan the store.
pub(crate) struct ProjectionCache {
    snapshots: Maintained<u64, SnapshotValue>,
    table_stats: Maintained<u64, TableStatsValue>,
    table_column_stats: Maintained<(u64, u64), TableColumnStatsValue>,
    /// The full current+history entity scan at one head: populating
    /// DuckLake's metadata tables issues ~two dozen per-kind dumps, and
    /// this serves them all from one scan pair. Not folded forward —
    /// entity writes are too varied — so any committed batch drops it
    /// and the next dump re-installs it at the new head.
    entities: Option<(u64, Arc<Vec<EntityRecord>>)>,
}

impl ProjectionCache {
    pub(crate) fn empty() -> Self {
        Self {
            snapshots: Maintained::empty(),
            table_stats: Maintained::empty(),
            table_column_stats: Maintained::empty(),
            entities: None,
        }
    }

    pub(crate) fn install_entities(&mut self, head: u64, records: Vec<EntityRecord>) {
        self.entities = Some((head, Arc::new(records)));
    }

    /// Serves the entity scan if it is exactly at `expected_head`.
    pub(crate) fn entities_at(&self, expected_head: u64) -> Option<Arc<Vec<EntityRecord>>> {
        self.entities
            .as_ref()
            .and_then(|(head, records)| (*head == expected_head).then(|| Arc::clone(records)))
    }

    pub(crate) fn install_snapshots(&mut self, head: u64, rows: Vec<SnapshotValue>) {
        self.snapshots
            .install(head, rows.into_iter().map(|r| (r.snapshot_id, r)).collect());
    }

    pub(crate) fn install_table_stats(&mut self, head: u64, rows: Vec<TableStatsValue>) {
        self.table_stats
            .install(head, rows.into_iter().map(|r| (r.table_id, r)).collect());
    }

    pub(crate) fn install_table_column_stats(
        &mut self,
        head: u64,
        rows: Vec<TableColumnStatsValue>,
    ) {
        self.table_column_stats.install(
            head,
            rows.into_iter()
                .map(|r| ((r.table_id, r.column_id), r))
                .collect(),
        );
    }

    /// Serves the snapshot projection if it is exactly at `expected_head`.
    pub(crate) fn snapshots_at(&self, expected_head: u64) -> Option<Vec<SnapshotValue>> {
        self.snapshots.serve(expected_head)
    }

    pub(crate) fn table_stats_at(&self, expected_head: u64) -> Option<Vec<TableStatsValue>> {
        self.table_stats.serve(expected_head)
    }

    pub(crate) fn table_column_stats_at(
        &self,
        expected_head: u64,
    ) -> Option<Vec<TableColumnStatsValue>> {
        self.table_column_stats.serve(expected_head)
    }

    /// Folds one committed batch, stamping every installed projection with
    /// `new_head` (unchanged for maintenance commits, which pass the old
    /// head). An undecodable key clears everything: the batch cannot be
    /// attributed, so no projection may claim the new head.
    pub(crate) fn apply_batch(&mut self, writes: &[StagedWrite], new_head: u64) {
        self.entities = None;
        for (encoded_key, write) in writes {
            let bytes = write.as_deref();
            match Key::decode(encoded_key) {
                Ok(Key::Snapshot { snapshot_id }) => self.snapshots.fold(snapshot_id, bytes),
                Ok(Key::Current(CurrentKey::Entity(EntityKey::TableStats { table_id }))) => {
                    self.table_stats.fold(table_id, bytes);
                }
                Ok(Key::Current(CurrentKey::Entity(EntityKey::TableColumnStats {
                    table_id,
                    column_id,
                }))) => self.table_column_stats.fold((table_id, column_id), bytes),
                Ok(_) => {}
                Err(_) => {
                    self.snapshots.clear();
                    self.table_stats.clear();
                    self.table_column_stats.clear();
                    return;
                }
            }
        }
        self.snapshots.advance(new_head);
        self.table_stats.advance(new_head);
        self.table_column_stats.advance(new_head);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        Catalog, CatalogOptions, ColumnDef,
        ffi_support::{dump_snapshots, dump_table_column_stats, dump_table_stats},
        store::{
            key::{EntityKey, Key},
            proto::{SnapshotValue, TableColumnStatsValue, TableStatsValue},
            value::encode_value,
        },
    };

    /// Snapshot rows read directly from the store, bypassing the cache.
    async fn scanned_snapshots(catalog: &Catalog) -> Vec<SnapshotValue> {
        let session = catalog.begin_read().await.unwrap();
        let rows = crate::store::read::scan_snapshots(session.handle())
            .await
            .unwrap();
        session.finish();
        rows
    }

    /// After every commit, the served projections equal fresh scans at the
    /// same head — served through the cache (installed by the first dump,
    /// folded forward by each commit), proven equal to the store's truth.
    #[tokio::test]
    async fn dumps_after_commits_match_fresh_scans() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();

        for round in 0..8u64 {
            // Prime/serve before the commit so the fold path (not the
            // install path) is what keeps the cache current.
            let _ = dump_snapshots(&catalog).await.unwrap();

            catalog
                .commit(|tx| {
                    let main = tx.schemas()[0].id;
                    let table = tx.create_table(
                        main,
                        &format!("t{round}"),
                        &[ColumnDef {
                            name: "id".into(),
                            column_type: "BIGINT".into(),
                            nulls_allowed: false,
                            default_value: None,
                        }],
                    )?;
                    if round % 2 == 0 {
                        tx.rename_table(table, &format!("t{round}_renamed"))?;
                    }
                    Ok(())
                })
                .await
                .unwrap();

            let served = dump_snapshots(&catalog).await.unwrap();
            assert_eq!(served, scanned_snapshots(&catalog).await, "round {round}");

            let head = served.last().unwrap().snapshot_id;
            let cache_current = {
                let guard = catalog
                    .projections()
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.snapshots_at(head).is_some()
            };
            assert!(
                cache_current,
                "cache must be current at head {head} after serving"
            );

            let _ = dump_table_stats(&catalog).await.unwrap();
            let _ = dump_table_column_stats(&catalog).await.unwrap();
        }
    }

    fn snapshot_value(id: u64) -> SnapshotValue {
        SnapshotValue {
            snapshot_id: id,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 1,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
        }
    }

    fn stats_value(table_id: u64, record_count: u64) -> TableStatsValue {
        TableStatsValue {
            table_id,
            record_count,
            next_row_id: record_count,
            file_size_bytes: 100,
        }
    }

    fn column_stats_value(table_id: u64, column_id: u64) -> TableColumnStatsValue {
        TableColumnStatsValue {
            table_id,
            column_id,
            contains_null: Some(false),
            contains_nan: None,
            min_value: Some("1".into()),
            max_value: Some("9".into()),
            extra_stats: None,
        }
    }

    fn installed_at_three() -> ProjectionCache {
        let mut cache = ProjectionCache::empty();
        cache.install_snapshots(3, (0..=3).map(snapshot_value).collect());
        cache.install_table_stats(3, vec![stats_value(7, 10)]);
        cache.install_table_column_stats(3, vec![column_stats_value(7, 1)]);
        cache
    }

    #[test]
    fn fold_inserts_snapshot_and_updates_stats() {
        let mut cache = installed_at_three();

        let writes = vec![
            (
                Key::Snapshot { snapshot_id: 4 }.encode(),
                Some(encode_value(&snapshot_value(4))),
            ),
            (
                Key::current(EntityKey::TableStats { table_id: 7 }).encode(),
                Some(encode_value(&stats_value(7, 11))),
            ),
        ];
        cache.apply_batch(&writes, 4);

        let snapshots = cache.snapshots_at(4).unwrap();
        assert_eq!(snapshots.len(), 5);
        assert_eq!(snapshots.last().unwrap().snapshot_id, 4);

        let stats = cache.table_stats_at(4).unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].record_count, 11);

        assert_eq!(cache.table_column_stats_at(4).unwrap().len(), 1);
    }

    #[test]
    fn fold_deletes_remove_rows() {
        let mut cache = installed_at_three();

        let writes = vec![
            (Key::Snapshot { snapshot_id: 2 }.encode(), None),
            (
                Key::current(EntityKey::TableColumnStats {
                    table_id: 7,
                    column_id: 1,
                })
                .encode(),
                None,
            ),
        ];
        cache.apply_batch(&writes, 3);

        let snapshots = cache.snapshots_at(3).unwrap();
        assert_eq!(snapshots.len(), 3);
        assert!(snapshots.iter().all(|s| s.snapshot_id != 2));
        assert!(cache.table_column_stats_at(3).unwrap().is_empty());
    }

    #[test]
    fn serve_refuses_a_mismatched_head() {
        let cache = installed_at_three();
        assert!(cache.snapshots_at(4).is_none());
        assert!(cache.table_stats_at(2).is_none());
    }

    #[test]
    fn fold_on_an_empty_cache_is_a_noop() {
        let mut cache = ProjectionCache::empty();
        cache.apply_batch(
            &[(
                Key::Snapshot { snapshot_id: 1 }.encode(),
                Some(encode_value(&snapshot_value(1))),
            )],
            1,
        );
        assert!(cache.snapshots_at(1).is_none());
    }

    #[test]
    fn an_undecodable_value_clears_only_the_touched_projection() {
        let mut cache = installed_at_three();
        cache.apply_batch(
            &[(
                Key::Snapshot { snapshot_id: 4 }.encode(),
                Some(vec![0xff, 0xff, 0xff, 0xff]),
            )],
            4,
        );
        // Snapshots degrade to a rescan; the untouched stats fold forward.
        assert!(cache.snapshots_at(4).is_none());
        assert!(cache.table_stats_at(4).is_some());
    }

    #[test]
    fn an_undecodable_key_clears_everything() {
        let mut cache = installed_at_three();
        cache.apply_batch(&[(vec![0xff, 0xee], Some(vec![]))], 4);
        assert!(cache.snapshots_at(4).is_none());
        assert!(cache.table_stats_at(4).is_none());
        assert!(cache.table_column_stats_at(4).is_none());
    }

    #[test]
    fn irrelevant_keys_still_advance_the_head() {
        let mut cache = installed_at_three();
        cache.apply_batch(
            &[(
                Key::current(EntityKey::Schema { schema_id: 9 }).encode(),
                Some(vec![1, 2, 3]),
            )],
            4,
        );
        assert_eq!(cache.snapshots_at(4).unwrap().len(), 4);
        assert!(cache.snapshots_at(3).is_none());
    }
}
