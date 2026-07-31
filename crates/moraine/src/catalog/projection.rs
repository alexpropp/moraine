//! Maintained projections: decoded snapshot and statistics rows a
//! read-write catalog serves without rescanning, folded forward from each
//! committed batch. Every serve is guarded by the head snapshot id the
//! caller observed; a mismatch (or an undecodable fold) degrades to a
//! fresh scan, never to wrong rows.

use std::{collections::BTreeMap, sync::Arc};

use crate::store::{
    proto::{SnapshotValue, TableColumnStatsValue, TableStatsValue},
    read::EntityRecord,
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

    fn serve(&self, expected_head: u64) -> Option<Vec<V>>
    where
        V: Clone,
    {
        (self.head == Some(expected_head)).then(|| self.rows.values().cloned().collect())
    }
}

/// The projections DuckLake re-reads per transaction, served from one scan
/// per head so a run of per-kind dumps does not rescan the store.
pub(crate) struct ProjectionCache {
    snapshots: Maintained<u64, SnapshotValue>,
    table_stats: Maintained<u64, TableStatsValue>,
    table_column_stats: Maintained<(u64, u64), TableColumnStatsValue>,
    /// The full current+history entity scan at one head: populating
    /// DuckLake's metadata tables issues ~two dozen per-kind dumps, and
    /// this serves them all from one scan pair. Any committed batch drops it
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::proto::{SnapshotValue, TableColumnStatsValue, TableStatsValue};

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
            transaction_id: None,
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
    fn serve_refuses_a_mismatched_head() {
        let cache = installed_at_three();
        assert!(cache.snapshots_at(4).is_none());
        assert!(cache.table_stats_at(2).is_none());
    }
}
