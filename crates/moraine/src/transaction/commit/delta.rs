//! The changed-key record a commit leaves for readers.
//!
//! A commit already knows exactly which `current` keys it touched — that is
//! the batch it stages. Persisting those keys lets a reader holding an older
//! view re-read just them instead of rescanning the live catalog. Only keys
//! are stored: the reader re-reads each one at its own pinned cut, so a
//! stored value could only restate what the store already answers, or
//! disagree with it.

use super::StagedWrite;
use crate::store::{
    key::{Key, Subspace, subspace_prefix},
    proto::CommitDeltaValue,
    value,
};

/// Largest key count a commit will record. Past it the record is omitted
/// and a refresh spanning the commit rematerializes, which bounds the
/// record against a bulk load registering hundreds of thousands of files.
pub(crate) const MAX_DELTA_KEYS: usize = 4096;

/// The delta write for a commit minting `snapshot_id`, or `None` when the
/// commit touched more keys than [`MAX_DELTA_KEYS`]. Omitting the record
/// is a complete action — absence is what tells a reader the gap is not
/// replayable — where a truncated one would refresh a view to a state no
/// commit ever produced.
///
/// Call this before the snapshot and head writes are staged, so the delta
/// never names them.
pub(crate) fn write_for(snapshot_id: u64, writes: &[StagedWrite]) -> Option<StagedWrite> {
    // The leading byte is the subspace, so this selects live keys without a
    // decode that could fail and silently drop one from the record.
    let prefix = subspace_prefix(Subspace::Current);
    let current_keys: Vec<Vec<u8>> = writes
        .iter()
        .map(|(key, _)| key)
        .filter(|key| key.starts_with(&prefix))
        .cloned()
        .collect();

    // An empty list still gets a record. Absence is reserved for "not
    // replayable", and a commit that touched no live key is replayable by
    // doing nothing — conflating the two would strand readers on a full
    // rescan over a commit with no work in it.
    if current_keys.len() > MAX_DELTA_KEYS {
        return None;
    }

    Some((
        Key::CommitDelta { snapshot_id }.encode(),
        Some(value::encode_value(&CommitDeltaValue { current_keys })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::key::{CurrentKey, EntityKey, SysKey};

    fn current(table_id: u64) -> Vec<u8> {
        Key::current(EntityKey::Table { table_id }).encode()
    }

    fn decoded(write: &StagedWrite) -> Vec<Vec<u8>> {
        let bytes = write.1.as_ref().unwrap();
        value::decode_value::<CommitDeltaValue>(bytes)
            .unwrap()
            .current_keys
    }

    /// The record names current keys and nothing else: a history or index
    /// key in the list would send a refresh to re-read a key that never
    /// appears in a head view.
    #[test]
    fn the_delta_names_only_current_keys() {
        let writes = vec![
            (current(1), Some(vec![1])),
            (
                Key::history(EntityKey::Table { table_id: 2 }, 3).encode(),
                Some(vec![2]),
            ),
            (Key::Sys(SysKey::Head).encode(), Some(vec![3])),
            (
                Key::Current(CurrentKey::GcFile { data_file_id: 9 }).encode(),
                None,
            ),
        ];

        let write = write_for(7, &writes).unwrap();
        assert_eq!(write.0, Key::CommitDelta { snapshot_id: 7 }.encode());
        assert_eq!(
            decoded(&write),
            vec![
                current(1),
                Key::Current(CurrentKey::GcFile { data_file_id: 9 }).encode(),
            ]
        );
    }

    /// A deletion is recorded by key like any other change; the refresh
    /// learns it was deleted by finding the key absent, not from the record.
    #[test]
    fn deletions_are_recorded_by_key() {
        let writes = vec![(current(4), None)];
        assert_eq!(decoded(&write_for(1, &writes).unwrap()), vec![current(4)]);
    }

    /// Over the cap the record is omitted entirely: a partial one would
    /// refresh a view to a state no commit ever produced.
    #[test]
    fn an_oversized_commit_records_nothing() {
        let writes: Vec<StagedWrite> = (0..=MAX_DELTA_KEYS as u64)
            .map(|table_id| (current(table_id), Some(vec![0])))
            .collect();
        assert!(write_for(1, &writes).is_none());
    }

    /// A batch that touched no live key still records one, empty: absence
    /// means "not replayable", and this commit is replayable by doing
    /// nothing.
    #[test]
    fn a_batch_without_current_keys_records_an_empty_list() {
        let writes = vec![(Key::Sys(SysKey::Head).encode(), Some(vec![1]))];
        assert!(decoded(&write_for(1, &writes).unwrap()).is_empty());
    }
}
