//! Index entry maintenance: turning writer-supplied entries into staged
//! `idx` writes, with commit-time uniqueness enforcement.
//!
//! Entries ride the same batch as the commit that owns them. A unique
//! entry's store key *is* the value, so staging its put arms SlateDB's
//! write-write detection: two commits inserting the same value collide
//! mechanically, and the loser re-runs and sees the winner's entry.

use std::collections::{HashMap, HashSet};

use crate::{
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        index_encoding::CanonicalKey,
        key::{IdxKey, Key, idx_multi_value_prefix},
    },
    transaction::commit::StagedWrite,
};

/// One index-entry mutation accumulated during a commit closure, resolved
/// against the store when the batch is staged.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StagedIndexEntry {
    /// The index this entry belongs to.
    pub(crate) index_id: u64,
    /// Whether the index is unique — selects the key shape and enforcement.
    pub(crate) unique: bool,
    /// The canonical indexed value.
    pub(crate) key: CanonicalKey,
    /// The row the entry points at.
    pub(crate) row_id: u64,
    /// Whether this removes the entry (`true`) or adds it (`false`).
    pub(crate) delete: bool,
}

fn entry_key(entry: &StagedIndexEntry) -> Key {
    if entry.unique {
        Key::Idx(IdxKey::Unique {
            index_id: entry.index_id,
            key: entry.key.clone(),
        })
    } else {
        Key::Idx(IdxKey::Multi {
            index_id: entry.index_id,
            key: entry.key.clone(),
            row_id: entry.row_id,
        })
    }
}

/// A unique entry's value is the holding row id, big-endian.
fn decode_row_id(bytes: &[u8]) -> Result<u64> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| {
        Error::Corruption(format!(
            "index entry value is {} bytes, expected 8",
            bytes.len()
        ))
    })?;
    Ok(u64::from_be_bytes(array))
}

/// Resolves accumulated entries into `writes`, enforcing uniqueness at
/// commit. Deletes are staged first so a delete-then-reinsert of one unique
/// value within a commit sees the value as absent. For each unique put:
/// present with a **different** row id → [`Error::Constraint`]; present with
/// the **same** row id → no-op (a re-derived entry); absent → staged.
/// Duplicates within the commit are caught in memory.
pub(crate) async fn stage_index_entries(
    reader: ReadHandle<'_>,
    entries: &[StagedIndexEntry],
    writes: &mut Vec<StagedWrite>,
) -> Result<()> {
    let mut deleted_unique: HashSet<Vec<u8>> = HashSet::new();
    for entry in entries.iter().filter(|entry| entry.delete) {
        let key_bytes = entry_key(entry).encode();
        if entry.unique {
            deleted_unique.insert(key_bytes.clone());
        }
        writes.push((key_bytes, None));
    }

    let mut staged_unique: HashMap<Vec<u8>, u64> = HashMap::new();
    for entry in entries.iter().filter(|entry| !entry.delete) {
        let key_bytes = entry_key(entry).encode();
        if !entry.unique {
            // The row id lives in the key; the value is empty.
            writes.push((key_bytes, Some(Vec::new())));
            continue;
        }
        if let Some(&existing) = staged_unique.get(&key_bytes) {
            if existing != entry.row_id {
                return Err(unique_violation(entry.index_id));
            }
            continue;
        }
        let present = if deleted_unique.contains(&key_bytes) {
            None
        } else {
            reader.get(key_bytes.clone()).await.map_err(Error::from)?
        };
        if let Some(bytes) = present {
            if decode_row_id(&bytes)? != entry.row_id {
                return Err(unique_violation(entry.index_id));
            }
            // Same row id: a re-derived entry for a rewrite file — no-op.
            continue;
        }
        writes.push((key_bytes.clone(), Some(entry.row_id.to_be_bytes().to_vec())));
        staged_unique.insert(key_bytes, entry.row_id);
    }
    Ok(())
}

/// A uniqueness error. The text is free of DuckLake's four retry substrings
/// (`conflict`, `concurrent`, `unique`, `primary key`) so a rejected bulk
/// INSERT surfaces at once instead of spinning DuckLake's commit loop.
fn unique_violation(index_id: u64) -> Error {
    Error::Constraint(format!(
        "duplicate value violates equality index {index_id}"
    ))
}

/// The row ids holding one indexed value: a point-get for a unique index,
/// an ascending prefix scan for a non-unique one. The non-unique row id
/// lives in the entry key, so each scanned key is decoded to recover it.
pub(crate) async fn lookup_row_ids(
    reader: ReadHandle<'_>,
    index_id: u64,
    unique: bool,
    key: &CanonicalKey,
) -> Result<Vec<u64>> {
    if unique {
        let entry_key = Key::Idx(IdxKey::Unique {
            index_id,
            key: key.clone(),
        })
        .encode();
        return match reader.get(entry_key).await.map_err(Error::from)? {
            Some(bytes) => Ok(vec![decode_row_id(&bytes)?]),
            None => Ok(Vec::new()),
        };
    }

    let prefix = idx_multi_value_prefix(index_id, key);
    let mut iter = reader.scan_prefix(prefix, ..).await.map_err(Error::from)?;
    let mut row_ids = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Idx(IdxKey::Multi { row_id, .. }) => row_ids.push(row_id),
            other => {
                return Err(Error::Corruption(format!(
                    "non-multi key in index scan: {other:?}"
                )));
            }
        }
    }
    Ok(row_ids)
}
