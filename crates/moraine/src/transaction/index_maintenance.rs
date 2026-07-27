//! Index entry maintenance: turning writer-supplied entries into staged
//! `index` writes, with commit-time uniqueness enforcement.
//!
//! Entries ride the same batch as the commit that owns them. A unique
//! entry's store key *is* the value, so staging its put arms SlateDB's
//! write-write detection: two commits inserting the same value collide
//! mechanically, and the loser re-runs and sees the winner's entry.

use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
};

use crate::{
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        index_encoding::CanonicalKey,
        key::{IndexKey, IndexKind, Key, index_index_prefix, index_multi_value_prefix},
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
        Key::Index(IndexKey::Unique {
            index_id: entry.index_id,
            key: entry.key.clone(),
        })
    } else {
        Key::Index(IndexKey::Multi {
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

/// Deletes up to `limit` orphaned entries of one dropped index inside an
/// open transaction, returning how many deletes were staged. An index is
/// exclusively one kind, so only one prefix holds entries; scanning both
/// is harmless.
pub(crate) async fn reclaim_entries(
    tx: &slatedb::DbTransaction,
    index_id: u64,
    limit: usize,
) -> Result<usize> {
    let mut deleted = 0;
    for kind in [IndexKind::Unique, IndexKind::Multi] {
        if deleted >= limit {
            break;
        }
        let (batch, _) = reclaim_entries_from(tx, kind, index_id, limit - deleted, None).await?;
        deleted += batch;
    }
    Ok(deleted)
}

/// Deletes up to `limit` entries of one dropped index of one kind,
/// resuming at `start_from` when given, and returns how many were staged
/// alongside the last key deleted.
///
/// Reclaiming a whole range takes one transaction per batch, and a batch
/// that restarted at the range's beginning would first have to step over
/// every tombstone the earlier batches left — turning a large range into
/// quadratic work. Handing the last key back lets the next batch resume
/// there instead. The resume is inclusive, so it re-reads exactly one
/// tombstone rather than needing an exclusive bound.
pub(crate) async fn reclaim_entries_from(
    tx: &slatedb::DbTransaction,
    kind: IndexKind,
    index_id: u64,
    limit: usize,
    start_from: Option<&[u8]>,
) -> Result<(usize, Option<Vec<u8>>)> {
    let prefix = index_index_prefix(kind, index_id);
    // `scan_prefix` takes its bounds as a suffix of the prefix.
    let suffix = match start_from {
        Some(key) if key.len() >= prefix.len() => key[prefix.len()..].to_vec(),
        _ => Vec::new(),
    };

    let mut iter = ReadHandle::Tx(tx).scan_prefix(prefix, suffix..).await?;
    let mut deleted = 0;
    let mut last = None;
    while deleted < limit {
        match iter.next().await? {
            Some(entry) => {
                let key = entry.key.to_vec();
                tx.delete(entry.key)?;
                deleted += 1;
                last = Some(key);
            }
            None => break,
        }
    }
    Ok((deleted, last))
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
        let entry_key = Key::Index(IndexKey::Unique {
            index_id,
            key: key.clone(),
        })
        .encode();
        return match reader.get(entry_key).await.map_err(Error::from)? {
            Some(bytes) => Ok(vec![decode_row_id(&bytes)?]),
            None => Ok(Vec::new()),
        };
    }

    let prefix = index_multi_value_prefix(index_id, key);
    let mut iter = reader.scan_prefix(prefix, ..).await.map_err(Error::from)?;
    let mut row_ids = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Index(IndexKey::Multi { row_id, .. }) => row_ids.push(row_id),
            other => {
                return Err(Error::Corruption(format!(
                    "non-multi key in index scan: {other:?}"
                )));
            }
        }
    }
    Ok(row_ids)
}

/// The row ids whose indexed value falls between `lower` and `upper`, in the
/// index's stored order. Ordered encoding makes byte order equal value order,
/// so the query is a bounded sub-scan of the index's contiguous range; the
/// bounds are the canonical values, already encoded in the columns' declared
/// directions. A unique entry carries its row id in the value, a non-unique
/// one in the key.
pub(crate) async fn range_row_ids(
    reader: ReadHandle<'_>,
    index_id: u64,
    unique: bool,
    lower: Bound<CanonicalKey>,
    upper: Bound<CanonicalKey>,
) -> Result<Vec<u64>> {
    let kind = if unique {
        IndexKind::Unique
    } else {
        IndexKind::Multi
    };
    let prefix = index_index_prefix(kind, index_id);
    let prefix_len = prefix.len();

    // The framed value bytes as they appear in an entry key after the
    // `(kind, index_id)` prefix — the suffix a subrange bounds against.
    let suffix = |canon: &CanonicalKey| -> Vec<u8> {
        let full = match kind {
            IndexKind::Unique => Key::Index(IndexKey::Unique {
                index_id,
                key: canon.clone(),
            })
            .encode(),
            IndexKind::Multi => index_multi_value_prefix(index_id, canon),
        };
        full[prefix_len..].to_vec()
    };

    // The exclusive byte bound just above every entry whose value starts with
    // `canon`: the value's own entries and, when `canon` names only a leading
    // prefix of the index's columns, every extension of it. The framed suffix
    // ends in the value's `0x00` terminator; dropping it leaves the escaped
    // body, whose increment sorts above every key that extends the body —
    // a longer value (further columns) or a trailing row id.
    let above = |canon: &CanonicalKey| -> Option<Vec<u8>> {
        let mut body = suffix(canon);
        body.pop();
        increment_prefix(&body)
    };

    let start = match lower {
        Bound::Included(canon) => Bound::Included(suffix(&canon)),
        // Skip every entry sharing the bound value: start above them all.
        Bound::Excluded(canon) => match above(&canon) {
            Some(above) => Bound::Included(above),
            None => Bound::Excluded(suffix(&canon)),
        },
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match upper {
        // Include every entry sharing the bound value: end above them all.
        Bound::Included(canon) => match above(&canon) {
            Some(above) => Bound::Excluded(above),
            None => Bound::Unbounded,
        },
        Bound::Excluded(canon) => Bound::Excluded(suffix(&canon)),
        Bound::Unbounded => Bound::Unbounded,
    };

    let mut iter = reader
        .scan_prefix(prefix, (start, end))
        .await
        .map_err(Error::from)?;
    let mut row_ids = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        if unique {
            row_ids.push(decode_row_id(entry.value.as_ref())?);
        } else {
            match Key::decode(&entry.key)? {
                Key::Index(IndexKey::Multi { row_id, .. }) => row_ids.push(row_id),
                other => {
                    return Err(Error::Corruption(format!(
                        "non-multi key in index range scan: {other:?}"
                    )));
                }
            }
        }
    }
    Ok(row_ids)
}

/// The row ids of live rows whose leading indexed columns match `prefix` — a
/// canonical key over a leading run of `= value` and `IS NULL` predicates.
/// A row with any NULL indexed column is stored multi-shaped, so `IS NULL`
/// queries scan the `multi` subrange; the value framing's terminator is
/// dropped from the scan prefix so it matches every key that extends the run.
pub(crate) async fn null_prefix_row_ids(
    reader: ReadHandle<'_>,
    index_id: u64,
    prefix: &CanonicalKey,
) -> Result<Vec<u64>> {
    let mut scan_prefix = index_multi_value_prefix(index_id, prefix);
    // `index_multi_value_prefix` frames the value and appends a terminator for an
    // exact-value scan; dropping it turns the bytes into a true leading prefix.
    scan_prefix.pop();
    let mut iter = reader
        .scan_prefix(scan_prefix, ..)
        .await
        .map_err(Error::from)?;
    let mut row_ids = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Index(IndexKey::Multi { row_id, .. }) => row_ids.push(row_id),
            other => {
                return Err(Error::Corruption(format!(
                    "non-multi key in null-prefix scan: {other:?}"
                )));
            }
        }
    }
    Ok(row_ids)
}

/// The smallest byte string lexicographically greater than every string that
/// begins with `prefix`, or `None` when `prefix` is all `0xff`.
fn increment_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bytes = prefix.to_vec();
    while let Some(last) = bytes.last_mut() {
        if *last != u8::MAX {
            *last += 1;
            return Some(bytes);
        }
        bytes.pop();
    }
    None
}
