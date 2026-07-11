//! Opening, bootstrap, and snapshot materialization. The commit cycle
//! itself builds on these.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::ObjectStore;
use slatedb::{Db, DbTransaction, IsolationLevel, config::WriteOptions};

use crate::{
    catalog::{CatalogSnapshot, SnapshotId},
    error::{Error, Result},
    store::{
        key::{EntityKey, Key, SysKey},
        open::open_store,
        proto, read, value,
    },
    txn::{
        ops::{ChangeSet, Op},
        verbs::Txn,
    },
};

/// Structural layout version this binary reads and writes.
pub(crate) const FORMAT_VERSION: u64 = 1;

/// Current time in microseconds since the Unix epoch. Clamped, never
/// panicking: a clock before the epoch stamps 0.
pub(crate) fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

fn durable() -> WriteOptions {
    WriteOptions {
        await_durable: true,
        ..Default::default()
    }
}

/// Refuses a store this binary must not touch: mid-migration, or a
/// format newer/older than it understands. `None` format means the store
/// is empty and needs bootstrap.
async fn validate_format(txn: &DbTransaction) -> Result<Option<proto::FormatValue>> {
    if read::read_migration(txn).await?.is_some() {
        return Err(Error::Corruption(
            "store is mid-migration; refusing to open".to_string(),
        ));
    }
    match read::read_format(txn).await? {
        Some(format) if format.format_version == FORMAT_VERSION => Ok(Some(format)),
        Some(format) => Err(Error::Corruption(format!(
            "store format {} is not the supported format {FORMAT_VERSION}",
            format.format_version
        ))),
        None => Ok(None),
    }
}

/// Stages the initial state of an empty store into `txn`: format stamp,
/// snapshot 0 (empty catalog, counters at zero), and head pointer.
fn stage_bootstrap(txn: &DbTransaction) -> Result<()> {
    let stage = |key: Key, bytes: Vec<u8>| txn.put(key.encode(), bytes).map_err(Error::from);
    stage(
        Key::Sys(SysKey::Format),
        value::encode_value(&proto::FormatValue {
            format_version: FORMAT_VERSION,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    )?;
    stage(
        Key::Snap { snapshot_id: 0 },
        value::encode_value(&proto::SnapshotValue {
            snapshot_id: 0,
            snapshot_time_micros: now_micros(),
            schema_version: 0,
            next_catalog_id: 0,
            next_file_id: 0,
            next_deletion_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
        }),
    )?;
    stage(
        Key::Sys(SysKey::Head),
        value::encode_value(&proto::HeadValue { snapshot_id: 0 }),
    )
}

/// Opens the store and ensures it is initialized: an empty store gets its
/// format stamp, snapshot 0 (empty catalog, counters at zero), and head
/// pointer in one atomic batch. The batch commits under write-write
/// conflict detection, so a lost bootstrap race degrades to "somebody
/// else initialized it" and is re-validated, never double-initialized.
///
/// Every exit that does not commit explicitly rolls the transaction back.
pub(crate) async fn open_initialized(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Db> {
    let db = open_store(path, object_store).await?;
    let txn = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;
    match validate_format(&txn).await {
        Ok(Some(_)) => {
            txn.rollback();
            return Ok(db);
        }
        Ok(None) => {}
        Err(err) => {
            txn.rollback();
            return Err(err);
        }
    }
    if let Err(err) = stage_bootstrap(&txn) {
        txn.rollback();
        return Err(err);
    }
    match txn.commit_with_options(&durable()).await {
        Ok(_) => Ok(db),
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => {
            // Lost the bootstrap race: someone initialized concurrently.
            let txn = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            let validated = validate_format(&txn).await;
            txn.rollback();
            if validated?.is_some() {
                Ok(db)
            } else {
                Err(Error::Corruption(
                    "bootstrap race left the store uninitialized".to_string(),
                ))
            }
        }
        Err(err) => Err(err.into()),
    }
}

/// Materializes a catalog view through an open transaction, so the view
/// and any subsequent staged writes share one consistent read point.
/// `at: None` reads the head — a scan of `cur` only, since column
/// allocation now reads the table's persisted counter rather than
/// deriving it from history. `at: Some(s)` time-travels to snapshot `s`,
/// which additionally scans `hist` to reconstruct entities live at `s`.
pub(crate) async fn materialize(txn: &DbTransaction, at: Option<u64>) -> Result<CatalogSnapshot> {
    let head = read::read_head(txn)
        .await?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))?
        .snapshot_id;
    let target = match at {
        Some(s) if s > head => {
            return Err(Error::NotFound(format!("snapshot {s} (head is {head})")));
        }
        Some(s) => s,
        None => head,
    };
    let snap = read::read_snapshot(txn, target)
        .await?
        .ok_or_else(|| Error::Corruption(format!("snapshot record {target} missing")))?;
    let cur = read::scan_cur_entities(txn).await?;
    let hist = match at {
        Some(_) => read::scan_hist_entities(txn).await?,
        None => Vec::new(),
    };
    Ok(CatalogSnapshot::build(snap, cur, hist, at.map(|_| target)))
}

/// One staged write: `Some` puts, `None` deletes.
type StagedWrite = (Vec<u8>, Option<Vec<u8>>);

fn stage_transition<M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    entity: EntityKey,
    base: Option<&M>,
    state: Option<&M>,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
) {
    match (base, state) {
        (Some(b), None) => {
            writes.push((Key::cur(entity).encode(), None));
            writes.push((
                Key::hist(entity, new_snapshot).encode(),
                Some(value::encode_value(&set_end(b))),
            ));
        }
        (Some(b), Some(s)) if b != s => {
            writes.push((
                Key::hist(entity, new_snapshot).encode(),
                Some(value::encode_value(&set_end(b))),
            ));
            writes.push((Key::cur(entity).encode(), Some(value::encode_value(s))));
        }
        (None, Some(s)) => {
            writes.push((Key::cur(entity).encode(), Some(value::encode_value(s))));
        }
        _ => {}
    }
}

/// The version bookkeeping: the write set that turns `base` into `state`
/// at snapshot `new_snapshot`. Ended versions move to history with their
/// end stamped; new and changed versions land live. Chained mutations of
/// one entity inside one commit collapse to a single transition.
pub(crate) fn diff_writes(
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) -> Vec<StagedWrite> {
    let mut writes = Vec::new();

    let schema_ids = base.schemas.keys().chain(state.schemas.keys());
    for &schema_id in schema_ids.collect::<std::collections::BTreeSet<_>>() {
        stage_transition(
            &mut writes,
            EntityKey::Schema { schema_id },
            base.schemas.get(&schema_id),
            state.schemas.get(&schema_id),
            new_snapshot,
            |b| proto::SchemaValue {
                end_snapshot: Some(new_snapshot),
                ..b.clone()
            },
        );
    }

    let table_ids = base.tables.keys().chain(state.tables.keys());
    for &table_id in table_ids.collect::<std::collections::BTreeSet<_>>() {
        stage_transition(
            &mut writes,
            EntityKey::Table { table_id },
            base.tables.get(&table_id),
            state.tables.get(&table_id),
            new_snapshot,
            |b| proto::TableValue {
                end_snapshot: Some(new_snapshot),
                ..b.clone()
            },
        );
    }

    let column_tables = base.columns.keys().chain(state.columns.keys());
    for &table_id in column_tables.collect::<std::collections::BTreeSet<_>>() {
        static EMPTY: std::collections::BTreeMap<u64, proto::ColumnValue> =
            std::collections::BTreeMap::new();
        let base_cols = base.columns.get(&table_id).unwrap_or(&EMPTY);
        let state_cols = state.columns.get(&table_id).unwrap_or(&EMPTY);
        for &column_id in base_cols
            .keys()
            .chain(state_cols.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            stage_transition(
                &mut writes,
                EntityKey::Column {
                    table_id,
                    column_id,
                },
                base_cols.get(&column_id),
                state_cols.get(&column_id),
                new_snapshot,
                |b| proto::ColumnValue {
                    end_snapshot: Some(new_snapshot),
                    ..b.clone()
                },
            );
        }
    }

    writes
}

/// Bounded internal retries before a benign race is reported as a
/// conflict. The attempt count mirrors the composing client's default
/// retry count; backoff and jitter are deliberately not implemented yet.
pub(crate) const MAX_COMMIT_ATTEMPTS: usize = 10;

/// The result of one commit attempt.
enum CommitOutcome {
    /// The attempt committed; carries the resulting snapshot id.
    Committed(SnapshotId),
    /// Lost the head race: what we tried to change, and the head our
    /// premise was read at — classification reads the commits above it.
    LostRace { ours: ChangeSet, head_before: u64 },
}

/// Runs one commit attempt: materialize, run the closure, stage, commit.
/// Returns [`CommitOutcome::Committed`] with the committed snapshot id (or
/// the unchanged head id when the closure staged nothing — no empty
/// snapshots), or [`CommitOutcome::LostRace`] when a concurrent commit won
/// the same head first.
///
/// Every exit that does not reach the final commit explicitly rolls the
/// transaction back first: a closure error, an empty op set, or a staging
/// write failure all abandon the transaction rather than leaving it
/// dangling.
async fn attempt_commit<F>(db: &Db, f: &F) -> Result<CommitOutcome>
where
    F: Fn(&mut Txn) -> Result<()>,
{
    let dbtxn = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;
    let base = match materialize(&dbtxn, None).await {
        Ok(base) => base,
        Err(err) => {
            dbtxn.rollback();
            return Err(err);
        }
    };
    let head = base.snap.snapshot_id;
    let new_id = head + 1;

    let mut txn = Txn::new(base.clone(), new_id);
    if let Err(err) = f(&mut txn) {
        dbtxn.rollback();
        return Err(err);
    }
    let (ops, state, next_catalog_id) = txn.into_parts();
    if ops.is_empty() {
        dbtxn.rollback();
        return Ok(CommitOutcome::Committed(SnapshotId::new(head)));
    }

    let mut writes = diff_writes(&base, &state, new_id);
    let schema_changed = ops.iter().any(Op::is_schema_changing);
    let ours = ChangeSet::from_ops(&ops);
    let snap = proto::SnapshotValue {
        snapshot_id: new_id,
        snapshot_time_micros: now_micros(),
        schema_version: base.snap.schema_version + u64::from(schema_changed),
        next_catalog_id,
        next_file_id: base.snap.next_file_id,
        next_deletion_id: base.snap.next_deletion_id,
        changes_made: ours.to_changes_made(),
        author: None,
        commit_message: None,
        commit_extra_info: None,
    };
    writes.push((
        Key::Snap {
            snapshot_id: new_id,
        }
        .encode(),
        Some(value::encode_value(&snap)),
    ));
    writes.push((
        Key::Sys(SysKey::Head).encode(),
        Some(value::encode_value(&proto::HeadValue {
            snapshot_id: new_id,
        })),
    ));

    for (key, write) in writes {
        let staged = match write {
            Some(bytes) => dbtxn.put(key, bytes),
            None => dbtxn.delete(key),
        };
        if let Err(err) = staged {
            dbtxn.rollback();
            return Err(err.into());
        }
    }

    match dbtxn.commit_with_options(&durable()).await {
        Ok(_) => Ok(CommitOutcome::Committed(SnapshotId::new(new_id))),
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => Ok(CommitOutcome::LostRace {
            ours,
            head_before: head,
        }),
        Err(err) => Err(err.into()),
    }
}

/// Commits through the closure, retrying benign races internally: a lost
/// head race whose intervening commits touched disjoint state re-runs
/// the whole cycle — fresh snapshot, closure re-run, fresh ids — so
/// logical premises are re-validated against the state that won. True
/// conflicts and an exhausted budget surface as [`Error::CommitConflict`].
pub(crate) async fn commit_cycle<F>(db: &Db, f: &F) -> Result<SnapshotId>
where
    F: Fn(&mut Txn) -> Result<()>,
{
    for _ in 0..MAX_COMMIT_ATTEMPTS {
        match attempt_commit(db, f).await? {
            CommitOutcome::Committed(id) => return Ok(id),
            CommitOutcome::LostRace { ours, head_before } => {
                let intervening = intervening_changes(db, head_before).await?;
                for (snapshot_id, theirs) in &intervening {
                    if crate::txn::ops::conflicts(&ours, theirs) {
                        return Err(Error::CommitConflict(format!(
                            "concurrent commit {snapshot_id} touched the same state"
                        )));
                    }
                }
            }
        }
    }
    Err(Error::CommitConflict(format!(
        "retry budget exhausted after {MAX_COMMIT_ATTEMPTS} attempts"
    )))
}

/// The change sets of every commit above `head_before`, read from their
/// snapshot records. Reads go straight through `db`: the transaction that
/// lost the race is dead, so classification reads outside any transaction.
/// A snapshot record missing below the head is store damage, not a race —
/// it surfaces as [`Error::Corruption`] and aborts the retry loop rather
/// than being retried.
async fn intervening_changes(db: &Db, head_before: u64) -> Result<Vec<(u64, ChangeSet)>> {
    let head_bytes = db
        .get(Key::Sys(SysKey::Head).encode())
        .await
        .map_err(Error::from)?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))?;
    let head: proto::HeadValue = value::decode_value(&head_bytes)?;
    let mut changes = Vec::new();
    for snapshot_id in (head_before + 1)..=head.snapshot_id {
        let bytes = db
            .get(Key::Snap { snapshot_id }.encode())
            .await
            .map_err(Error::from)?
            .ok_or_else(|| Error::Corruption(format!("snapshot record {snapshot_id} missing")))?;
        let snap: proto::SnapshotValue = value::decode_value(&bytes)?;
        changes.push((snapshot_id, ChangeSet::parse(&snap.changes_made)));
    }
    Ok(changes)
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    /// A store stamped with a newer structural format must be refused,
    /// not misread.
    #[tokio::test]
    async fn unknown_format_is_refused() {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let db = open_store("", object_store.clone()).await.unwrap();
        db.put(
            &Key::Sys(SysKey::Format).encode(),
            &value::encode_value(&proto::FormatValue {
                format_version: FORMAT_VERSION + 1,
                writer_version: "future".into(),
            }),
        )
        .await
        .unwrap();
        db.close().await.unwrap();

        // `Result::unwrap_err` needs `T: Debug`, and `slatedb::Db` has no
        // `Debug` impl; `err().unwrap()` only needs it on the error side.
        let err = open_initialized("", object_store).await.err().unwrap();
        assert!(matches!(err, Error::Corruption(_)));
    }

    /// A mid-migration marker refuses the open outright.
    #[tokio::test]
    async fn migration_marker_is_refused() {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let db = open_store("", object_store.clone()).await.unwrap();
        db.put(
            &Key::Sys(SysKey::Migration).encode(),
            &value::encode_value(&proto::MigrationValue {
                from_format: 1,
                to_format: 2,
                cursor: vec![],
            }),
        )
        .await
        .unwrap();
        db.close().await.unwrap();

        let err = open_initialized("", object_store).await.err().unwrap();
        assert!(matches!(err, Error::Corruption(_)));
    }

    /// A fresh reader opened after commit returns resolves the new head:
    /// commit durability must imply visibility to subsequently opened
    /// handles.
    #[tokio::test]
    async fn fresh_reader_sees_committed_head() {
        use crate::catalog::{Catalog, CatalogOptions};
        use slatedb::DbReader;

        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let catalog = Catalog::open(object_store.clone(), CatalogOptions::default())
            .await
            .unwrap();
        catalog
            .commit(|txn| txn.create_schema("visible").map(|_| ()))
            .await
            .unwrap();

        let reader = DbReader::builder("", object_store)
            .with_segment_extractor(Arc::new(crate::store::segment::TagSegmentExtractor))
            .build()
            .await
            .unwrap();
        let head_bytes = reader
            .get(Key::Sys(SysKey::Head).encode())
            .await
            .unwrap()
            .expect("fresh reader must see the head");
        let head: proto::HeadValue = value::decode_value(&head_bytes).unwrap();
        assert_eq!(head.snapshot_id, 1);
        reader.close().await.unwrap();
        catalog.close().await.unwrap();
    }
}
