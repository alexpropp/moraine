//! Opening, bootstrap, and snapshot materialization. The commit cycle
//! itself builds on these.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::ObjectStore;
use slatedb::{Db, DbTransaction, IsolationLevel, config::WriteOptions};
use uuid::Uuid;

use crate::{
    catalog::{CatalogSnapshot, SnapshotId},
    error::{Error, Result},
    store::{
        key::{EntityKey, Key, SysKey},
        open::open_store,
        proto, read, value,
    },
    transaction::{
        operations::{ChangeSet, Operation},
        verbs::Transaction,
    },
};

/// Structural layout version this binary reads and writes.
pub(crate) const FORMAT_VERSION: u64 = 1;
/// Bounded internal retries before a benign race is reported as a
/// conflict. The attempt count mirrors the composing client's default
/// retry count; backoff and jitter are deliberately not implemented yet.
pub(crate) const MAX_COMMIT_ATTEMPTS: usize = 10;

/// Current time in microseconds since the Unix epoch. Clamped, never
/// panicking: a clock before the epoch stamps 0.
pub(crate) fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

pub(crate) fn durable() -> WriteOptions {
    WriteOptions {
        await_durable: true,
        ..Default::default()
    }
}

/// Refuses a store this binary must not touch: mid-migration, or a
/// format newer/older than it understands. `None` format means the store
/// is empty and needs bootstrap.
async fn validate_format(tx: &DbTransaction) -> Result<Option<proto::FormatValue>> {
    if read::read_migration(tx).await?.is_some() {
        return Err(Error::Corruption(
            "store is mid-migration; refusing to open".to_string(),
        ));
    }
    match read::read_format(tx).await? {
        Some(format) if format.format_version == FORMAT_VERSION => Ok(Some(format)),
        Some(format) => Err(Error::Corruption(format!(
            "store format {} is not the supported format {FORMAT_VERSION}",
            format.format_version
        ))),
        None => Ok(None),
    }
}

/// Stages the initial state of an empty store into `tx`: format stamp,
/// snapshot 0 (carrying the default `main` schema, counters advanced past
/// its id), the `main` schema record itself, and head pointer. Mints the
/// same starting catalog shape a fresh DuckLake metadata store carries,
/// so a moraine store is attachable from birth.
fn stage_bootstrap(tx: &DbTransaction) -> Result<()> {
    let stage = |key: Key, bytes: Vec<u8>| tx.put(key.encode(), bytes).map_err(Error::from);
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
            next_catalog_id: 1,
            next_file_id: 0,
            next_deletion_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
        }),
    )?;
    stage(
        Key::cur(EntityKey::Schema { schema_id: 0 }),
        value::encode_value(&proto::SchemaValue {
            schema_id: 0,
            schema_uuid: Uuid::new_v4().to_string(),
            begin_snapshot: 0,
            end_snapshot: None,
            schema_name: "main".to_string(),
            path: "main/".to_string(),
            path_is_relative: true,
        }),
    )?;
    stage(
        Key::Sys(SysKey::Head),
        value::encode_value(&proto::HeadValue { snapshot_id: 0 }),
    )
}

/// Opens the store, bootstrapping an empty one in one atomic batch under
/// conflict detection — a lost bootstrap race re-validates instead of
/// double-initializing. Every exit that does not commit rolls back.
pub(crate) async fn open_initialized(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Db> {
    let db = open_store(path, object_store).await?;
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;
    match validate_format(&tx).await {
        Ok(Some(_)) => {
            tx.rollback();
            return Ok(db);
        }
        Ok(None) => {}
        Err(err) => {
            tx.rollback();
            return Err(err);
        }
    }
    if let Err(err) = stage_bootstrap(&tx) {
        tx.rollback();
        return Err(err);
    }
    match tx.commit_with_options(&durable()).await {
        Ok(_) => Ok(db),
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => {
            // Lost the bootstrap race: someone initialized concurrently.
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            let validated = validate_format(&tx).await;
            tx.rollback();
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
/// and any staged writes share one read point. `at: None` reads the head
/// (`cur` only); `at: Some(s)` also scans `hist` to reconstruct the
/// entities live at `s`.
pub(crate) async fn materialize(tx: &DbTransaction, at: Option<u64>) -> Result<CatalogSnapshot> {
    let head = read::read_head(tx)
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
    let snap = read::read_snapshot(tx, target)
        .await?
        .ok_or_else(|| Error::Corruption(format!("snapshot record {target} missing")))?;
    let cur = read::scan_cur_entities(tx).await?;
    let hist = match at {
        Some(_) => read::scan_hist_entities(tx).await?,
        None => Vec::new(),
    };
    Ok(CatalogSnapshot::build(snap, cur, hist, at.map(|_| target)))
}

/// One staged write: `Some` puts, `None` deletes.
pub(crate) type StagedWrite = (Vec<u8>, Option<Vec<u8>>);

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

/// Stages an unversioned record: overwritten in place, never mirrored to
/// history — statistics carry no begin/end lifecycle.
fn stage_overwrite<M: prost::Message + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    entity: EntityKey,
    base: Option<&M>,
    state: Option<&M>,
) {
    match (base, state) {
        (Some(_), None) => writes.push((Key::cur(entity).encode(), None)),
        (base, Some(s)) if base != Some(s) => {
            writes.push((Key::cur(entity).encode(), Some(value::encode_value(s))));
        }
        _ => {}
    }
}

fn diff_schemas(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    let schema_ids = base.schemas.keys().chain(state.schemas.keys());
    for &schema_id in schema_ids.collect::<std::collections::BTreeSet<_>>() {
        stage_transition(
            writes,
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
}

fn diff_tables(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    let table_ids = base.tables.keys().chain(state.tables.keys());
    for &table_id in table_ids.collect::<std::collections::BTreeSet<_>>() {
        stage_transition(
            writes,
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
}

fn diff_views(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    let view_ids = base.views.keys().chain(state.views.keys());
    for &view_id in view_ids.collect::<std::collections::BTreeSet<_>>() {
        stage_transition(
            writes,
            EntityKey::View { view_id },
            base.views.get(&view_id),
            state.views.get(&view_id),
            new_snapshot,
            |b| proto::ViewValue {
                end_snapshot: Some(new_snapshot),
                ..b.clone()
            },
        );
    }
}

fn diff_options(writes: &mut Vec<StagedWrite>, base: &CatalogSnapshot, state: &CatalogSnapshot) {
    let scopes = base.options.keys().chain(state.options.keys());
    for &(scope_kind, scope_id) in scopes.collect::<std::collections::BTreeSet<_>>() {
        stage_overwrite(
            writes,
            EntityKey::Option {
                scope_kind,
                scope_id,
            },
            base.options.get(&(scope_kind, scope_id)),
            state.options.get(&(scope_kind, scope_id)),
        );
    }
}

fn diff_columns(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
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
                writes,
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
}

fn diff_data_files(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    let file_tables = base.data_files.keys().chain(state.data_files.keys());
    for &table_id in file_tables.collect::<std::collections::BTreeSet<_>>() {
        static EMPTY: std::collections::BTreeMap<u64, proto::DataFileValue> =
            std::collections::BTreeMap::new();
        let base_files = base.data_files.get(&table_id).unwrap_or(&EMPTY);
        let state_files = state.data_files.get(&table_id).unwrap_or(&EMPTY);
        for &data_file_id in base_files
            .keys()
            .chain(state_files.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            stage_transition(
                writes,
                EntityKey::File {
                    table_id,
                    data_file_id,
                },
                base_files.get(&data_file_id),
                state_files.get(&data_file_id),
                new_snapshot,
                |b| proto::DataFileValue {
                    end_snapshot: Some(new_snapshot),
                    ..b.clone()
                },
            );
        }
    }
}

fn diff_delete_files(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    let delete_file_tables = base.delete_files.keys().chain(state.delete_files.keys());
    for &table_id in delete_file_tables.collect::<std::collections::BTreeSet<_>>() {
        static EMPTY: std::collections::BTreeMap<u64, proto::DeleteFileValue> =
            std::collections::BTreeMap::new();
        let base_files = base.delete_files.get(&table_id).unwrap_or(&EMPTY);
        let state_files = state.delete_files.get(&table_id).unwrap_or(&EMPTY);
        for &delete_file_id in base_files
            .keys()
            .chain(state_files.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            stage_transition(
                writes,
                EntityKey::DeleteFile {
                    table_id,
                    delete_file_id,
                },
                base_files.get(&delete_file_id),
                state_files.get(&delete_file_id),
                new_snapshot,
                |b| proto::DeleteFileValue {
                    end_snapshot: Some(new_snapshot),
                    ..b.clone()
                },
            );
        }
    }
}

fn diff_table_stats(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    let table_ids = base.table_stats.keys().chain(state.table_stats.keys());
    for &table_id in table_ids.collect::<std::collections::BTreeSet<_>>() {
        stage_overwrite(
            writes,
            EntityKey::TableStats { table_id },
            base.table_stats.get(&table_id),
            state.table_stats.get(&table_id),
        );
    }
}

fn diff_table_column_stats(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    let table_ids = base
        .table_column_stats
        .keys()
        .chain(state.table_column_stats.keys());
    for &table_id in table_ids.collect::<std::collections::BTreeSet<_>>() {
        static EMPTY: std::collections::BTreeMap<u64, proto::TableColumnStatsValue> =
            std::collections::BTreeMap::new();
        let base_cols = base.table_column_stats.get(&table_id).unwrap_or(&EMPTY);
        let state_cols = state.table_column_stats.get(&table_id).unwrap_or(&EMPTY);
        for &column_id in base_cols
            .keys()
            .chain(state_cols.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            stage_overwrite(
                writes,
                EntityKey::TableColumnStats {
                    table_id,
                    column_id,
                },
                base_cols.get(&column_id),
                state_cols.get(&column_id),
            );
        }
    }
}

fn diff_file_column_stats(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    let table_ids = base
        .file_column_stats
        .keys()
        .chain(state.file_column_stats.keys());
    for &table_id in table_ids.collect::<std::collections::BTreeSet<_>>() {
        static EMPTY: std::collections::BTreeMap<(u64, u64), proto::FileColumnStatsValue> =
            std::collections::BTreeMap::new();
        static EMPTY_FILES: std::collections::BTreeMap<u64, proto::DataFileValue> =
            std::collections::BTreeMap::new();
        let base_cols = base.file_column_stats.get(&table_id).unwrap_or(&EMPTY);
        let state_cols = state.file_column_stats.get(&table_id).unwrap_or(&EMPTY);
        let base_files = base.data_files.get(&table_id).unwrap_or(&EMPTY_FILES);
        let state_files = state.data_files.get(&table_id).unwrap_or(&EMPTY_FILES);
        for &(data_file_id, column_id) in base_cols
            .keys()
            .chain(state_cols.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            // A file registered and expired within this commit exists in
            // neither side's data_files; its stats must not be staged.
            if !base_files.contains_key(&data_file_id) && !state_files.contains_key(&data_file_id) {
                continue;
            }
            stage_overwrite(
                writes,
                EntityKey::FileColumnStats {
                    table_id,
                    data_file_id,
                    column_id,
                },
                base_cols.get(&(data_file_id, column_id)),
                state_cols.get(&(data_file_id, column_id)),
            );
        }
    }
}

/// The write set turning `base` into `state` at `new_snapshot`: ended
/// versions move to history, new and changed versions land live, and
/// chained mutations of one entity collapse to a single transition.
/// Statistics are overwritten in place with no history mirror.
pub(crate) fn diff_writes(
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) -> Vec<StagedWrite> {
    let mut writes = Vec::new();
    diff_schemas(&mut writes, base, state, new_snapshot);
    diff_tables(&mut writes, base, state, new_snapshot);
    diff_views(&mut writes, base, state, new_snapshot);
    diff_columns(&mut writes, base, state, new_snapshot);
    diff_data_files(&mut writes, base, state, new_snapshot);
    diff_delete_files(&mut writes, base, state, new_snapshot);
    diff_table_stats(&mut writes, base, state);
    diff_table_column_stats(&mut writes, base, state);
    diff_file_column_stats(&mut writes, base, state);
    diff_options(&mut writes, base, state);
    writes
}

/// The result of one commit attempt.
enum CommitOutcome {
    /// The attempt committed; carries the resulting snapshot id.
    Committed(SnapshotId),
    /// Lost the head race: what we tried to change, and the head our
    /// premise was read at — classification reads the commits above it.
    LostRace {
        ours: Box<ChangeSet>,
        head_before: u64,
    },
}

/// Runs one commit attempt: materialize, run the closure, stage, commit.
/// An empty op set commits nothing and returns the unchanged head. Every
/// exit that does not reach the final commit rolls the transaction back.
async fn attempt_commit<F>(db: &Db, f: &F) -> Result<CommitOutcome>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    let db_tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;

    match prepare_and_stage(&db_tx, f).await {
        Ok(Prepared::Nothing { head }) => {
            db_tx.rollback();
            Ok(CommitOutcome::Committed(SnapshotId::new(head)))
        }
        Ok(Prepared::Staged {
            ours,
            head_before,
            commits,
        }) => finish_commit(db_tx, ours, head_before, commits).await,
        Err(err) => {
            db_tx.rollback();
            Err(err)
        }
    }
}

/// What one attempt staged onto its transaction.
enum Prepared {
    /// The closure changed nothing; the head is unchanged.
    Nothing {
        /// The head snapshot id the attempt read.
        head: u64,
    },
    /// Writes are staged and ready to commit.
    Staged {
        /// This attempt's change set, empty for an options-only commit.
        ours: Box<ChangeSet>,
        /// The head snapshot id the attempt's premise was read at.
        head_before: u64,
        /// The snapshot id a successful commit reports.
        commits: u64,
    },
}

/// Materializes, runs the closure, and stages the resulting writes.
/// Options-only commits stage no snapshot record and no head advance.
async fn prepare_and_stage<F>(db_tx: &DbTransaction, f: &F) -> Result<Prepared>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    let base = materialize(db_tx, None).await?;
    let head = base.snap.snapshot_id;
    let new_id = head + 1;

    let mut tx = Transaction::new(base.clone(), new_id);
    f(&mut tx)?;
    let (operations, state, next_catalog_id, next_file_id) = tx.into_parts();

    if operations.is_empty() {
        let mut writes = Vec::new();
        diff_options(&mut writes, &base, &state);
        if writes.is_empty() {
            return Ok(Prepared::Nothing { head });
        }
        stage_writes(db_tx, writes)?;
        return Ok(Prepared::Staged {
            ours: Box::default(),
            head_before: head,
            commits: head,
        });
    }

    let mut writes = diff_writes(&base, &state, new_id);
    let schema_changed = operations.iter().any(Operation::is_schema_changing);
    let ours = ChangeSet::from_operations(&operations);

    let snap = proto::SnapshotValue {
        snapshot_id: new_id,
        snapshot_time_micros: now_micros(),
        schema_version: base.snap.schema_version + u64::from(schema_changed),
        next_catalog_id,
        next_file_id,
        next_deletion_id: base.snap.next_deletion_id,
        changes_made: ours.to_changes_made(),
        author: None,
        commit_message: None,
        commit_extra_info: None,
        schema_changed_table_ids: Vec::new(),
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
    stage_writes(db_tx, writes)?;

    Ok(Prepared::Staged {
        ours: Box::new(ours),
        head_before: head,
        commits: new_id,
    })
}

pub(crate) fn stage_writes(db_tx: &DbTransaction, writes: Vec<StagedWrite>) -> Result<()> {
    for (key, write) in writes {
        match write {
            Some(bytes) => db_tx.put(key, bytes),
            None => db_tx.delete(key),
        }
        .map_err(Error::from)?;
    }
    Ok(())
}

async fn finish_commit(
    db_tx: DbTransaction,
    ours: Box<ChangeSet>,
    head_before: u64,
    commits: u64,
) -> Result<CommitOutcome> {
    match db_tx.commit_with_options(&durable()).await {
        Ok(_) => Ok(CommitOutcome::Committed(SnapshotId::new(commits))),
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => {
            Ok(CommitOutcome::LostRace { ours, head_before })
        }
        Err(err) => Err(err.into()),
    }
}

/// Commits through the closure, retrying benign races with a full re-run
/// — fresh snapshot, closure, ids — so premises re-validate against the
/// state that won. True conflicts and an exhausted budget surface as
/// [`Error::CommitConflict`].
pub(crate) async fn commit_cycle<F>(db: &Db, f: &F) -> Result<SnapshotId>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    for _ in 0..MAX_COMMIT_ATTEMPTS {
        match attempt_commit(db, f).await? {
            CommitOutcome::Committed(id) => return Ok(id),
            CommitOutcome::LostRace { ours, head_before } => {
                // An options-only loser is last-write-wins: always benign.
                if ours.is_empty() {
                    continue;
                }
                let intervening = intervening_changes(db, head_before).await?;
                for (snapshot_id, theirs) in &intervening {
                    if crate::transaction::operations::conflicts(&ours, theirs) {
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

/// The change sets of every commit above `head_before`, read outside any
/// transaction (the loser's is dead). A missing snapshot record below
/// the head is store damage: [`Error::Corruption`], not a retry.
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

    /// A file registered and expired within one commit exists in neither
    /// `base` nor `state`'s `data_files`: its per-file column stats are
    /// orphaned and must never be staged as a write.
    #[test]
    fn register_then_expire_in_one_commit_stages_no_orphaned_file_column_stats() {
        use crate::catalog::{ColumnDef, DataFile, FileColumnStats};
        use crate::store::key::{CurKey, HistKey};

        let snap0 = proto::SnapshotValue {
            snapshot_id: 0,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 0,
            next_file_id: 0,
            next_deletion_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
        };
        let empty = CatalogSnapshot::build(snap0, vec![], vec![], None);
        let mut setup = Transaction::new(empty, 1);
        let schema = setup.create_schema("s").unwrap();
        let table = setup
            .create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                }],
            )
            .unwrap();
        let column = setup.columns_of(table)[0].id;
        let (_, base, _, _) = setup.into_parts();

        // Register a file with column stats, then expire it — all inside
        // this one commit's transaction.
        let mut tx = Transaction::new(base.clone(), 2);
        let file = tx
            .register_data_file(
                table,
                DataFile {
                    path: "f.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 10,
                    file_size_bytes: 100,
                    footer_size: 4,
                    column_stats: vec![FileColumnStats {
                        column_id: column,
                        column_size_bytes: 10,
                        value_count: 10,
                        null_count: 0,
                        min_value: Some("1".into()),
                        max_value: Some("2".into()),
                        contains_nan: None,
                        extra_stats: None,
                    }],
                },
            )
            .unwrap();
        tx.expire_data_file(table, file).unwrap();
        let (_, state, _, _) = tx.into_parts();

        let writes = diff_writes(&base, &state, 2);
        for (key_bytes, _) in &writes {
            let key = Key::decode(key_bytes).unwrap();
            let is_file_column_stats = matches!(
                key,
                Key::Cur(CurKey::Entity(EntityKey::FileColumnStats { .. }))
                    | Key::Hist(HistKey {
                        entity: EntityKey::FileColumnStats { .. },
                        ..
                    })
            );
            assert!(
                !is_file_column_stats,
                "orphaned file_column_stats write staged: {key:?}"
            );
        }
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
            .commit(|tx| tx.create_schema("visible").map(|_| ()))
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
