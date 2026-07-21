//! Opening, bootstrap, and snapshot materialization. The commit cycle
//! itself builds on these.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use slatedb::{Db, DbReader, DbTransaction, IsolationLevel, config::WriteOptions};
use uuid::Uuid;

use crate::{
    catalog::{
        CatalogSnapshot, SnapshotId,
        projection::{ProjectionCache, fold_committed_batch},
    },
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{EntityKey, Key, SysKey},
        open::StoreBuilder,
        proto, read, value,
    },
    transaction::{
        index_maintenance,
        operations::{ChangeSet, Operation},
        verbs::{Transaction, TransactionParts},
    },
};

/// Structural layout version a fresh store bootstraps at — format 1 plus
/// nothing. Index-free stores stay here, byte-identical to pre-index
/// stores and readable by older binaries.
pub(crate) const FORMAT_VERSION: u64 = 1;
/// Format stamped lazily the first time an equality index exists: format 1
/// plus the `idx` subspace and `index` kind. Older binaries, which
/// maintain no entries, refuse it.
pub(crate) const FORMAT_WITH_INDEX: u64 = 2;
/// Format stamped the first time a staged (multi-commit) index build
/// exists. A format-2 binary would read a `building` definition as a ready
/// index and serve from an under-covered entry set, so it must refuse this.
pub(crate) const FORMAT_WITH_STAGED_INDEX: u64 = 3;
/// The highest format this binary understands. It opens any store in
/// `FORMAT_VERSION..=MAX_FORMAT_VERSION` and refuses a newer one.
pub(crate) const MAX_FORMAT_VERSION: u64 = FORMAT_WITH_STAGED_INDEX;
/// Bounded internal retries before a benign race is reported as a
/// conflict.
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
async fn validate_format(tx: ReadHandle<'_>) -> Result<Option<proto::FormatValue>> {
    if read::read_migration(tx).await?.is_some() {
        return Err(Error::Corruption(
            "store is mid-migration; refusing to open".to_string(),
        ));
    }
    match read::read_format(tx).await? {
        Some(format) if (FORMAT_VERSION..=MAX_FORMAT_VERSION).contains(&format.format_version) => {
            Ok(Some(format))
        }
        Some(format) => Err(Error::Corruption(format!(
            "store format {} is outside the supported range {FORMAT_VERSION}..={MAX_FORMAT_VERSION}",
            format.format_version
        ))),
        None => Ok(None),
    }
}

/// Stages the initial state of an empty store into `tx`: format stamp,
/// snapshot 0 (carrying the default `main` schema, counters advanced past
/// its id), the `main` schema record itself, the global `encrypted`
/// option, and head pointer — the same starting catalog shape a fresh
/// DuckLake metadata store carries.
///
/// `encrypted` is recorded explicitly (as `"true"`/`"false"`) and only
/// here: whether data files are encrypted is fixed when the catalog is
/// created, exactly as DuckLake fixes it when initializing a metadata
/// store.
fn stage_bootstrap(tx: &DbTransaction, encrypted: bool, data_path: Option<&str>) -> Result<()> {
    let stage = |key: Key, bytes: Vec<u8>| tx.put(key.encode(), bytes).map_err(Error::from);
    stage(
        Key::Sys(SysKey::Format),
        value::encode_value(&proto::FormatValue {
            format_version: FORMAT_VERSION,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    )?;
    // Bootstrap's snapshot records minting `main`, byte-identical to the
    // `created_schema:"main"` DuckLake's own initialization writes.
    let mut bootstrap_changes = ChangeSet::default();
    bootstrap_changes.created_schemas.insert("main".to_string());
    stage(
        Key::Snapshot { snapshot_id: 0 },
        value::encode_value(&proto::SnapshotValue {
            snapshot_id: 0,
            snapshot_time_micros: now_micros(),
            schema_version: 0,
            next_catalog_id: 1,
            next_file_id: 0,
            changes_made: bootstrap_changes.to_changes_made(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
        }),
    )?;
    stage(
        Key::current(EntityKey::Schema { schema_id: 0 }),
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
    // The global option record carries `encrypted` and, when the lake was
    // given a data root, `data_path` — so a later open reads the root back
    // without being told it again.
    let mut options = std::collections::HashMap::from([(
        "encrypted".to_string(),
        if encrypted { "true" } else { "false" }.to_string(),
    )]);
    if let Some(path) = data_path {
        options.insert("data_path".to_string(), path.to_string());
    }
    stage(
        Key::current(EntityKey::Option {
            scope_kind: 0,
            scope_id: 0,
        }),
        value::encode_value(&proto::OptionScopeValue { options }),
    )?;
    stage(
        Key::Sys(SysKey::Head),
        value::encode_value(&proto::HeadValue { snapshot_id: 0 }),
    )
}

/// Opens the store, bootstrapping an empty one in one atomic batch under
/// conflict detection — a lost bootstrap race re-validates instead of
/// double-initializing. Every exit that does not commit rolls back.
pub(crate) async fn open_initialized(
    store: StoreBuilder<'_>,
    encrypted: bool,
    data_path: Option<&str>,
) -> Result<Db> {
    let db = store.open_writer().await?;
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;

    match validate_format(ReadHandle::Tx(&tx)).await {
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

    if let Err(err) = stage_bootstrap(&tx, encrypted, data_path) {
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
            let validated = validate_format(ReadHandle::Tx(&tx)).await;
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

/// Opens the store read-only as a [`DbReader`], validating the format it
/// finds. Never opens a `Db`, so it never fences a live writer, and never
/// bootstraps — a read-only attach against an uninitialized store is refused
/// (there is nothing committed to read).
pub(crate) async fn open_reader_initialized(store: StoreBuilder<'_>) -> Result<DbReader> {
    let reader = store.open_reader().await?;
    match validate_format(ReadHandle::Reader(&reader)).await? {
        Some(_) => Ok(reader),
        None => Err(Error::Corruption(
            "store is not an initialized moraine catalog; a read-only attach \
             needs a writer to have created it first"
                .to_string(),
        )),
    }
}

/// Materializes a catalog view through an open transaction, so the view
/// and any staged writes share one read point. `at: None` reads the head
/// (`current` only); `at: Some(s)` also scans `history` to reconstruct the
/// entities live at `s`.
pub(crate) async fn materialize(tx: ReadHandle<'_>, at: Option<u64>) -> Result<CatalogSnapshot> {
    let head = read::read_head(tx)
        .await?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))?
        .snapshot_id;
    let target = match at {
        Some(requested) if requested > head => {
            return Err(Error::NotFound(format!(
                "snapshot {requested} (head is {head})"
            )));
        }
        Some(requested) => requested,
        None => head,
    };
    // A missing record at or below head is an expired snapshot, not
    // corruption: expiry deletes snapshot records without renumbering.
    // The caller re-resolves from head.
    let snapshot = read::read_snapshot(tx, target)
        .await?
        .ok_or_else(|| Error::NotFound(format!("snapshot {target} (expired or never minted)")))?;
    let current = read::scan_current_entities(tx).await?;
    let history = match at {
        Some(_) => read::scan_history_entities(tx).await?,
        None => Vec::new(),
    };

    Ok(CatalogSnapshot::build(
        snapshot,
        current,
        history,
        at.map(|_| target),
    ))
}

/// One staged write: `Some` puts, `None` deletes.
pub(crate) type StagedWrite = (Vec<u8>, Option<Vec<u8>>);

mod diff;
use diff::diff_options;
pub(crate) use diff::diff_writes;

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
async fn attempt_commit<F>(
    db: &Db,
    f: &F,
    projections: &std::sync::RwLock<ProjectionCache>,
) -> Result<CommitOutcome>
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
            writes,
        }) => finish_commit(db_tx, ours, head_before, commits, writes, projections).await,
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
        /// The staged batch, kept so a successful commit can fold it into
        /// the maintained projections.
        writes: Vec<StagedWrite>,
    },
}

/// The store format the staged state requires: a `building` index implies
/// [`FORMAT_WITH_STAGED_INDEX`], any other index [`FORMAT_WITH_INDEX`],
/// else the base [`FORMAT_VERSION`].
fn target_format(state: &CatalogSnapshot) -> u64 {
    if state
        .indexes
        .values()
        .flat_map(BTreeMap::values)
        .any(|index| index.build_state.is_some())
    {
        FORMAT_WITH_STAGED_INDEX
    } else if state
        .indexes
        .values()
        .any(|per_table| !per_table.is_empty())
    {
        FORMAT_WITH_INDEX
    } else {
        FORMAT_VERSION
    }
}

/// Materializes, runs the closure, and stages the resulting writes.
/// Options-only commits stage no snapshot record and no head advance.
async fn prepare_and_stage<F>(db_tx: &DbTransaction, f: &F) -> Result<Prepared>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    let base = materialize(ReadHandle::Tx(db_tx), None).await?;
    let head = base.snapshot.snapshot_id;
    let new_id = head + 1;

    let mut tx = Transaction::new(base.clone(), new_id);
    f(&mut tx)?;
    let TransactionParts {
        operations,
        index_entries,
        state,
        next_catalog_id,
        next_file_id,
    } = tx.into_parts();

    if operations.is_empty() {
        let mut writes = Vec::new();
        diff_options(&mut writes, &base, &state);
        if writes.is_empty() {
            return Ok(Prepared::Nothing { head });
        }
        // Re-put the unchanged head as a conflict anchor: every
        // snapshot-minting commit writes it, so a racing drop of this
        // option's scope forces a re-run that re-validates the scope
        // against the winner's state instead of committing blind.
        writes.push((
            Key::Sys(SysKey::Head).encode(),
            Some(value::encode_value(&proto::HeadValue { snapshot_id: head })),
        ));
        stage_writes(db_tx, &writes)?;
        return Ok(Prepared::Staged {
            ours: Box::default(),
            head_before: head,
            commits: head,
            writes,
        });
    }

    let mut writes = diff_writes(&base, &state, new_id);

    // Stamp the format lazily, in the same batch, and only ever upward — a
    // completed or dropped build never downgrades the stamp.
    let target_format = target_format(&state);
    if target_format > FORMAT_VERSION {
        let current = read::read_format(ReadHandle::Tx(db_tx))
            .await?
            .map_or(FORMAT_VERSION, |format| format.format_version);
        if current < target_format {
            writes.push((
                Key::Sys(SysKey::Format).encode(),
                Some(value::encode_value(&proto::FormatValue {
                    format_version: target_format,
                    writer_version: env!("CARGO_PKG_VERSION").to_string(),
                })),
            ));
        }
    }
    index_maintenance::stage_index_entries(ReadHandle::Tx(db_tx), &index_entries, &mut writes)
        .await?;

    let schema_changed = operations.iter().any(Operation::is_schema_changing);
    let schema_changed_table_ids: Vec<u64> = operations
        .iter()
        .filter_map(Operation::schema_changed_table_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let ours = ChangeSet::from_operations(&operations);

    let snapshot = proto::SnapshotValue {
        snapshot_id: new_id,
        snapshot_time_micros: now_micros(),
        schema_version: base.snapshot.schema_version + u64::from(schema_changed),
        next_catalog_id,
        next_file_id,
        changes_made: ours.to_changes_made(),
        author: None,
        commit_message: None,
        commit_extra_info: None,
        schema_changed_table_ids,
    };
    writes.push((
        Key::Snapshot {
            snapshot_id: new_id,
        }
        .encode(),
        Some(value::encode_value(&snapshot)),
    ));
    writes.push((
        Key::Sys(SysKey::Head).encode(),
        Some(value::encode_value(&proto::HeadValue {
            snapshot_id: new_id,
        })),
    ));
    stage_writes(db_tx, &writes)?;

    Ok(Prepared::Staged {
        ours: Box::new(ours),
        head_before: head,
        commits: new_id,
        writes,
    })
}

pub(crate) fn stage_writes(db_tx: &DbTransaction, writes: &[StagedWrite]) -> Result<()> {
    for (key, write) in writes {
        match write {
            Some(bytes) => db_tx.put(key.clone(), bytes.clone()),
            None => db_tx.delete(key.clone()),
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
    writes: Vec<StagedWrite>,
    projections: &std::sync::RwLock<ProjectionCache>,
) -> Result<CommitOutcome> {
    match db_tx.commit_with_options(&durable()).await {
        Ok(_) => {
            fold_committed_batch(projections, &writes, commits);
            Ok(CommitOutcome::Committed(SnapshotId::new(commits)))
        }
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
pub(crate) async fn commit_cycle<F>(
    db: &Db,
    f: &F,
    projections: &std::sync::RwLock<ProjectionCache>,
) -> Result<SnapshotId>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    for _ in 0..MAX_COMMIT_ATTEMPTS {
        match attempt_commit(db, f, projections).await? {
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
/// transaction (the loser's is dead). A record that has already been
/// expired by a racing maintenance commit classifies as an unknowable
/// change (forcing the conflict path), never as corruption — the caller
/// re-drives against the new head.
async fn intervening_changes(db: &Db, head_before: u64) -> Result<Vec<(u64, ChangeSet)>> {
    let head_bytes = db
        .get(Key::Sys(SysKey::Head).encode())
        .await
        .map_err(Error::from)?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))?;
    let head: proto::HeadValue = value::decode_value(&head_bytes)?;
    let mut changes = Vec::new();

    for snapshot_id in (head_before + 1)..=head.snapshot_id {
        let change_set = match db
            .get(Key::Snapshot { snapshot_id }.encode())
            .await
            .map_err(Error::from)?
        {
            Some(bytes) => {
                let snapshot: proto::SnapshotValue = value::decode_value(&bytes)?;
                ChangeSet::parse(&snapshot.changes_made)
            }
            None => ChangeSet {
                has_unknown: true,
                ..ChangeSet::default()
            },
        };
        changes.push((snapshot_id, change_set));
    }

    Ok(changes)
}

#[cfg(test)]
mod tests;
