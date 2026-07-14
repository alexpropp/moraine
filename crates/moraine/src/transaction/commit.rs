//! Opening, bootstrap, and snapshot materialization. The commit cycle
//! itself builds on these.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use object_store::ObjectStore;
use slatedb::{Db, DbReader, DbTransaction, IsolationLevel, config::WriteOptions};
use uuid::Uuid;

use crate::{
    catalog::{CatalogSnapshot, SnapshotId},
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{CurrentKey, EntityKey, Key, SysKey},
        open::{open_reader, open_store},
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
/// its id), the `main` schema record itself, the global `encrypted`
/// option, and head pointer — the same starting catalog shape a fresh
/// DuckLake metadata store carries.
///
/// `encrypted` is recorded explicitly (as `"true"`/`"false"`) and only
/// here: whether data files are encrypted is fixed when the catalog is
/// created, exactly as DuckLake fixes it when initializing a metadata
/// store.
fn stage_bootstrap(tx: &DbTransaction, encrypted: bool) -> Result<()> {
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
    stage(
        Key::current(EntityKey::Option {
            scope_kind: 0,
            scope_id: 0,
        }),
        value::encode_value(&proto::OptionScopeValue {
            options: std::collections::HashMap::from([(
                "encrypted".to_string(),
                if encrypted { "true" } else { "false" }.to_string(),
            )]),
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
pub(crate) async fn open_initialized(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    encrypted: bool,
) -> Result<Db> {
    let db = open_store(path, object_store).await?;
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

    if let Err(err) = stage_bootstrap(&tx, encrypted) {
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
pub(crate) async fn open_reader_initialized(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<DbReader> {
    let reader = open_reader(path, object_store).await?;
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

fn stage_transition<M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    entity: EntityKey,
    base: Option<&M>,
    state: Option<&M>,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
) {
    match (base, state) {
        (Some(base), None) => {
            writes.push((Key::current(entity).encode(), None));
            writes.push((
                Key::history(entity, new_snapshot).encode(),
                Some(value::encode_value(&set_end(base))),
            ));
        }
        (Some(base), Some(state)) if base != state => {
            writes.push((
                Key::history(entity, new_snapshot).encode(),
                Some(value::encode_value(&set_end(base))),
            ));
            writes.push((
                Key::current(entity).encode(),
                Some(value::encode_value(state)),
            ));
        }
        (None, Some(state)) => {
            writes.push((
                Key::current(entity).encode(),
                Some(value::encode_value(state)),
            ));
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
        (Some(_), None) => writes.push((Key::current(entity).encode(), None)),
        (base, Some(state)) if base != Some(state) => {
            writes.push((
                Key::current(entity).encode(),
                Some(value::encode_value(state)),
            ));
        }
        _ => {}
    }
}

/// Stages the transition of every entity in one id-keyed map pair.
fn diff_versioned_map<K: Copy + Ord, M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    base: &std::collections::BTreeMap<K, M>,
    state: &std::collections::BTreeMap<K, M>,
    make_key: impl Fn(K) -> EntityKey,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
) {
    for &id in base
        .keys()
        .chain(state.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        stage_transition(
            writes,
            make_key(id),
            base.get(&id),
            state.get(&id),
            new_snapshot,
            &set_end,
        );
    }
}

/// Stages the transition of every entity in one table-scoped nested map
/// pair (`table_id` → `id` → record).
fn diff_nested_versioned<K: Copy + Ord, M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    base: &std::collections::BTreeMap<u64, std::collections::BTreeMap<K, M>>,
    state: &std::collections::BTreeMap<u64, std::collections::BTreeMap<K, M>>,
    make_key: impl Fn(u64, K) -> EntityKey,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
) {
    let empty = std::collections::BTreeMap::new();
    for &table_id in base
        .keys()
        .chain(state.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        diff_versioned_map(
            writes,
            base.get(&table_id).unwrap_or(&empty),
            state.get(&table_id).unwrap_or(&empty),
            |id| make_key(table_id, id),
            new_snapshot,
            &set_end,
        );
    }
}

/// Stages the in-place overwrite of every record in one id-keyed map
/// pair — unversioned kinds with no history mirror.
fn diff_overwrite_map<K: Copy + Ord, M: prost::Message + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    base: &std::collections::BTreeMap<K, M>,
    state: &std::collections::BTreeMap<K, M>,
    make_key: impl Fn(K) -> EntityKey,
) {
    for &id in base
        .keys()
        .chain(state.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        stage_overwrite(writes, make_key(id), base.get(&id), state.get(&id));
    }
}

fn diff_schemas(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_versioned_map(
        writes,
        &base.schemas,
        &state.schemas,
        |schema_id| EntityKey::Schema { schema_id },
        new_snapshot,
        |prior| proto::SchemaValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_tables(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    // The table row minus its field-id counter: the part whose change
    // means a version transition.
    fn sans_counter(value: &proto::TableValue) -> proto::TableValue {
        proto::TableValue {
            next_column_id: 0,
            ..value.clone()
        }
    }

    let table_ids = base.tables.keys().chain(state.tables.keys());
    for &table_id in table_ids.collect::<std::collections::BTreeSet<_>>() {
        let entity = EntityKey::Table { table_id };

        // A counter-only change overwrites the record in place: the
        // field-id counter is moraine-internal bookkeeping, so the table
        // row did not transition and must not mint a history version.
        if let (Some(prior), Some(next)) = (base.tables.get(&table_id), state.tables.get(&table_id))
            && prior != next
            && sans_counter(prior) == sans_counter(next)
        {
            writes.push((
                Key::current(entity).encode(),
                Some(value::encode_value(next)),
            ));
            continue;
        }

        stage_transition(
            writes,
            entity,
            base.tables.get(&table_id),
            state.tables.get(&table_id),
            new_snapshot,
            |prior| proto::TableValue {
                end_snapshot: Some(new_snapshot),
                ..prior.clone()
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
    diff_versioned_map(
        writes,
        &base.views,
        &state.views,
        |view_id| EntityKey::View { view_id },
        new_snapshot,
        |prior| proto::ViewValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_options(writes: &mut Vec<StagedWrite>, base: &CatalogSnapshot, state: &CatalogSnapshot) {
    diff_overwrite_map(
        writes,
        &base.options,
        &state.options,
        |(scope_kind, scope_id)| EntityKey::Option {
            scope_kind,
            scope_id,
        },
    );
}

fn diff_tags(writes: &mut Vec<StagedWrite>, base: &CatalogSnapshot, state: &CatalogSnapshot) {
    let object_ids = base.tags.keys().chain(state.tags.keys());
    for &object_id in object_ids.collect::<std::collections::BTreeSet<_>>() {
        stage_overwrite(
            writes,
            EntityKey::Tag { object_id },
            base.tags.get(&object_id),
            state.tags.get(&object_id),
        );
    }
}

/// Deletion-schedule rows: live bookkeeping under `current/gcfile`,
/// overwritten or removed in place — never mirrored to history.
fn diff_gc_files(writes: &mut Vec<StagedWrite>, base: &CatalogSnapshot, state: &CatalogSnapshot) {
    let file_ids = base.gc_files.keys().chain(state.gc_files.keys());
    for &data_file_id in file_ids.collect::<std::collections::BTreeSet<_>>() {
        let key = Key::Current(CurrentKey::GcFile { data_file_id });
        match (
            base.gc_files.get(&data_file_id),
            state.gc_files.get(&data_file_id),
        ) {
            (Some(_), None) => writes.push((key.encode(), None)),
            (prior, Some(next)) if prior != Some(next) => {
                writes.push((key.encode(), Some(value::encode_value(next))));
            }
            _ => {}
        }
    }
}

fn diff_macros(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    let macro_ids = base.macros.keys().chain(state.macros.keys());
    for &macro_id in macro_ids.collect::<std::collections::BTreeSet<_>>() {
        stage_transition(
            writes,
            EntityKey::Macro { macro_id },
            base.macros.get(&macro_id),
            state.macros.get(&macro_id),
            new_snapshot,
            |prior| proto::MacroValue {
                end_snapshot: Some(new_snapshot),
                ..prior.clone()
            },
        );
    }
}

/// Mappings are immutable create-only records: `stage_overwrite`'s
/// `(None, Some)` arm writes the `current` key with no history mirror,
/// and its equality guard makes a base-present record (always
/// byte-identical — the staged path rejects re-insertion) a no-op.
/// Iterating `state` alone suffices: mappings are never removed from the
/// working state, so the delete arm is unreachable.
fn diff_mappings(writes: &mut Vec<StagedWrite>, base: &CatalogSnapshot, state: &CatalogSnapshot) {
    for (&table_id, per_table) in &state.mappings {
        for (&mapping_id, value) in per_table {
            stage_overwrite(
                writes,
                EntityKey::Mapping {
                    table_id,
                    mapping_id,
                },
                base.mappings
                    .get(&table_id)
                    .and_then(|b| b.get(&mapping_id)),
                Some(value),
            );
        }
    }
}

fn diff_columns(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    // The column row minus its embedded tag entries: the part whose
    // change means a version transition.
    fn sans_tags(value: &proto::ColumnValue) -> proto::ColumnValue {
        proto::ColumnValue {
            tags: Vec::new(),
            ..value.clone()
        }
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
            let entity = EntityKey::Column {
                table_id,
                column_id,
            };

            // A tags-only change overwrites the record in place: tag
            // entries carry their own begin/end, so the column row did
            // not transition and must not mint a history version.
            if let (Some(prior), Some(next)) =
                (base_cols.get(&column_id), state_cols.get(&column_id))
                && prior != next
                && sans_tags(prior) == sans_tags(next)
            {
                writes.push((
                    Key::current(entity).encode(),
                    Some(value::encode_value(next)),
                ));
                continue;
            }

            stage_transition(
                writes,
                entity,
                base_cols.get(&column_id),
                state_cols.get(&column_id),
                new_snapshot,
                |prior| proto::ColumnValue {
                    end_snapshot: Some(new_snapshot),
                    ..prior.clone()
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
    diff_nested_versioned(
        writes,
        &base.data_files,
        &state.data_files,
        |table_id, data_file_id| EntityKey::File {
            table_id,
            data_file_id,
        },
        new_snapshot,
        |prior| proto::DataFileValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_delete_files(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
        &base.delete_files,
        &state.delete_files,
        |table_id, delete_file_id| EntityKey::DeleteFile {
            table_id,
            delete_file_id,
        },
        new_snapshot,
        |prior| proto::DeleteFileValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_partitions(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
        &base.partitions,
        &state.partitions,
        |table_id, partition_id| EntityKey::Partition {
            table_id,
            partition_id,
        },
        new_snapshot,
        |prior| proto::PartitionValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_sorts(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
        &base.sorts,
        &state.sorts,
        |table_id, sort_id| EntityKey::Sort { table_id, sort_id },
        new_snapshot,
        |prior| proto::SortValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_table_stats(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    diff_overwrite_map(writes, &base.table_stats, &state.table_stats, |table_id| {
        EntityKey::TableStats { table_id }
    });
}

fn diff_table_column_stats(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    let empty = std::collections::BTreeMap::new();
    let table_ids = base
        .table_column_stats
        .keys()
        .chain(state.table_column_stats.keys());
    for &table_id in table_ids.collect::<std::collections::BTreeSet<_>>() {
        diff_overwrite_map(
            writes,
            base.table_column_stats.get(&table_id).unwrap_or(&empty),
            state.table_column_stats.get(&table_id).unwrap_or(&empty),
            |column_id| EntityKey::TableColumnStats {
                table_id,
                column_id,
            },
        );
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
    diff_partitions(&mut writes, base, state, new_snapshot);
    diff_sorts(&mut writes, base, state, new_snapshot);
    diff_macros(&mut writes, base, state, new_snapshot);
    diff_mappings(&mut writes, base, state);
    diff_table_stats(&mut writes, base, state);
    diff_table_column_stats(&mut writes, base, state);
    diff_file_column_stats(&mut writes, base, state);
    diff_options(&mut writes, base, state);
    diff_tags(&mut writes, base, state);
    diff_gc_files(&mut writes, base, state);
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
    let base = materialize(ReadHandle::Tx(db_tx), None).await?;
    let head = base.snapshot.snapshot_id;
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
        // Re-put the unchanged head as a conflict anchor: every
        // snapshot-minting commit writes it, so a racing drop of this
        // option's scope forces a re-run that re-validates the scope
        // against the winner's state instead of committing blind.
        writes.push((
            Key::Sys(SysKey::Head).encode(),
            Some(value::encode_value(&proto::HeadValue { snapshot_id: head })),
        ));
        stage_writes(db_tx, writes)?;
        return Ok(Prepared::Staged {
            ours: Box::default(),
            head_before: head,
            commits: head,
        });
    }

    let mut writes = diff_writes(&base, &state, new_id);
    let schema_changed = operations.iter().any(Operation::is_schema_changing);
    let schema_changed_table_ids: Vec<u64> = operations
        .iter()
        .filter_map(Operation::schema_changed_table_id)
        .collect::<std::collections::BTreeSet<_>>()
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
        let err = open_initialized("", object_store, false)
            .await
            .err()
            .unwrap();
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

        let err = open_initialized("", object_store, false)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, Error::Corruption(_)));
    }

    /// A file registered and expired within one commit exists in neither
    /// `base` nor `state`'s `data_files`: its per-file column stats are
    /// orphaned and must never be staged as a write.
    #[test]
    fn register_then_expire_in_one_commit_stages_no_orphaned_file_column_stats() {
        use crate::{
            catalog::{ColumnDef, DataFile, FileColumnStats},
            store::key::{CurrentKey, HistoryKey},
        };

        let snap0 = proto::SnapshotValue {
            snapshot_id: 0,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 0,
            next_file_id: 0,
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
                    encryption_key: None,
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
                Key::Current(CurrentKey::Entity(EntityKey::FileColumnStats { .. }))
                    | Key::History(HistoryKey {
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
        use slatedb::DbReader;

        use crate::catalog::{Catalog, CatalogOptions};

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

    /// Verb-path DDL records the shape-changed table ids on its snapshot,
    /// one per changed table or view — the id set `ducklake_schema_versions`
    /// rows are served from. Data-only commits record none.
    #[tokio::test]
    async fn verb_ddl_records_schema_changed_table_ids() {
        use crate::catalog::{Catalog, CatalogOptions, ColumnDef, DataFile};

        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();

        // One commit creating a table and altering it twice: the id set
        // dedups to that one table.
        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.create_schema("s")?;
                let table = tx.create_table(
                    schema,
                    "t",
                    &[ColumnDef {
                        name: "a".into(),
                        column_type: "BIGINT".into(),
                        nulls_allowed: true,
                        default_value: None,
                    }],
                )?;
                tx.rename_table(table, "t2")?;
                created.set(Some(table));
                Ok(())
            })
            .await
            .unwrap();
        let table = created.get().unwrap();

        // A data-only commit changes no table's shape.
        catalog
            .commit(|tx| {
                tx.register_data_file(
                    table,
                    DataFile {
                        path: "f.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 1,
                        file_size_bytes: 10,
                        footer_size: 4,
                        encryption_key: None,
                        column_stats: vec![],
                    },
                )
                .map(|_| ())
            })
            .await
            .unwrap();

        let tx = catalog.begin_write_tx().await.unwrap();
        let ddl = read::read_snapshot(ReadHandle::Tx(&tx), 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ddl.schema_changed_table_ids, vec![table.get()]);
        let data_only = read::read_snapshot(ReadHandle::Tx(&tx), 2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data_only.schema_changed_table_ids, Vec::<u64>::new());
        tx.rollback();
        catalog.close().await.unwrap();
    }
}
