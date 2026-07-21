//! Opening, bootstrap, and snapshot materialization. The commit cycle
//! itself builds on these.

use std::time::{SystemTime, UNIX_EPOCH};

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
        key::{CurrentKey, EntityKey, Key, SysKey},
        open::StoreBuilder,
        proto, read, value,
    },
    transaction::{
        index_maintenance,
        operations::{ChangeSet, Operation},
        verbs::Transaction,
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

fn diff_indexes(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
        &base.indexes,
        &state.indexes,
        |table_id, index_id| EntityKey::Index { table_id, index_id },
        new_snapshot,
        |prior| proto::IndexValue {
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
    diff_indexes(&mut writes, base, state, new_snapshot);
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
    let (operations, index_entries, state, next_catalog_id, next_file_id) = tx.into_parts();

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

    // A staged (`building`) index implies format 3; any other index implies
    // format 2. Stamp lazily, in the same batch, and only ever upward — a
    // completed or dropped build never downgrades the stamp.
    let target_format = if state
        .indexes
        .values()
        .flat_map(std::collections::BTreeMap::values)
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
    };
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
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;

    /// A store stamped with a newer structural format must be refused,
    /// not misread.
    #[tokio::test]
    async fn unknown_format_is_refused() {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let db = StoreBuilder::new("", object_store.clone())
            .open_writer()
            .await
            .unwrap();
        db.put(
            &Key::Sys(SysKey::Format).encode(),
            &value::encode_value(&proto::FormatValue {
                format_version: MAX_FORMAT_VERSION + 1,
                writer_version: "future".into(),
            }),
        )
        .await
        .unwrap();
        db.close().await.unwrap();

        // `Result::unwrap_err` needs `T: Debug`, and `slatedb::Db` has no
        // `Debug` impl; `err().unwrap()` only needs it on the error side.
        let err = open_initialized(StoreBuilder::new("", object_store), false, None)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, Error::Corruption(_)));
    }

    /// A mid-migration marker refuses the open outright.
    #[tokio::test]
    async fn migration_marker_is_refused() {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let db = StoreBuilder::new("", object_store.clone())
            .open_writer()
            .await
            .unwrap();
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

        let err = open_initialized(StoreBuilder::new("", object_store), false, None)
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
        let (_, _, base, _, _) = setup.into_parts();

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
                &[],
            )
            .unwrap();
        tx.expire_data_file(table, file).unwrap();
        let (_, _, state, _, _) = tx.into_parts();

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
                    &[],
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

    async fn catalog_with_two_column_table() -> (crate::catalog::Catalog, crate::catalog::TableId) {
        use crate::catalog::{Catalog, CatalogOptions, ColumnDef};
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let table = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.create_schema("s")?;
                let column = |name: &str| ColumnDef {
                    name: name.into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                };
                let created = tx.create_table(schema, "t", &[column("a"), column("b")])?;
                table.set(Some(created));
                Ok(())
            })
            .await
            .unwrap();
        (catalog, table.get().unwrap())
    }

    fn entry(row_id: u64, value: i128) -> crate::catalog::IndexEntry {
        crate::catalog::IndexEntry {
            row_id,
            values: vec![Some(crate::store::index_encoding::IndexKeyValue::Int {
                value,
                width: crate::store::index_encoding::IntWidth::I64,
            })],
        }
    }

    async fn read_format_version(catalog: &crate::catalog::Catalog) -> u64 {
        let tx = catalog.begin_write_tx().await.unwrap();
        let format = read::read_format(ReadHandle::Tx(&tx)).await.unwrap();
        tx.rollback();
        format.map_or(FORMAT_VERSION, |f| f.format_version)
    }

    #[tokio::test]
    async fn create_index_persists_definition_stamps_format_and_lands_entries() {
        use crate::{
            catalog::{ColumnId, IndexDef, IndexState},
            store::key::{IdxKind, idx_index_prefix},
        };
        let (catalog, table) = catalog_with_two_column_table().await;
        assert_eq!(read_format_version(&catalog).await, FORMAT_VERSION);

        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[entry(0, 10), entry(1, 20)],
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let index_id = index.get().unwrap();

        let snapshot = catalog.snapshot().await.unwrap();
        let infos = snapshot.indexes_of(table);
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].id, index_id);
        assert_eq!(infos[0].columns, vec![ColumnId::new(1)]);
        assert!(infos[0].unique);
        assert_eq!(infos[0].state, IndexState::Ready);
        assert_eq!(read_format_version(&catalog).await, FORMAT_WITH_INDEX);

        // Both backfill rows produced a stored entry.
        let tx = catalog.begin_write_tx().await.unwrap();
        let mut iter = ReadHandle::Tx(&tx)
            .scan_prefix(idx_index_prefix(IdxKind::Unique, index_id.get()), ..)
            .await
            .unwrap();
        let mut count = 0;
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        tx.rollback();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_unique_value_in_backfill_aborts_create() {
        use crate::catalog::{ColumnId, IndexDef};
        let (catalog, table) = catalog_with_two_column_table().await;
        let err = catalog
            .commit(|tx| {
                tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[entry(0, 10), entry(1, 10)],
                )
                .map(|_| ())
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "{err}");

        // The aborted commit left no index and did not stamp the format.
        assert!(
            catalog
                .snapshot()
                .await
                .unwrap()
                .indexes_of(table)
                .is_empty()
        );
        assert_eq!(read_format_version(&catalog).await, FORMAT_VERSION);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn non_unique_index_accepts_duplicate_values() {
        use crate::catalog::{ColumnId, IndexDef};
        let (catalog, table) = catalog_with_two_column_table().await;
        catalog
            .commit(|tx| {
                tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: false,
                    },
                    &[entry(0, 10), entry(1, 10)],
                )
                .map(|_| ())
            })
            .await
            .unwrap();
        assert_eq!(catalog.snapshot().await.unwrap().indexes_of(table).len(), 1);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn null_indexed_value_gets_no_entry_so_unique_admits_many() {
        use crate::catalog::{ColumnId, IndexDef, IndexEntry};
        let (catalog, table) = catalog_with_two_column_table().await;
        let null_entry = |row_id| IndexEntry {
            row_id,
            values: vec![None],
        };
        // Two NULL rows under a unique index: NULLs get no entry, so no
        // collision.
        catalog
            .commit(|tx| {
                tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[null_entry(0), null_entry(1)],
                )
                .map(|_| ())
            })
            .await
            .unwrap();
        assert_eq!(catalog.snapshot().await.unwrap().indexes_of(table).len(), 1);
        catalog.close().await.unwrap();
    }

    async fn register_three_row_file(
        catalog: &crate::catalog::Catalog,
        table: crate::catalog::TableId,
    ) -> crate::catalog::DataFileId {
        use crate::catalog::DataFile;
        let file = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.register_data_file(
                    table,
                    DataFile {
                        path: "f.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 3,
                        file_size_bytes: 30,
                        footer_size: 4,
                        encryption_key: None,
                        column_stats: vec![],
                    },
                    &[],
                )?;
                file.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        file.get().unwrap()
    }

    #[tokio::test]
    async fn index_lookup_resolves_unique_value_to_its_data_file_row() {
        use crate::catalog::{ColumnId, IndexDef, RowHolder};
        let (catalog, table) = catalog_with_two_column_table().await;
        // Rows 0,1,2 land in this file (row_id_start = 0).
        let file = register_three_row_file(&catalog, table).await;

        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[entry(0, 10), entry(1, 20), entry(2, 30)],
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let index = index.get().unwrap();

        let value = |v: i128| crate::store::index_encoding::IndexKeyValue::Int {
            value: v,
            width: crate::store::index_encoding::IntWidth::I64,
        };
        let hits = catalog
            .index_lookup(table, index, &[value(20)])
            .await
            .unwrap();
        assert_eq!(
            hits,
            vec![crate::catalog::RowLocation {
                row_id: 1,
                holder: RowHolder::DataFile(file),
            }]
        );
        // A value no row holds resolves to nothing.
        assert!(
            catalog
                .index_lookup(table, index, &[value(99)])
                .await
                .unwrap()
                .is_empty()
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn index_lookup_returns_all_rows_for_a_non_unique_value() {
        use crate::catalog::{ColumnId, IndexDef};
        let (catalog, table) = catalog_with_two_column_table().await;
        register_three_row_file(&catalog, table).await;
        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                // Rows 0 and 2 share value 10; row 1 is 20.
                let id = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: false,
                    },
                    &[entry(0, 10), entry(1, 20), entry(2, 10)],
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();

        let value = crate::store::index_encoding::IndexKeyValue::Int {
            value: 10,
            width: crate::store::index_encoding::IntWidth::I64,
        };
        let mut rows: Vec<u64> = catalog
            .index_lookup(table, index.get().unwrap(), &[value])
            .await
            .unwrap()
            .into_iter()
            .map(|location| location.row_id)
            .collect();
        rows.sort_unstable();
        assert_eq!(rows, vec![0, 2]);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn index_lookup_on_missing_index_is_not_found() {
        use crate::catalog::IndexId;
        let (catalog, table) = catalog_with_two_column_table().await;
        let value = crate::store::index_encoding::IndexKeyValue::Int {
            value: 1,
            width: crate::store::index_encoding::IntWidth::I64,
        };
        let err = catalog
            .index_lookup(table, IndexId::new(999), &[value])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err}");
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn register_data_file_must_supply_index_entries_and_they_are_looked_up() {
        use crate::{
            catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef},
            store::index_encoding::{IndexKeyValue, IntWidth},
        };
        let (catalog, table) = catalog_with_two_column_table().await;
        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[],
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let index = index.get().unwrap();

        let file = || DataFile {
            path: "f.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: 2,
            file_size_bytes: 20,
            footer_size: 4,
            encryption_key: None,
            column_stats: vec![],
        };
        let int = |value: i128| IndexKeyValue::Int {
            value,
            width: IntWidth::I64,
        };

        // A non-empty file on an indexed table with no entries is refused.
        let refused = catalog
            .commit(|tx| tx.register_data_file(table, file(), &[]).map(|_| ()))
            .await;
        assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");

        // With entries it lands; ordinals map to row ids 0 and 1.
        catalog
            .commit(|tx| {
                tx.register_data_file(
                    table,
                    file(),
                    &[
                        FileIndexEntry {
                            index,
                            ordinal: 0,
                            values: vec![Some(int(10))],
                        },
                        FileIndexEntry {
                            index,
                            ordinal: 1,
                            values: vec![Some(int(20))],
                        },
                    ],
                )
                .map(|_| ())
            })
            .await
            .unwrap();

        let hits = catalog
            .index_lookup(table, index, &[int(20)])
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].row_id, 1);
        catalog.close().await.unwrap();
    }

    /// Sets up an indexed table holding one two-row data file (values 10 and
    /// 20 at row ids 0 and 1), returning the catalog, table, index and file.
    async fn catalog_with_indexed_data_file() -> (
        crate::catalog::Catalog,
        crate::catalog::TableId,
        crate::catalog::IndexId,
        crate::catalog::DataFileId,
    ) {
        use crate::{
            catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef},
            store::index_encoding::{IndexKeyValue, IntWidth},
        };
        let (catalog, table) = catalog_with_two_column_table().await;
        let index = std::cell::Cell::new(None);
        let file = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[],
                )?;
                index.set(Some(id));
                let int = |value: i128| IndexKeyValue::Int {
                    value,
                    width: IntWidth::I64,
                };
                let registered = tx.register_data_file(
                    table,
                    DataFile {
                        path: "f.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 2,
                        file_size_bytes: 20,
                        footer_size: 4,
                        encryption_key: None,
                        column_stats: vec![],
                    },
                    &[
                        FileIndexEntry {
                            index: id,
                            ordinal: 0,
                            values: vec![Some(int(10))],
                        },
                        FileIndexEntry {
                            index: id,
                            ordinal: 1,
                            values: vec![Some(int(20))],
                        },
                    ],
                )?;
                file.set(Some(registered));
                Ok(())
            })
            .await
            .unwrap();
        (catalog, table, index.get().unwrap(), file.get().unwrap())
    }

    fn delete_file(data_file: crate::catalog::DataFileId) -> crate::catalog::DeleteFile {
        crate::catalog::DeleteFile {
            data_file_id: data_file,
            path: "d.parquet".into(),
            path_is_relative: true,
            format: "parquet".into(),
            delete_count: 1,
            file_size_bytes: 10,
            footer_size: 4,
            encryption_key: None,
        }
    }

    /// A delete file names the rows it kills, so their entries go with them
    /// and the value is free to be indexed again.
    #[tokio::test]
    async fn register_delete_file_removes_the_entries_it_names() {
        use crate::{
            catalog::FileIndexEntry,
            store::index_encoding::{IndexKeyValue, IntWidth},
        };
        let (catalog, table, index, file) = catalog_with_indexed_data_file().await;
        let int = |value: i128| IndexKeyValue::Int {
            value,
            width: IntWidth::I64,
        };

        catalog
            .commit(|tx| {
                tx.register_delete_file(
                    table,
                    delete_file(file),
                    &[FileIndexEntry {
                        index,
                        ordinal: 1,
                        values: vec![Some(int(20))],
                    }],
                )
                .map(|_| ())
            })
            .await
            .unwrap();

        assert!(
            catalog
                .index_lookup(table, index, &[int(20)])
                .await
                .unwrap()
                .is_empty(),
            "the killed row's entry is gone"
        );
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int(10)])
                .await
                .unwrap()
                .len(),
            1,
            "the surviving row is still indexed"
        );
        catalog.close().await.unwrap();
    }

    /// Supplying no entries on an indexed table is refused, exactly as it is
    /// on the register side — a silently under-covered index is a lie.
    #[tokio::test]
    async fn register_delete_file_must_supply_index_entries() {
        let (catalog, table, _, file) = catalog_with_indexed_data_file().await;
        let refused = catalog
            .commit(|tx| {
                tx.register_delete_file(table, delete_file(file), &[])
                    .map(|_| ())
            })
            .await;
        assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");
        catalog.close().await.unwrap();
    }

    /// An ordinal past the target file's rows would name a row id outside it.
    #[tokio::test]
    async fn register_delete_file_rejects_an_out_of_range_index_ordinal() {
        use crate::{
            catalog::FileIndexEntry,
            store::index_encoding::{IndexKeyValue, IntWidth},
        };
        let (catalog, table, index, file) = catalog_with_indexed_data_file().await;
        let refused = catalog
            .commit(|tx| {
                tx.register_delete_file(
                    table,
                    delete_file(file),
                    &[FileIndexEntry {
                        index,
                        ordinal: 2,
                        values: vec![Some(IndexKeyValue::Int {
                            value: 30,
                            width: IntWidth::I64,
                        })],
                    }],
                )
                .map(|_| ())
            })
            .await;
        assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn unique_index_rejects_a_duplicate_value_across_commits() {
        use crate::{
            catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef},
            store::index_encoding::{IndexKeyValue, IntWidth},
        };
        let (catalog, table) = catalog_with_two_column_table().await;
        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[],
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let index = index.get().unwrap();

        let one_row_with = |value: i128| {
            let file = DataFile {
                path: "f.parquet".into(),
                path_is_relative: true,
                file_format: "parquet".into(),
                record_count: 1,
                file_size_bytes: 10,
                footer_size: 4,
                encryption_key: None,
                column_stats: vec![],
            };
            (
                file,
                FileIndexEntry {
                    index,
                    ordinal: 0,
                    values: vec![Some(IndexKeyValue::Int {
                        value,
                        width: IntWidth::I64,
                    })],
                },
            )
        };

        // First value 10 lands.
        catalog
            .commit(|tx| {
                let (file, entry) = one_row_with(10);
                tx.register_data_file(table, file, &[entry]).map(|_| ())
            })
            .await
            .unwrap();
        // A later commit inserting the same value 10 (different row) is
        // rejected by the point-get against the winner's entry.
        let dup = catalog
            .commit(|tx| {
                let (file, entry) = one_row_with(10);
                tx.register_data_file(table, file, &[entry]).map(|_| ())
            })
            .await;
        assert!(matches!(dup, Err(Error::Constraint(_))), "{dup:?}");
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn scoped_read_covers_a_registration_end_to_end() {
        use std::sync::Arc;

        use arrow::{
            array::{Int64Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        };
        use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
        use parquet::arrow::ArrowWriter;

        use crate::{
            catalog::{ColumnId, DataFile, IndexDef},
            store::index_encoding::{IndexKeyValue, IntWidth},
        };

        let (catalog, table) = catalog_with_two_column_table().await;
        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[],
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let index = index.get().unwrap();

        // A DATA_PATH object store holds a Parquet file with the indexed
        // column "a" at physical position 0.
        let data = InMemory::new();
        let path = Path::from("t/data-1.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20, 30]))])
                .unwrap();
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        data.put(&path, buffer.into()).await.unwrap();

        // moraine derives coverage entries by the scoped read (column "a"
        // at position 0), then registration lands them — DuckLake supplied
        // none, and the read stands in for the refusal.
        let entries = catalog
            .scoped_file_index_entries(&data, &path, index, &[0])
            .await
            .unwrap();
        assert_eq!(entries.len(), 3);
        catalog
            .commit(|tx| {
                tx.register_data_file(
                    table,
                    DataFile {
                        path: "t/data-1.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 3,
                        file_size_bytes: 30,
                        footer_size: 4,
                        encryption_key: None,
                        column_stats: vec![],
                    },
                    &entries,
                )
                .map(|_| ())
            })
            .await
            .unwrap();

        let value = IndexKeyValue::Int {
            value: 20,
            width: IntWidth::I64,
        };
        let hits = catalog.index_lookup(table, index, &[value]).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].row_id, 1);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn ddl_on_an_indexed_column_is_guarded() {
        use crate::catalog::{ColumnAlteration, ColumnId, IndexDef};
        let (catalog, table) = catalog_with_two_column_table().await;
        catalog
            .commit(|tx| {
                tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[entry(0, 10)],
                )
                .map(|_| ())
            })
            .await
            .unwrap();

        // Dropping or retyping the indexed column is refused.
        let dropped = catalog
            .commit(|tx| tx.drop_column(table, ColumnId::new(1)))
            .await;
        assert!(matches!(dropped, Err(Error::Constraint(_))), "{dropped:?}");
        let retyped = catalog
            .commit(|tx| {
                tx.alter_column(
                    table,
                    ColumnId::new(1),
                    ColumnAlteration {
                        column_type: Some("INTEGER".into()),
                        ..ColumnAlteration::default()
                    },
                )
            })
            .await;
        assert!(matches!(retyped, Err(Error::Constraint(_))), "{retyped:?}");

        // Renaming the indexed column, and retyping a non-indexed column,
        // are unaffected.
        catalog
            .commit(|tx| tx.rename_column(table, ColumnId::new(1), "a2"))
            .await
            .unwrap();
        catalog
            .commit(|tx| {
                tx.alter_column(
                    table,
                    ColumnId::new(2),
                    ColumnAlteration {
                        column_type: Some("INTEGER".into()),
                        ..ColumnAlteration::default()
                    },
                )
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
    }

    async fn scan_idx_entries(
        catalog: &crate::catalog::Catalog,
        index: crate::catalog::IndexId,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        use crate::store::key::{IdxKind, idx_index_prefix};
        let tx = catalog.begin_write_tx().await.unwrap();
        let mut entries = Vec::new();
        for kind in [IdxKind::Unique, IdxKind::Multi] {
            let mut iter = ReadHandle::Tx(&tx)
                .scan_prefix(idx_index_prefix(kind, index.get()), ..)
                .await
                .unwrap();
            while let Some(entry) = iter.next().await.unwrap() {
                entries.push((entry.key.to_vec(), entry.value.to_vec()));
            }
        }
        tx.rollback();
        entries.sort();
        entries
    }

    #[tokio::test]
    async fn staged_build_gates_lookups_flips_ready_and_matches_single_commit() {
        use crate::{
            catalog::{ColumnId, IndexDef, IndexState},
            store::index_encoding::{IndexKeyValue, IntWidth},
        };
        let def = || IndexDef {
            name: "by_a".into(),
            columns: vec![ColumnId::new(1)],
            unique: true,
        };
        let value = |v: i128| IndexKeyValue::Int {
            value: v,
            width: IntWidth::I64,
        };

        // Reference: a single-commit build over rows 0,1,2.
        let (single, table_single) = catalog_with_two_column_table().await;
        register_three_row_file(&single, table_single).await;
        let single_index = std::cell::Cell::new(None);
        single
            .commit(|tx| {
                let id = tx.create_index(
                    table_single,
                    &def(),
                    &[entry(0, 10), entry(1, 20), entry(2, 30)],
                )?;
                single_index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let single_index = single_index.get().unwrap();

        // Staged: same table shape, same rows, built in two batches.
        let (staged, table_staged) = catalog_with_two_column_table().await;
        register_three_row_file(&staged, table_staged).await;
        let staged_index = std::cell::Cell::new(None);
        staged
            .commit(|tx| {
                let id = tx.create_index_staged(table_staged, &def())?;
                staged_index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let staged_index = staged_index.get().unwrap();
        // Identical allocation sequence → identical index id, so the idx
        // keys can be compared directly.
        assert_eq!(single_index, staged_index);

        // While building: format 3, lookups fail typed.
        assert_eq!(read_format_version(&staged).await, FORMAT_WITH_STAGED_INDEX);
        assert!(matches!(
            staged
                .index_lookup(table_staged, staged_index, &[value(20)])
                .await,
            Err(Error::IndexBuilding(_))
        ));

        // Two batches, the second final.
        staged
            .commit(|tx| {
                tx.build_index_step(staged_index, &[entry(0, 10), entry(1, 20)], false)
                    .map(|_| ())
            })
            .await
            .unwrap();
        let final_state = std::cell::Cell::new(None);
        staged
            .commit(|tx| {
                let state = tx.build_index_step(staged_index, &[entry(2, 30)], true)?;
                final_state.set(Some(state));
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(final_state.get().unwrap(), IndexState::Ready);

        // After the flip: lookups serve, and the idx range is byte-identical
        // to the single-commit build over the same rows.
        let hits = staged
            .index_lookup(table_staged, staged_index, &[value(20)])
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].row_id, 1);
        assert_eq!(
            scan_idx_entries(&single, single_index).await,
            scan_idx_entries(&staged, staged_index).await
        );

        single.close().await.unwrap();
        staged.close().await.unwrap();
    }

    #[tokio::test]
    async fn staged_build_step_rejects_a_duplicate_and_a_ready_index() {
        use crate::catalog::{ColumnId, IndexDef};
        let (catalog, table) = catalog_with_two_column_table().await;
        register_three_row_file(&catalog, table).await;
        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index_staged(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let index = index.get().unwrap();

        // A duplicate value within a batch fails the step.
        let dup = catalog
            .commit(|tx| {
                tx.build_index_step(index, &[entry(0, 10), entry(1, 10)], false)
                    .map(|_| ())
            })
            .await;
        assert!(matches!(dup, Err(Error::Constraint(_))), "{dup:?}");

        // Complete the build, then a further step on the ready index is
        // refused.
        catalog
            .commit(|tx| {
                tx.build_index_step(index, &[entry(0, 10)], true)
                    .map(|_| ())
            })
            .await
            .unwrap();
        let after_ready = catalog
            .commit(|tx| {
                tx.build_index_step(index, &[entry(1, 20)], false)
                    .map(|_| ())
            })
            .await;
        assert!(
            matches!(after_ready, Err(Error::Constraint(_))),
            "{after_ready:?}"
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn reclaiming_a_dropped_index_deletes_its_orphaned_entries() {
        use crate::{
            catalog::{ColumnId, IndexDef},
            store::key::{IdxKind, idx_index_prefix},
        };
        let (catalog, table) = catalog_with_two_column_table().await;
        register_three_row_file(&catalog, table).await;
        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[entry(0, 10), entry(1, 20), entry(2, 30)],
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let index = index.get().unwrap();

        // Reclaiming a live index is refused.
        assert!(matches!(
            catalog.reclaim_index_entries(index, 100).await,
            Err(Error::Constraint(_))
        ));

        catalog.commit(|tx| tx.drop_index(index)).await.unwrap();

        // A bounded sweep deletes the three orphaned entries, then reports
        // nothing left.
        let first = catalog.reclaim_index_entries(index, 2).await.unwrap();
        assert_eq!(first, 2);
        let second = catalog.reclaim_index_entries(index, 100).await.unwrap();
        assert_eq!(second, 1);
        assert_eq!(catalog.reclaim_index_entries(index, 100).await.unwrap(), 0);

        // The idx range is empty afterward.
        let tx = catalog.begin_write_tx().await.unwrap();
        let mut iter = ReadHandle::Tx(&tx)
            .scan_prefix(idx_index_prefix(IdxKind::Unique, index.get()), ..)
            .await
            .unwrap();
        assert!(iter.next().await.unwrap().is_none());
        tx.rollback();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn drop_index_ends_definition_and_keeps_format() {
        use crate::catalog::{ColumnId, IndexDef};
        let (catalog, table) = catalog_with_two_column_table().await;
        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: true,
                    },
                    &[entry(0, 10)],
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();

        catalog
            .commit(|tx| tx.drop_index(index.get().unwrap()))
            .await
            .unwrap();
        assert!(
            catalog
                .snapshot()
                .await
                .unwrap()
                .indexes_of(table)
                .is_empty()
        );
        // Dropping the last index does not downgrade the stamp.
        assert_eq!(read_format_version(&catalog).await, FORMAT_WITH_INDEX);
        catalog.close().await.unwrap();
    }
}
